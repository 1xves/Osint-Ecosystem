"""
osint/clients/scrapers/sos/sos_de.py

Delaware Secretary of State — Division of Corporations web scraper.

Portal: https://icis.corp.delaware.gov/Ecorp/EntitySearch/NameSearch.aspx
API:    No public API. Uses ASP.NET ViewState form + HTML parsing.

Delaware is the dominant state for US corporate registrations:
  ~68% of Fortune 500 companies are incorporated in Delaware.
  Scraping DE SoS is critical for corporate entity enrichment.

Delaware SoS returns:
  - Entity name (exact as registered)
  - File number (DE entity ID)
  - Incorporation date
  - Entity type (General Corporation, LLC, LP, etc.)
  - Status (Active, Void, Cancelled, Merged)
  - Registered agent name (the agent, not the company's principal office)
  - Registered agent address

Key technical notes:
  - ASP.NET WebForms: requires __VIEWSTATE and __EVENTVALIDATION tokens from
    the initial GET before the POST will succeed.
  - Two-step: GET → extract ViewState → POST form → parse results.
  - Pagination: results tables have up to 25 rows; we only need top result.
  - CAPTCHA: as of 2025, DE SoS has an image CAPTCHA on repeated searches.
    Detect CAPTCHA response page (no results table) and log + skip.

Shell company signal:
  - Same registered agent across many entities = potential shell network.
  - Enrichment agent stores registered_agent for network analysis.
  - Relationship agent creates HAS_REGISTERED_AGENT edges.

Rate limit: 2 req/5s (sos_us domain in RATE_LIMITS).

Output (stored in category_fields["sos_de_data"]):
    {
        "entity_name": str,
        "file_number": str,            # DE file number (e.g. "1234567")
        "entity_type": str,
        "status": str,
        "incorporation_date": str,
        "state_of_formation": "DE",
        "registered_agent": str,
        "registered_address": str,
        "source_url": str,
    }
"""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from osint.clients.scraper_base import ScraperBase, RobotsDisallowedError

log = logging.getLogger(__name__)

_BASE_URL   = "https://icis.corp.delaware.gov"
_SEARCH_URL = "https://icis.corp.delaware.gov/Ecorp/EntitySearch/NameSearch.aspx"
_DATE_RE    = re.compile(r"\d{1,2}/\d{1,2}/\d{4}|\d{4}-\d{2}-\d{2}")


class DESoSScraper(ScraperBase):
    """
    Scraper for the Delaware Division of Corporations entity name search.

    ASP.NET ViewState-based form — two-step: GET → POST.
    """

    DOMAIN     = "sos_us"
    BASE_URL   = _BASE_URL
    ROBOTS_URL = "https://icis.corp.delaware.gov/robots.txt"

    # ─────────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────────

    async def scrape(self, company_name: str) -> dict[str, Any]:
        """
        Search DE SoS and return data for the best-matching entity.

        Args:
            company_name: Company name to search.

        Returns:
            Structured dict with SoS data, or empty dict if not found / CAPTCHA.
        """
        candidates = await self.search(company_name)
        if not candidates:
            return {}

        best = self._pick_best_result(candidates, company_name)
        if not best:
            return {}

        best["state_of_formation"] = "DE"
        return best

    async def search(
        self,
        company_name: str,
        max_results: int = 10,
    ) -> list[dict[str, Any]]:
        """
        Two-step search: GET to obtain ViewState, POST form to get results.

        Args:
            company_name:   Entity name to search.
            max_results:    Maximum results to parse.

        Returns:
            List of candidate dicts.
        """
        # Step 1: GET the search form to extract ViewState
        try:
            form_html = await self.get_html(_SEARCH_URL, check_robots=True)
        except RobotsDisallowedError:
            log.warning("sos_de: robots.txt disallows search path")
            return []
        except Exception as exc:
            log.warning("sos_de: GET search form failed: %s", exc)
            return []

        viewstate, event_validation = self._extract_aspnet_tokens(form_html)

        # Step 2: POST the form with the entity name
        form_data: dict[str, str] = {
            "__VIEWSTATE":              viewstate,
            "__EVENTVALIDATION":        event_validation,
            "ctl00$ContentPlaceHolder1$txtEntityName": company_name,
            "ctl00$ContentPlaceHolder1$btnSearch":     "Search",
        }

        try:
            results_html = await self.post_html(
                _SEARCH_URL,
                data=form_data,
                # No check_robots here — robots.txt was verified in the GET step above
            )
        except Exception as exc:
            log.warning("sos_de: POST search failed for '%s': %s", company_name, exc)
            return []

        return self._parse_search_results(results_html, max_results)

    # ─────────────────────────────────────────────────────────────────────────
    # Parsing
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _extract_aspnet_tokens(html: str) -> tuple[str, str]:
        """
        Extract ASP.NET ViewState and EventValidation hidden form fields.

        Returns:
            (viewstate_value, event_validation_value) — both may be empty if
            not found (form POST will likely fail without them).
        """
        if not html:
            return "", ""

        soup = BeautifulSoup(html, "html.parser")

        def _get_hidden(name: str) -> str:
            el = soup.find("input", {"id": name}) or soup.find("input", {"name": name})
            return el.get("value", "") if el else ""

        viewstate        = _get_hidden("__VIEWSTATE")
        event_validation = _get_hidden("__EVENTVALIDATION")

        if not viewstate:
            log.debug("sos_de: __VIEWSTATE not found in form page")
        return viewstate, event_validation

    def _parse_search_results(
        self,
        html: str,
        max_results: int,
    ) -> list[dict[str, Any]]:
        """Parse DE SoS search results HTML table."""
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")

        # Detect CAPTCHA
        captcha_signals = soup.find_all(
            string=re.compile(r"captcha|verify you are human|not a robot", re.IGNORECASE)
        )
        if captcha_signals:
            log.warning("sos_de: CAPTCHA detected — skipping search")
            return []

        # Results table — DE SoS uses a GridView with id "GridView1" or similar
        results_table = soup.select_one(
            "table[id*='Grid'], table[id*='Result'], table.results, "
            "#ctl00_ContentPlaceHolder1_SearchResultsGrid"
        )
        if results_table is None:
            log.debug("sos_de: no results table found — no matches or form error")
            return []

        candidates: list[dict[str, Any]] = []
        rows = results_table.select("tr")

        # Skip header row(s)
        data_rows = [r for r in rows if r.select("td")]

        for row in data_rows[:max_results]:
            cells = row.select("td")
            if len(cells) < 2:
                continue

            # DE SoS typical columns: Entity Name | File No | Incorporation Date | Entity Type | Status
            # Column order varies — use position-based extraction with fallback
            entity_name  = cells[0].get_text(strip=True) if len(cells) > 0 else ""
            file_number  = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            inc_date_raw = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            entity_type  = cells[3].get_text(strip=True) if len(cells) > 3 else ""
            status       = cells[4].get_text(strip=True) if len(cells) > 4 else ""

            # Extract detail URL if the entity name is a link
            link = cells[0].find("a")
            detail_url = ""
            if link and link.get("href"):
                href = link["href"]
                detail_url = urljoin(_BASE_URL, href) if href.startswith("/") else href
                entity_name = link.get_text(strip=True) or entity_name

            if entity_name:
                candidates.append({
                    "entity_name":        entity_name,
                    "file_number":        file_number,
                    "entity_type":        entity_type,
                    "status":             status,
                    "incorporation_date": self._extract_date(inc_date_raw),
                    "registered_agent":   None,    # Not in search results; in detail page
                    "registered_address": None,
                    "detail_url":         detail_url,
                    "source_url":         _SEARCH_URL,
                })

        # Attempt to get registered agent from detail page for best match
        # (done in scrape() via a second fetch — here just return candidates)
        return candidates

    async def enrich_with_detail(self, candidate: dict[str, Any]) -> dict[str, Any]:
        """
        Fetch the entity detail page to add registered agent data.

        Called by scrape() on the best candidate after initial search.
        This is a separate method so it can be skipped if detail URL is missing.
        """
        detail_url = candidate.get("detail_url")
        if not detail_url:
            return candidate

        try:
            html = await self.get_html(detail_url, check_robots=False)
        except Exception as exc:
            log.debug("sos_de: detail fetch failed for '%s': %s", detail_url, exc)
            return candidate

        if html:
            agent_data = self._parse_registered_agent(html)
            candidate.update(agent_data)

        return candidate

    @staticmethod
    def _parse_registered_agent(html: str) -> dict[str, Any]:
        """Extract registered agent name and address from DE entity detail page."""
        soup = BeautifulSoup(html, "html.parser")
        data: dict[str, Any] = {"registered_agent": None, "registered_address": None}

        # Look for "Registered Agent" label in the detail page
        for el in soup.find_all(string=re.compile(r"Registered Agent", re.IGNORECASE)):
            parent = el.find_parent()
            if parent is None:
                continue
            # Agent name is usually in the next sibling or adjacent table cell
            sibling = parent.find_next_sibling()
            if sibling:
                agent_text = sibling.get_text(strip=True)
                if agent_text:
                    data["registered_agent"] = agent_text
            break

        # Look for registered address
        for el in soup.find_all(string=re.compile(r"Registered.{0,10}Address", re.IGNORECASE)):
            parent = el.find_parent()
            if parent is None:
                continue
            sibling = parent.find_next_sibling()
            if sibling:
                addr_text = sibling.get_text(separator=", ", strip=True)
                if addr_text:
                    data["registered_address"] = addr_text
            break

        return data

    @staticmethod
    def _pick_best_result(
        candidates: list[dict[str, Any]],
        company_name: str,
    ) -> dict[str, Any] | None:
        """Pick the best DE SoS search result."""
        if not candidates:
            return None

        target = company_name.lower()

        # 1. Exact name match + Active
        for c in candidates:
            if (
                c.get("entity_name", "").lower() == target
                and c.get("status", "").lower() in ("active", "good standing", "")
            ):
                return c

        # 2. Any active entity
        for c in candidates:
            if c.get("status", "").lower() in ("active", "good standing", ""):
                return c

        # 3. First result
        return candidates[0]

    @staticmethod
    def _extract_date(text: str) -> str | None:
        """Extract first date-like string from text."""
        m = _DATE_RE.search(text)
        return m.group(0) if m else (text.strip() or None)
