"""
osint/clients/scrapers/sos/sos_pa.py

Pennsylvania Secretary of State — Bureau of Corporations web scraper.

Portal: https://www.corporations.pa.gov/Search/CorpSearch
API:    No public API. Uses POST form submission + HTML table parsing.

PA SoS Search notes:
  - POST to /Search/CorpSearch with form body: {SearchTerm, SearchType, ...}
  - Returns HTML table of matching entities
  - Each row links to a detail page at /Details/CorpDetail?id={CORP_ID}
  - Detail page has: entity type, status, incorporation date, registered agent
  - No officer list on PA SoS — officers are not publicly listed in PA
  - Registered address is often the registered agent's address

CAPTCHA handling:
  - PA SoS uses a simple client-side CAPTCHA on the search form.
  - If we get a CAPTCHA challenge page (no results table), log and skip.
  - Do NOT attempt to bypass — log warning, return empty.

Rate limit: 2 req/5s per the sos_us domain in RATE_LIMITS config.
robots.txt: corporations.pa.gov allows crawling (checked on 2025-01).

Output (stored in category_fields["sos_pa_data"]):
    {
        "entity_name": str,
        "entity_number": str,          # PA entity ID
        "entity_type": str,            # "Domestic Business Corporation", etc.
        "status": str,                 # "Active", "Dissolved", etc.
        "incorporation_date": str,     # ISO date or raw string
        "state_of_formation": "PA",
        "registered_agent": str,
        "registered_address": str,
        "source_url": str,
    }
"""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import urljoin, urlencode

from bs4 import BeautifulSoup

from osint.clients.scraper_base import ScraperBase, RobotsDisallowedError

log = logging.getLogger(__name__)

_BASE_URL    = "https://www.corporations.pa.gov"
_SEARCH_URL  = "https://www.corporations.pa.gov/Search/CorpSearch"
_DETAIL_URL  = "https://www.corporations.pa.gov/Details/CorpDetail"
_DATE_RE     = re.compile(r"\d{1,2}/\d{1,2}/\d{4}|\d{4}-\d{2}-\d{2}")
_ENTITY_ID_RE = re.compile(r"id=(\d+)", re.IGNORECASE)


class PASoSScraper(ScraperBase):
    """
    Scraper for Pennsylvania Secretary of State corporation search.

    Pennsylvania does not expose an API. This scraper POSTs to the search
    form and parses the resulting HTML table.
    """

    DOMAIN     = "sos_us"
    BASE_URL   = _BASE_URL
    ROBOTS_URL = "https://www.corporations.pa.gov/robots.txt"

    # ─────────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────────

    async def scrape(
        self,
        company_name: str,
        entity_type_filter: str = "",
    ) -> dict[str, Any]:
        """
        Search PA SoS and return data for the best-matching entity.

        Args:
            company_name:         Company name to search.
            entity_type_filter:   Optional entity type to prefer (e.g. "corp").

        Returns:
            Structured dict with SoS data, or empty dict if not found / CAPTCHA.
        """
        candidates = await self.search(company_name)
        if not candidates:
            return {}

        # If multiple results, prefer active corporations
        best = self._pick_best_result(candidates, company_name, entity_type_filter)
        if not best:
            return {}

        detail_url = best.get("detail_url")
        if not detail_url:
            # Return what we have from search results
            best["state_of_formation"] = "PA"
            return best

        detail = await self.get_entity_detail(detail_url)
        detail["state_of_formation"] = "PA"
        detail["source_url"] = detail_url
        return detail

    async def search(
        self,
        company_name: str,
        max_results: int = 10,
    ) -> list[dict[str, Any]]:
        """
        POST to the PA SoS search form and parse results.

        Args:
            company_name:   Company name to search.
            max_results:    Maximum results to parse.

        Returns:
            List of candidate dicts: [{entity_name, entity_number, entity_type,
            status, incorporation_date, detail_url}]
        """
        form_data = {
            "SearchTerm": company_name,
            "SearchType": "B",          # B = Business name search
        }

        # Robots check — POST path must be allowed before submitting the form
        from urllib.parse import urlparse
        search_path = urlparse(_SEARCH_URL).path
        if not await self.is_allowed(search_path):
            log.warning("sos_pa: robots.txt disallows search path '%s'", search_path)
            return []

        try:
            html = await self.post_html(
                _SEARCH_URL,
                data=form_data,
            )
        except Exception as exc:
            log.warning("sos_pa: search POST failed for '%s': %s", company_name, exc)
            return []

        return self._parse_search_results(html, max_results)

    async def get_entity_detail(self, detail_url: str) -> dict[str, Any]:
        """
        Fetch and parse a PA SoS entity detail page.

        Args:
            detail_url: Full URL to the PA SoS entity detail page.

        Returns:
            Structured dict with entity data.
        """
        try:
            html = await self.get_html(detail_url, check_robots=True)
        except RobotsDisallowedError:
            log.warning("sos_pa: robots.txt disallows detail path")
            return {}
        except Exception as exc:
            log.warning("sos_pa: detail fetch failed for '%s': %s", detail_url, exc)
            return {}

        return self._parse_entity_detail(html)

    # ─────────────────────────────────────────────────────────────────────────
    # Parsing
    # ─────────────────────────────────────────────────────────────────────────

    def _parse_search_results(
        self,
        html: str,
        max_results: int,
    ) -> list[dict[str, Any]]:
        """Parse PA SoS search results HTML table."""
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")

        # CAPTCHA detection: if no results table exists, likely CAPTCHA or no results
        results_table = soup.select_one(
            "table.results, table#resultsTable, table.search-results, "
            "table[id*='result'], table[class*='result']"
        )
        if results_table is None:
            # Check for CAPTCHA indicators
            captcha_indicators = soup.find_all(
                string=re.compile(r"captcha|robot|automated", re.IGNORECASE)
            )
            if captcha_indicators:
                log.warning("sos_pa: CAPTCHA detected — skipping")
            else:
                log.debug("sos_pa: no results table found in search response")
            return []

        candidates: list[dict[str, Any]] = []
        rows = results_table.select("tr")[1:]  # Skip header row

        for row in rows[:max_results]:
            cells = row.select("td")
            if len(cells) < 2:
                continue

            # Extract entity name and detail link from first cell
            link = cells[0].find("a")
            entity_name = link.get_text(strip=True) if link else cells[0].get_text(strip=True)
            detail_url = ""
            if link and link.get("href"):
                href = link["href"]
                if href.startswith("/"):
                    detail_url = urljoin(_BASE_URL, href)
                else:
                    detail_url = href

            # Extract entity number from URL or second column
            entity_number = ""
            m = _ENTITY_ID_RE.search(detail_url)
            if m:
                entity_number = m.group(1)
            elif len(cells) > 1:
                entity_number = cells[1].get_text(strip=True)

            # Status and type from remaining columns
            entity_type = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            status      = cells[3].get_text(strip=True) if len(cells) > 3 else ""
            inc_date    = cells[4].get_text(strip=True) if len(cells) > 4 else ""

            if entity_name:
                candidates.append({
                    "entity_name":        entity_name,
                    "entity_number":      entity_number,
                    "entity_type":        entity_type,
                    "status":             status,
                    "incorporation_date": self._extract_date(inc_date),
                    "detail_url":         detail_url,
                })

        return candidates

    def _parse_entity_detail(self, html: str) -> dict[str, Any]:
        """Parse a PA SoS entity detail page."""
        if not html:
            return {}

        soup = BeautifulSoup(html, "html.parser")

        data: dict[str, Any] = {
            "entity_name":        None,
            "entity_number":      None,
            "entity_type":        None,
            "status":             None,
            "incorporation_date": None,
            "registered_agent":   None,
            "registered_address": None,
        }

        # PA SoS detail page uses labeled rows: <span class="label"> + <span class="data">
        label_els = soup.select(".label, label, th, dt")
        for label_el in label_els:
            label_text = label_el.get_text(strip=True).lower().rstrip(":")
            # Get the adjacent data element
            data_el = label_el.find_next_sibling() or label_el.parent.find_next_sibling()
            if data_el is None:
                continue
            value = data_el.get_text(strip=True)

            if "entity name" in label_text or "corporation name" in label_text:
                data["entity_name"] = value
            elif "entity number" in label_text or "corporation number" in label_text:
                data["entity_number"] = value
            elif "entity type" in label_text or "corporation type" in label_text:
                data["entity_type"] = value
            elif "status" in label_text:
                data["status"] = value
            elif "date" in label_text and ("incorporat" in label_text or "formed" in label_text or "registered" in label_text):
                data["incorporation_date"] = self._extract_date(value)
            elif "registered agent" in label_text:
                data["registered_agent"] = value
            elif "registered" in label_text and "address" in label_text:
                data["registered_address"] = value
            elif "principal" in label_text and "address" in label_text:
                if not data.get("registered_address"):
                    data["registered_address"] = value

        return data

    @staticmethod
    def _pick_best_result(
        candidates: list[dict[str, Any]],
        company_name: str,
        entity_type_filter: str,
    ) -> dict[str, Any] | None:
        """Pick the best candidate from search results."""
        if not candidates:
            return None

        # Prefer exact name match + active status
        target = company_name.lower()
        for c in candidates:
            if (
                c.get("entity_name", "").lower() == target
                and c.get("status", "").lower() in ("active", "")
            ):
                return c

        # Fall back to first active entity
        for c in candidates:
            if c.get("status", "").lower() in ("active", ""):
                return c

        # Fall back to first result
        return candidates[0]

    @staticmethod
    def _extract_date(text: str) -> str | None:
        """Extract first date-like string from text."""
        m = _DATE_RE.search(text)
        return m.group(0) if m else (text.strip() or None)
