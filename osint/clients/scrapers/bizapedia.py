"""
osint/clients/scrapers/bizapedia.py

Bizapedia corporate records scraper.

Bizapedia aggregates Secretary of State data from all 50 US states, presenting
a single unified search interface. For each corporate entity it shows:
  - Registered agent name and address
  - Incorporation / registration date
  - Entity status (Active, Dissolved, etc.)
  - Officer and director names with titles
  - Registered address

This scraper is used for corporate entities where the API-based sources
(Crunchbase, EDGAR) return thin governance data. Bizapedia is particularly
useful for LLCs and private companies that don't file publicly.

Usage pattern (two-step):
    1. search(company_name, city, state) → list of candidate results
    2. get_company_detail(profile_url) → structured data dict

Enrichment agent calls search() first, takes the top result if name similarity
is high, then calls get_company_detail() for the full record.

Stores in category_fields["bizapedia_data"]:
    {
        "company_name": str,
        "entity_type": str,            # "LLC", "Corporation", etc.
        "status": str,                 # "Active", "Dissolved", etc.
        "state": str,                  # State of incorporation
        "registration_date": str,      # ISO date or raw string
        "registered_agent": str,
        "registered_address": str,
        "officers": [{"name": str, "title": str}],
        "source_url": str,
    }

robots.txt: bizapedia.com allows general crawling.
Rate limit: 20 req/min (1 req/3s) — conservative to avoid blocks.
User-Agent: Standard browser UA (required — bot UA gets blocked).
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Any
from urllib.parse import urljoin, quote_plus

from bs4 import BeautifulSoup

from osint.clients.scraper_base import ScraperBase, RobotsDisallowedError

log = logging.getLogger(__name__)

_SEARCH_URL  = "https://www.bizapedia.com/search/"
_PROFILE_RE  = re.compile(r"/[a-z]{2}/[^/]+\.html$")   # matches /pa/acme-corp.html
_DATE_RE     = re.compile(r"\d{1,2}/\d{1,2}/\d{4}|\d{4}-\d{2}-\d{2}")


class BizapediaScraper(ScraperBase):
    """
    Scraper for Bizapedia corporate records.

    One instance per enrichment run (or shared — stateless beyond ScraperBase).
    """

    DOMAIN     = "bizapedia"
    BASE_URL   = "https://www.bizapedia.com"
    ROBOTS_URL = "https://www.bizapedia.com/robots.txt"

    # ─────────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────────

    async def scrape(
        self,
        company_name: str,
        city: str = "",
        state_abbr: str = "",
    ) -> dict[str, Any]:
        """
        Main entry point. Searches Bizapedia for a company and returns
        structured data for the best match.

        Args:
            company_name:   Company name to search.
            city:           Optional city to narrow results.
            state_abbr:     Optional 2-letter state abbreviation (e.g. "PA").

        Returns:
            Structured dict with company data, or empty dict if not found.
        """
        candidates = await self.search(company_name, city=city, state_abbr=state_abbr)
        if not candidates:
            log.debug("bizapedia: no results for '%s'", company_name)
            return {}

        # Take top result — search is already ordered by relevance
        top = candidates[0]
        profile_url = top.get("profile_url")
        if not profile_url:
            return {}

        detail = await self.get_company_detail(profile_url)
        if detail:
            detail["source_url"] = profile_url
        return detail

    async def search(
        self,
        company_name: str,
        city: str = "",
        state_abbr: str = "",
        max_results: int = 5,
    ) -> list[dict[str, Any]]:
        """
        Search Bizapedia for companies matching the query.

        Args:
            company_name:   Query string (company name).
            city:           Optional city to append to query.
            state_abbr:     Optional state filter — appended to query.
            max_results:    Max candidate results to return.

        Returns:
            List of candidate dicts: [{company_name, profile_url, state, status}]
        """
        query_parts = [company_name]
        if city:
            query_parts.append(city)
        if state_abbr:
            query_parts.append(state_abbr)
        query = " ".join(query_parts)

        path = f"/search/?q={quote_plus(query)}"

        try:
            html = await self.get_html(
                _SEARCH_URL,
                params={"q": query},
                check_robots=True,
            )
        except RobotsDisallowedError:
            log.warning("bizapedia: robots.txt disallows search path")
            return []
        except Exception as exc:
            log.warning("bizapedia: search failed for '%s': %s", company_name, exc)
            return []

        return self._parse_search_results(html, max_results)

    async def get_company_detail(self, profile_url: str) -> dict[str, Any]:
        """
        Fetch and parse a Bizapedia company profile page.

        Args:
            profile_url: Full URL to the Bizapedia company profile.

        Returns:
            Structured dict with company data fields. Empty dict on failure.
        """
        try:
            html = await self.get_html(
                profile_url,
                check_robots=True,
            )
        except RobotsDisallowedError:
            log.warning("bizapedia: robots.txt disallows profile path")
            return {}
        except Exception as exc:
            log.warning("bizapedia: detail fetch failed for '%s': %s", profile_url, exc)
            return {}

        return self._parse_company_detail(html)

    # ─────────────────────────────────────────────────────────────────────────
    # Parsing helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _parse_search_results(
        self,
        html: str,
        max_results: int,
    ) -> list[dict[str, Any]]:
        """Parse Bizapedia search results page into a list of candidates."""
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        results: list[dict[str, Any]] = []

        # Bizapedia search results are in <div class="search-result"> blocks
        # Each block has an <a> to the company profile
        for block in soup.select(".search-result, .result-item, article.company"):
            link = block.find("a", href=_PROFILE_RE)
            if link is None:
                # Fallback: any link matching the profile URL pattern
                link = block.find("a", href=lambda h: h and _PROFILE_RE.search(h))
            if link is None:
                continue

            href = link.get("href", "")
            if href.startswith("/"):
                href = urljoin(self.BASE_URL, href)

            name = link.get_text(strip=True) or ""
            status_el = block.select_one(".status, .entity-status, .badge")
            state_el  = block.select_one(".state, .jurisdiction")

            results.append({
                "company_name": name,
                "profile_url":  href,
                "state":        state_el.get_text(strip=True) if state_el else "",
                "status":       status_el.get_text(strip=True) if status_el else "",
            })

            if len(results) >= max_results:
                break

        if not results:
            log.debug("bizapedia: search result parse found 0 candidates (HTML may have changed)")

        return results

    def _parse_company_detail(self, html: str) -> dict[str, Any]:
        """Parse a Bizapedia company profile page."""
        if not html:
            return {}

        soup = BeautifulSoup(html, "html.parser")

        data: dict[str, Any] = {
            "company_name":        None,
            "entity_type":         None,
            "status":              None,
            "state":               None,
            "registration_date":   None,
            "registered_agent":    None,
            "registered_address":  None,
            "officers":            [],
        }

        # Company name — h1 or .company-name
        h1 = soup.find("h1")
        if h1:
            data["company_name"] = h1.get_text(strip=True)

        # Structured data table — Bizapedia uses definition lists (dt/dd) or tables
        # Pattern: <dt>Label</dt><dd>Value</dd>
        for dt in soup.select("dt"):
            label = dt.get_text(strip=True).lower().rstrip(":")
            dd = dt.find_next_sibling("dd")
            if dd is None:
                continue
            value = dd.get_text(strip=True)

            if "entity type" in label or "company type" in label:
                data["entity_type"] = value
            elif "status" in label:
                data["status"] = value
            elif "state" in label and "registered" not in label:
                data["state"] = value
            elif "registration date" in label or "formed" in label or "incorporated" in label:
                data["registration_date"] = self._extract_date(value)
            elif "registered agent" in label:
                data["registered_agent"] = value
            elif "registered address" in label or "principal address" in label:
                data["registered_address"] = value

        # Officers section — Bizapedia lists officers in a table or list
        officers = self._extract_officers(soup)
        if officers:
            data["officers"] = officers

        return data

    def _extract_officers(self, soup: BeautifulSoup) -> list[dict[str, str]]:
        """Extract officer/director list from profile page."""
        officers: list[dict[str, str]] = []

        # Look for a section with a heading containing "officer" or "director"
        for heading in soup.find_all(["h2", "h3", "h4"]):
            heading_text = heading.get_text(strip=True).lower()
            if "officer" not in heading_text and "director" not in heading_text:
                continue

            # Find table or list following the heading
            sibling = heading.find_next_sibling()
            while sibling and sibling.name not in ("h2", "h3", "h4"):
                if sibling.name == "table":
                    for row in sibling.select("tr"):
                        cells = row.select("td")
                        if len(cells) >= 2:
                            name  = cells[0].get_text(strip=True)
                            title = cells[1].get_text(strip=True)
                            if name:
                                officers.append({"name": name, "title": title})
                    break
                elif sibling.name in ("ul", "ol"):
                    for li in sibling.select("li"):
                        text = li.get_text(strip=True)
                        # Common format: "Name — Title" or "Name (Title)"
                        parts = re.split(r"\s*[—–-]\s*|\s*\(\s*|\)\s*", text, maxsplit=1)
                        name  = parts[0].strip() if parts else text
                        title = parts[1].strip() if len(parts) > 1 else ""
                        if name:
                            officers.append({"name": name, "title": title})
                    break
                sibling = sibling.find_next_sibling()

        return officers

    @staticmethod
    def _extract_date(text: str) -> str | None:
        """Extract first date-like string from text."""
        m = _DATE_RE.search(text)
        return m.group(0) if m else (text.strip() or None)
