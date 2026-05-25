"""
osint/clients/scrapers/wayback_scraper.py

Wayback Machine historical content scraper.

Uses the Internet Archive Wayback Machine to fetch archived versions of
company websites — specifically the /about, /team, and /leadership pages —
to extract historical officer/executive names from before corporate
restructuring, rebranding, or website scrubbing.

This is distinct from `osint/clients/wayback.py`, which handles dead-link
recovery for the verification agent. That client finds snapshot URLs.
This scraper fetches and parses the content of those snapshots.

Use case:
    Corporate entities with founded_year < 2015 and thin current data.
    Particularly useful for:
      - Companies that went through M&A (new owner removed old leadership)
      - Entities that scrubbed public officer records post-litigation
      - Shell company registration lookups where current website is blank

Trigger condition (checked by enrichment agent):
    - Entity type: "corporate" or "investor"
    - category_fields.get("founded_year", 9999) < 2015
    - len(category_fields.get("executives", [])) < 3

Output (stored in category_fields["wayback_executives"]):
    [
        {
            "name": "Jane Smith",
            "title": "Co-Founder & CEO",
            "snapshot_url": "https://web.archive.org/web/20130601120000/https://acme.com/about",
            "snapshot_date": "2013-06-01",
            "source": "wayback",
        }
    ]

The relationship agent reads this list and creates FORMERLY_EMPLOYED_BY edges
(lower confidence than current employment — these may be outdated).

Rate limit: 10 req/min (wayback domain in RateLimiter). Enforced by WaybackClient.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any
from urllib.parse import urlparse, urljoin

from bs4 import BeautifulSoup

from osint.clients.wayback import WaybackClient
from osint.utils.document_fetcher import DocumentFetcher, DocumentFetchError

log = logging.getLogger(__name__)

# Paths to try on the company domain for leadership/team pages
_TEAM_PATHS = [
    "/about",
    "/team",
    "/leadership",
    "/about/team",
    "/about/leadership",
    "/company/team",
    "/company/about",
]

# Minimum archive year — avoid very stale pre-social-media era pages
_MIN_YEAR = "2010"

# Regex for plausible name strings: 2-4 capitalized words
_NAME_RE = re.compile(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3}\b")

# Common title keywords to identify executive entries
_TITLE_KEYWORDS = {
    "ceo", "coo", "cfo", "cto", "cmo", "cso", "president", "founder",
    "co-founder", "partner", "director", "vp", "vice president", "managing",
    "principal", "chairman", "chair", "executive", "officer",
}


class WaybackScraper:
    """
    Fetches historical executive/team data from Wayback Machine snapshots.

    Uses WaybackClient for snapshot discovery and DocumentFetcher for content.
    """

    def __init__(
        self,
        wayback_client: WaybackClient,
        doc_fetcher: DocumentFetcher | None = None,
    ) -> None:
        self._wayback = wayback_client
        self._fetcher = doc_fetcher or DocumentFetcher()

    # ─────────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────────

    async def scrape_historical_executives(
        self,
        company_website: str,
        min_year: str = _MIN_YEAR,
        max_pages: int = 3,
    ) -> list[dict[str, Any]]:
        """
        Attempt to extract historical executives from Wayback snapshots of a
        company's team/about pages.

        Args:
            company_website:    Base URL of the company website
                                (e.g. "https://www.acme.com").
            min_year:           Earliest archive year to accept (YYYY string).
            max_pages:          Maximum team-path variants to try.

        Returns:
            List of executive dicts with name, title, snapshot_url, snapshot_date.
            Empty list if nothing found.
        """
        if not company_website:
            return []

        base = self._normalize_base(company_website)
        if not base:
            log.warning("wayback_scraper: invalid company_website '%s'", company_website)
            return []

        executives: list[dict[str, Any]] = []

        for path in _TEAM_PATHS[:max_pages]:
            target_url = base.rstrip("/") + path
            snapshot = await self._wayback.find_snapshot(target_url, min_year=min_year)
            if snapshot is None:
                continue

            archive_url   = snapshot.get("archive_url", "")
            timestamp_raw = snapshot.get("timestamp", "")
            snapshot_date = self._parse_timestamp(timestamp_raw)

            log.debug(
                "wayback_scraper: found snapshot for '%s' at %s (%s)",
                target_url, archive_url, snapshot_date,
            )

            try:
                html = await self._fetcher.fetch_html_text(
                    archive_url,
                    selector="main, article, .team, .leadership, .about, body",
                    strict=False,
                )
            except DocumentFetchError as exc:
                log.debug("wayback_scraper: fetch failed for '%s': %s", archive_url, exc)
                continue

            if not html:
                continue

            found = self._extract_executives_from_html(html, archive_url, snapshot_date)
            # Merge — deduplicate by name
            existing_names = {e["name"].lower() for e in executives}
            for exec_entry in found:
                if exec_entry["name"].lower() not in existing_names:
                    executives.append(exec_entry)
                    existing_names.add(exec_entry["name"].lower())

            if len(executives) >= 10:
                break

        if executives:
            log.info(
                "wayback_scraper: extracted %d historical executives from '%s'",
                len(executives), base,
            )

        return executives

    # ─────────────────────────────────────────────────────────────────────────
    # HTML parsing
    # ─────────────────────────────────────────────────────────────────────────

    def _extract_executives_from_html(
        self,
        html: str,
        archive_url: str,
        snapshot_date: str,
    ) -> list[dict[str, Any]]:
        """
        Parse HTML from a Wayback snapshot to extract executive names and titles.

        Strategy:
          1. Look for common team-card patterns (name + title in adjacent elements).
          2. Fall back to heuristic: capitalized Name followed by title keyword.
        """
        soup = BeautifulSoup(html, "html.parser")
        executives: list[dict[str, Any]] = []

        # Strategy 1: team card pattern — look for repeated blocks with name+title
        # Common patterns: <div class="team-member">, <article>, <li class="person">
        cards = soup.select(
            ".team-member, .person, .staff-member, .bio, .profile, "
            "[class*='team'], [class*='leader'], [class*='executive']"
        )

        for card in cards:
            name, title = self._extract_name_title_from_card(card)
            if name and self._looks_like_real_name(name):
                executives.append({
                    "name":          name,
                    "title":         title,
                    "snapshot_url":  archive_url,
                    "snapshot_date": snapshot_date,
                    "source":        "wayback",
                })

        if executives:
            return executives

        # Strategy 2: heuristic scan — find Name followed by title-keyword text
        text_blocks = soup.find_all(["p", "li", "dt", "span", "div"])
        for block in text_blocks:
            direct_text = block.get_text(separator=" ", strip=True)
            if not direct_text or len(direct_text) > 300:
                continue

            name_match = _NAME_RE.search(direct_text)
            if not name_match:
                continue

            lower_text = direct_text.lower()
            has_title = any(kw in lower_text for kw in _TITLE_KEYWORDS)
            if not has_title:
                continue

            name = name_match.group(0)
            if not self._looks_like_real_name(name):
                continue

            # Extract title: text after the name
            rest = direct_text[name_match.end():].strip().lstrip(",—–-").strip()
            title = rest[:80] if rest else ""

            executives.append({
                "name":          name,
                "title":         title,
                "snapshot_url":  archive_url,
                "snapshot_date": snapshot_date,
                "source":        "wayback",
            })

            if len(executives) >= 15:
                break

        return executives

    @staticmethod
    def _extract_name_title_from_card(
        card: Any,
    ) -> tuple[str, str]:
        """
        Extract name and title from a team-card HTML element.

        Tries common sub-element selectors: h2/h3/h4 for name, p/.title for title.
        """
        # Name: prefer heading elements inside the card
        name_el = card.find(["h2", "h3", "h4", "strong", ".name"])
        name = name_el.get_text(strip=True) if name_el else ""

        # Title: prefer elements with class "title", "role", "position"
        title_el = card.select_one(
            ".title, .role, .position, .job-title, p.title, span.title"
        )
        if title_el is None:
            # Fallback: first <p> or <span> that isn't the name
            for el in card.find_all(["p", "span"]):
                text = el.get_text(strip=True)
                if text and text != name:
                    title_el = el
                    break

        title = title_el.get_text(strip=True) if title_el else ""
        return name, title

    @staticmethod
    def _looks_like_real_name(name: str) -> bool:
        """
        Heuristic filter: reject strings that look like headings, labels, or
        other non-name text that match the capitalized word regex.
        """
        if not name or len(name) < 4 or len(name) > 60:
            return False

        parts = name.split()
        if len(parts) < 2 or len(parts) > 4:
            return False

        # Reject names that contain common non-name words
        stopwords = {
            "About", "Contact", "Team", "Leadership", "Board", "Directors",
            "Management", "Staff", "Our", "The", "Meet", "View", "More",
        }
        if any(p in stopwords for p in parts):
            return False

        return True

    @staticmethod
    def _normalize_base(url: str) -> str | None:
        """Ensure URL has a scheme and return just the base (scheme + host)."""
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        parsed = urlparse(url)
        if not parsed.netloc:
            return None
        return f"{parsed.scheme}://{parsed.netloc}"

    @staticmethod
    def _parse_timestamp(ts: str) -> str:
        """Convert Wayback 14-digit timestamp to YYYY-MM-DD."""
        if len(ts) >= 8:
            try:
                dt = datetime.strptime(ts[:8], "%Y%m%d")
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass
        return ts[:8] if ts else ""
