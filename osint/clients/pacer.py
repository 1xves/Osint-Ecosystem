"""
osint/clients/pacer.py

CourtListener API client for federal and state court records.

CourtListener (Free Law Project) provides structured access to PACER court data:
  - Docket search by party name
  - Opinion and docket entry text
  - Case metadata (court, date, parties, outcome)

API: https://www.courtlistener.com/api/rest/v4/
Auth: API key in Authorization header. Free registration at courtlistener.com.
      Without API key: 5000 req/day unauthenticated. With key: 10 req/sec.

Rate limit: 30 req/min (conservative) — see RATE_LIMITS["courtlistener"]

Phase 7 role:
    This client searches CourtListener for cases involving a named entity,
    fetches the docket text (or available opinion text), and passes it to
    DocumentExtractor for structured extraction.

    Output stored in category_fields["litigation"]:
        [
            {
                "case_name":      str,
                "case_number":    str,
                "court":          str,
                "filing_date":    str | None,
                "case_type":      str,
                "outcome":        str | None,
                "monetary_judgment": int | None,
                "summary":        str,
                "docket_url":     str,
            }
        ]

    Relationship agent reads this → LITIGATION_AGAINST edges.

Important limitations:
  - CourtListener has full coverage of federal courts (PACER) and
    selected state courts. Many state courts are not covered.
  - Docket text may be truncated for large cases. We use the first
    available opinion text or docket entry summary.
  - Criminal cases with sealed records return no content.
"""

from __future__ import annotations

import logging
from typing import Any

from osint.core.config import settings
from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

DOMAIN   = "courtlistener"
BASE_URL = "https://www.courtlistener.com/api/rest/v4"

# Minimum docket entries to consider a case "substantive"
# (avoids picking up one-line administrative entries)
MIN_DOCKET_ENTRIES = 2

# Case types we care about — filter out trivial administrative matters
RELEVANT_CASE_TYPES = {
    "civil", "criminal", "bankruptcy", "regulatory",
}


class CourtListenerClient:
    """
    Async client for the CourtListener REST API v4.

    Provides:
      - search_cases(party_name) — keyword search for cases involving a party
      - get_docket(docket_id) — fetch docket metadata and entry count
      - get_docket_text(docket_id) — fetch text from the docket entries
    """

    def __init__(self, rate_limiter: RateLimiter) -> None:
        self._rl = rate_limiter
        self._api_key = settings.courtlistener_api_key

    def _headers(self) -> dict[str, str]:
        h = {"Accept": "application/json"}
        if self._api_key:
            h["Authorization"] = f"Token {self._api_key}"
        return h

    # ─────────────────────────────────────────────────────────────────────────
    # Search
    # ─────────────────────────────────────────────────────────────────────────

    async def search_cases(
        self,
        party_name: str,
        max_results: int = 10,
        case_type: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Search CourtListener dockets for cases where party_name appears
        as a plaintiff or defendant.

        Args:
            party_name:   Entity name to search (company or person).
            max_results:  Max case records to return.
            case_type:    Optional filter: "civil", "criminal", "bankruptcy".

        Returns:
            List of case dicts with metadata. Each dict has:
                {docket_id, case_name, case_number, court, date_filed,
                 nature_of_suit, docket_url, entry_count}
        """
        params: dict[str, Any] = {
            "q":              party_name,
            "type":           "r",        # "r" = RECAP/PACER dockets
            "order_by":       "score desc",
            "page_size":      max_results,
        }
        if case_type:
            params["nature_of_suit"] = case_type

        try:
            response = await self._rl.get(
                DOMAIN,
                f"{BASE_URL}/dockets/",
                params=params,
                extra_headers=self._headers(),
            )
        except Exception as exc:
            log.warning("courtlistener: search_cases failed for '%s': %s", party_name, exc)
            return []

        results = response.get("results", [])
        if not isinstance(results, list):
            return []

        cases = []
        for r in results:
            docket_id = r.get("id")
            cases.append({
                "docket_id":    docket_id,
                "case_name":    r.get("case_name", ""),
                "case_number":  r.get("docket_number", ""),
                "court":        self._extract_court_name(r),
                "date_filed":   r.get("date_filed"),
                "nature_of_suit": r.get("nature_of_suit", ""),
                "docket_url":   f"https://www.courtlistener.com/docket/{docket_id}/"
                                if docket_id else "",
                "entry_count":  r.get("docket_entries_count", 0),
            })

        return cases

    # ─────────────────────────────────────────────────────────────────────────
    # Docket detail
    # ─────────────────────────────────────────────────────────────────────────

    async def get_docket(self, docket_id: int | str) -> dict[str, Any]:
        """
        Fetch full docket metadata for a specific docket ID.

        Returns:
            Raw docket dict from CourtListener API, or empty dict on failure.
        """
        try:
            response = await self._rl.get(
                DOMAIN,
                f"{BASE_URL}/dockets/{docket_id}/",
                extra_headers=self._headers(),
            )
            return response or {}
        except Exception as exc:
            log.debug("courtlistener: get_docket failed for id=%s: %s", docket_id, exc)
            return {}

    async def get_docket_entries(
        self,
        docket_id: int | str,
        max_entries: int = 20,
    ) -> list[dict[str, Any]]:
        """
        Fetch the most recent docket entries for a case.

        Docket entries contain the plain-text descriptions of court actions
        (filings, orders, rulings). This is the richest source for
        case_type and outcome detection.

        Args:
            docket_id:    CourtListener docket ID.
            max_entries:  Max entries to return (ordered most recent first).

        Returns:
            List of entry dicts: [{description, date_filed, entry_number}]
        """
        try:
            response = await self._rl.get(
                DOMAIN,
                f"{BASE_URL}/docket-entries/",
                params={
                    "docket": docket_id,
                    "order_by": "-entry_number",
                    "page_size": max_entries,
                },
                extra_headers=self._headers(),
            )
        except Exception as exc:
            log.debug("courtlistener: get_docket_entries failed for %s: %s", docket_id, exc)
            return []

        results = response.get("results", [])
        return [
            {
                "description":    e.get("description", ""),
                "date_filed":     e.get("date_filed"),
                "entry_number":   e.get("entry_number"),
            }
            for e in results
            if isinstance(e, dict)
        ]

    async def get_docket_text(self, docket_id: int | str) -> str:
        """
        Compile a text summary of a docket suitable for LLM extraction.

        Combines: case metadata + docket entry descriptions.

        Returns:
            Plain text string. Empty string on failure.
        """
        docket = await self.get_docket(docket_id)
        if not docket:
            return ""

        entries = await self.get_docket_entries(docket_id, max_entries=30)

        lines = [
            f"Case: {docket.get('case_name', '')}",
            f"Docket Number: {docket.get('docket_number', '')}",
            f"Court: {self._extract_court_name(docket)}",
            f"Date Filed: {docket.get('date_filed', '')}",
            f"Nature of Suit: {docket.get('nature_of_suit', '')}",
            f"Cause: {docket.get('cause', '')}",
            f"Jurisdiction: {docket.get('jurisdiction_type', '')}",
            "",
            "--- Docket Entries ---",
        ]

        for entry in entries:
            date    = entry.get("date_filed", "")
            desc    = entry.get("description", "").strip()
            num     = entry.get("entry_number", "")
            if desc:
                lines.append(f"[{num}] {date}: {desc[:400]}")

        return "\n".join(lines)

    # ─────────────────────────────────────────────────────────────────────────
    # Opinion search (richer text for closed cases)
    # ─────────────────────────────────────────────────────────────────────────

    async def search_opinions(
        self,
        party_name: str,
        max_results: int = 5,
    ) -> list[dict[str, Any]]:
        """
        Search for court opinions (published rulings) mentioning party_name.

        Opinions provide richer text than docket entries for cases that
        reached a substantive ruling.

        Returns:
            List of opinion dicts: [{case_name, court, date_filed, text_excerpt, url}]
        """
        try:
            response = await self._rl.get(
                DOMAIN,
                f"{BASE_URL}/opinions/",
                params={
                    "q":        party_name,
                    "order_by": "score desc",
                    "page_size": max_results,
                },
                extra_headers=self._headers(),
            )
        except Exception as exc:
            log.debug("courtlistener: search_opinions failed for '%s': %s", party_name, exc)
            return []

        results = response.get("results", [])
        return [
            {
                "case_name":    r.get("case_name", ""),
                "court":        r.get("court_id", ""),
                "date_filed":   r.get("date_filed"),
                "text_excerpt": (r.get("plain_text", "") or "")[:2000],
                "url":          r.get("absolute_url", ""),
            }
            for r in results
            if isinstance(r, dict)
        ]

    # ─────────────────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _extract_court_name(record: dict[str, Any]) -> str:
        """Extract readable court name from a docket record."""
        # CourtListener returns court as a URL or short ID; get the full_name if present
        court = record.get("court", "")
        if isinstance(court, dict):
            return court.get("full_name") or court.get("short_name", "")
        if isinstance(court, str) and court.startswith("http"):
            # Extract court ID from URL: .../courts/paed/ → "paed"
            court_id = court.rstrip("/").split("/")[-1]
            return _COURT_ID_TO_NAME.get(court_id, court_id)
        return str(court)


# Mapping of common CourtListener court IDs to readable names
_COURT_ID_TO_NAME: dict[str, str] = {
    "scotus":  "U.S. Supreme Court",
    "ca1":     "U.S. Court of Appeals, First Circuit",
    "ca2":     "U.S. Court of Appeals, Second Circuit",
    "ca3":     "U.S. Court of Appeals, Third Circuit",
    "ca4":     "U.S. Court of Appeals, Fourth Circuit",
    "ca5":     "U.S. Court of Appeals, Fifth Circuit",
    "ca6":     "U.S. Court of Appeals, Sixth Circuit",
    "ca7":     "U.S. Court of Appeals, Seventh Circuit",
    "ca8":     "U.S. Court of Appeals, Eighth Circuit",
    "ca9":     "U.S. Court of Appeals, Ninth Circuit",
    "ca10":    "U.S. Court of Appeals, Tenth Circuit",
    "ca11":    "U.S. Court of Appeals, Eleventh Circuit",
    "cadc":    "U.S. Court of Appeals, D.C. Circuit",
    "paed":    "U.S. District Court, E.D. Pennsylvania",
    "pawd":    "U.S. District Court, W.D. Pennsylvania",
    "nyed":    "U.S. District Court, E.D. New York",
    "nysd":    "U.S. District Court, S.D. New York",
    "ded":     "U.S. District Court, District of Delaware",
    "dcd":     "U.S. District Court, District of Columbia",
    "ilnd":    "U.S. District Court, N.D. Illinois",
    "cand":    "U.S. District Court, N.D. California",
    "cacd":    "U.S. District Court, C.D. California",
    "txnd":    "U.S. District Court, N.D. Texas",
    "txsd":    "U.S. District Court, S.D. Texas",
}
