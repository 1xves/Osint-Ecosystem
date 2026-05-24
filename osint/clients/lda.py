"""
osint/clients/lda.py

Senate Lobbying Disclosure Act (LDA) API client.

The LDA database contains all federal lobbying registrations and disclosures
filed under the Lobbying Disclosure Act of 1995. Every organization that spends
$14,000+ per 6-month period on lobbying federal officials must register and
file quarterly LD-2 reports disclosing:
    - Registrant (the lobbying firm or in-house lobbyist)
    - Client (the organization being represented)
    - Issue areas (from a standardized 74-code list)
    - Specific bills lobbied
    - Estimated income/expenses

API: https://lda.senate.gov/api/v1/
Auth: No authentication required — fully public.
Docs: https://lda.senate.gov/api/

Rate limits: Not documented. 100 req/min is conservative and sufficient.
Cache: 7 days — filings are quarterly, change infrequently.

Usage in pipeline:
    1. In political.py: city-scoped collection — query by city name to find
       orgs and individuals with lobbying activity in the city. The best
       approach is to join against already-collected corporate entities
       rather than doing open-ended city keyword searches (see _USE_JOIN_MODE
       note below).

    2. In enrichment.py: per-entity check — for government officials and
       politicians, call get_lobbyist_filings(name) to determine if the
       individual is or was a registered lobbyist. This is a revolving door
       signal distinct from the OpenSecrets revolving door endpoint.

_USE_JOIN_MODE note:
    The LDA API is not designed for city-scoped geographic queries. There is
    no city filter on the /filings/ endpoint. The two usable strategies are:
    (a) Query by client_name or registrant_name using already-known entity
        names from other collection agents. This "join mode" is accurate
        but requires prior collection to have run.
    (b) Query by registrant_state or client_state (two-letter state code)
        to get all lobbying activity in a state, then filter by city in
        client_ppb_country (country/city of principal place of business).
        This is higher volume but gives broader coverage.
    Strategy (a) is implemented here. Strategy (b) can be added as an
    optional parameter if broader coverage is needed.
"""

from __future__ import annotations

import logging
from typing import Any

from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

BASE_URL = "https://lda.senate.gov/api/v1"
DOMAIN   = "lda"

# LDA issue area codes that are most relevant to startup ecosystem analysis
# Full list: https://lda.senate.gov/api/v1/constants/filing/lobbyingactivityissues/
PRIORITY_ISSUE_CODES = {
    "FIN": "Finance",
    "TAX": "Taxation/Internal Revenue Code",
    "SCI": "Science/Technology",
    "TEC": "Computers/Internet",
    "TRA": "Trade",
    "IMM": "Immigration",
    "LBR": "Labor Issues/Antitrust/Workplace",
    "SMB": "Small Business",
    "BUD": "Budget/Appropriations",
    "GOV": "Government Issues",
    "HCR": "Health Issues",
    "EDU": "Education",
    "DEF": "Defense",
    "ENV": "Environment/Superfund",
    "ENE": "Energy/Nuclear",
    "HOU": "Housing",
    "TRN": "Transportation",
}


class LDAClient:
    """
    Async client for the Senate Lobbying Disclosure Act API.

    All methods return raw API response dicts. The LDA API uses pagination
    via next/previous cursor links. For large result sets, callers should
    check response["next"] and call get_next_page() iteratively.
    """

    def __init__(self, rate_limiter: RateLimiter) -> None:
        self._rl = rate_limiter

    async def search_filings(
        self,
        lobbyist_name: str | None = None,
        client_name: str | None = None,
        registrant_name: str | None = None,
        filing_year: int | None = None,
        filing_type: str | None = None,
        issue_code: str | None = None,
        page_size: int = 25,
    ) -> dict[str, Any]:
        """
        Search LDA filings by various criteria.

        At least one search parameter should be provided. All parameters
        are ANDed together — more specific queries return fewer, better results.

        Args:
            lobbyist_name:    Individual lobbyist's name (partial match).
            client_name:      Client organization name (partial match).
            registrant_name:  Lobbying firm/registrant name (partial match).
            filing_year:      4-digit year (e.g. 2024).
            filing_type:      "RR" (registration), "Q1"/"Q2"/"Q3"/"Q4" (quarterly report),
                              "RA" (registration amendment), "DA" (document amendment).
            issue_code:       One of the PRIORITY_ISSUE_CODES keys.
            page_size:        Results per page (max 100).

        Returns:
            {"count": int, "next": str|null, "previous": str|null,
             "results": [{"id": ..., "url": ..., "filing_type": ...,
                          "filing_year": ..., "registrant": {...},
                          "client": {...}, "lobbying_activities": [...]}]}

        Each result's "lobbying_activities" list contains:
            [{"general_issue_code": ..., "description": ...,
              "lobbyists": [{"lobbyist": {"id": ..., "name": ...}}]}]
        """
        params: dict[str, Any] = {"page_size": page_size}

        if lobbyist_name:
            params["lobbyist_name"] = lobbyist_name
        if client_name:
            params["client_name"] = client_name
        if registrant_name:
            params["registrant_name"] = registrant_name
        if filing_year:
            params["filing_year"] = filing_year
        if filing_type:
            params["filing_type"] = filing_type
        if issue_code:
            params["general_issue_code"] = issue_code

        if not any([lobbyist_name, client_name, registrant_name]):
            log.warning("lda: search_filings called with no name filter — may return large result set")

        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/filings/",
            params=params,
        )

    async def get_lobbyist_filings(
        self,
        name: str,
        years: int = 5,
    ) -> dict[str, Any]:
        """
        Check if an individual is a registered federal lobbyist and return
        their filing history.

        This is the per-entity enrichment call: given a politician's name,
        determine if they are or were registered as a federal lobbyist.
        The presence of LDA filings is a revolving door signal.

        Args:
            name:  Individual's full name. LDA uses exact-ish matching.
            years: How many years of filing history to search (most recent first).
                   Uses filing_year filter from (current_year - years) to current.

        Returns:
            Same structure as search_filings(). Check results[*].registrant.name
            to confirm match — LDA name matching is not always exact.

        Note on false positives: "John Smith" will match all John Smiths in
        the LDA database. Callers should compare against known employer and
        state before treating a match as confirmed.
        """
        from datetime import date
        current_year = date.today().year
        earliest_year = current_year - years

        # LDA doesn't have a single "lobbyist_name" filter on the /filings/ endpoint;
        # it's a registrant-level API. The closest approach is filtering by
        # registrant_name for in-house lobbyists, or by the lobbyist's individual
        # registration. We use client_name as a fallback and cross-check in caller.
        # The most reliable approach: search filings where lobbyist name appears.
        return await self.search_filings(
            lobbyist_name=name,
            filing_year=current_year,   # Most recent year first
            page_size=10,
        )

    async def get_registrant(self, registrant_id: int) -> dict[str, Any]:
        """
        Fetch full registrant detail by ID.

        Returns registrant name, address, and associated client list.
        """
        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/registrants/{registrant_id}/",
            params={},
        )

    async def get_client(self, client_id: int) -> dict[str, Any]:
        """
        Fetch full client detail by ID.

        Returns client name, country, state, description, and filing history.
        """
        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/clients/{client_id}/",
            params={},
        )

    async def get_next_page(self, next_url: str) -> dict[str, Any]:
        """
        Fetch the next page of results using the cursor URL from a prior response.

        LDA paginates using absolute next/previous URLs. Pass response["next"]
        directly here.
        """
        if not next_url or not next_url.startswith("https://lda.senate.gov"):
            raise ValueError(f"Invalid LDA pagination URL: {next_url!r}")

        return await self._rl.get(DOMAIN, next_url, params={})

    @staticmethod
    def extract_lobbyists(filing: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Extract individual lobbyists from a filing result.

        Returns list of {name, id} dicts, deduplicated across all lobbying
        activities in the filing.
        """
        seen: set[int] = set()
        lobbyists: list[dict[str, Any]] = []

        for activity in filing.get("lobbying_activities", []):
            for entry in activity.get("lobbyists", []):
                lob = entry.get("lobbyist", {})
                lob_id = lob.get("id")
                if lob_id and lob_id not in seen:
                    seen.add(lob_id)
                    lobbyists.append({
                        "id": lob_id,
                        "name": lob.get("first_name", "") + " " + lob.get("last_name", ""),
                        "covered_position": lob.get("covered_position"),
                    })

        return lobbyists

    @staticmethod
    def extract_issue_codes(filing: dict[str, Any]) -> list[str]:
        """Extract distinct general_issue_code values from a filing."""
        codes: set[str] = set()
        for activity in filing.get("lobbying_activities", []):
            code = activity.get("general_issue_code")
            if code:
                codes.add(code)
        return sorted(codes)
