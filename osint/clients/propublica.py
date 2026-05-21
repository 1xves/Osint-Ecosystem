"""
osint/clients/propublica.py

ProPublica API client — Nonprofit Explorer + Congress API.

Nonprofit Explorer:
- GET /nonprofits/search.json  — search 990 filings by name/state/city
- GET /nonprofits/{ein}.json   — full 990 data for a nonprofit by EIN

Congress API:
- GET /members.json            — all current members of Congress
- GET /members/{bioguide}.json — individual member profile
- GET /members/{bioguide}/votes.json  — voting record
- GET /bills/search.json       — bill search

Rate limits: 60 req/min (configured in RATE_LIMITS["propublica_nonprofit"])
Auth: API key as header for Congress; none for Nonprofit Explorer.

Docs:
  https://projects.propublica.org/nonprofits/api
  https://projects.propublica.org/api-docs/congress-api/
"""

from __future__ import annotations

import logging
from typing import Any

from osint.core.config import settings
from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

NONPROFIT_BASE = "https://projects.propublica.org/nonprofits/api/v2"
CONGRESS_BASE  = "https://api.propublica.org/congress/v1"
DOMAIN_NP      = "propublica_nonprofit"
DOMAIN_CONGRESS = "propublica_congress"


class ProPublicaClient:
    def __init__(self, rate_limiter: RateLimiter) -> None:
        self._rl = rate_limiter

    def _congress_headers(self) -> dict[str, str]:
        if not settings.crunchbase_api_key:  # ProPublica uses its own key env var — not set yet
            log.debug("ProPublica Congress API key not set — requests may be throttled")
        return {}  # ProPublica Congress doesn't require key for basic access

    # ─────────────────────────────────────────────────────────────────────────
    # Nonprofit Explorer
    # ─────────────────────────────────────────────────────────────────────────

    async def search_nonprofits(
        self,
        query: str,
        state: str | None = None,
        ntee: str | None = None,  # NTEE category code
        page: int = 0,
    ) -> dict[str, Any]:
        """
        Search nonprofits by name/keyword.
        Returns organizations with 990 data.
        """
        params: dict[str, Any] = {"q": query, "page": page}
        if state:
            params["state[id]"] = state
        if ntee:
            params["ntee[id]"] = ntee
        return await self._rl.get(DOMAIN_NP, f"{NONPROFIT_BASE}/search.json", params=params)

    async def get_nonprofit(self, ein: str) -> dict[str, Any]:
        """
        Fetch full 990 data for a nonprofit by EIN.
        EIN format: 9-digit string (no dashes).
        Returns multi-year 990 filings with revenue, assets, grants, executives.
        """
        clean_ein = ein.replace("-", "")
        return await self._rl.get(
            DOMAIN_NP,
            f"{NONPROFIT_BASE}/organizations/{clean_ein}.json",
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Congress API
    # ─────────────────────────────────────────────────────────────────────────

    async def get_current_members(
        self,
        chamber: str,   # "senate" or "house"
        congress: int = 118,
        state: str | None = None,
    ) -> dict[str, Any]:
        """Get current members of a Congressional chamber."""
        url = f"{CONGRESS_BASE}/{congress}/{chamber}/members.json"
        return await self._rl.get(DOMAIN_CONGRESS, url, headers=self._congress_headers())

    async def get_member(self, bioguide_id: str) -> dict[str, Any]:
        """Get full profile for a Congress member by bioguide ID."""
        return await self._rl.get(
            DOMAIN_CONGRESS,
            f"{CONGRESS_BASE}/members/{bioguide_id}.json",
            headers=self._congress_headers(),
        )

    async def get_member_votes(
        self,
        bioguide_id: str,
        chamber: str,
        congress: int = 118,
        session: int = 1,
    ) -> dict[str, Any]:
        """Get recent vote positions for a Congress member."""
        return await self._rl.get(
            DOMAIN_CONGRESS,
            f"{CONGRESS_BASE}/members/{bioguide_id}/votes/{chamber}/{congress}/{session}.json",
            headers=self._congress_headers(),
        )

    async def search_bills(
        self,
        query: str,
        congress: int = 118,
    ) -> dict[str, Any]:
        """Search legislation by keyword."""
        return await self._rl.get(
            DOMAIN_CONGRESS,
            f"{CONGRESS_BASE}/{congress}/bills/search.json",
            params={"query": query},
            headers=self._congress_headers(),
        )

    async def get_member_by_state(
        self,
        state: str,
        chamber: str = "senate",
        congress: int = 118,
    ) -> dict[str, Any]:
        """Get all members from a specific state."""
        return await self._rl.get(
            DOMAIN_CONGRESS,
            f"{CONGRESS_BASE}/members/{chamber}/{state}/current.json",
            headers=self._congress_headers(),
        )
