"""
osint/clients/congress.py

Congress.gov API client for federal legislator data.

Provides: committee assignments, sponsored legislation, member detail,
and member search by name/state. Federal officials only — state and
local officials must be discovered via FollowTheMoney + SerpAPI.

API: https://api.congress.gov/v3/
Auth: Free API key from https://api.data.gov/signup/
      Set CONGRESS_API_KEY in .env

Rate limits: 5,000 req/hour — generous. Conservative default: 100 req/min.
Cache: 24 hours for member detail (congressional info changes slowly).

Key endpoints:
    /member                         — search members by name, state, congress
    /member/{bioguideId}            — member detail with full bio
    /member/{bioguideId}/committees — committee assignments
    /member/{bioguideId}/sponsored-legislation — bills sponsored

Committee membership → REGULATORY_OVERSIGHT edges:
    The calling agent derives REGULATORY_OVERSIGHT edges by mapping committee
    names to industry sectors. This client returns raw committee data; edge
    derivation is the agent's responsibility.

    Example mapping (implement in agent, not here):
        "Senate Banking Committee"        → banking, fintech, insurance
        "House Judiciary"                 → legal, regulatory enforcement
        "Senate Commerce"                 → tech, telecom, transportation
        "House Financial Services"        → banking, investment, housing
        "Senate Intelligence"             → national security, surveillance tech
"""

from __future__ import annotations

import logging
from typing import Any

from osint.core.config import settings
from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

BASE_URL = "https://api.congress.gov/v3"
DOMAIN   = "congress"

# Congress number to use when no specific congress is requested.
# Update this when a new Congress is seated (every 2 years).
CURRENT_CONGRESS = 119  # 119th Congress: Jan 2025 – Jan 2027


class CongressClient:
    """
    Async client for the Congress.gov API.

    All methods return raw API response dicts. Callers are responsible for
    parsing and entity construction.
    """

    def __init__(self, rate_limiter: RateLimiter) -> None:
        self._rl = rate_limiter

    def _params(self, **kwargs: Any) -> dict[str, Any]:
        """Build query params, injecting API key."""
        params: dict[str, Any] = {"api_key": settings.congress_api_key or "", **kwargs}
        if not params["api_key"]:
            log.debug("congress: CONGRESS_API_KEY not set — requests will be unauthenticated (rate-limited)")
        return {k: v for k, v in params.items() if v is not None and v != ""}

    def _check_key(self) -> None:
        if not settings.congress_api_key:
            raise ValueError(
                "CONGRESS_API_KEY is not set. "
                "Get a free key at https://api.data.gov/signup/"
            )

    async def search_member_by_name(
        self,
        name: str,
        state_code: str | None = None,
        congress: int | None = None,
        limit: int = 10,
    ) -> dict[str, Any]:
        """
        Search members of Congress by name.

        Args:
            name:       Full or partial name (e.g. "John Smith" or "Smith").
            state_code: Two-letter state abbreviation (e.g. "TX", "CA").
                        Optional — narrows results significantly.
            congress:   Congress number (e.g. 119). Defaults to CURRENT_CONGRESS.
            limit:      Max results.

        Returns:
            {"members": [{"bioguideId": ..., "name": ..., "state": ...,
                          "district": ..., "party": ..., "chamber": ...,
                          "terms": [...]}], "pagination": {...}}

        Note: Congress.gov member search uses the member's name as a keyword
        query across multiple fields. For common names, filter by state_code.
        """
        self._check_key()
        params = self._params(
            name=name,
            currentMember="true",  # Prefer current members; also returns former
            limit=limit,
        )
        if state_code:
            params["stateCode"] = state_code.upper()
        if congress:
            params["congress"] = congress

        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/member",
            params=params,
        )

    async def get_member(self, bioguide_id: str) -> dict[str, Any]:
        """
        Fetch full member detail by BioGuide ID.

        BioGuide IDs are stable identifiers for all Congress members
        (e.g. "S000033" for Bernie Sanders).

        Returns:
            {"member": {"bioguideId": ..., "name": ..., "party": ...,
                        "state": ..., "birthYear": ..., "leadership": [...],
                        "sponsoredLegislation": {...}, "cosponsoredLegislation": {...},
                        "terms": [{"congress": ..., "chamber": ..., "startYear": ...,
                                   "endYear": ...}]}}
        """
        self._check_key()
        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/member/{bioguide_id}",
            params=self._params(),
        )

    async def get_member_committees(
        self,
        bioguide_id: str,
        congress: int | None = None,
    ) -> dict[str, Any]:
        """
        Fetch committee assignments for a member.

        Args:
            bioguide_id: Member's BioGuide ID.
            congress:    Congress number. Defaults to CURRENT_CONGRESS.

        Returns:
            {"committees": [{"name": ..., "chamber": ..., "systemCode": ...,
                             "subcommittees": [...]}]}

        Committee names are the primary input for REGULATORY_OVERSIGHT edge
        derivation. The calling agent is responsible for the name→sector mapping.
        """
        self._check_key()
        cong = congress or CURRENT_CONGRESS
        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/member/{bioguide_id}/committee-assignment/{cong}",
            params=self._params(),
        )

    async def get_member_sponsored(
        self,
        bioguide_id: str,
        limit: int = 20,
    ) -> dict[str, Any]:
        """
        Fetch bills sponsored by a member (most recent first).

        Returns:
            {"sponsoredLegislation": [{"congress": ..., "number": ...,
                                       "title": ..., "policyArea": ...,
                                       "latestAction": {...}}]}

        policyArea.name is useful for building REGULATORY_OVERSIGHT edges
        and for characterizing a politician's legislative priorities.
        """
        self._check_key()
        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/member/{bioguide_id}/sponsored-legislation",
            params=self._params(limit=limit),
        )
