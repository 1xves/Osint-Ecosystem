"""
osint/clients/fec.py

FEC (Federal Election Commission) API client.

Endpoints used:
- GET /candidates/search/  — search candidates
- GET /committee/          — search committees
- GET /schedules/schedule_a/  — individual contributions to committees
- GET /schedules/schedule_b/  — disbursements from committees

Rate limits: 60 req/min (configured in RATE_LIMITS["fec_api"])
Auth: API key as query param `api_key`

Docs: https://api.open.fec.gov/developers/
"""

from __future__ import annotations

import logging
from typing import Any

from osint.core.config import settings
from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

BASE_URL = "https://api.open.fec.gov/v1"
DOMAIN   = "fec_api"


class FECClient:
    def __init__(self, rate_limiter: RateLimiter) -> None:
        self._rl = rate_limiter

    def _params(self, extra: dict | None = None) -> dict[str, Any]:
        p: dict[str, Any] = {"api_key": settings.fec_api_key or "DEMO_KEY"}
        if extra:
            p.update(extra)
        return p

    async def search_candidates(
        self,
        name: str | None = None,
        state: str | None = None,
        office: str | None = None,  # "H" House, "S" Senate, "P" President
        cycle: int | None = None,
        per_page: int = 20,
    ) -> dict[str, Any]:
        """Search for election candidates."""
        params: dict[str, Any] = {"per_page": per_page}
        if name:
            params["name"] = name
        if state:
            params["state"] = state
        if office:
            params["office"] = office
        if cycle:
            params["election_year"] = cycle
        return await self._rl.get(DOMAIN, f"{BASE_URL}/candidates/search/", params=self._params(params))

    async def get_candidate(self, candidate_id: str) -> dict[str, Any]:
        """Fetch full profile for a candidate by FEC candidate ID (e.g., P00009423)."""
        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/candidate/{candidate_id}/",
            params=self._params({"fields": "*"}),
        )

    async def search_committees(
        self,
        name: str | None = None,
        state: str | None = None,
        committee_type: str | None = None,
        per_page: int = 20,
    ) -> dict[str, Any]:
        """Search for PACs, Super PACs, party committees."""
        params: dict[str, Any] = {"per_page": per_page}
        if name:
            params["name"] = name
        if state:
            params["state"] = state
        if committee_type:
            params["committee_type"] = committee_type
        return await self._rl.get(DOMAIN, f"{BASE_URL}/committees/", params=self._params(params))

    async def get_individual_contributions(
        self,
        contributor_name: str | None = None,
        contributor_city: str | None = None,
        contributor_state: str | None = None,
        min_date: str | None = None,
        max_date: str | None = None,
        per_page: int = 20,
    ) -> dict[str, Any]:
        """
        Search Schedule A (individual contributions to committees).
        Used to find political donations by individuals.
        """
        params: dict[str, Any] = {"per_page": per_page, "sort": "-contribution_receipt_date"}
        if contributor_name:
            params["contributor_name"] = contributor_name
        if contributor_city:
            params["contributor_city"] = contributor_city
        if contributor_state:
            params["contributor_state"] = contributor_state
        if min_date:
            params["min_date"] = min_date
        if max_date:
            params["max_date"] = max_date
        return await self._rl.get(DOMAIN, f"{BASE_URL}/schedules/schedule_a/", params=self._params(params))

    async def get_candidate_financials(
        self, candidate_id: str, cycle: int | None = None
    ) -> dict[str, Any]:
        """Get total raised/spent for a candidate."""
        params: dict[str, Any] = {}
        if cycle:
            params["cycle"] = cycle
        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/candidate/{candidate_id}/totals/",
            params=self._params(params),
        )

    async def get_top_donors_for_candidate(
        self, candidate_id: str, per_page: int = 20
    ) -> dict[str, Any]:
        """Get top individual contributors to a candidate's committee."""
        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/schedules/schedule_a/",
            params=self._params({
                "candidate_id": candidate_id,
                "per_page": per_page,
                "sort": "-contribution_receipt_amount",
            }),
        )
