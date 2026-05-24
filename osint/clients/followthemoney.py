"""
osint/clients/followthemoney.py

FollowTheMoney.org API client.

FollowTheMoney covers state and local campaign finance for all 50 US states —
the data FEC misses below the federal threshold. This is the primary source for
city-level political donation intelligence: who is funding local elections, PACs,
and ballot measure campaigns.

API: https://api.followthemoney.org/
Documentation: https://www.followthemoney.org/our-data/api/
Authentication: Optional API key — unauthenticated allowed but rate-limited.
                Key provides higher rate limits. Free registration.

Key endpoints used:
    /api/?mode=search&s=        — full-text search for candidates/committees
    /api/?mode=top20donors&     — top donors to a candidate
    /api/?mode=sumcontribs&     — contribution summary (totals by candidate/org)
    /api/?mode=indexp&          — independent expenditures

All responses are JSON. The API returns a 'records' list and 'totalrecords' count.

Rate limits:
    Unauthenticated: ~30 req/min
    Authenticated:   ~100 req/min (key gives ~3x)

Important: FollowTheMoney only covers US state/local data.
           For federal data use FECClient.
"""

from __future__ import annotations

import logging
from typing import Any

from osint.core.config import settings
from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

DOMAIN = "followthemoney"
BASE_URL = "https://api.followthemoney.org/api/"


class FollowTheMoneyClient:
    """
    Async client for FollowTheMoney.org API.

    All public methods return raw API response dicts.
    Callers are responsible for parsing and entity construction.
    """

    def __init__(self, rate_limiter: RateLimiter) -> None:
        self._rl = rate_limiter

    def _params(self, **kwargs: Any) -> dict[str, Any]:
        """Build query params, injecting API key if configured."""
        params: dict[str, Any] = {"APIKey": settings.followthemoney_api_key or "", **kwargs}
        if not params["APIKey"]:
            del params["APIKey"]
        return params

    # ─────────────────────────────────────────────────────────────────────────
    # Search: candidates and committees
    # ─────────────────────────────────────────────────────────────────────────

    async def search(
        self,
        query: str,
        state: str | None = None,
        year: str | None = None,
        limit: int = 25,
    ) -> dict[str, Any]:
        """
        Full-text search for candidates, committees, and organizations.

        Args:
            query:  Search string (name of candidate, committee, or organization)
            state:  Two-letter state code (e.g. 'TX', 'CA'). None = all states.
            year:   Election year (e.g. '2022', '2020'). None = all years.
            limit:  Max results (default 25, max 100).

        Returns:
            Raw API response dict with 'records' list.
        """
        params = self._params(
            mode="search",
            s=query,
            recs=min(limit, 100),
        )
        if state:
            params["state"] = state.upper()
        if year:
            params["year"] = year

        return await self._rl.get(DOMAIN, BASE_URL, params=params)

    # ─────────────────────────────────────────────────────────────────────────
    # Top donors to a candidate or committee
    # ─────────────────────────────────────────────────────────────────────────

    async def top_donors(
        self,
        candidate_id: str | None = None,
        committee_id: str | None = None,
        limit: int = 20,
        year: str | None = None,
    ) -> dict[str, Any]:
        """
        Retrieve top donors to a specific candidate or committee.
        Either candidate_id or committee_id must be provided.

        Returns:
            Raw API response with 'records' list — each record has donor name,
            amount, employer, state, occupation, date.
        """
        if not candidate_id and not committee_id:
            raise ValueError("Either candidate_id or committee_id is required")

        params = self._params(mode="top20donors", recs=min(limit, 100))
        if candidate_id:
            params["can_id"] = candidate_id
        if committee_id:
            params["com_id"] = committee_id
        if year:
            params["year"] = year

        return await self._rl.get(DOMAIN, BASE_URL, params=params)

    # ─────────────────────────────────────────────────────────────────────────
    # Contribution summary
    # ─────────────────────────────────────────────────────────────────────────

    async def contribution_summary(
        self,
        query: str,
        state: str | None = None,
        year: str | None = None,
        limit: int = 25,
    ) -> dict[str, Any]:
        """
        Summarize political contributions by candidate or committee name.
        Returns total raised, number of contributors, by-state breakdowns.

        Args:
            query:  Name search string.
            state:  Two-letter state filter.
            year:   Election year filter.
            limit:  Max records.
        """
        params = self._params(mode="sumcontribs", s=query, recs=min(limit, 100))
        if state:
            params["state"] = state.upper()
        if year:
            params["year"] = year

        return await self._rl.get(DOMAIN, BASE_URL, params=params)

    # ─────────────────────────────────────────────────────────────────────────
    # Independent expenditures
    # ─────────────────────────────────────────────────────────────────────────

    async def independent_expenditures(
        self,
        query: str,
        state: str | None = None,
        year: str | None = None,
        limit: int = 25,
    ) -> dict[str, Any]:
        """
        Retrieve independent expenditure records for a search query.
        Covers PAC and outside spending not coordinated with a campaign.

        Returns records with spender name, amount, recipient, date.
        """
        params = self._params(mode="indexp", s=query, recs=min(limit, 100))
        if state:
            params["state"] = state.upper()
        if year:
            params["year"] = year

        return await self._rl.get(DOMAIN, BASE_URL, params=params)

    # ─────────────────────────────────────────────────────────────────────────
    # Donor profile
    # ─────────────────────────────────────────────────────────────────────────

    async def donor_profile(
        self,
        donor_name: str,
        state: str | None = None,
        year: str | None = None,
        limit: int = 25,
    ) -> dict[str, Any]:
        """
        Find all political contributions by a named donor across state races.
        Useful for profiling an individual's or organization's political giving.

        Returns records with recipient, amount, date, state, election type.
        """
        params = self._params(mode="donor", s=donor_name, recs=min(limit, 100))
        if state:
            params["state"] = state.upper()
        if year:
            params["year"] = year

        return await self._rl.get(DOMAIN, BASE_URL, params=params)

    # ─────────────────────────────────────────────────────────────────────────
    # Static helpers for parsing
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def extract_records(response: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract 'records' list from a raw API response."""
        records = response.get("records", [])
        if not isinstance(records, list):
            return []
        return records

    @staticmethod
    def total_records(response: dict[str, Any]) -> int:
        """Return total record count from a raw API response."""
        return int(response.get("totalrecords", 0))
