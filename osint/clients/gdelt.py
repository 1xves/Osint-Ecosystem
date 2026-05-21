"""
osint/clients/gdelt.py

GDELT Project API client.

GDELT monitors news media worldwide and provides near-real-time event data
and document search. Used for:
- News coverage analysis for entities
- Identifying media presence / community leader signals
- Event-based relationship detection

Endpoints used:
- GET https://api.gdeltproject.org/api/v2/doc/doc  — document search (GKG)
- GET https://api.gdeltproject.org/api/v2/timeline/timeline  — timeline of coverage

Rate limits: 60 req/min (configured in RATE_LIMITS["gdelt"])
Auth: None required — public API.

Docs: https://blog.gdeltproject.org/gdelt-2-0-our-global-world-in-realtime/
"""

from __future__ import annotations

import logging
from typing import Any

from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

BASE_URL = "https://api.gdeltproject.org/api/v2"
DOMAIN   = "gdelt"


class GDELTClient:
    def __init__(self, rate_limiter: RateLimiter) -> None:
        self._rl = rate_limiter

    async def search_articles(
        self,
        query: str,
        mode: str = "artlist",      # "artlist" | "timelinevol" | "tone"
        max_records: int = 25,
        sort: str = "DateDesc",
        start_date: str | None = None,   # YYYYMMDDHHMMSS
        end_date: str | None = None,
    ) -> dict[str, Any]:
        """
        Search GDELT news articles.

        Args:
            query: Search terms (supports boolean: "Austin VC" AND startup)
            mode: Return type. "artlist" = article list with URLs.
            max_records: Number of results.
            sort: Sort order ("DateDesc", "DateAsc", "Relevance").
            start_date, end_date: Optional date range in YYYYMMDDHHMMSS format.

        Returns:
            Dict with articles list, each including url, title, seendate, domain, tone.
        """
        params: dict[str, Any] = {
            "query": query,
            "mode": mode,
            "maxrecords": max_records,
            "sort": sort,
            "format": "json",
        }
        if start_date:
            params["startdatetime"] = start_date
        if end_date:
            params["enddatetime"] = end_date

        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/doc/doc",
            params=params,
        )

    async def search_entity_coverage(
        self,
        entity_name: str,
        city: str,
        max_records: int = 15,
    ) -> dict[str, Any]:
        """
        Convenience: search for news coverage of an entity in a city.
        Used to assess media presence for community leader scoring.
        """
        query = f'"{entity_name}" {city}'
        return await self.search_articles(query=query, max_records=max_records)

    async def get_timeline_volume(
        self,
        query: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, Any]:
        """Get timeline of coverage volume for a query."""
        return await self.search_articles(
            query=query,
            mode="timelinevol",
            start_date=start_date,
            end_date=end_date,
        )
