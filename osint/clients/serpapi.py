"""
osint/clients/serpapi.py

SerpAPI client for Google search results.

Used as a general-purpose fallback search and for entities not in structured APIs.
Budget: 2000 req/month, 2 req/second (configured in RATE_LIMITS["serpapi"])

Endpoints used:
- GET /search  — Google Search (engine=google)

Docs: https://serpapi.com/search-api
"""

from __future__ import annotations

import logging
from typing import Any

from osint.core.config import settings
from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

BASE_URL = "https://serpapi.com"
DOMAIN = "serpapi"


class SerpApiClient:
    def __init__(self, rate_limiter: RateLimiter) -> None:
        self._rl = rate_limiter

    def _check_key(self) -> None:
        if not settings.serpapi_api_key:
            raise ValueError(
                "SERPAPI_API_KEY is not set. Add it to .env. "
                "Get a key at https://serpapi.com"
            )

    async def search(
        self,
        query: str,
        num: int = 10,
        gl: str = "us",
        hl: str = "en",
    ) -> dict[str, Any]:
        """
        Execute a Google search via SerpAPI.

        Args:
            query: Search query string.
            num: Number of results (max 100).
            gl: Country code for results.
            hl: Language code.

        Returns:
            Raw SerpAPI response dict with organic_results, knowledge_graph, etc.
        """
        self._check_key()
        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/search",
            params={
                "engine": "google",
                "q": query,
                "api_key": settings.serpapi_api_key,
                "num": num,
                "gl": gl,
                "hl": hl,
            },
        )

    async def search_news(self, query: str, num: int = 10) -> dict[str, Any]:
        """Search Google News for a query."""
        self._check_key()
        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/search",
            params={
                "engine": "google",
                "q": query,
                "tbm": "nws",  # news tab
                "api_key": settings.serpapi_api_key,
                "num": num,
            },
        )

    async def search_entity(self, entity_name: str, city: str, context: str = "") -> dict[str, Any]:
        """
        Convenience method: search for an entity with city context.
        Used when structured APIs return nothing.
        """
        query = f'"{entity_name}" {city}'
        if context:
            query += f" {context}"
        return await self.search(query, num=10)
