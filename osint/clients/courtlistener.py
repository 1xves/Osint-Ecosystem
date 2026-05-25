"""
osint/clients/courtlistener.py

CourtListener (Free Law Project) API client.

Provides federal and state court case data — used for the Illicit category
to search for fraud, securities violations, organized crime cases.

Endpoints used:
- GET /api/rest/v3/search/  — full-text case search
- GET /api/rest/v3/docket/  — case docket detail
- GET /api/rest/v3/party/   — search parties by name

Rate limits: 30 req/min (configured in RATE_LIMITS["courtlistener"])
Auth: Token auth header (free account required)

Docs: https://www.courtlistener.com/help/api/
"""

from __future__ import annotations

import logging
from typing import Any

from osint.core.config import settings
from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

BASE_URL = "https://www.courtlistener.com"
DOMAIN   = "courtlistener"


class CourtListenerClient:
    def __init__(self, rate_limiter: RateLimiter, token: str = "") -> None:
        self._rl = rate_limiter
        self._token = token or settings.courtlistener_api_key

    def _headers(self) -> dict[str, str]:
        if self._token:
            return {"Authorization": f"Token {self._token}"}
        return {}

    async def search_cases(
        self,
        query: str,
        case_type: str | None = None,    # "o" (opinions) | "r" (RECAP/federal docs) | "oa" (oral arguments)
        court: str | None = None,         # e.g., "scotus", "ca9"
        filed_after: str | None = None,   # YYYY-MM-DD
        filed_before: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        """
        Full-text search of court cases.
        Returns list of cases with case number, parties, court, filing date.
        """
        params: dict[str, Any] = {
            "q": query,
            "page": page,
            "page_size": page_size,
            "format": "json",
        }
        if case_type:
            params["type"] = case_type
        if court:
            params["court"] = court
        if filed_after:
            params["filed_after"] = filed_after
        if filed_before:
            params["filed_before"] = filed_before

        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/api/rest/v3/search/",
            params=params,
            headers=self._headers(),
        )

    async def search_parties(
        self,
        name: str,
        page_size: int = 20,
    ) -> dict[str, Any]:
        """Search cases where a named individual or organization is a party."""
        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/api/rest/v3/party/",
            params={"name": name, "page_size": page_size, "format": "json"},
            headers=self._headers(),
        )

    async def get_docket(self, docket_id: str) -> dict[str, Any]:
        """Fetch full case docket detail."""
        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/api/rest/v3/docket/{docket_id}/",
            params={"format": "json"},
            headers=self._headers(),
        )

    async def search_fraud_cases(self, entity_name: str, city: str | None = None) -> dict[str, Any]:
        """
        Convenience method: search for fraud/securities cases involving an entity.
        Searches federal courts.
        """
        query = f'"{entity_name}" fraud OR "securities violation" OR "wire fraud" OR "money laundering"'
        if city:
            query += f" {city}"
        return await self.search_cases(query=query, case_type="r", page_size=10)
