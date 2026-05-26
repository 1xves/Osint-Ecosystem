"""
osint/clients/opencorporates.py

OpenCorporates API client.

Endpoints used:
- GET /companies/search  — search companies by name + jurisdiction
- GET /companies/{jurisdiction}/{company_number}  — company detail
- GET /officers/search   — search company officers

Rate limits: 30 req/min (configured in RATE_LIMITS["opencorporates"])
Auth: API key as query param `api_token` (optional but increases rate limit)

Docs: https://api.opencorporates.com/documentation/API-Reference
"""

from __future__ import annotations

import logging
from typing import Any

from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

BASE_URL = "https://api.opencorporates.com/v0.4"
DOMAIN   = "opencorporates"


class OpenCorporatesClient:
    def __init__(self, rate_limiter: RateLimiter) -> None:
        self._rl = rate_limiter

    def _params(self, extra: dict | None = None) -> dict[str, Any]:
        # No API key configured yet — works without key at lower rate limits
        p: dict[str, Any] = {"format": "json"}
        if extra:
            p.update(extra)
        return p

    async def search_companies(
        self,
        name: str,
        jurisdiction_code: str | None = None,  # e.g., "us_tx" for Texas
        inactive: bool = False,
        per_page: int = 20,
        page: int = 1,
    ) -> dict[str, Any]:
        """
        Search companies by name.

        Args:
            name: Company name to search.
            jurisdiction_code: e.g., "us_tx" (Texas), "us_de" (Delaware)
            inactive: Include inactive/dissolved companies.
            per_page: Results per page.
        """
        params: dict[str, Any] = {
            "q": name,
            "per_page": per_page,
            "page": page,
        }
        if jurisdiction_code:
            params["jurisdiction_code"] = jurisdiction_code
        if inactive:
            params["inactive"] = "true"

        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/companies/search",
            params=self._params(params),
            timeout=10.0,   # OC is slow — fail fast rather than 30s default
        )

    async def get_company(
        self, jurisdiction_code: str, company_number: str
    ) -> dict[str, Any]:
        """Fetch full company detail by jurisdiction and registration number."""
        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/companies/{jurisdiction_code}/{company_number}",
            params=self._params(),
            timeout=10.0,
        )

    async def search_officers(
        self,
        name: str,
        jurisdiction_code: str | None = None,
        per_page: int = 20,
    ) -> dict[str, Any]:
        """Search company officers (directors, registered agents) by name."""
        params: dict[str, Any] = {"q": name, "per_page": per_page}
        if jurisdiction_code:
            params["jurisdiction_code"] = jurisdiction_code
        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/officers/search",
            params=self._params(params),
            timeout=10.0,
        )

    async def get_company_network(
        self, jurisdiction_code: str, company_number: str
    ) -> dict[str, Any]:
        """Get corporate network (subsidiaries, parent companies) for a company."""
        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/companies/{jurisdiction_code}/{company_number}/network",
            params=self._params(),
            timeout=10.0,
        )
