"""
osint/clients/opensecrets.py

OpenSecrets API client for campaign finance data.

Endpoints used:
- getLegislators   — legislators by state
- candIndByInd     — candidate top industries
- candContrib      — top contributors to a candidate
- orgSummary       — organization PAC summary
- independentExpend — outside spending

Rate limits: 500 req/day (configured in RATE_LIMITS["opensecrets"])
Auth: API key as query param `apikey`

Docs: https://www.opensecrets.org/api/
"""

from __future__ import annotations

import logging
from typing import Any

from osint.core.config import settings
from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

BASE_URL = "https://www.opensecrets.org/api/"
DOMAIN   = "opensecrets"


class OpenSecretsClient:
    def __init__(self, rate_limiter: RateLimiter) -> None:
        self._rl = rate_limiter

    def _params(self, method: str, extra: dict | None = None) -> dict[str, Any]:
        p: dict[str, Any] = {
            "apikey": settings.opensecrets_api_key or "",
            "method": method,
            "output": "json",
        }
        if extra:
            p.update(extra)
        return p

    def _check_key(self) -> None:
        if not settings.opensecrets_api_key:
            raise ValueError(
                "OPENSECRETS_API_KEY is not set. "
                "Get a key at https://www.opensecrets.org/api/admin/apikey"
            )

    async def get_legislators(self, state: str) -> dict[str, Any]:
        """Get all legislators for a state with OpenSecrets IDs."""
        self._check_key()
        return await self._rl.get(
            DOMAIN, BASE_URL, params=self._params("getLegislators", {"id": state})
        )

    async def get_candidate_industries(self, cid: str, cycle: str = "2024") -> dict[str, Any]:
        """Top industries donating to a candidate."""
        self._check_key()
        return await self._rl.get(
            DOMAIN, BASE_URL, params=self._params("candIndByInd", {"cid": cid, "cycle": cycle})
        )

    async def get_candidate_contributors(self, cid: str, cycle: str = "2024") -> dict[str, Any]:
        """Top contributors (individuals and PACs) to a candidate."""
        self._check_key()
        return await self._rl.get(
            DOMAIN, BASE_URL, params=self._params("candContrib", {"cid": cid, "cycle": cycle})
        )

    async def get_org_summary(self, org_name: str) -> dict[str, Any]:
        """PAC summary and political giving for an organization."""
        self._check_key()
        return await self._rl.get(
            DOMAIN, BASE_URL, params=self._params("orgSummary", {"id": org_name})
        )

    async def get_candidate_summary(self, cid: str, cycle: str = "2024") -> dict[str, Any]:
        """Total raised, spent, cash-on-hand for a candidate."""
        self._check_key()
        return await self._rl.get(
            DOMAIN, BASE_URL, params=self._params("candSummary", {"cid": cid, "cycle": cycle})
        )
