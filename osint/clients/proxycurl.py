"""
osint/clients/proxycurl.py

Proxycurl LinkedIn data API client.

PAID PER CALL — every call costs money. Must check budget before calling.
Cost: $0.01 per person profile lookup.
Budget cap: $25 per run (configurable via PROXYCURL_BUDGET_PER_RUN_USD in .env)

Endpoints used:
- GET /proxycurl/api/v2/linkedin  — person profile by LinkedIn URL

Rate limits: 5 req/second (configured in RATE_LIMITS["proxycurl"])

Usage:
    # ALWAYS check budget before calling
    await rate_limiter.check_budget("proxycurl", run_id)
    data = await proxycurl_client.get_person_profile(linkedin_url)
    await rate_limiter.record_spend("proxycurl", run_id, 0.01)

Docs: https://nubela.co/proxycurl/docs
"""

from __future__ import annotations

import logging
from typing import Any

from osint.core.config import settings
from osint.core.rate_limiter import RateLimiter, BudgetExceeded

log = logging.getLogger(__name__)

BASE_URL = "https://nubela.co"
DOMAIN   = "proxycurl"
COST_PER_CALL = 0.01  # USD


class ProxycurlClient:
    def __init__(self, rate_limiter: RateLimiter) -> None:
        self._rl = rate_limiter

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {settings.proxycurl_api_key}"}

    def _check_key(self) -> None:
        if not settings.proxycurl_api_key:
            raise ValueError(
                "PROXYCURL_API_KEY is not set. "
                "Get a key at https://nubela.co/proxycurl — paid service, $0.01/call."
            )

    async def get_person_profile(
        self,
        linkedin_url: str,
        run_id: str,
        extra_fields: bool = True,
    ) -> dict[str, Any]:
        """
        Fetch a LinkedIn person profile.

        IMPORTANT: This method checks and records budget automatically.
        It will raise BudgetExceeded if the run's spend cap is reached.

        Args:
            linkedin_url: Full LinkedIn profile URL.
            run_id: Current run ID for budget tracking.
            extra_fields: Request extra profile data (education, experience).

        Returns:
            Proxycurl person profile dict.

        Raises:
            BudgetExceeded: If run spend cap is reached.
            ValueError: If PROXYCURL_API_KEY is not set.
        """
        self._check_key()

        # Always check budget before calling — this is a paid API
        await self._rl.check_budget(DOMAIN, run_id)

        params: dict[str, Any] = {
            "url": linkedin_url,
            "fallback_to_cache": "on-error",
        }
        if extra_fields:
            params.update({
                "education": "include",
                "experience": "include",
                "skills": "exclude",  # Not needed, saves tokens
                "certifications": "exclude",
                "extra": "include",
            })

        result = await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/proxycurl/api/v2/linkedin",
            params=params,
            headers=self._headers(),
        )

        # Record spend after successful call
        await self._rl.record_spend(DOMAIN, run_id, COST_PER_CALL)
        log.info(
            "proxycurl: fetched profile for %s (run=%s, cost=$%.2f)",
            linkedin_url, run_id, COST_PER_CALL
        )

        return result

    async def get_company_profile(
        self,
        linkedin_url: str,
        run_id: str,
    ) -> dict[str, Any]:
        """
        Fetch a LinkedIn company profile.
        Cost: $0.01 per call.
        """
        self._check_key()
        await self._rl.check_budget(DOMAIN, run_id)

        result = await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/proxycurl/api/linkedin/company",
            params={"url": linkedin_url},
            headers=self._headers(),
        )

        await self._rl.record_spend(DOMAIN, run_id, COST_PER_CALL)
        return result
