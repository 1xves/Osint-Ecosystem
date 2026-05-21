"""
osint/clients/people_data_labs.py

People Data Labs (PDL) API client.

PDL provides enriched person and company data.
Used for individuals who aren't on LinkedIn or for enriching incomplete profiles.

Endpoints used:
- POST /v5/person/enrich   — enrich a person by name+location+email
- POST /v5/company/enrich  — enrich a company by name+website
- POST /v5/person/search   — search people by city + title keywords

Rate limits: 60 req/min (configured in RATE_LIMITS["people_data_labs"])
Auth: API key as header `X-Api-Key`

Docs: https://docs.peopledatalabs.com/
"""

from __future__ import annotations

import logging
from typing import Any

from osint.core.config import settings
from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

BASE_URL = "https://api.peopledatalabs.com"
DOMAIN   = "people_data_labs"


class PeopleDataLabsClient:
    def __init__(self, rate_limiter: RateLimiter) -> None:
        self._rl = rate_limiter

    def _headers(self) -> dict[str, str]:
        return {"X-Api-Key": settings.pdl_api_key or ""}

    def _check_key(self) -> None:
        if not settings.pdl_api_key:
            raise ValueError(
                "PDL_API_KEY is not set. "
                "Get a key at https://www.peopledatalabs.com"
            )

    async def enrich_person(
        self,
        name: str | None = None,
        first_name: str | None = None,
        last_name: str | None = None,
        company: str | None = None,
        location: str | None = None,
        linkedin_url: str | None = None,
        min_likelihood: int = 6,  # 0-10, higher = more confident match
    ) -> dict[str, Any]:
        """
        Enrich a person profile with PDL data.
        Provide as many identifying fields as possible for best match quality.
        """
        self._check_key()
        params: dict[str, Any] = {"min_likelihood": min_likelihood}
        if name:
            params["name"] = name
        if first_name:
            params["first_name"] = first_name
        if last_name:
            params["last_name"] = last_name
        if company:
            params["company"] = company
        if location:
            params["location"] = location
        if linkedin_url:
            params["profile"] = linkedin_url
        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/v5/person/enrich",
            params=params,
            headers=self._headers(),
        )

    async def enrich_company(
        self,
        name: str | None = None,
        website: str | None = None,
        profile: str | None = None,  # LinkedIn company URL
    ) -> dict[str, Any]:
        """Enrich a company profile with PDL data."""
        self._check_key()
        params: dict[str, Any] = {}
        if name:
            params["name"] = name
        if website:
            params["website"] = website
        if profile:
            params["profile"] = profile
        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/v5/company/enrich",
            params=params,
            headers=self._headers(),
        )

    async def search_people(
        self,
        city: str,
        state: str | None = None,
        job_title_keywords: list[str] | None = None,
        company_name: str | None = None,
        size: int = 20,
    ) -> dict[str, Any]:
        """
        Search people by location and role keywords.
        Uses PDL SQL API for structured search.
        """
        self._check_key()
        conditions = [f"location_locality = '{city}'"]
        if state:
            conditions.append(f"location_region = '{state}'")
        if job_title_keywords:
            title_conditions = " OR ".join(
                f"job_title LIKE '%{kw}%'" for kw in job_title_keywords
            )
            conditions.append(f"({title_conditions})")
        if company_name:
            conditions.append(f"job_company_name = '{company_name}'")

        sql = "SELECT * FROM person WHERE " + " AND ".join(conditions)
        payload = {"sql": sql, "size": size}
        return await self._rl.post(
            DOMAIN,
            f"{BASE_URL}/v5/person/search",
            json_body=payload,
            headers=self._headers(),
        )
