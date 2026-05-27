"""
osint/clients/crunchbase.py

Crunchbase API v4 client.

Endpoints used:
- POST /searches/organizations  — search for companies by city
- POST /searches/people         — search for people by city
- GET  /entities/organizations/{permalink}  — full org profile
- GET  /entities/people/{permalink}         — full person profile

Rate limits: 10 req/min, 1000/day (configured in RATE_LIMITS["crunchbase"])
Auth: API key as query param `user_key`

Docs: https://data.crunchbase.com/docs/crunchbase-basic-using-the-api
"""

from __future__ import annotations

import logging
from typing import Any

from osint.core.config import settings
from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

BASE_URL = "https://api.crunchbase.com/api/v4"
DOMAIN = "crunchbase"


class CrunchbaseClient:
    def __init__(self, rate_limiter: RateLimiter) -> None:
        self._rl = rate_limiter

    def _headers(self) -> dict[str, str]:
        return {"X-cb-user-key": settings.crunchbase_api_key}

    def _check_key(self) -> None:
        if not settings.crunchbase_api_key:
            raise ValueError(
                "CRUNCHBASE_API_KEY is not set. Add it to .env. "
                "Get a key at https://www.crunchbase.com/api"
            )

    async def search_organizations(
        self,
        city: str,
        country: str = "United States",
        categories: list[str] | None = None,
        funding_stage: list[str] | None = None,
        limit: int = 25,
    ) -> dict[str, Any]:
        """
        Search for organizations in a city.
        Returns raw Crunchbase response dict.
        """
        self._check_key()
        predicate_values = [
            {"field_id": "location_identifiers", "operator_id": "includes", "values": [city]},
            {"field_id": "location_identifiers", "operator_id": "includes", "values": [country]},
        ]
        if categories:
            predicate_values.append({
                "field_id": "category_groups", "operator_id": "includes", "values": categories
            })
        if funding_stage:
            predicate_values.append({
                "field_id": "funding_stage", "operator_id": "includes", "values": funding_stage
            })

        payload = {
            "field_ids": [
                "identifier", "short_description", "location_identifiers",
                "primary_job_title", "primary_organization", "founded_on",
                "funding_total", "last_funding_type", "num_employees_enum",
                "rank_org", "website_url", "linkedin", "twitter",
                "crunchbase_id", "categories", "category_groups",
                # Relationship fields — Basic tier may return empty; fail-safe in corporate.py
                "founder_identifiers",  # → corporate.category_fields["founder_names"]
                "investors",            # → corporate.category_fields["investors_list"]
            ],
            "predicate_values": predicate_values,
            "limit": limit,
        }
        return await self._rl.post(
            DOMAIN,
            f"{BASE_URL}/searches/organizations",
            json_body=payload,
            headers=self._headers(),
        )

    async def search_people(
        self,
        city: str,
        country: str = "United States",
        limit: int = 25,
    ) -> dict[str, Any]:
        """Search for people (investors, executives) in a city."""
        self._check_key()
        payload = {
            "field_ids": [
                "identifier", "first_name", "last_name", "primary_job_title",
                "primary_organization", "location_identifiers",
                "linkedin", "twitter", "website", "rank_person",
            ],
            "predicate_values": [
                {"field_id": "location_identifiers", "operator_id": "includes", "values": [city]},
            ],
            "limit": limit,
        }
        return await self._rl.post(
            DOMAIN,
            f"{BASE_URL}/searches/people",
            json_body=payload,
            headers=self._headers(),
        )

    async def get_organization(self, permalink: str) -> dict[str, Any]:
        """Fetch a full organization profile by Crunchbase permalink."""
        self._check_key()
        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/entities/organizations/{permalink}",
            params={
                "user_key": settings.crunchbase_api_key,
                "field_ids": "identifier,short_description,long_description,founded_on,"
                             "location_identifiers,website_url,linkedin,twitter,"
                             "funding_total,last_funding_type,num_employees_enum,"
                             "categories,category_groups,board_members_and_advisors,"
                             "current_employees_featured_order_field,parent_org_identifier",
            },
        )

    async def get_person(self, permalink: str) -> dict[str, Any]:
        """Fetch a full person profile by Crunchbase permalink."""
        self._check_key()
        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/entities/people/{permalink}",
            params={
                "user_key": settings.crunchbase_api_key,
                "field_ids": "identifier,first_name,last_name,primary_job_title,"
                             "primary_organization,location_identifiers,"
                             "linkedin,twitter,website,investments,founded_companies,"
                             "board_of_directors_jobs,advisory_jobs",
            },
        )
