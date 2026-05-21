"""
osint/clients/usaspending.py

USASpending.gov API client.

Endpoints used:
- POST /api/v2/search/spending_by_award/  — search contracts and grants
- POST /api/v2/recipient/            — search recipient organizations
- GET  /api/v2/award/{id}/           — award detail

Rate limits: 60 req/min (configured in RATE_LIMITS["usaspending"])
Auth: None required.

Docs: https://api.usaspending.gov/
"""

from __future__ import annotations

import logging
from typing import Any

from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

BASE_URL = "https://api.usaspending.gov"
DOMAIN   = "usaspending"


class USASpendingClient:
    def __init__(self, rate_limiter: RateLimiter) -> None:
        self._rl = rate_limiter

    async def search_awards(
        self,
        recipient_name: str | None = None,
        award_types: list[str] | None = None,  # ["A","B","C","D"] contracts, ["02","03","04","05"] grants
        recipient_state: str | None = None,
        recipient_city: str | None = None,
        date_range: tuple[str, str] | None = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        """
        Search federal contracts and grants.

        award_types:
          Contracts: ["A","B","C","D"] (IDV types)
          Grants: ["02","03","04","05"] (grant types)
          Leave None to search both.
        """
        filters: dict[str, Any] = {}
        if recipient_name:
            filters["recipient_search_text"] = [recipient_name]
        if award_types:
            filters["award_type_codes"] = award_types
        if recipient_state:
            filters["recipient_locations"] = [
                {"country": "USA", "state": recipient_state, "city": recipient_city}
                if recipient_city else {"country": "USA", "state": recipient_state}
            ]
        if date_range:
            filters["time_period"] = [{"start_date": date_range[0], "end_date": date_range[1]}]

        payload = {
            "filters": filters,
            "fields": [
                "Award ID", "Recipient Name", "Start Date", "End Date",
                "Award Amount", "Awarding Agency", "Awarding Sub Agency",
                "Award Type", "recipient_id", "Place of Performance State Code",
                "Place of Performance City Name", "Description",
            ],
            "limit": limit,
            "sort": "Award Amount",
            "order": "desc",
        }
        return await self._rl.post(
            DOMAIN,
            f"{BASE_URL}/api/v2/search/spending_by_award/",
            json_body=payload,
        )

    async def search_contracts(
        self,
        recipient_name: str,
        recipient_state: str | None = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        """Convenience: search federal contracts for a named recipient."""
        return await self.search_awards(
            recipient_name=recipient_name,
            award_types=["A", "B", "C", "D"],
            recipient_state=recipient_state,
            limit=limit,
        )

    async def search_grants(
        self,
        recipient_name: str,
        recipient_state: str | None = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        """Convenience: search federal grants for a named recipient."""
        return await self.search_awards(
            recipient_name=recipient_name,
            award_types=["02", "03", "04", "05"],
            recipient_state=recipient_state,
            limit=limit,
        )

    async def get_award_detail(self, award_id: str) -> dict[str, Any]:
        """Fetch full details for a specific award."""
        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/api/v2/awards/{award_id}/",
        )

    async def get_recipient_profile(self, recipient_id: str) -> dict[str, Any]:
        """Fetch a recipient organization profile."""
        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/api/v2/recipient/{recipient_id}/",
        )
