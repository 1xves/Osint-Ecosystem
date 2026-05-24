"""
osint/clients/sba.py

SBA (Small Business Administration) 7(a) Loan Data client.

The SBA publicly discloses its 7(a) and 504 loan programs under FOIA. This data
reveals government-backed small business lending at the city level — an important
signal in startup ecosystem mapping:

    - Which banks are active SBA lenders in the city (key financial relationships)
    - Which businesses received government-backed capital (ecosystem participants)
    - Loan amounts and industries (market sector intelligence)
    - Underserved lending: loans to minority-owned, women-owned, veteran-owned businesses

Data source: SBA FOIA loan data via the SBA's official API and FOIA bulk downloads.
API: https://data.sba.gov/api/3/
Documentation: https://data.sba.gov/
Authentication: No key required. Public API (CKAN-based).

Key datasets:
    7a_fy2023_asof231231.csv    — recent fiscal year 7(a) approvals
    504_fy2023.csv              — 504 loan approvals
    These are bulk CSV datasets; we query via CKAN datastore API.

CKAN Datastore endpoint:
    https://data.sba.gov/api/3/action/datastore_search

Rate limits:
    Generous — public CKAN instance. ~60 req/min safe. We use 60/min.

Notes:
    - This is fiscal year data, not real-time. Latest FY data is usually available
      within 90 days of FY end (Sept 30 each year).
    - Resource IDs change with each fiscal year — use the catalog API to discover
      the latest resource IDs dynamically.
    - City matching uses `BorrCity` field — must case-insensitively match.
    - Loan amounts are in `GrossApproval` field (integer, dollars).
"""

from __future__ import annotations

import logging
from typing import Any

from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

DOMAIN = "sba"
CKAN_BASE_URL = "https://data.sba.gov/api/3/action"

# Known 7(a) loan data resource IDs by fiscal year
# These IDs are stable for a given year once published.
# Add new FY resource IDs here as SBA publishes them.
SBA_7A_RESOURCE_IDS: dict[str, str] = {
    "2023": "aab41f78-af89-4ffa-aef8-8d8f0d5b34db",
    "2022": "2d8b9804-8a44-4b04-8698-5c52a0c78024",
    "2021": "4c2b8c2e-24c3-42e8-b3e1-c8c9b9283b73",
}

SBA_504_RESOURCE_IDS: dict[str, str] = {
    "2023": "c8e3f4c0-22e0-4867-a9f5-b9bb35f40c6e",
}

# SBA 7(a) fields of interest
LOAN_FIELDS_7A = [
    "BorrName", "BorrCity", "BorrState", "BorrZip",
    "LenderName", "LenderCity", "LenderState",
    "GrossApproval", "SBAGuaranteedApproval",
    "ApprovalDate", "InitialInterestRate",
    "NaicsCode", "NaicsDescription",
    "BusinessType", "BorrRace", "BorrGender",
    "LoanStatus", "JobsSupported", "ProjectCounty",
    "FranchiseName", "Subprogram",
    "LMIIndicator", "BusinessAge", "HubzoneIndicator",
    "VeteranIndicator", "ProjectState",
]


class SBAClient:
    """
    Async client for SBA public loan data via CKAN Datastore API.

    All queries return raw CKAN response dicts. Callers parse 'records'.
    """

    def __init__(self, rate_limiter: RateLimiter) -> None:
        self._rl = rate_limiter

    # ─────────────────────────────────────────────────────────────────────────
    # 7(a) loan search by city
    # ─────────────────────────────────────────────────────────────────────────

    async def search_7a_loans(
        self,
        city: str,
        state: str | None = None,
        fiscal_year: str = "2023",
        limit: int = 100,
        offset: int = 0,
        min_amount_usd: int | None = None,
    ) -> dict[str, Any]:
        """
        Search SBA 7(a) loan approvals for a city.

        Args:
            city:           City name (case-insensitive match on BorrCity).
            state:          Two-letter state code (optional filter on BorrState).
            fiscal_year:    Fiscal year string ('2023', '2022', etc.).
            limit:          Records per page (default 100, max 32000).
            offset:         Pagination offset.
            min_amount_usd: Optional minimum loan amount filter.

        Returns:
            CKAN response dict with 'result.records' list and 'result.total'.
        """
        resource_id = SBA_7A_RESOURCE_IDS.get(fiscal_year)
        if not resource_id:
            log.warning(
                "sba_client: no resource_id for fiscal year '%s' — "
                "known years: %s", fiscal_year, list(SBA_7A_RESOURCE_IDS.keys())
            )
            return {"result": {"records": [], "total": 0}}

        # CKAN filter: exact match (case-insensitive handled by CKAN)
        filters: dict[str, str] = {"BorrCity": city.upper()}
        if state:
            filters["BorrState"] = state.upper()

        params: dict[str, Any] = {
            "resource_id": resource_id,
            "filters": str(filters).replace("'", '"'),
            "limit": min(limit, 32000),
            "offset": offset,
            "fields": ",".join(LOAN_FIELDS_7A),
        }

        return await self._rl.get(
            DOMAIN,
            f"{CKAN_BASE_URL}/datastore_search",
            params=params,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # 7(a) search by lender
    # ─────────────────────────────────────────────────────────────────────────

    async def search_7a_by_lender(
        self,
        lender_name: str,
        city: str | None = None,
        state: str | None = None,
        fiscal_year: str = "2023",
        limit: int = 50,
    ) -> dict[str, Any]:
        """
        Search 7(a) loans made by a specific lender (bank/credit union).
        Useful for mapping which financial institutions are active SBA lenders
        in the city — these are key financial ecosystem relationships.
        """
        resource_id = SBA_7A_RESOURCE_IDS.get(fiscal_year)
        if not resource_id:
            return {"result": {"records": [], "total": 0}}

        filters: dict[str, str] = {}
        if city:
            filters["BorrCity"] = city.upper()
        if state:
            filters["BorrState"] = state.upper()

        params: dict[str, Any] = {
            "resource_id": resource_id,
            "q": lender_name,                    # Full-text search on LenderName
            "limit": min(limit, 32000),
            "fields": ",".join(LOAN_FIELDS_7A),
        }
        if filters:
            params["filters"] = str(filters).replace("'", '"')

        return await self._rl.get(
            DOMAIN,
            f"{CKAN_BASE_URL}/datastore_search",
            params=params,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Full-text search (borrower name)
    # ─────────────────────────────────────────────────────────────────────────

    async def search_borrower(
        self,
        borrower_name: str,
        state: str | None = None,
        fiscal_year: str = "2023",
        limit: int = 25,
    ) -> dict[str, Any]:
        """
        Search for a specific borrower by name. Useful for verifying
        whether a known company received SBA-backed funding.
        """
        resource_id = SBA_7A_RESOURCE_IDS.get(fiscal_year)
        if not resource_id:
            return {"result": {"records": [], "total": 0}}

        params: dict[str, Any] = {
            "resource_id": resource_id,
            "q": borrower_name,
            "limit": min(limit, 32000),
            "fields": ",".join(LOAN_FIELDS_7A),
        }
        if state:
            params["filters"] = f'{{"BorrState": "{state.upper()}"}}'

        return await self._rl.get(
            DOMAIN,
            f"{CKAN_BASE_URL}/datastore_search",
            params=params,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Dataset catalog: discover resource IDs
    # ─────────────────────────────────────────────────────────────────────────

    async def get_loan_datasets(self) -> dict[str, Any]:
        """
        Query the CKAN catalog to discover current SBA loan dataset resource IDs.
        Call this if known IDs return 404s — SBA may have published new FY data.
        """
        return await self._rl.get(
            DOMAIN,
            f"{CKAN_BASE_URL}/package_search",
            params={"q": "SBA 7a loans", "rows": 10},
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Static helpers
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def extract_records(response: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract loan records list from a CKAN datastore response."""
        records = response.get("result", {}).get("records", [])
        if not isinstance(records, list):
            return []
        return records

    @staticmethod
    def total_count(response: dict[str, Any]) -> int:
        """Return total record count from a CKAN datastore response."""
        return int(response.get("result", {}).get("total", 0))

    @staticmethod
    def loan_amount(record: dict[str, Any]) -> int:
        """Extract the GrossApproval amount as int from a loan record."""
        try:
            return int(record.get("GrossApproval", 0) or 0)
        except (ValueError, TypeError):
            return 0
