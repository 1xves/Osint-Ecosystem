"""
osint/clients/edgar.py

SEC EDGAR EFTS (full-text search) and EDGAR data API client.

Endpoints used:
- GET https://efts.sec.gov/LATEST/search-index?q=...  — full-text search
- GET https://data.sec.gov/submissions/{cik}.json      — company submissions
- GET https://data.sec.gov/api/xbrl/companyfacts/{cik}.json  — financial facts

Rate limits: 10 req/second (EDGAR policy) — set in RATE_LIMITS["sec_edgar"]
Auth: None required. Must set User-Agent header per EDGAR fair-access policy.

EDGAR policy: https://www.sec.gov/developer
User-Agent must identify app name + contact email.
"""

from __future__ import annotations

import logging
from typing import Any

from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

EFTS_BASE   = "https://efts.sec.gov/LATEST"
DATA_BASE   = "https://data.sec.gov"
DOMAIN      = "sec_edgar"

# Required by EDGAR fair-access policy
USER_AGENT  = "OSINT-System research@osint-system.local"


class EdgarClient:
    def __init__(self, rate_limiter: RateLimiter) -> None:
        self._rl = rate_limiter

    def _headers(self) -> dict[str, str]:
        return {"User-Agent": USER_AGENT}

    async def search(
        self,
        query: str,
        category: str | None = None,  # e.g., "form-type"
        forms: list[str] | None = None,  # e.g., ["10-K", "DEF 14A"]
        date_range: tuple[str, str] | None = None,  # (start_date, end_date) as YYYY-MM-DD
        hits_from: int = 0,
        hits_size: int = 20,
    ) -> dict[str, Any]:
        """
        Full-text search of EDGAR filings using EFTS.

        Args:
            query: Search terms.
            category: Optional form category filter.
            forms: Optional list of specific form types.
            date_range: Optional (start, end) date tuple.
            hits_from: Pagination offset.
            hits_size: Number of results.

        Returns:
            Raw EDGAR search response with hits array.
        """
        params: dict[str, Any] = {
            "q": f'"{query}"',
            "dateRange": "custom" if date_range else None,
            "startdt": date_range[0] if date_range else None,
            "enddt": date_range[1] if date_range else None,
            "from": hits_from,
            "hits.hits.total.value": hits_size,
        }
        if forms:
            params["forms"] = ",".join(forms)
        # Remove None values
        params = {k: v for k, v in params.items() if v is not None}

        return await self._rl.get(
            DOMAIN,
            f"{EFTS_BASE}/search-index",
            params=params,
            headers=self._headers(),
        )

    async def get_company_submissions(self, cik: str) -> dict[str, Any]:
        """
        Fetch all filing submissions for a company by CIK.
        CIK must be zero-padded to 10 digits.

        Returns: company info + list of all filings.
        """
        padded_cik = str(cik).zfill(10)
        return await self._rl.get(
            DOMAIN,
            f"{DATA_BASE}/submissions/CIK{padded_cik}.json",
            headers=self._headers(),
        )

    async def get_company_facts(self, cik: str) -> dict[str, Any]:
        """
        Fetch structured XBRL financial facts for a company.
        Returns revenue, assets, employee count, etc. as time series.
        """
        padded_cik = str(cik).zfill(10)
        return await self._rl.get(
            DOMAIN,
            f"{DATA_BASE}/api/xbrl/companyfacts/CIK{padded_cik}.json",
            headers=self._headers(),
        )

    async def search_company_name(self, name: str, city: str | None = None) -> dict[str, Any]:
        """
        Search EDGAR for a company by name, optionally filtering by city.
        Looks in 10-K and DEF 14A filings.
        """
        query = name
        if city:
            query = f"{name} {city}"
        return await self.search(
            query=query,
            forms=["10-K", "DEF 14A", "S-1"],
            hits_size=10,
        )

    def get_filing_document_url(self, accession_number: str, cik: str, document_name: str) -> str:
        """
        Build the direct URL for a specific EDGAR filing document.

        NOTE: Filing documents are HTML/XML, not JSON. Do NOT use RateLimiter.get()
        to fetch these — it will try to parse JSON and fail.
        Use httpx directly with the EDGAR User-Agent header and respect the
        10 req/second rate limit:

            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    url,
                    headers={"User-Agent": USER_AGENT},
                )
                text = resp.text  # HTML/XML content

        accession_number format: 0001234567-24-000001 (with dashes)
        """
        clean_accession = accession_number.replace("-", "")
        clean_cik = str(cik).lstrip("0")
        return f"https://www.sec.gov/Archives/edgar/data/{clean_cik}/{clean_accession}/{document_name}"
