"""
osint/clients/edgar.py

SEC EDGAR EFTS (full-text search) and EDGAR data API client.

Endpoints used:
- GET https://efts.sec.gov/LATEST/search-index?q=...  — full-text search
- GET https://data.sec.gov/submissions/{cik}.json      — company submissions
- GET https://data.sec.gov/api/xbrl/companyfacts/{cik}.json  — financial facts

Phase 7 extensions:
- get_proxy_executive_compensation(cik, company_name, year, extractor)
    → fetches DEF 14A filing index → downloads HTML/PDF → DocumentExtractor
    → returns structured exec compensation dict
- get_annual_report_officers(cik, company_name, year, extractor)
    → fetches 10-K filing index → downloads HTML → DocumentExtractor
    → returns structured officer/director list

Both methods accept a DocumentExtractor instance injected by the caller,
keeping EdgarClient stateless and dependency-free from the LLM layer.

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

    async def search_filings(
        self,
        query: str,
        form_type: str | None = None,
        hits_size: int = 20,
    ) -> dict[str, Any]:
        """
        Search EDGAR filings by keyword and optional form type.
        Thin wrapper around search() for agent convenience.

        Args:
            query: Search terms (name, city, keyword).
            form_type: SEC form type, e.g. "4", "10-K", "DEF 14A".
            hits_size: Number of results to return.

        Returns:
            Raw EDGAR EFTS response with hits array.
        """
        forms = [form_type] if form_type else None
        return await self.search(query=query, forms=forms, hits_size=hits_size)

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

    # ─────────────────────────────────────────────────────────────────────────
    # Phase 7 — Document extraction methods
    # Each method: find filing → get index → download document → extract via LLM
    # ─────────────────────────────────────────────────────────────────────────

    async def get_proxy_executive_compensation(
        self,
        cik: str,
        company_name: str,
        year: int | None = None,
        extractor: Any = None,
    ) -> dict[str, Any]:
        """
        Fetch a DEF 14A proxy statement and extract executive compensation.

        Steps:
          1. Search EDGAR for the most recent DEF 14A filing for this CIK.
          2. Fetch the filing index page (JSON) to find the primary document URL.
          3. Download the filing document (HTML or PDF) via DocumentFetcher.
          4. Pass text to DocumentExtractor for structured compensation extraction.

        Args:
            cik:          SEC CIK number (zero-padded or raw).
            company_name: Company name — used as entity context for extraction.
            year:         Preferred fiscal year. Selects most recent if None.
            extractor:    DocumentExtractor instance (injected by caller).

        Returns:
            Dict from DocumentExtractor.extract_proxy():
            {
                "executives": [{name, title, base_salary, bonus, total_compensation, fiscal_year}],
                "directors":  [{name, title, committee_memberships}],
                "filing_company": str,
                "filing_year": int | None,
            }
            Empty dict if filing not found or extraction fails.
        """
        if extractor is None:
            log.warning("edgar: get_proxy_executive_compensation called without extractor")
            return {}

        from osint.utils.document_fetcher import DocumentFetcher
        fetcher = DocumentFetcher()

        # Step 1: Find DEF 14A filings for this CIK
        filing_index_url = await self._find_filing_index_url(
            cik=cik,
            form_type="DEF 14A",
            preferred_year=year,
        )
        if not filing_index_url:
            log.debug("edgar: no DEF 14A found for CIK '%s'", cik)
            return {}

        # Step 2: Get primary document URL from index
        doc_url = await self._get_primary_doc_url(filing_index_url, fetcher)
        if not doc_url:
            log.debug("edgar: could not locate primary document in DEF 14A index for CIK '%s'", cik)
            return {}

        # Step 3: Download document
        text = await self._fetch_document_text(doc_url, fetcher)
        if not text:
            log.debug("edgar: empty document text for DEF 14A '%s'", doc_url)
            return {}

        log.info(
            "edgar: extracted %d chars from DEF 14A for '%s' (CIK %s)",
            len(text), company_name, cik,
        )

        # Step 4: LLM extraction
        result = await extractor.extract_proxy(
            text=text,
            company_name=company_name,
            cik=cik,
            filing_year=year,
        )
        return result

    async def get_annual_report_officers(
        self,
        cik: str,
        company_name: str,
        year: int | None = None,
        extractor: Any = None,
    ) -> dict[str, Any]:
        """
        Fetch a 10-K annual report and extract the officer/director list.

        Steps:
          1. Search EDGAR for the most recent 10-K for this CIK.
          2. Fetch filing index to get primary document URL.
          3. Download HTML via DocumentFetcher, targeting the directors section.
          4. Extract via DocumentExtractor.

        Args:
            cik:          SEC CIK number.
            company_name: Company name for entity context.
            year:         Preferred fiscal year. Uses most recent if None.
            extractor:    DocumentExtractor instance.

        Returns:
            Dict from DocumentExtractor.extract_annual_report():
            {
                "officers":  [{name, age, title, bio_summary, tenure_start_year}],
                "directors": [{name, age, title, independence, bio_summary}],
                "fiscal_year": int | None,
                "filing_company": str,
            }
            Empty dict if not found or extraction fails.
        """
        if extractor is None:
            log.warning("edgar: get_annual_report_officers called without extractor")
            return {}

        from osint.utils.document_fetcher import DocumentFetcher
        fetcher = DocumentFetcher()

        # Step 1: Find 10-K filing index
        filing_index_url = await self._find_filing_index_url(
            cik=cik,
            form_type="10-K",
            preferred_year=year,
        )
        if not filing_index_url:
            log.debug("edgar: no 10-K found for CIK '%s'", cik)
            return {}

        # Step 2: Get primary document URL
        doc_url = await self._get_primary_doc_url(filing_index_url, fetcher)
        if not doc_url:
            log.debug("edgar: could not locate primary document in 10-K index for CIK '%s'", cik)
            return {}

        # Step 3: Download document — target the directors/officers section
        # 10-Ks are large; use CSS selector to focus on the relevant part
        text = await self._fetch_document_text(
            doc_url,
            fetcher,
            # These selectors match common 10-K HTML structures for the officers section
            selector=(
                "#item10, #ITEM10, [id*='item10'], [id*='ITEM10'], "
                "[id*='directors'], [id*='executive-officer'], "
                ".item10, section.directors"
            ),
        )
        if not text:
            # Fall back to full document if section selector failed
            text = await self._fetch_document_text(doc_url, fetcher)

        if not text:
            log.debug("edgar: empty document text for 10-K '%s'", doc_url)
            return {}

        log.info(
            "edgar: extracted %d chars from 10-K for '%s' (CIK %s)",
            len(text), company_name, cik,
        )

        result = await extractor.extract_annual_report(
            text=text,
            company_name=company_name,
            fiscal_year=year,
        )
        return result

    # ─────────────────────────────────────────────────────────────────────────
    # Private helpers for Phase 7 document methods
    # ─────────────────────────────────────────────────────────────────────────

    async def _find_filing_index_url(
        self,
        cik: str,
        form_type: str,
        preferred_year: int | None = None,
    ) -> str | None:
        """
        Find the EDGAR filing index URL for the most recent filing of a given
        form type for a CIK. Uses the submissions JSON endpoint.

        Returns:
            URL string of the filing index page, or None if not found.
        """
        # Pad CIK to 10 digits — submissions endpoint requires this
        padded_cik = str(cik).lstrip("0").zfill(10)
        submissions_url = f"{DATA_BASE}/submissions/CIK{padded_cik}.json"

        try:
            submissions = await self._rl.get(
                DOMAIN,
                submissions_url,
                extra_headers=self._headers(),
            )
        except Exception as exc:
            log.debug("edgar: submissions fetch failed for CIK '%s': %s", cik, exc)
            return None

        # Navigate to recent filings
        filings = submissions.get("filings", {}).get("recent", {})
        form_types  = filings.get("form", [])
        accessions  = filings.get("accessionNumber", [])
        filing_dates = filings.get("filingDate", [])

        if not form_types or not accessions:
            return None

        # Find indices matching the form type, ordered newest-first (EDGAR default)
        matches = [
            i for i, ft in enumerate(form_types)
            if ft.strip().upper() == form_type.upper()
        ]
        if not matches:
            return None

        # If preferred year given, try to match; otherwise take first (most recent)
        chosen_idx = matches[0]
        if preferred_year:
            for i in matches:
                date_str = filing_dates[i] if i < len(filing_dates) else ""
                if date_str.startswith(str(preferred_year)):
                    chosen_idx = i
                    break

        accession = accessions[chosen_idx]
        clean_accession = accession.replace("-", "")
        clean_cik_num   = str(cik).lstrip("0")

        index_url = (
            f"https://www.sec.gov/Archives/edgar/data/"
            f"{clean_cik_num}/{clean_accession}/{accession}-index.htm"
        )
        return index_url

    async def _get_primary_doc_url(
        self,
        index_url: str,
        fetcher: Any,
    ) -> str | None:
        """
        Parse the EDGAR filing index page to find the primary document URL.

        The index page has a table of filing documents. The primary document
        is usually type "10-K", "DEF 14A", or similar — the first non-index
        document in the table.

        Returns:
            Full URL to the primary document, or None.
        """
        try:
            html = await fetcher.fetch_html_text(index_url, strict=False)
        except Exception as exc:
            log.debug("edgar: index page fetch failed for '%s': %s", index_url, exc)
            return None

        if not html:
            return None

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")

        # EDGAR index table: column headers include "Document", "Type", "Description"
        # Primary document is usually the first .htm or .html link in the table
        table = soup.find("table", {"class": "tableFile"}) or soup.find("table")
        if table is None:
            return None

        base = "https://www.sec.gov"
        for row in table.find_all("tr")[1:]:  # skip header
            cells = row.find_all("td")
            if len(cells) < 3:
                continue
            # Column 3 (index 2) = Document type; Column 2 (index 1) = Document link
            doc_type = cells[3].get_text(strip=True) if len(cells) > 3 else ""
            link_cell = cells[2] if len(cells) > 2 else cells[1]
            link = link_cell.find("a")
            if link and link.get("href"):
                href = link["href"]
                # Skip index pages themselves
                if "index" in href.lower():
                    continue
                if href.startswith("/"):
                    return base + href
                if href.startswith("http"):
                    return href

        return None

    async def _fetch_document_text(
        self,
        url: str,
        fetcher: Any,
        selector: str | None = None,
    ) -> str:
        """
        Download and extract text from a filing document (HTML or PDF).

        Tries HTML first; falls back to PDF extraction if URL ends in .pdf.
        """
        url_lower = url.lower()
        try:
            if url_lower.endswith(".pdf"):
                return await fetcher.fetch_pdf_text(url, strict=False)
            else:
                return await fetcher.fetch_html_text(url, selector=selector, strict=False)
        except Exception as exc:
            log.debug("edgar: document fetch failed for '%s': %s", url, exc)
            return ""
