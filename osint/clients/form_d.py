"""
osint/clients/form_d.py

SEC Form D (Regulation D offering disclosure) client.

Form D is a mandatory SEC filing for every private company that raises capital
under Regulation D exemptions. It is the single best free source of:
  - Private company existence and city/state
  - Executive officer and director names ("Related Persons" section)
  - Offering type and approximate raise amount
  - Year of incorporation

Filing volume: ~20,000–25,000 Form D filings/quarter nationally.

Architecture:
  Two-phase approach:
    Phase 1 (EFTS search)  — rate-limited via RateLimiter.get(), returns JSON
    Phase 2 (XML fetch)    — direct httpx call with EDGAR User-Agent; XML is not JSON
                             so it cannot go through RateLimiter.get()

  A per-instance asyncio.Semaphore keeps Phase 2 concurrency within EDGAR's
  10 req/sec policy. Callers share the instance, so the semaphore is effective
  across concurrent agent calls.

Endpoints used:
    EFTS search:   GET https://efts.sec.gov/LATEST/search-index
    Filing XML:    GET https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{filename}

Rate limits:
    EFTS search    → domain "sec_edgar" (10 req/sec, shared with EdgarClient)
    XML fetch      → asyncio.Semaphore(8) + 0.12s inter-request sleep

Auth: None required. Must set User-Agent per EDGAR fair-access policy.

Docs:
    https://www.sec.gov/info/edgar/forms/formindex.htm
    Form D XML schema: https://www.sec.gov/info/edgar/formd.xsd
    EDGAR developer: https://www.sec.gov/developer
"""

from __future__ import annotations

import asyncio
import logging
import xml.etree.ElementTree as ET
from typing import Any

import httpx

from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

EFTS_BASE    = "https://efts.sec.gov/LATEST"
ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data"
DOMAIN       = "sec_edgar"          # shared rate-limit bucket with EdgarClient

# EDGAR fair-access policy requires this header
USER_AGENT   = "OSINT-System research@osint-system.local"

# Form D XML namespace — EDGAR uses this for all Form D XML documents
FORMD_NS = "http://www.sec.gov/edgar/document/formd"

# Semaphore: caps concurrent XML fetches at 8 to stay under EDGAR's 10 req/sec
# Multiple await points between fetches keep actual rate lower
_XML_FETCH_SEMAPHORE = asyncio.Semaphore(8)


class FormDClient:
    """
    Client for SEC Form D (Reg D offering disclosure) filings.

    Provides:
      - search_city_filings()      — find Form D filers in a city
      - get_recent_filings()       — latest Form D filings for a CIK
      - fetch_filing_xml()         — download and return raw Form D XML
      - parse_form_d_xml()         — parse XML → structured dict
      - extract_company_entity()   — pull company fields from parsed Form D
      - extract_related_persons()  — pull person list from parsed Form D

    Usage pattern:
        client = FormDClient(rate_limiter)
        hits = await client.search_city_filings("Philadelphia", "PA", lookback_days=365)
        for hit in hits:
            xml = await client.fetch_filing_xml(hit["accession_no"], hit["cik"])
            if xml:
                parsed = FormDClient.parse_form_d_xml(xml)
                company = FormDClient.extract_company_entity(parsed)
                persons = FormDClient.extract_related_persons(parsed)
    """

    def __init__(self, rate_limiter: RateLimiter) -> None:
        self._rl = rate_limiter

    # ─────────────────────────────────────────────────────────────────────────
    # Phase 1: EFTS search — find Form D filings for a city
    # ─────────────────────────────────────────────────────────────────────────

    async def search_city_filings(
        self,
        city_name: str,
        state_abbr: str | None = None,
        lookback_days: int = 730,
        hits_size: int = 40,
        hits_from: int = 0,
    ) -> list[dict[str, Any]]:
        """
        Search EDGAR EFTS for Form D filings mentioning a city name.

        EDGAR full-text search indexes Form D XML body text, so issuer addresses
        (including city names) are searchable. A search for "Philadelphia" returns
        Form D filings where the issuer's city is Philadelphia.

        Args:
            city_name:      City to search (e.g., "Philadelphia").
            state_abbr:     Optional two-letter state code to add to query for
                            disambiguation (e.g., "PA"). Reduces false positives
                            for common city names (Springfield, Franklin, etc.).
            lookback_days:  Only return filings from the past N days.
                            Default 730 = 2 years of Reg D activity.
            hits_size:      Max results per page (EFTS max: 40).
            hits_from:      Pagination offset.

        Returns:
            List of filing metadata dicts, each with:
              cik           — SEC CIK (zero-padded 10 digits)
              accession_no  — SEC accession number (with dashes)
              company_name  — Filing entity name from EFTS
              file_date     — Filing date (YYYY-MM-DD string)
              form_type     — "D" or "D/A" (amendment)
        """
        from datetime import datetime, timezone, timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        today  = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Build query: city name + optional state abbreviation
        # Quoted phrase reduces spurious hits for common city names
        query = f'"{city_name}"'
        if state_abbr:
            query = f'"{city_name}" "{state_abbr}"'

        params: dict[str, Any] = {
            "q":          query,
            "forms":      "D,D/A",
            "dateRange":  "custom",
            "startdt":    cutoff,
            "enddt":      today,
            "from":       hits_from,
            "hits.hits.total.value": hits_size,
        }

        try:
            response = await self._rl.get(
                DOMAIN,
                f"{EFTS_BASE}/search-index",
                params=params,
                headers={"User-Agent": USER_AGENT},
            )
        except Exception as e:
            log.warning("FormDClient.search_city_filings failed for %s: %s", city_name, e)
            return []

        hits = response.get("hits", {}).get("hits", [])
        results: list[dict[str, Any]] = []

        for hit in hits:
            src = hit.get("_source", {})
            cik_raw = src.get("entity_id", "")
            accession_no = src.get("accession_no", "")
            if not cik_raw or not accession_no:
                continue

            # EFTS stores CIK without leading zeros; zero-pad to 10 digits
            cik = str(cik_raw).zfill(10)
            display_names = src.get("display_names", [])
            company_name  = display_names[0] if display_names else ""

            results.append({
                "cik":          cik,
                "accession_no": accession_no,
                "company_name": company_name,
                "file_date":    src.get("file_date", ""),
                "form_type":    src.get("form_type", "D"),
            })

        log.info(
            "FormDClient: EFTS search for '%s' returned %d filings (lookback %dd)",
            city_name, len(results), lookback_days,
        )
        return results

    async def get_recent_filings(
        self,
        cik: str,
        max_results: int = 5,
    ) -> list[dict[str, Any]]:
        """
        Fetch the most recent Form D filings for a specific CIK.

        Uses the EDGAR submissions API which returns structured JSON — no XML
        parsing required for the filing list. Returns filing metadata only;
        call fetch_filing_xml() for the full document.

        Args:
            cik:         SEC CIK (any format — will be zero-padded).
            max_results: Max Form D filings to return.

        Returns:
            List of filing metadata dicts (same shape as search_city_filings).
        """
        padded_cik = str(cik).zfill(10)
        try:
            submissions = await self._rl.get(
                DOMAIN,
                f"https://data.sec.gov/submissions/CIK{padded_cik}.json",
                headers={"User-Agent": USER_AGENT},
            )
        except Exception as e:
            log.warning("FormDClient.get_recent_filings failed for CIK %s: %s", cik, e)
            return []

        recent = submissions.get("filings", {}).get("recent", {})
        form_types   = recent.get("form", [])
        accessions   = recent.get("accessionNumber", [])
        filing_dates = recent.get("filingDate", [])

        results: list[dict[str, Any]] = []
        company_name = submissions.get("name", "")

        for i, ftype in enumerate(form_types):
            if ftype.upper() not in ("D", "D/A"):
                continue
            if i >= len(accessions):
                break

            results.append({
                "cik":          padded_cik,
                "accession_no": accessions[i],
                "company_name": company_name,
                "file_date":    filing_dates[i] if i < len(filing_dates) else "",
                "form_type":    ftype,
            })

            if len(results) >= max_results:
                break

        return results

    # ─────────────────────────────────────────────────────────────────────────
    # Phase 2: XML fetch — download the Form D XML document
    # ─────────────────────────────────────────────────────────────────────────

    async def fetch_filing_xml(
        self,
        accession_no: str,
        cik: str,
        timeout: float = 20.0,
    ) -> str | None:
        """
        Download the primary Form D XML document for a filing.

        Cannot use RateLimiter.get() — it calls resp.json() and would fail on XML.
        Uses httpx directly with the EDGAR User-Agent and a semaphore to cap
        concurrency at 8 simultaneous requests.

        Args:
            accession_no:  SEC accession number with dashes, e.g.
                           "0001234567-24-000001"
            cik:           SEC CIK (any format — will be cleaned).
            timeout:       Request timeout in seconds.

        Returns:
            Raw XML string, or None on any error.

        Notes on Form D XML filename:
            SEC Form D filings consistently use "primary_doc.xml" as the
            primary document filename. The submissions API confirms this in
            the `primaryDocument` field. Older (pre-2009) filings used a
            different schema but EFTS only indexes current filings.
        """
        clean_cik      = str(int(cik))                      # strip leading zeros for URL
        clean_accession = accession_no.replace("-", "")     # "0001234567-24-000001" → "0001234567240000001"
        url = f"{ARCHIVES_BASE}/{clean_cik}/{clean_accession}/primary_doc.xml"

        async with _XML_FETCH_SEMAPHORE:
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    resp = await client.get(url, headers={"User-Agent": USER_AGENT})

                if resp.status_code == 404:
                    # Try alternate filename pattern used by some filers
                    alt_url = f"{ARCHIVES_BASE}/{clean_cik}/{clean_accession}/{clean_accession}.xml"
                    async with httpx.AsyncClient(timeout=timeout) as client:
                        resp = await client.get(alt_url, headers={"User-Agent": USER_AGENT})

                if resp.status_code != 200:
                    log.debug(
                        "FormDClient: HTTP %d for CIK %s accession %s",
                        resp.status_code, cik, accession_no,
                    )
                    return None

                # Respect EDGAR's 10 req/sec policy
                await asyncio.sleep(0.12)
                return resp.text

            except (httpx.ConnectError, httpx.TimeoutException) as e:
                log.warning("FormDClient: network error fetching %s: %s", url, e)
                return None

    # ─────────────────────────────────────────────────────────────────────────
    # XML parsing — static methods, no I/O
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def parse_form_d_xml(xml_text: str) -> dict[str, Any]:
        """
        Parse SEC Form D XML into a structured dict.

        Handles both namespaced and un-namespaced Form D XML. EDGAR uses
        the namespace http://www.sec.gov/edgar/document/formd but some filings
        omit it — we try both.

        Returns:
            {
              "issuer_name":    str | None,
              "issuer_city":    str | None,
              "issuer_state":   str | None,
              "issuer_zip":     str | None,
              "issuer_phone":   str | None,
              "year_of_inc":    str | None,
              "offering_type":  str | None,   # "Equity" | "Debt" | "Option to Acquire" | ...
              "total_offering_amount": float | None,
              "date_of_first_sale":    str | None,
              "related_persons": [
                {
                  "first_name":     str,
                  "last_name":      str,
                  "relationship":   list[str],   # ["Executive Officer", "Director"]
                  "clarification":  str | None,
                }
              ],
            }

        On parse error: returns {"parse_error": str} with all other keys absent.
        """
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            log.debug("FormDClient.parse_form_d_xml: XML parse error: %s", e)
            return {"parse_error": str(e)}

        # Handle both namespaced and bare Form D XML
        ns_prefix = f"{{{FORMD_NS}}}"

        def find(element: ET.Element, tag: str) -> ET.Element | None:
            """Try namespaced tag first, then bare tag."""
            result = element.find(f"{ns_prefix}{tag}")
            if result is None:
                result = element.find(tag)
            return result

        def find_text(element: ET.Element, tag: str) -> str | None:
            """Find element and return its text, stripped."""
            el = find(element, tag)
            if el is not None and el.text:
                return el.text.strip()
            return None

        def find_all(element: ET.Element, tag: str) -> list[ET.Element]:
            """Find all matching children (both namespaced and bare)."""
            results = element.findall(f"{ns_prefix}{tag}")
            if not results:
                results = element.findall(tag)
            return results

        result: dict[str, Any] = {
            "issuer_name":           None,
            "issuer_city":           None,
            "issuer_state":          None,
            "issuer_zip":            None,
            "issuer_phone":          None,
            "year_of_inc":           None,
            "offering_type":         None,
            "total_offering_amount": None,
            "date_of_first_sale":    None,
            "related_persons":       [],
        }

        # ── Primary issuer ──────────────────────────────────────────────────
        issuer = find(root, "primaryIssuer")
        if issuer is None:
            # Some Form D/A amendments nest under a different root
            issuer = find(root, "issuerList") or root

        if issuer is not None:
            result["issuer_name"]  = find_text(issuer, "entityName")
            result["issuer_phone"] = find_text(issuer, "issuerPhoneNumber")

            addr = find(issuer, "issuerAddress")
            if addr is not None:
                result["issuer_city"]  = find_text(addr, "city")
                result["issuer_state"] = find_text(addr, "stateOrCountry")
                result["issuer_zip"]   = find_text(addr, "zipCode")

            year_el = find(issuer, "yearOfInc")
            if year_el is not None:
                result["year_of_inc"] = find_text(year_el, "value")

        # ── Offering data ───────────────────────────────────────────────────
        offering = find(root, "offeringData")
        if offering is not None:
            # Offering type from industryGroup
            industry_group = find(offering, "industryGroup")
            if industry_group is not None:
                result["offering_type"] = find_text(industry_group, "groupType")

            # Total offering amount
            offering_amounts = find(offering, "offeringSalesAmounts")
            if offering_amounts is not None:
                total_str = find_text(offering_amounts, "totalOfferingAmount")
                if total_str:
                    try:
                        result["total_offering_amount"] = float(total_str.replace(",", ""))
                    except ValueError:
                        pass

            # Date of first sale
            dates = find(offering, "salesCommissionFindersFees")  # Try alternate
            date_el = find(offering, "dateOfFirstSale")
            if date_el is not None:
                result["date_of_first_sale"] = find_text(date_el, "value") or date_el.text

        # ── Related persons ─────────────────────────────────────────────────
        related_list_el = find(root, "relatedPersonsList")
        if related_list_el is not None:
            for person_el in find_all(related_list_el, "relatedPersonInfo"):
                person: dict[str, Any] = {
                    "first_name":    None,
                    "last_name":     None,
                    "relationship":  [],
                    "clarification": None,
                }

                name_el = find(person_el, "relatedPersonName")
                if name_el is not None:
                    person["first_name"] = find_text(name_el, "firstName")
                    person["last_name"]  = find_text(name_el, "lastName")

                if not person["last_name"]:
                    continue  # Skip entries with no name (malformed)

                rel_list_el = find(person_el, "relatedPersonRelationshipList")
                if rel_list_el is not None:
                    for rel_el in find_all(rel_list_el, "relationship"):
                        if rel_el.text:
                            person["relationship"].append(rel_el.text.strip())

                person["clarification"] = find_text(person_el, "relationshipClarification")
                result["related_persons"].append(person)

        return result

    # ─────────────────────────────────────────────────────────────────────────
    # Extraction helpers — translate parsed Form D into entity/person dicts
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def extract_company_entity(
        parsed: dict[str, Any],
        cik: str | None = None,
        accession_no: str | None = None,
        file_date: str | None = None,
    ) -> dict[str, Any] | None:
        """
        Build a minimal company entity dict from a parsed Form D.

        Returns None if parsed contains a parse error or has no company name.
        The returned dict is not a full entity record — callers (agents) must
        merge these fields into their standard entity template. Keys returned:

            canonical_name, issuer_city, issuer_state, issuer_zip,
            year_of_inc, offering_type, total_offering_amount,
            date_of_first_sale, sec_cik, form_d_accession,
            source_url, file_date

        City/state values are raw Form D strings — normalize to match pipeline
        conventions (title-case city, 2-letter state abbreviation) in the agent.
        """
        if "parse_error" in parsed:
            return None

        name = parsed.get("issuer_name")
        if not name:
            return None

        clean_cik = str(int(cik)).zfill(10) if cik else None
        clean_accession = accession_no.replace("-", "") if accession_no else None
        source_url = (
            f"https://www.sec.gov/Archives/edgar/data/{int(cik) if cik else 0}/"
            f"{clean_accession}/primary_doc.xml"
            if clean_accession else
            f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=D"
            if cik else None
        )

        return {
            "canonical_name":         name,
            "issuer_city":            parsed.get("issuer_city"),
            "issuer_state":           parsed.get("issuer_state"),
            "issuer_zip":             parsed.get("issuer_zip"),
            "issuer_phone":           parsed.get("issuer_phone"),
            "year_of_inc":            parsed.get("year_of_inc"),
            "offering_type":          parsed.get("offering_type"),
            "total_offering_amount":  parsed.get("total_offering_amount"),
            "date_of_first_sale":     parsed.get("date_of_first_sale"),
            "sec_cik":                clean_cik,
            "form_d_accession":       accession_no,
            "source_url":             source_url,
            "file_date":              file_date,
        }

    @staticmethod
    def extract_related_persons(
        parsed: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """
        Extract the Related Persons list from a parsed Form D.

        Returns a list of dicts, each with:
            first_name:   str | None
            last_name:    str
            full_name:    str          — "{first} {last}".strip()
            roles:        list[str]    — ["Executive Officer", "Director", "Promoter"]
            clarification: str | None  — free-text role clarification from filer
            is_executive: bool         — True if "Executive Officer" in roles
            is_director:  bool         — True if "Director" in roles

        Returns [] if parsed contains a parse error or no persons are listed.
        """
        if "parse_error" in parsed:
            return []

        persons: list[dict[str, Any]] = []
        for p in parsed.get("related_persons", []):
            last  = (p.get("last_name") or "").strip()
            first = (p.get("first_name") or "").strip()
            if not last:
                continue

            full_name = f"{first} {last}".strip() if first else last
            roles = p.get("relationship", [])

            persons.append({
                "first_name":    first or None,
                "last_name":     last,
                "full_name":     full_name,
                "roles":         roles,
                "clarification": p.get("clarification"),
                "is_executive":  "Executive Officer" in roles,
                "is_director":   "Director" in roles,
            })

        return persons

    # ─────────────────────────────────────────────────────────────────────────
    # Convenience: full pipeline for a city in one call
    # ─────────────────────────────────────────────────────────────────────────

    async def get_city_companies_and_officers(
        self,
        city_name: str,
        state_abbr: str | None = None,
        lookback_days: int = 730,
        max_filings: int = 40,
        max_xml_fetches: int = 30,
    ) -> list[dict[str, Any]]:
        """
        High-level method: search → fetch XML → parse → return structured data.

        Searches for Form D filings for the city, fetches XML for up to
        `max_xml_fetches` of them, and returns a list of dicts each containing:
            company  — extract_company_entity() result
            persons  — extract_related_persons() result
            metadata — {cik, accession_no, company_name, file_date, form_type}

        Items where XML fetch failed or XML parse yielded no company name are
        excluded silently.

        Args:
            city_name:       City to search.
            state_abbr:      Optional state abbreviation for disambiguation.
            lookback_days:   Only include filings from past N days.
            max_filings:     Max EFTS hits to fetch (caps search result set).
            max_xml_fetches: Max XML documents to actually download and parse.
                             Caps total HTTP calls to avoid overloading EDGAR.
                             Remaining filings have metadata only (no parsed data).

        Returns:
            List of {company, persons, metadata} dicts.
        """
        filing_hits = await self.search_city_filings(
            city_name=city_name,
            state_abbr=state_abbr,
            lookback_days=lookback_days,
            hits_size=min(max_filings, 40),
        )

        if not filing_hits:
            log.info("FormDClient: no Form D filings found for %s", city_name)
            return []

        results: list[dict[str, Any]] = []

        # Fetch XML for the first max_xml_fetches filings
        xml_fetches = 0
        tasks = []
        for hit in filing_hits:
            if xml_fetches >= max_xml_fetches:
                # No XML for remaining hits — include metadata-only entry
                results.append({"company": None, "persons": [], "metadata": hit})
                continue

            tasks.append(hit)
            xml_fetches += 1

        # Fetch XML concurrently (semaphore inside fetch_filing_xml limits concurrency)
        xml_texts = await asyncio.gather(
            *(self.fetch_filing_xml(h["accession_no"], h["cik"]) for h in tasks),
            return_exceptions=True,
        )

        for hit, xml_result in zip(tasks, xml_texts):
            if isinstance(xml_result, Exception) or not xml_result:
                results.append({"company": None, "persons": [], "metadata": hit})
                continue

            parsed = self.parse_form_d_xml(xml_result)
            company = self.extract_company_entity(
                parsed,
                cik=hit["cik"],
                accession_no=hit["accession_no"],
                file_date=hit["file_date"],
            )
            persons = self.extract_related_persons(parsed)

            results.append({
                "company":  company,
                "persons":  persons,
                "metadata": hit,
            })

        # Count successes for logging
        successful = sum(1 for r in results if r["company"] is not None)
        log.info(
            "FormDClient: %d/%d filings parsed successfully for %s",
            successful, len(results), city_name,
        )
        return results
