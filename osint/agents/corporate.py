"""
osint/agents/corporate.py

Corporate Intelligence Agent — Phase 1 collection agent.

Collects: large employers, anchor institutions, corporate accelerators, major industry
players, publicly-traded companies with local presence, corporate VC arms, and
established private companies with significant ecosystem influence.

Data sources (in priority order):
1. Crunchbase     — Company profiles, funding history, category data
2. EDGAR          — 10-K annual reports, S-1 filings for public/pre-IPO companies
3. OpenCorporates — State business registry data (jurisdiction filings)
4. SerpAPI        — Fills gaps for unlisted companies

Output:
- Appends to state["raw_entities"] — raw pre-resolution entity dicts
- Writes evidence records for every sourced field
- Writes search records for every search attempt
- Does NOT resolve or deduplicate

Entity type: "corporate"
Subtypes: large_employer | public_company | private_company | startup |
          corporate_accelerator | anchor_institution | franchise

Notes:
- Crunchbase "company" search returns startups AND large corporations — we filter
  by employee count (>100) and funding stage to identify true corporate entities
  vs. startups (which are covered by pipeline_agent)
- EDGAR 10-K search reveals public companies headquartered in the city
- OpenCorporates confirms legal registration and status — useful for due diligence
- We specifically look for corporate accelerator programs (e.g., "Wells Fargo
  Innovation Lab") as these are ecosystem participants even if the parent is national
"""

from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from osint.agents.base import BaseAgent
from osint.clients.crunchbase import CrunchbaseClient
from osint.clients.edgar import EdgarClient
from osint.clients.opencorporates import OpenCorporatesClient
from osint.clients.serpapi import SerpApiClient
from osint.core.config import settings
from osint.llm.routing import TaskType

log = logging.getLogger(__name__)

# Crunchbase categories indicating corporate entities (not pure VC/finance)
CB_CORPORATE_CATEGORIES = [
    "Technology", "Software", "Financial Services", "Healthcare",
    "Manufacturing", "Real Estate", "Retail", "Energy", "Transportation",
    "Media and Entertainment", "Telecommunications", "Biotechnology",
]

# Employee count thresholds
LARGE_EMPLOYER_THRESHOLD = 500     # >500 = large employer
MIDSIZE_EMPLOYER_THRESHOLD = 100   # >100 = midsize, ecosystem-relevant

SERPAPI_EXTRACTION_SYSTEM = """You are a data extraction agent for an OSINT pipeline.
Extract corporate entity information from search result text.
Return ONLY valid JSON. Do not include explanation or commentary.
If a field cannot be determined from the text, use null.
Never fabricate information — only extract what is explicitly stated in the source text."""

SERPAPI_EXTRACTION_PROMPT = """Extract corporate entity information from this search result.

City context: {city_name}

Search result:
---
{search_text}
---

Return a JSON object:
{{
  "name": "<company name, or null>",
  "entity_subtype": "<large_employer|public_company|private_company|corporate_accelerator|anchor_institution|null>",
  "description": "<brief factual description, or null>",
  "website_url": "<if present, or null>",
  "industry": "<primary industry sector, or null>",
  "employee_count_range": "<'1-50'|'51-200'|'201-500'|'501-1000'|'1001-5000'|'5000+' if mentioned, or null>",
  "is_public": "<true if publicly traded, false if private, null if unclear>",
  "ticker_symbol": "<stock ticker if mentioned, or null>",
  "has_accelerator_program": "<true if company runs an accelerator/innovation program, false otherwise>",
  "evidence_snippet": "<exact quote supporting this being a corporate entity in {city_name}>"
}}"""


class CorporateAgent(BaseAgent):
    """
    Phase 1 collection agent for corporate entities.
    """

    AGENT_NAME = "corporate_agent"
    AGENT_VERSION = "1.0"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._crunchbase = CrunchbaseClient(self._rl)
        self._edgar = EdgarClient(self._rl)
        self._opencorporates = OpenCorporatesClient(self._rl)
        self._serpapi = SerpApiClient(self._rl)

    async def run(self, state: dict[str, Any]) -> dict[str, Any]:
        city_name = state["city_name"]
        country_or_region = state.get("country_or_region", "United States")
        run_id = state["run_id"]
        pass_number = state.get("pass_number", 1)
        pass2_targets = state.get("pass2_targets", [])

        log.info("CorporateAgent: collecting for %s (pass %d)", city_name, pass_number)

        new_raw_entities: list[dict[str, Any]] = []

        # ── Pass 2 targeting ──────────────────────────────────────────────────
        targeted_queries: list[str] = []
        if pass_number == 2:
            for target in pass2_targets:
                if target.get("entity_type") == "corporate":
                    targeted_queries.extend(target.get("suggested_queries", []))
            targeted_queries = [q for q in targeted_queries if q]

        # ── Source 1: Crunchbase ──────────────────────────────────────────────
        cb_entities = await self._collect_from_crunchbase(
            city_name, country_or_region, run_id
        )
        new_raw_entities.extend(cb_entities)
        log.info("CorporateAgent: Crunchbase yielded %d raw entities", len(cb_entities))

        # ── Source 2: EDGAR (public company 10-K filers) ──────────────────────
        edgar_entities = await self._collect_from_edgar(city_name, run_id)
        new_raw_entities.extend(edgar_entities)
        log.info("CorporateAgent: EDGAR yielded %d raw entities", len(edgar_entities))

        # ── Source 3: OpenCorporates (state registry) ─────────────────────────
        oc_entities = await self._collect_from_opencorporates(city_name, run_id)
        new_raw_entities.extend(oc_entities)
        log.info("CorporateAgent: OpenCorporates yielded %d raw entities", len(oc_entities))

        # ── Source 4: SerpAPI ─────────────────────────────────────────────────
        serp_entities = await self._collect_from_serpapi(
            city_name, country_or_region, run_id, targeted_queries=targeted_queries
        )
        new_raw_entities.extend(serp_entities)
        log.info("CorporateAgent: SerpAPI yielded %d raw entities", len(serp_entities))

        log.info("CorporateAgent: %d total raw entities collected", len(new_raw_entities))

        patch: dict[str, Any] = {
            "raw_entities": new_raw_entities,          # delta only
            **self.agent_status_patch("success"),
            **self.token_count_patch(),
            **self.entity_count_patch(),
        }
        return patch

    # ─────────────────────────────────────────────────────────────────────────
    # Crunchbase
    # ─────────────────────────────────────────────────────────────────────────

    async def _collect_from_crunchbase(
        self,
        city_name: str,
        country_or_region: str,
        run_id: str,
    ) -> list[dict[str, Any]]:
        """
        Search Crunchbase for corporate entities — filter by employee count
        and category to avoid overlap with investor_agent and pipeline_agent.
        """
        if not settings.crunchbase_api_key:
            log.info("CorporateAgent: Crunchbase key not set — skipping")
            await self.write_search_record(
                source_searched="crunchbase",
                query_used=f"corporate organizations in {city_name}",
                result_found=False,
                entity_type="corporate",
                failure_reason="CRUNCHBASE_API_KEY not set",
            )
            return []

        search_start = time.monotonic()
        try:
            response = await self._crunchbase.search_organizations(
                city=city_name,
                country=country_or_region,
                categories=CB_CORPORATE_CATEGORIES,
                limit=25,
            )
        except Exception as e:
            log.warning("CorporateAgent: Crunchbase search failed: %s", e)
            await self.write_search_record(
                source_searched="crunchbase",
                query_used=f"corporate organizations in {city_name}",
                result_found=False,
                entity_type="corporate",
                failure_reason=str(e),
                response_time_ms=int((time.monotonic() - search_start) * 1000),
            )
            return []

        elapsed_ms = int((time.monotonic() - search_start) * 1000)
        entities_data = response.get("entities", [])

        await self.write_search_record(
            source_searched="crunchbase",
            query_used=f"corporate organizations in {city_name}",
            result_found=bool(entities_data),
            entity_type="corporate",
            result_count=len(entities_data),
            response_time_ms=elapsed_ms,
        )

        entities = []
        now_iso = datetime.now(timezone.utc).isoformat()

        for raw in entities_data:
            props = raw.get("properties", {})
            identifier = props.get("identifier", {})
            canonical_name = identifier.get("value", "")
            if not canonical_name:
                continue

            employee_count = props.get("num_employees_enum", "")
            # Skip very small companies (likely startups covered by pipeline_agent)
            if employee_count in ("1_10", "11_50") and not props.get("ipo_status") == "public":
                continue

            permalink = identifier.get("permalink", "")
            source_url = f"https://www.crunchbase.com/organization/{permalink}"

            location_ids = props.get("location_identifiers", [])
            location_city = None
            location_state = None
            for loc in location_ids:
                if loc.get("location_type") == "city":
                    location_city = loc.get("value")
                elif loc.get("location_type") == "region":
                    location_state = loc.get("value")

            ipo_status = props.get("ipo_status", "")
            subtype = self._infer_corporate_subtype(props, employee_count, ipo_status)

            category_fields: dict[str, Any] = {
                "corporate_subtype": subtype,
                "corporate_subtype_status": "REPORTED",
                "crunchbase_id": permalink,
                "employee_count_range": employee_count,
                "employee_count_range_status": "REPORTED" if employee_count else "NOT_COLLECTED",
                "ipo_status": ipo_status,
                "ipo_status_status": "REPORTED" if ipo_status else "NOT_COLLECTED",
                "total_funding": props.get("funding_total", {}).get("value_usd"),
                "total_funding_status": "REPORTED" if props.get("funding_total") else "NOT_COLLECTED",
                "founded_year": props.get("founded_on", {}).get("value", "")[:4] if props.get("founded_on") else None,
            }

            entity: dict[str, Any] = {
                "entity_id": None,
                "canonical_name": canonical_name,
                "entity_type": "corporate",
                "entity_subtype": subtype,
                "aliases": [],
                "valid_from": now_iso,
                "valid_to": None,
                "superseded_by": None,

                "primary_city": location_city or city_name,
                "primary_city_status": "REPORTED" if location_city else "NOT_COLLECTED",
                "primary_state": location_state,
                "primary_state_status": "REPORTED" if location_state else "NOT_COLLECTED",
                "primary_country": "United States",
                "primary_country_status": "REPORTED",

                "website_url": props.get("website_url"),
                "website_url_status": "REPORTED" if props.get("website_url") else "NOT_REPORTED",
                "linkedin_url": props.get("linkedin", {}).get("value") if isinstance(props.get("linkedin"), dict) else None,
                "linkedin_url_status": "REPORTED" if props.get("linkedin") else "NOT_COLLECTED",
                "twitter_handle": props.get("twitter", {}).get("value") if isinstance(props.get("twitter"), dict) else None,
                "twitter_handle_status": "REPORTED" if props.get("twitter") else "NOT_COLLECTED",

                "description": props.get("short_description"),
                "description_status": "REPORTED" if props.get("short_description") else "NOT_REPORTED",
                "description_source_url": source_url,

                "external_ids": {"crunchbase_id": permalink} if permalink else {},
                "source_agent": self.AGENT_NAME,
                "source_run_ids": [run_id],
                "merge_provenance": [],
                "source_urls": [source_url],
                "last_seen": now_iso,
                "last_verified": None,

                "overall_confidence": "medium",
                "source_count": 1,
                "corroboration_count": 0,

                "partner_candidate": False,
                "competitor_candidate": False,
                "blocker_candidate": False,
                "investment_candidate": False,
                "support_candidate": False,
                "recruiter_candidate": False,
                "top_influencer": False,

                "score_influence": 0,
                "score_startup_relevance": 0,
                "score_partner_potential": 0,
                "score_supporter_potential": 0,
                "score_competitor_potential": 0,
                "score_blocker_risk": 0,
                "score_investment_potential": 0,
                "score_support_target": 0,
                "score_recruiting_potential": 0,

                "needs_review": False,
                "sensitivity_tier": "standard",

                "category_fields": category_fields,

                "_raw_entity_id": str(uuid.uuid4()),
                "_source": "crunchbase",
                "_pending_evidence": [
                    {
                        "entity_id": None,
                        "run_id": run_id,
                        "supported_field": "canonical_name",
                        "supported_value": canonical_name,
                        "source_url": source_url,
                        "source_type": "api_response",
                        "source_api": "crunchbase",
                        "retrieved_at": now_iso,
                        "evidence_snippet": f"Crunchbase lists {canonical_name} as a company in {location_city or city_name}",
                        "claim_type": "direct_statement",
                        "confidence": "high",
                        "agent_name": self.AGENT_NAME,
                        "prompt_version": self.AGENT_VERSION,
                    }
                ],
            }
            entities.append(entity)

        return entities

    def _infer_corporate_subtype(
        self, props: dict[str, Any], employee_count: str, ipo_status: str
    ) -> str:
        """Infer corporate subtype from Crunchbase properties."""
        if ipo_status == "public":
            return "public_company"
        categories = " ".join(
            c.get("value", "").lower() for c in props.get("category_groups", [])
        )
        if "accelerator" in categories or "incubator" in categories:
            return "corporate_accelerator"
        if employee_count in ("1001_5000", "5001_10000", "10001+"):
            return "large_employer"
        return "private_company"

    # ─────────────────────────────────────────────────────────────────────────
    # EDGAR — public company 10-K filers
    # ─────────────────────────────────────────────────────────────────────────

    async def _collect_from_edgar(
        self,
        city_name: str,
        run_id: str,
    ) -> list[dict[str, Any]]:
        """
        Search EDGAR for publicly-traded companies filing 10-K annual reports.
        These are the highest-profile corporate entities.
        """
        search_start = time.monotonic()
        try:
            response = await self._edgar.search_company_name(
                name=city_name,
                city=city_name,
            )
        except Exception as e:
            log.warning("CorporateAgent: EDGAR search failed: %s", e)
            await self.write_search_record(
                source_searched="sec_edgar",
                query_used=f"10-K annual reports {city_name}",
                result_found=False,
                entity_type="corporate",
                failure_reason=str(e),
                response_time_ms=int((time.monotonic() - search_start) * 1000),
            )
            return []

        elapsed_ms = int((time.monotonic() - search_start) * 1000)
        hits = response.get("hits", {}).get("hits", [])

        await self.write_search_record(
            source_searched="sec_edgar",
            query_used=f"10-K annual report filers in {city_name}",
            result_found=bool(hits),
            entity_type="corporate",
            result_count=len(hits),
            response_time_ms=elapsed_ms,
        )

        entities = []
        now_iso = datetime.now(timezone.utc).isoformat()

        for hit in hits[:15]:
            source = hit.get("_source", {})
            entity_name = source.get("display_names", [None])[0]
            if not entity_name:
                continue

            cik = source.get("entity_id", "")
            source_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=10-K&dateb=&owner=include&count=10"

            category_fields: dict[str, Any] = {
                "corporate_subtype": "public_company",
                "corporate_subtype_status": "REPORTED",
                "sec_cik": cik,
                "sec_cik_status": "REPORTED" if cik else "NOT_COLLECTED",
                "sec_sic_code": source.get("category", ""),
                "is_sec_registrant": True,
            }

            entity: dict[str, Any] = {
                "entity_id": None,
                "canonical_name": entity_name,
                "entity_type": "corporate",
                "entity_subtype": "public_company",
                "aliases": [],
                "valid_from": now_iso,
                "valid_to": None,
                "superseded_by": None,

                "primary_city": city_name,
                "primary_city_status": "NOT_COLLECTED",
                "primary_state": None,
                "primary_state_status": "NOT_COLLECTED",
                "primary_country": "United States",
                "primary_country_status": "NOT_COLLECTED",

                "website_url": None,
                "website_url_status": "NOT_COLLECTED",
                "linkedin_url": None,
                "linkedin_url_status": "NOT_COLLECTED",
                "twitter_handle": None,
                "twitter_handle_status": "NOT_COLLECTED",

                "description": None,
                "description_status": "NOT_COLLECTED",
                "description_source_url": source_url,

                "external_ids": {"sec_cik": cik} if cik else {},
                "source_agent": self.AGENT_NAME,
                "source_run_ids": [run_id],
                "merge_provenance": [],
                "source_urls": [source_url],
                "last_seen": now_iso,
                "last_verified": None,

                "overall_confidence": "medium",
                "source_count": 1,
                "corroboration_count": 0,

                "partner_candidate": False,
                "competitor_candidate": False,
                "blocker_candidate": False,
                "investment_candidate": False,
                "support_candidate": False,
                "recruiter_candidate": False,
                "top_influencer": False,

                "score_influence": 0,
                "score_startup_relevance": 0,
                "score_partner_potential": 0,
                "score_supporter_potential": 0,
                "score_competitor_potential": 0,
                "score_blocker_risk": 0,
                "score_investment_potential": 0,
                "score_support_target": 0,
                "score_recruiting_potential": 0,

                "needs_review": False,
                "sensitivity_tier": "standard",

                "category_fields": category_fields,

                "_raw_entity_id": str(uuid.uuid4()),
                "_source": "sec_edgar",
                "_pending_evidence": [
                    {
                        "entity_id": None,
                        "run_id": run_id,
                        "supported_field": "canonical_name",
                        "supported_value": entity_name,
                        "source_url": source_url,
                        "source_type": "regulatory_filing",
                        "source_api": "sec_edgar",
                        "retrieved_at": now_iso,
                        "evidence_snippet": f"SEC EDGAR: {entity_name} is a registered 10-K filer (CIK: {cik})",
                        "claim_type": "direct_statement",
                        "confidence": "high",
                        "agent_name": self.AGENT_NAME,
                        "prompt_version": self.AGENT_VERSION,
                    }
                ],
            }
            entities.append(entity)

        return entities

    # ─────────────────────────────────────────────────────────────────────────
    # OpenCorporates — state business registry
    # ─────────────────────────────────────────────────────────────────────────

    async def _collect_from_opencorporates(
        self,
        city_name: str,
        run_id: str,
    ) -> list[dict[str, Any]]:
        """
        Query OpenCorporates for companies registered in the city.
        Focuses on active corporations (not LLCs/sole proprietors) to find
        significant incorporated entities.
        """
        search_start = time.monotonic()
        try:
            response = await self._opencorporates.search_companies(
                name=city_name,
                jurisdiction_code="us",
            )
        except Exception as e:
            log.warning("CorporateAgent: OpenCorporates search failed: %s", e)
            await self.write_search_record(
                source_searched="opencorporates",
                query_used=f"companies in {city_name}",
                result_found=False,
                entity_type="corporate",
                failure_reason=str(e),
                response_time_ms=int((time.monotonic() - search_start) * 1000),
            )
            return []

        elapsed_ms = int((time.monotonic() - search_start) * 1000)
        companies = response.get("results", {}).get("companies", [])

        await self.write_search_record(
            source_searched="opencorporates",
            query_used=f"active corporations in {city_name}",
            result_found=bool(companies),
            entity_type="corporate",
            result_count=len(companies),
            response_time_ms=elapsed_ms,
        )

        entities = []
        now_iso = datetime.now(timezone.utc).isoformat()

        for item in companies[:15]:
            company = item.get("company", {})
            name = company.get("name", "").strip()
            if not name:
                continue

            company_type = company.get("company_type", "")
            # Skip sole proprietors and LLCs (too small/numerous)
            if company_type in ("Sole Proprietorship", "General Partnership"):
                continue

            jurisdiction = company.get("jurisdiction_code", "")
            source_url = company.get("opencorporates_url", "")

            category_fields: dict[str, Any] = {
                "corporate_subtype": "private_company",
                "corporate_subtype_status": "REPORTED",
                "opencorporates_company_number": company.get("company_number"),
                "registered_jurisdiction": jurisdiction,
                "registered_jurisdiction_status": "REPORTED" if jurisdiction else "NOT_COLLECTED",
                "incorporation_date": company.get("incorporation_date"),
                "company_type": company_type,
                "company_status": company.get("current_status"),
            }

            entity: dict[str, Any] = {
                "entity_id": None,
                "canonical_name": name,
                "entity_type": "corporate",
                "entity_subtype": "private_company",
                "aliases": [],
                "valid_from": now_iso,
                "valid_to": None,
                "superseded_by": None,

                "primary_city": city_name,
                "primary_city_status": "NOT_COLLECTED",
                "primary_state": jurisdiction.split("_")[1].upper() if "_" in jurisdiction else None,
                "primary_state_status": "REPORTED" if "_" in jurisdiction else "NOT_COLLECTED",
                "primary_country": "United States",
                "primary_country_status": "REPORTED",

                "website_url": None,
                "website_url_status": "NOT_COLLECTED",
                "linkedin_url": None,
                "linkedin_url_status": "NOT_COLLECTED",
                "twitter_handle": None,
                "twitter_handle_status": "NOT_COLLECTED",

                "description": None,
                "description_status": "NOT_COLLECTED",
                "description_source_url": source_url,

                "external_ids": {
                    "opencorporates_id": company.get("company_number", "")
                } if company.get("company_number") else {},
                "source_agent": self.AGENT_NAME,
                "source_run_ids": [run_id],
                "merge_provenance": [],
                "source_urls": [source_url] if source_url else [],
                "last_seen": now_iso,
                "last_verified": None,

                "overall_confidence": "medium",
                "source_count": 1,
                "corroboration_count": 0,

                "partner_candidate": False,
                "competitor_candidate": False,
                "blocker_candidate": False,
                "investment_candidate": False,
                "support_candidate": False,
                "recruiter_candidate": False,
                "top_influencer": False,

                "score_influence": 0,
                "score_startup_relevance": 0,
                "score_partner_potential": 0,
                "score_supporter_potential": 0,
                "score_competitor_potential": 0,
                "score_blocker_risk": 0,
                "score_investment_potential": 0,
                "score_support_target": 0,
                "score_recruiting_potential": 0,

                "needs_review": False,
                "sensitivity_tier": "standard",

                "category_fields": category_fields,

                "_raw_entity_id": str(uuid.uuid4()),
                "_source": "opencorporates",
                "_pending_evidence": [
                    {
                        "entity_id": None,
                        "run_id": run_id,
                        "supported_field": "canonical_name",
                        "supported_value": name,
                        "source_url": source_url,
                        "source_type": "government_record",
                        "source_api": "opencorporates",
                        "retrieved_at": now_iso,
                        "evidence_snippet": (
                            f"OpenCorporates: {name} is registered as a {company_type or 'corporation'} "
                            f"in {jurisdiction or 'US'}"
                            + (f", status: {company.get('current_status')}" if company.get('current_status') else "")
                        ),
                        "claim_type": "direct_statement",
                        "confidence": "high",
                        "agent_name": self.AGENT_NAME,
                        "prompt_version": self.AGENT_VERSION,
                    }
                ],
            }
            entities.append(entity)

        return entities

    # ─────────────────────────────────────────────────────────────────────────
    # SerpAPI
    # ─────────────────────────────────────────────────────────────────────────

    async def _collect_from_serpapi(
        self,
        city_name: str,
        country_or_region: str,
        run_id: str,
        targeted_queries: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Search for major employers and corporate accelerator programs."""
        if not settings.serpapi_api_key:
            log.info("CorporateAgent: SerpAPI key not set — skipping")
            await self.write_search_record(
                source_searched="serpapi",
                query_used=f"major employers {city_name}",
                result_found=False,
                entity_type="corporate",
                failure_reason="SERPAPI_API_KEY not set",
            )
            return []

        queries = targeted_queries if targeted_queries else [
            f"largest employers companies {city_name}",
            f"corporate accelerator innovation lab {city_name}",
            f"Fortune 500 headquarters {city_name}",
        ]

        entities = []
        for query in queries[:3]:
            search_start = time.monotonic()
            try:
                response = await self._serpapi.search(query, num=10)
            except Exception as e:
                log.warning("CorporateAgent: SerpAPI '%s' failed: %s", query, e)
                await self.write_search_record(
                    source_searched="serpapi",
                    query_used=query,
                    result_found=False,
                    entity_type="corporate",
                    failure_reason=str(e),
                    response_time_ms=int((time.monotonic() - search_start) * 1000),
                )
                continue

            elapsed_ms = int((time.monotonic() - search_start) * 1000)
            organic_results = response.get("organic_results", [])

            await self.write_search_record(
                source_searched="serpapi",
                query_used=query,
                result_found=bool(organic_results),
                entity_type="corporate",
                result_count=len(organic_results),
                response_time_ms=elapsed_ms,
            )

            for result in organic_results[:5]:
                extracted = await self._extract_from_serp_result(result, city_name, run_id)
                if extracted:
                    entities.append(extracted)

        return entities

    async def _extract_from_serp_result(
        self,
        result: dict[str, Any],
        city_name: str,
        run_id: str,
    ) -> dict[str, Any] | None:
        """LLM extraction for SerpAPI corporate results."""
        search_text = (
            f"Title: {result.get('title', '')}\n"
            f"URL: {result.get('link', '')}\n"
            f"Snippet: {result.get('snippet', '')}\n"
        )
        prompt = SERPAPI_EXTRACTION_PROMPT.format(
            city_name=city_name,
            search_text=search_text,
        )

        try:
            extracted_json, _ = await self.llm_generate_json(
                task_type=TaskType.STRUCTURED_EXTRACTION_CLEAN,
                prompt=prompt,
                system=SERPAPI_EXTRACTION_SYSTEM,
            )
        except Exception as e:
            log.debug("CorporateAgent: LLM extraction failed: %s", e)
            return None

        name = extracted_json.get("name")
        evidence_snippet = extracted_json.get("evidence_snippet")
        if not name or not evidence_snippet:
            return None

        source_url = result.get("link", "")
        now_iso = datetime.now(timezone.utc).isoformat()
        subtype = extracted_json.get("entity_subtype") or "private_company"

        category_fields: dict[str, Any] = {
            "corporate_subtype": subtype,
            "corporate_subtype_status": "REPORTED",
            "industry": extracted_json.get("industry"),
            "industry_status": "REPORTED" if extracted_json.get("industry") else "NOT_COLLECTED",
            "employee_count_range": extracted_json.get("employee_count_range"),
            "employee_count_range_status": "REPORTED" if extracted_json.get("employee_count_range") else "NOT_COLLECTED",
            "ticker_symbol": extracted_json.get("ticker_symbol"),
            "has_accelerator_program": extracted_json.get("has_accelerator_program", False),
        }

        return {
            "entity_id": None,
            "canonical_name": name,
            "entity_type": "corporate",
            "entity_subtype": subtype,
            "aliases": [],
            "valid_from": now_iso,
            "valid_to": None,
            "superseded_by": None,

            "primary_city": city_name,
            "primary_city_status": "NOT_COLLECTED",
            "primary_state": None,
            "primary_state_status": "NOT_COLLECTED",
            "primary_country": "United States",
            "primary_country_status": "NOT_COLLECTED",

            "website_url": extracted_json.get("website_url"),
            "website_url_status": "REPORTED" if extracted_json.get("website_url") else "NOT_COLLECTED",
            "linkedin_url": None,
            "linkedin_url_status": "NOT_COLLECTED",
            "twitter_handle": None,
            "twitter_handle_status": "NOT_COLLECTED",

            "description": extracted_json.get("description"),
            "description_status": "REPORTED" if extracted_json.get("description") else "NOT_COLLECTED",
            "description_source_url": source_url,

            "external_ids": {},
            "source_agent": self.AGENT_NAME,
            "source_run_ids": [run_id],
            "merge_provenance": [],
            "source_urls": [source_url],
            "last_seen": now_iso,
            "last_verified": None,

            "overall_confidence": "low",
            "source_count": 1,
            "corroboration_count": 0,

            "partner_candidate": False,
            "competitor_candidate": False,
            "blocker_candidate": False,
            "investment_candidate": False,
            "support_candidate": False,
            "recruiter_candidate": False,
            "top_influencer": False,

            "score_influence": 0,
            "score_startup_relevance": 0,
            "score_partner_potential": 0,
            "score_supporter_potential": 0,
            "score_competitor_potential": 0,
            "score_blocker_risk": 0,
            "score_investment_potential": 0,
            "score_support_target": 0,
            "score_recruiting_potential": 0,

            "needs_review": False,
            "sensitivity_tier": "standard",

            "category_fields": category_fields,

            "_raw_entity_id": str(uuid.uuid4()),
            "_source": "serpapi",
            "_pending_evidence": [
                {
                    "entity_id": None,
                    "run_id": run_id,
                    "supported_field": "canonical_name",
                    "supported_value": name,
                    "source_url": source_url,
                    "source_type": "web_page",
                    "source_api": "serpapi",
                    "retrieved_at": now_iso,
                    "evidence_snippet": evidence_snippet[:1000],
                    "claim_type": "inferred",
                    "confidence": "low",
                    "agent_name": self.AGENT_NAME,
                    "prompt_version": self.AGENT_VERSION,
                }
            ],
        }
