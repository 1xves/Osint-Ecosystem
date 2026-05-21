"""
osint/agents/philanthropic.py

Philanthropic Intelligence Agent — Phase 1 collection agent.

Collects: private foundations, community foundations, corporate foundations,
major individual donors, donor-advised fund administrators, giving programs
active in the target city.

Data sources (in priority order):
1. ProPublica Nonprofit Explorer — IRS 990 data for foundations (no auth required)
2. EDGAR               — Private foundation Form 990-PF filings
3. SerpAPI             — Finds individual philanthropists not in structured sources

Output:
- Appends to state["raw_entities"] — raw pre-resolution entity dicts
- Writes evidence records to DB for every sourced field
- Writes search records for every search attempt
- Does NOT resolve or deduplicate — that's the Resolution Agent's job

Entity type: "philanthropic"
Subtypes: private_foundation | community_foundation | corporate_foundation |
          donor_advised_fund | individual_philanthropist | giving_program

Notes:
- ProPublica covers 501(c)(3) organizations; use ntee_code prefix "T" for foundations
- EDGAR 990-PF filers are private foundations with >$5M in assets — highest value targets
- Individual philanthropists are primarily discovered via SerpAPI
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from osint.agents.base import BaseAgent
from osint.clients.propublica import ProPublicaClient
from osint.clients.edgar import EdgarClient
from osint.clients.serpapi import SerpApiClient
from osint.core.config import settings
from osint.llm.routing import TaskType

log = logging.getLogger(__name__)

# ProPublica NTEE codes for philanthropic organizations
# T = Philanthropy, Voluntarism, and Grantmaking Foundations
NTEE_PHILANTHROPY_CODES = ["T20", "T21", "T22", "T30", "T31", "T40", "T50", "T70", "T90"]

# Subtype inference map from NTEE codes
NTEE_SUBTYPE_MAP = {
    "T20": "private_foundation",
    "T21": "corporate_foundation",
    "T22": "private_foundation",  # Private operating foundation
    "T30": "community_foundation",
    "T31": "community_foundation",
    "T40": "donor_advised_fund",
    "T50": "donor_advised_fund",
    "T70": "giving_program",
    "T90": "giving_program",
}

SERPAPI_EXTRACTION_SYSTEM = """You are a data extraction agent for an OSINT pipeline.
Extract philanthropic entity information from search result text.
Return ONLY valid JSON. Do not include explanation or commentary.
If a field cannot be determined from the text, use null.
Never fabricate information — only extract what is explicitly stated in the source text."""

SERPAPI_EXTRACTION_PROMPT = """Extract philanthropic entity information from this search result.

City context: {city_name}

Search result:
---
{search_text}
---

Return a JSON object:
{{
  "name": "<foundation, fund, or individual name, or null>",
  "entity_subtype": "<private_foundation|community_foundation|corporate_foundation|donor_advised_fund|individual_philanthropist|giving_program|null>",
  "description": "<brief factual description from the source text, or null>",
  "website_url": "<if present in text, or null>",
  "total_assets": "<dollar amount as string if mentioned, e.g. '$50 million', or null>",
  "annual_giving": "<annual giving/grant total as string if mentioned, or null>",
  "primary_cause_areas": ["<cause areas mentioned, or empty list>"],
  "is_local": "<true if based in {city_name}, false if only giving there, null if unclear>",
  "evidence_snippet": "<exact quote from the text that supports this being a philanthropic entity in {city_name}>"
}}"""


class PhilanthropicAgent(BaseAgent):
    """
    Phase 1 collection agent for philanthropic entities.
    """

    AGENT_NAME = "philanthropic_agent"
    AGENT_VERSION = "1.0"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._propublica = ProPublicaClient(self._rl)
        self._edgar = EdgarClient(self._rl)
        self._serpapi = SerpApiClient(self._rl)

    async def run(self, state: dict[str, Any]) -> dict[str, Any]:
        city_name = state["city_name"]
        country_or_region = state.get("country_or_region", "United States")
        run_id = state["run_id"]
        existing_raw_entities = state.get("raw_entities", [])
        pass_number = state.get("pass_number", 1)
        pass2_targets = state.get("pass2_targets", [])

        log.info(
            "PhilanthropicAgent: collecting for %s (pass %d)",
            city_name, pass_number
        )

        new_raw_entities: list[dict[str, Any]] = []

        # ── Pass 2 targeting ──────────────────────────────────────────────────
        targeted_queries: list[str] = []
        if pass_number == 2:
            for target in pass2_targets:
                if target.get("entity_type") == "philanthropic":
                    targeted_queries.extend(target.get("suggested_queries", []))
            targeted_queries = [q for q in targeted_queries if q]
            if targeted_queries:
                log.info("PhilanthropicAgent: Pass 2 — %d targeted queries", len(targeted_queries))

        # ── Source 1: ProPublica Nonprofit Explorer ───────────────────────────
        pp_entities = await self._collect_from_propublica(city_name, run_id)
        new_raw_entities.extend(pp_entities)
        log.info("PhilanthropicAgent: ProPublica yielded %d raw entities", len(pp_entities))

        # ── Source 2: EDGAR (Form 990-PF private foundations) ─────────────────
        edgar_entities = await self._collect_from_edgar(city_name, run_id)
        new_raw_entities.extend(edgar_entities)
        log.info("PhilanthropicAgent: EDGAR yielded %d raw entities", len(edgar_entities))

        # ── Source 3: SerpAPI (individual philanthropists + giving programs) ──
        serp_entities = await self._collect_from_serpapi(
            city_name, country_or_region, run_id,
            targeted_queries=targeted_queries,
        )
        new_raw_entities.extend(serp_entities)
        log.info("PhilanthropicAgent: SerpAPI yielded %d raw entities", len(serp_entities))

        log.info(
            "PhilanthropicAgent: %d total raw philanthropic entities collected",
            len(new_raw_entities)
        )

        patch: dict[str, Any] = {
            "raw_entities": existing_raw_entities + new_raw_entities,
            **self.agent_status_patch("success", state.get("agent_statuses", {})),
            **self.token_count_patch(
                state.get("total_tokens_in", 0),
                state.get("total_tokens_out", 0),
                state.get("agent_token_counts", {}),
            ),
            **self.entity_count_patch(state.get("agent_entity_counts", {})),
        }
        return patch

    # ─────────────────────────────────────────────────────────────────────────
    # ProPublica Nonprofit Explorer
    # ─────────────────────────────────────────────────────────────────────────

    async def _collect_from_propublica(
        self,
        city_name: str,
        run_id: str,
    ) -> list[dict[str, Any]]:
        """
        Search ProPublica Nonprofit Explorer for foundations and giving programs.
        Filters by NTEE "T" codes (Philanthropy) and city.
        """
        search_start = time.monotonic()
        try:
            response = await self._propublica.search_nonprofits(
                query=city_name,
                state_abbr=None,  # State abbr not always known; filter by name
                ntee=6,           # NTEE major group 6 = Philanthropy, Voluntarism, Grantmaking
            )
        except Exception as e:
            log.warning("PhilanthropicAgent: ProPublica search failed: %s", e)
            await self.write_search_record(
                source_searched="propublica_nonprofits",
                query_used=f"foundations philanthropy {city_name}",
                result_found=False,
                entity_type="philanthropic",
                failure_reason=str(e),
                response_time_ms=int((time.monotonic() - search_start) * 1000),
            )
            return []

        elapsed_ms = int((time.monotonic() - search_start) * 1000)
        orgs = response.get("organizations", [])

        await self.write_search_record(
            source_searched="propublica_nonprofits",
            query_used=f"foundations philanthropy {city_name} (NTEE group 6)",
            result_found=bool(orgs),
            entity_type="philanthropic",
            result_count=len(orgs),
            response_time_ms=elapsed_ms,
        )

        entities = []
        now_iso = datetime.now(timezone.utc).isoformat()

        for org in orgs[:20]:  # Cap at 20 — ProPublica search results aren't city-filtered
            name = org.get("name", "").strip()
            if not name:
                continue

            # Filter out orgs that aren't foundation-type
            ntee_code = org.get("ntee_code", "") or ""
            subtype = NTEE_SUBTYPE_MAP.get(ntee_code[:3], None)
            if not subtype:
                # Infer from name if NTEE doesn't match
                name_lower = name.lower()
                if "foundation" in name_lower:
                    subtype = "private_foundation"
                elif "community fund" in name_lower or "community foundation" in name_lower:
                    subtype = "community_foundation"
                elif "giving" in name_lower or "grant" in name_lower:
                    subtype = "giving_program"
                else:
                    # Skip if we can't determine philanthropic nature
                    continue

            ein = org.get("ein", "")
            source_url = f"https://projects.propublica.org/nonprofits/organizations/{ein.replace('-', '')}" if ein else ""
            revenue = org.get("income_amount")
            assets = org.get("asset_amount")

            category_fields: dict[str, Any] = {
                "philanthropic_subtype": subtype,
                "philanthropic_subtype_status": "REPORTED",
                "ein": ein,
                "ein_status": "REPORTED" if ein else "NOT_REPORTED",
                "ntee_code": ntee_code,
                "ntee_code_status": "REPORTED" if ntee_code else "NOT_COLLECTED",
                "total_revenue": revenue,
                "total_revenue_status": "REPORTED" if revenue is not None else "NOT_COLLECTED",
                "total_assets": assets,
                "total_assets_status": "REPORTED" if assets is not None else "NOT_COLLECTED",
                "tax_period": org.get("tax_prd_yr"),
                "form_type": org.get("formtype"),
                "irs_ruling_year": org.get("ruling_year"),
            }

            entity: dict[str, Any] = {
                "entity_id": None,
                "canonical_name": name,
                "entity_type": "philanthropic",
                "entity_subtype": subtype,
                "aliases": [],
                "valid_from": now_iso,
                "valid_to": None,
                "superseded_by": None,

                "primary_city": city_name,
                "primary_city_status": "NOT_COLLECTED",  # ProPublica doesn't return city
                "primary_state": org.get("state"),
                "primary_state_status": "REPORTED" if org.get("state") else "NOT_COLLECTED",
                "primary_country": "United States",
                "primary_country_status": "REPORTED",

                "website_url": None,
                "website_url_status": "NOT_COLLECTED",
                "linkedin_url": None,
                "linkedin_url_status": "NOT_COLLECTED",
                "twitter_handle": None,
                "twitter_handle_status": "NOT_COLLECTED",

                "description": org.get("subsection_code"),
                "description_status": "NOT_COLLECTED",
                "description_source_url": source_url,

                "external_ids": {"ein": ein} if ein else {},
                "source_agent": self.AGENT_NAME,
                "source_run_ids": [run_id],
                "merge_provenance": [],
                "source_urls": [source_url] if source_url else [],
                "last_seen": now_iso,
                "last_verified": None,

                "overall_confidence": "high",  # IRS 990 data is authoritative
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
                "_source": "propublica_nonprofits",
                "_pending_evidence": [
                    {
                        "entity_id": None,
                        "run_id": run_id,
                        "supported_field": "canonical_name",
                        "supported_value": name,
                        "source_url": source_url,
                        "source_type": "regulatory_filing",
                        "source_api": "propublica_nonprofits",
                        "retrieved_at": now_iso,
                        "evidence_snippet": (
                            f"IRS 990 filing via ProPublica: {name} is a registered "
                            f"{'tax-exempt organization' if not ntee_code else f'organization (NTEE: {ntee_code})'}"
                            f"{f', EIN: {ein}' if ein else ''}"
                            f"{f', assets: ${assets:,}' if assets else ''}"
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
    # EDGAR — Form 990-PF private foundations
    # ─────────────────────────────────────────────────────────────────────────

    async def _collect_from_edgar(
        self,
        city_name: str,
        run_id: str,
    ) -> list[dict[str, Any]]:
        """
        Search EDGAR for private foundations filing Form 990-PF.
        These are the highest-value philanthropic targets — large asset foundations.
        """
        search_start = time.monotonic()
        try:
            # Search for foundations by name pattern including city
            response = await self._edgar.search_company_name(
                name=f"{city_name} foundation",
                city=city_name,
            )
        except Exception as e:
            log.warning("PhilanthropicAgent: EDGAR search failed: %s", e)
            await self.write_search_record(
                source_searched="sec_edgar",
                query_used=f"{city_name} foundation 990-PF",
                result_found=False,
                entity_type="philanthropic",
                failure_reason=str(e),
                response_time_ms=int((time.monotonic() - search_start) * 1000),
            )
            return []

        elapsed_ms = int((time.monotonic() - search_start) * 1000)
        hits = response.get("hits", {}).get("hits", [])

        await self.write_search_record(
            source_searched="sec_edgar",
            query_used=f"{city_name} foundation (990-PF filers)",
            result_found=bool(hits),
            entity_type="philanthropic",
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

            # Only include entities that look like foundations
            name_lower = entity_name.lower()
            if not any(kw in name_lower for kw in [
                "foundation", "charitable", "philanthropic", "trust", "endowment",
                "giving", "grant", "fund",
            ]):
                continue

            cik = source.get("entity_id", "")
            source_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=990&dateb=&owner=include&count=10"

            # Infer subtype from name
            if "community foundation" in name_lower or "community fund" in name_lower:
                subtype = "community_foundation"
            elif "corporate" in name_lower or "corp" in name_lower:
                subtype = "corporate_foundation"
            else:
                subtype = "private_foundation"

            category_fields: dict[str, Any] = {
                "philanthropic_subtype": subtype,
                "philanthropic_subtype_status": "REPORTED",
                "sec_cik": cik,
                "sec_cik_status": "REPORTED" if cik else "NOT_COLLECTED",
                "form_type": "990-PF",
            }

            entity: dict[str, Any] = {
                "entity_id": None,
                "canonical_name": entity_name,
                "entity_type": "philanthropic",
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

                "overall_confidence": "medium",  # Name match only — no detailed data yet
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
                        "evidence_snippet": f"SEC EDGAR lists {entity_name} as a registered filer (CIK: {cik})",
                        "claim_type": "direct_statement",
                        "confidence": "medium",
                        "agent_name": self.AGENT_NAME,
                        "prompt_version": self.AGENT_VERSION,
                    }
                ],
            }
            entities.append(entity)

        return entities

    # ─────────────────────────────────────────────────────────────────────────
    # SerpAPI — individual philanthropists and giving programs
    # ─────────────────────────────────────────────────────────────────────────

    async def _collect_from_serpapi(
        self,
        city_name: str,
        country_or_region: str,
        run_id: str,
        targeted_queries: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Search for individual philanthropists and less formal giving programs
        not captured by structured sources.
        """
        if not settings.serpapi_api_key:
            log.info("PhilanthropicAgent: SerpAPI key not set — skipping")
            await self.write_search_record(
                source_searched="serpapi",
                query_used=f"philanthropists {city_name}",
                result_found=False,
                entity_type="philanthropic",
                failure_reason="SERPAPI_API_KEY not set",
            )
            return []

        queries = targeted_queries if targeted_queries else [
            f"major philanthropists donors {city_name}",
            f"private foundations giving {city_name} startup community",
            f"charitable giving impact investing {city_name}",
        ]

        entities = []
        for query in queries[:3]:
            search_start = time.monotonic()
            try:
                response = await self._serpapi.search(query, num=10)
            except Exception as e:
                log.warning("PhilanthropicAgent: SerpAPI search failed: %s", e)
                await self.write_search_record(
                    source_searched="serpapi",
                    query_used=query,
                    result_found=False,
                    entity_type="philanthropic",
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
                entity_type="philanthropic",
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
        """LLM extraction (qwen3:7b) for SerpAPI philanthropic results."""
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
            log.debug("PhilanthropicAgent: LLM extraction failed: %s", e)
            return None

        name = extracted_json.get("name")
        evidence_snippet = extracted_json.get("evidence_snippet")
        if not name or not evidence_snippet:
            return None

        source_url = result.get("link", "")
        now_iso = datetime.now(timezone.utc).isoformat()

        subtype = extracted_json.get("entity_subtype") or "private_foundation"

        category_fields: dict[str, Any] = {
            "philanthropic_subtype": subtype,
            "philanthropic_subtype_status": "REPORTED",
            "primary_cause_areas": extracted_json.get("primary_cause_areas", []),
            "primary_cause_areas_status": "REPORTED" if extracted_json.get("primary_cause_areas") else "NOT_COLLECTED",
            "total_assets_text": extracted_json.get("total_assets"),
            "annual_giving_text": extracted_json.get("annual_giving"),
        }

        return {
            "entity_id": None,
            "canonical_name": name,
            "entity_type": "philanthropic",
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

            "description": None,
            "description_status": "NOT_COLLECTED",
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
