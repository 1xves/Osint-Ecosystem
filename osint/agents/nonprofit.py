"""
osint/agents/nonprofit.py

Nonprofit Intelligence Agent — Phase 1 collection agent.

Collects: civic nonprofits, advocacy organizations, startup accelerators/incubators,
economic development organizations, workforce development groups, arts organizations,
civic tech groups, and other 501(c) entities with startup ecosystem relevance.

Data sources (in priority order):
1. ProPublica Nonprofit Explorer — IRS 990 data, city-filtered
2. USASpending                  — Federal grant recipients (reveals gov-connected orgs)
3. SerpAPI                      — Fills gaps for newer/informal organizations

Output:
- Appends to state["raw_entities"] — raw pre-resolution entity dicts
- Writes evidence records to DB for every sourced field
- Writes search records for every search attempt
- Does NOT resolve or deduplicate — that's the Resolution Agent's job

Entity type: "nonprofit"
Subtypes: civic | advocacy | accelerator_incubator | economic_development |
          workforce | arts_culture | civic_tech | social_services | trade_association

Notes:
- ProPublica does NOT filter by city — we search by keyword + city name and filter results
- USASpending reveals which nonprofits receive federal contracts/grants, indicating
  their scale and government relationships (important for political mapping)
- NTEE codes: C/D (environment), E/F/G/H (health), I/J/K/L (human services),
  N (recreation), O/P (youth/family), Q (international), R/S (community),
  U/V/W (public benefit), X/Y/Z (religion/mutual benefit)
- Accelerators/incubators often don't file 990s — SerpAPI catches these
"""

from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from osint.agents.base import BaseAgent
from osint.clients.propublica import ProPublicaClient
from osint.clients.usaspending import USASpendingClient
from osint.clients.serpapi import SerpApiClient
from osint.core.config import settings
from osint.llm.routing import TaskType

log = logging.getLogger(__name__)

# NTEE major group numbers for ProPublica API (1-based)
# 3 = Education, 4 = Health, 6 = Philanthropy, 7 = Public Benefit,
# 8 = Religion, 10 = Mutual Benefit — we want 1-10 depending on context
# We use group 7 (Public/Societal Benefit) which covers civic orgs
NTEE_GROUP_PUBLIC_BENEFIT = 7

# Keywords that indicate startup-ecosystem-relevant nonprofits
ECOSYSTEM_KEYWORDS = [
    "incubator", "accelerator", "innovation", "entrepreneurship", "startup",
    "economic development", "workforce", "tech", "technology", "venture",
    "small business", "community development", "civic", "chamber",
]

SERPAPI_EXTRACTION_SYSTEM = """You are a data extraction agent for an OSINT pipeline.
Extract nonprofit organization information from search result text.
Return ONLY valid JSON. Do not include explanation or commentary.
If a field cannot be determined from the text, use null.
Never fabricate information — only extract what is explicitly stated in the source text."""

SERPAPI_EXTRACTION_PROMPT = """Extract nonprofit organization information from this search result.

City context: {city_name}

Search result:
---
{search_text}
---

Return a JSON object:
{{
  "name": "<organization name, or null>",
  "entity_subtype": "<civic|advocacy|accelerator_incubator|economic_development|workforce|arts_culture|civic_tech|social_services|trade_association|null>",
  "description": "<brief factual description from the source text, or null>",
  "website_url": "<if present in text, or null>",
  "mission": "<mission statement or purpose if stated, or null>",
  "annual_budget": "<budget/revenue as string if mentioned, or null>",
  "focus_areas": ["<program areas mentioned, or empty list>"],
  "government_funded": "<true if mentions government contracts/grants, false otherwise, null if unclear>",
  "is_local": "<true if based in {city_name}, false if regional/national, null if unclear>",
  "evidence_snippet": "<exact quote from the text that supports this being a relevant organization in {city_name}>"
}}"""


class NonprofitAgent(BaseAgent):
    """
    Phase 1 collection agent for nonprofit entities.
    """

    AGENT_NAME = "nonprofit_agent"
    AGENT_VERSION = "1.0"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._propublica = ProPublicaClient(self._rl)
        self._usaspending = USASpendingClient(self._rl)
        self._serpapi = SerpApiClient(self._rl)

    async def run(self, state: dict[str, Any]) -> dict[str, Any]:
        city_name = state["city_name"]
        country_or_region = state.get("country_or_region", "United States")
        run_id = state["run_id"]
        existing_raw_entities = state.get("raw_entities", [])
        pass_number = state.get("pass_number", 1)
        pass2_targets = state.get("pass2_targets", [])

        log.info("NonprofitAgent: collecting for %s (pass %d)", city_name, pass_number)

        new_raw_entities: list[dict[str, Any]] = []

        # ── Pass 2 targeting ──────────────────────────────────────────────────
        targeted_queries: list[str] = []
        if pass_number == 2:
            for target in pass2_targets:
                if target.get("entity_type") == "nonprofit":
                    targeted_queries.extend(target.get("suggested_queries", []))
            targeted_queries = [q for q in targeted_queries if q]

        # ── Source 1: ProPublica ──────────────────────────────────────────────
        pp_entities = await self._collect_from_propublica(city_name, run_id)
        new_raw_entities.extend(pp_entities)
        log.info("NonprofitAgent: ProPublica yielded %d raw entities", len(pp_entities))

        # ── Source 2: USASpending (federal grant recipients) ──────────────────
        usa_entities = await self._collect_from_usaspending(city_name, run_id)
        new_raw_entities.extend(usa_entities)
        log.info("NonprofitAgent: USASpending yielded %d raw entities", len(usa_entities))

        # ── Source 3: SerpAPI ─────────────────────────────────────────────────
        serp_entities = await self._collect_from_serpapi(
            city_name, country_or_region, run_id, targeted_queries=targeted_queries
        )
        new_raw_entities.extend(serp_entities)
        log.info("NonprofitAgent: SerpAPI yielded %d raw entities", len(serp_entities))

        log.info("NonprofitAgent: %d total raw entities collected", len(new_raw_entities))

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
        Search ProPublica for ecosystem-relevant nonprofits.
        Runs two passes: general city query + targeted ecosystem keyword query.
        """
        entities = []
        queries = [
            (city_name, None),
            (f"economic development {city_name}", None),
        ]

        for query_text, ntee_group in queries:
            search_start = time.monotonic()
            try:
                response = await self._propublica.search_nonprofits(
                    query=query_text,
                    ntee=ntee_group,
                )
            except Exception as e:
                log.warning("NonprofitAgent: ProPublica search '%s' failed: %s", query_text, e)
                await self.write_search_record(
                    source_searched="propublica_nonprofits",
                    query_used=query_text,
                    result_found=False,
                    entity_type="nonprofit",
                    failure_reason=str(e),
                    response_time_ms=int((time.monotonic() - search_start) * 1000),
                )
                continue

            elapsed_ms = int((time.monotonic() - search_start) * 1000)
            orgs = response.get("organizations", [])

            await self.write_search_record(
                source_searched="propublica_nonprofits",
                query_used=query_text,
                result_found=bool(orgs),
                entity_type="nonprofit",
                result_count=len(orgs),
                response_time_ms=elapsed_ms,
            )

            now_iso = datetime.now(timezone.utc).isoformat()
            for org in orgs[:20]:
                name = org.get("name", "").strip()
                if not name:
                    continue

                # Filter for ecosystem relevance
                name_lower = name.lower()
                is_relevant = any(kw in name_lower for kw in ECOSYSTEM_KEYWORDS)
                if not is_relevant:
                    continue

                subtype = self._infer_nonprofit_subtype(name_lower, org.get("ntee_code", ""))
                ein = org.get("ein", "")
                source_url = (
                    f"https://projects.propublica.org/nonprofits/organizations/{ein.replace('-', '')}"
                    if ein else ""
                )

                category_fields: dict[str, Any] = {
                    "nonprofit_subtype": subtype,
                    "nonprofit_subtype_status": "REPORTED",
                    "ein": ein,
                    "ein_status": "REPORTED" if ein else "NOT_REPORTED",
                    "ntee_code": org.get("ntee_code"),
                    "ntee_code_status": "REPORTED" if org.get("ntee_code") else "NOT_COLLECTED",
                    "total_revenue": org.get("income_amount"),
                    "total_revenue_status": "REPORTED" if org.get("income_amount") is not None else "NOT_COLLECTED",
                    "total_assets": org.get("asset_amount"),
                    "total_assets_status": "REPORTED" if org.get("asset_amount") is not None else "NOT_COLLECTED",
                    "irs_ruling_year": org.get("ruling_year"),
                    "form_type": org.get("formtype"),
                }

                entity: dict[str, Any] = {
                    "entity_id": None,
                    "canonical_name": name,
                    "entity_type": "nonprofit",
                    "entity_subtype": subtype,
                    "aliases": [],
                    "valid_from": now_iso,
                    "valid_to": None,
                    "superseded_by": None,

                    "primary_city": city_name,
                    "primary_city_status": "NOT_COLLECTED",
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

                    "description": None,
                    "description_status": "NOT_COLLECTED",
                    "description_source_url": source_url,

                    "external_ids": {"ein": ein} if ein else {},
                    "source_agent": self.AGENT_NAME,
                    "source_run_ids": [run_id],
                    "merge_provenance": [],
                    "source_urls": [source_url] if source_url else [],
                    "last_seen": now_iso,
                    "last_verified": None,

                    "overall_confidence": "high",
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
                                f"IRS 990 filing: {name} is a registered tax-exempt organization"
                                f"{f', EIN: {ein}' if ein else ''}"
                                + (f", NTEE: {org.get('ntee_code')}" if org.get('ntee_code') else "")
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

    def _infer_nonprofit_subtype(self, name_lower: str, ntee_code: str) -> str:
        """Infer nonprofit subtype from name keywords and NTEE code."""
        if any(kw in name_lower for kw in ["incubator", "accelerator", "launchpad", "hatchery"]):
            return "accelerator_incubator"
        if any(kw in name_lower for kw in ["chamber", "trade", "industry", "association", "alliance"]):
            return "trade_association"
        if any(kw in name_lower for kw in ["economic development", "edc", "development corp"]):
            return "economic_development"
        if any(kw in name_lower for kw in ["workforce", "employment", "job training", "career"]):
            return "workforce"
        if any(kw in name_lower for kw in ["civic tech", "open data", "code for", "hack"]):
            return "civic_tech"
        if any(kw in name_lower for kw in ["arts", "culture", "museum", "theater", "gallery"]):
            return "arts_culture"
        if any(kw in name_lower for kw in ["advocacy", "policy", "rights", "justice"]):
            return "advocacy"
        # NTEE S = Community Improvement, S20 = Community Economic Development
        if ntee_code and ntee_code.startswith("S"):
            return "economic_development"
        return "civic"

    # ─────────────────────────────────────────────────────────────────────────
    # USASpending — federal grant recipients
    # ─────────────────────────────────────────────────────────────────────────

    async def _collect_from_usaspending(
        self,
        city_name: str,
        run_id: str,
    ) -> list[dict[str, Any]]:
        """
        Identify nonprofits receiving federal grants in the city.
        These orgs have government relationships — important for political mapping.
        Focuses on SBA, EDA, CDFI, and DOL grants which go to ecosystem orgs.
        """
        search_start = time.monotonic()
        try:
            # Search for grants (not contracts) to nonprofit recipients
            response = await self._usaspending.search_grants(
                city=city_name,
                limit=20,
            )
        except Exception as e:
            log.warning("NonprofitAgent: USASpending search failed: %s", e)
            await self.write_search_record(
                source_searched="usaspending",
                query_used=f"grants to nonprofits in {city_name}",
                result_found=False,
                entity_type="nonprofit",
                failure_reason=str(e),
                response_time_ms=int((time.monotonic() - search_start) * 1000),
            )
            return []

        elapsed_ms = int((time.monotonic() - search_start) * 1000)
        results = response.get("results", [])

        await self.write_search_record(
            source_searched="usaspending",
            query_used=f"federal grants recipients in {city_name}",
            result_found=bool(results),
            entity_type="nonprofit",
            result_count=len(results),
            response_time_ms=elapsed_ms,
        )

        entities = []
        now_iso = datetime.now(timezone.utc).isoformat()
        seen_names: set[str] = set()

        for award in results:
            recipient = award.get("recipient", {})
            name = recipient.get("recipient_name", "").strip()
            if not name or name in seen_names:
                continue

            # Filter for nonprofit-sounding recipients; skip government agencies and companies
            name_lower = name.lower()
            # Skip obvious government recipients
            if any(kw in name_lower for kw in ["city of", "county of", "department of",
                                                 "state of", "board of", "district of"]):
                continue
            # Skip obvious for-profit companies
            if any(name_lower.endswith(sfx) for sfx in [" inc", " llc", " corp", " ltd", " co"]):
                if not any(kw in name_lower for kw in ECOSYSTEM_KEYWORDS):
                    continue

            seen_names.add(name)

            award_amount = award.get("award_amount")
            source_url = f"https://www.usaspending.gov/award/{award.get('award_id', '')}"

            category_fields: dict[str, Any] = {
                "nonprofit_subtype": self._infer_nonprofit_subtype(name_lower, ""),
                "nonprofit_subtype_status": "REPORTED",
                "federal_grant_recipient": True,
                "federal_grant_amount": award_amount,
                "federal_grant_amount_status": "REPORTED" if award_amount is not None else "NOT_COLLECTED",
                "federal_awarding_agency": award.get("awarding_agency", {}).get("toptier_agency", {}).get("name"),
                "uei": recipient.get("uei"),
                "uei_status": "REPORTED" if recipient.get("uei") else "NOT_COLLECTED",
            }

            entity: dict[str, Any] = {
                "entity_id": None,
                "canonical_name": name,
                "entity_type": "nonprofit",
                "entity_subtype": category_fields["nonprofit_subtype"],
                "aliases": [],
                "valid_from": now_iso,
                "valid_to": None,
                "superseded_by": None,

                "primary_city": recipient.get("location", {}).get("city_name") or city_name,
                "primary_city_status": "REPORTED" if recipient.get("location", {}).get("city_name") else "NOT_COLLECTED",
                "primary_state": recipient.get("location", {}).get("state_code"),
                "primary_state_status": "REPORTED" if recipient.get("location", {}).get("state_code") else "NOT_COLLECTED",
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

                "external_ids": {"uei": recipient.get("uei")} if recipient.get("uei") else {},
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
                "_source": "usaspending",
                "_pending_evidence": [
                    {
                        "entity_id": None,
                        "run_id": run_id,
                        "supported_field": "canonical_name",
                        "supported_value": name,
                        "source_url": source_url,
                        "source_type": "government_record",
                        "source_api": "usaspending",
                        "retrieved_at": now_iso,
                        "evidence_snippet": (
                            f"USASpending.gov: {name} received a federal grant"
                            f"{f' of ${award_amount:,.0f}' if award_amount else ''}"
                            f" from {category_fields.get('federal_awarding_agency', 'federal agency')}"
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
    # SerpAPI — accelerators, newer orgs, informal groups
    # ─────────────────────────────────────────────────────────────────────────

    async def _collect_from_serpapi(
        self,
        city_name: str,
        country_or_region: str,
        run_id: str,
        targeted_queries: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Search for accelerators and civic tech organizations not in structured DBs."""
        if not settings.serpapi_api_key:
            log.info("NonprofitAgent: SerpAPI key not set — skipping")
            await self.write_search_record(
                source_searched="serpapi",
                query_used=f"nonprofits {city_name}",
                result_found=False,
                entity_type="nonprofit",
                failure_reason="SERPAPI_API_KEY not set",
            )
            return []

        queries = targeted_queries if targeted_queries else [
            f"startup accelerator incubator {city_name} nonprofit",
            f"civic tech economic development nonprofit {city_name}",
            f"small business support organization {city_name}",
        ]

        entities = []
        for query in queries[:3]:
            search_start = time.monotonic()
            try:
                response = await self._serpapi.search(query, num=10)
            except Exception as e:
                log.warning("NonprofitAgent: SerpAPI '%s' failed: %s", query, e)
                await self.write_search_record(
                    source_searched="serpapi",
                    query_used=query,
                    result_found=False,
                    entity_type="nonprofit",
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
                entity_type="nonprofit",
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
        """LLM extraction for SerpAPI nonprofit results."""
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
            log.debug("NonprofitAgent: LLM extraction failed: %s", e)
            return None

        name = extracted_json.get("name")
        evidence_snippet = extracted_json.get("evidence_snippet")
        if not name or not evidence_snippet:
            return None

        source_url = result.get("link", "")
        now_iso = datetime.now(timezone.utc).isoformat()
        subtype = extracted_json.get("entity_subtype") or "civic"

        category_fields: dict[str, Any] = {
            "nonprofit_subtype": subtype,
            "nonprofit_subtype_status": "REPORTED",
            "focus_areas": extracted_json.get("focus_areas", []),
            "focus_areas_status": "REPORTED" if extracted_json.get("focus_areas") else "NOT_COLLECTED",
            "government_funded": extracted_json.get("government_funded"),
            "government_funded_status": "REPORTED" if extracted_json.get("government_funded") is not None else "NOT_COLLECTED",
            "mission": extracted_json.get("mission"),
            "mission_status": "REPORTED" if extracted_json.get("mission") else "NOT_COLLECTED",
        }

        return {
            "entity_id": None,
            "canonical_name": name,
            "entity_type": "nonprofit",
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
