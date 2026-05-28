"""
osint/agents/investor.py

Investor Intelligence Agent — Phase 1 collection agent.

Collects: VC firms, angel investors, family offices, PE firms, corporate VC arms,
investment syndicates active in the target city.

Data sources (in priority order):
1. Crunchbase — structured investor data, portfolio, fund details
2. SerpAPI   — fills gaps, finds investors not in Crunchbase
3. EDGAR     — SEC 13F filings for institutional investors

Output:
- Appends to state["raw_entities"] — raw pre-resolution entity dicts
- Writes evidence records to DB for every sourced field
- Writes search records for every search attempt
- Does NOT resolve or deduplicate — that's the Resolution Agent's job

Extraction approach:
- Crunchbase is the primary structured source
- LLM extraction (qwen3:7b) is used only for SerpAPI text results
- Direct field mapping is used for Crunchbase JSON (no LLM needed for clean structured data)
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from osint.agents.base import BaseAgent
from osint.clients.crunchbase import CrunchbaseClient
from osint.clients.serpapi import SerpApiClient
from osint.clients.edgar import EdgarClient
from osint.core.config import settings
from osint.llm.routing import TaskType

log = logging.getLogger(__name__)

# Crunchbase investor category groups — used to filter search results
CB_INVESTOR_CATEGORIES = [
    "Financial Services", "Investment Management", "Venture Capital",
    "Private Equity", "Angel Investment",
]

# Investment stage to our internal vocabulary
CB_STAGE_MAP = {
    "pre_seed": "pre_seed",
    "seed": "seed",
    "early_stage_venture": "seed",
    "series_a": "series_a",
    "series_b": "series_b",
    "late_stage_venture": "growth",
    "technology_growth": "growth",
    "private_equity": "late_stage",
    "post_ipo": "late_stage",
}

# qwen3:7b extraction system prompt for SerpAPI results
SERPAPI_EXTRACTION_SYSTEM = """You are a data extraction agent for an OSINT pipeline.
Extract investor entity information from search result text.
Return ONLY valid JSON. Do not include explanation or commentary.
If a field cannot be determined from the text, use null.
Never fabricate information — only extract what is explicitly stated in the source text."""

SERPAPI_EXTRACTION_PROMPT = """Extract investor information from this search result.

CRITICAL — "name" field rules:
- MUST be a specific, proper-noun organization or individual name.
  Good: "First Round Capital", "Josh Kopelman", "Safeguard Scientifics"
  Bad:  "Top 10 VCs in Philadelphia", "Best Venture Capital Firms", "Investors in PA"
- Article titles, list headings, rankings, and generic category labels are NOT valid names.
- If this search result is an article listing multiple investors rather than describing
  one specific named investor, return null for "name".

City context: {city_name}

Search result:
---
{search_text}
---

Return a JSON object:
{{
  "name": "<specific proper-noun organization or individual name, or null if this is a list/article>",
  "entity_subtype": "<vc|angel|family_office|pe|corporate_vc|syndicate|null>",
  "description": "<brief factual description from the source text, or null>",
  "website_url": "<if present in text, or null>",
  "linkedin_url": "<if present in text, or null>",
  "investment_stage_focus": ["<stages mentioned, or empty list>"],
  "sector_focus": ["<sectors mentioned, or empty list>"],
  "managing_partner": "<if a specific partner name is stated, or null>",
  "is_local": "<true if this investor is based in {city_name}, false if only investing there, null if unclear>",
  "evidence_snippet": "<exact quote from the text that supports this being an investor in {city_name}>"
}}"""


class InvestorAgent(BaseAgent):
    """
    Phase 1 collection agent for investor entities.
    """

    AGENT_NAME = "investor_agent"
    AGENT_VERSION = "1.0"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._crunchbase = CrunchbaseClient(self._rl)
        self._serpapi = SerpApiClient(self._rl)
        self._edgar = EdgarClient(self._rl)

    async def run(self, state: dict[str, Any]) -> dict[str, Any]:
        city_name = state["city_name"]
        country_or_region = state.get("country_or_region", "United States")
        run_id = state["run_id"]
        pass_number = state.get("pass_number", 1)
        pass2_targets = state.get("pass2_targets", [])

        log.info(
            "InvestorAgent: collecting investors for %s (pass %d)",
            city_name, pass_number
        )

        new_raw_entities: list[dict[str, Any]] = []
        search_records: list[dict[str, Any]] = []

        # ── Pass 2 targeting ──────────────────────────────────────────────────
        # If pass2_targets contains investor-specific targets, use those queries
        # instead of the default city-wide search
        targeted_queries: list[str] = []
        if pass_number == 2:
            for target in pass2_targets:
                if target.get("entity_type") == "investor":
                    targeted_queries.extend(target.get("suggested_queries", []))
            targeted_queries = [q for q in targeted_queries if q]
            if targeted_queries:
                log.info("InvestorAgent: Pass 2 — %d targeted queries", len(targeted_queries))

        # ── Source 0: Curated seeds (always runs first, API-independent) ───────
        seed_entities = await self._collect_from_seeds("investor", city_name, run_id)
        new_raw_entities.extend(seed_entities)
        log.info("InvestorAgent: Seeds yielded %d entities", len(seed_entities))

        # ── Source 1: Crunchbase ───────────────────────────────────────────────
        cb_entities = await self._collect_from_crunchbase(
            city_name, country_or_region, run_id,
            targeted_queries=targeted_queries,
        )
        new_raw_entities.extend(cb_entities)
        log.info("InvestorAgent: Crunchbase yielded %d raw entities", len(cb_entities))

        # ── Source 2: SerpAPI ─────────────────────────────────────────────────
        serp_entities = await self._collect_from_serpapi(
            city_name, country_or_region, run_id,
            targeted_queries=targeted_queries,
        )
        new_raw_entities.extend(serp_entities)
        log.info("InvestorAgent: SerpAPI yielded %d raw entities", len(serp_entities))

        # ── Source 3: EDGAR (SEC 13F institutional investors) ─────────────────
        edgar_entities = await self._collect_from_edgar(
            city_name, run_id,
        )
        new_raw_entities.extend(edgar_entities)
        log.info("InvestorAgent: EDGAR yielded %d raw entities", len(edgar_entities))

        total_new = len(new_raw_entities)
        log.info("InvestorAgent: %d total raw investor entities collected", total_new)

        # ── Build state patch (delta only — reducers handle merging) ──────────
        patch: dict[str, Any] = {
            "raw_entities": new_raw_entities,          # delta: only this agent's finds
            **self.agent_status_patch("success"),
            **self.token_count_patch(),
            **self.entity_count_patch(),
        }
        return patch

    # ─────────────────────────────────────────────────────────────────────────
    # Crunchbase collection
    # ─────────────────────────────────────────────────────────────────────────

    async def _collect_from_crunchbase(
        self,
        city_name: str,
        country_or_region: str,
        run_id: str,
        targeted_queries: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Query Crunchbase for investors in the city. Direct field mapping — no LLM."""
        if not settings.crunchbase_api_key:
            log.info("InvestorAgent: Crunchbase key not set — skipping")
            await self.write_search_record(
                source_searched="crunchbase",
                query_used=f"organizations in {city_name}",
                result_found=False,
                entity_type="investor",
                failure_reason="CRUNCHBASE_API_KEY not set",
            )
            return []

        entities = []
        search_start = time.monotonic()
        try:
            response = await self._crunchbase.search_organizations(
                city=city_name,
                country=country_or_region,
                categories=CB_INVESTOR_CATEGORIES,
                limit=25,
            )
        except Exception as e:
            log.warning("InvestorAgent: Crunchbase search failed: %s", e)
            await self.write_search_record(
                source_searched="crunchbase",
                query_used=f"investor organizations in {city_name}",
                result_found=False,
                entity_type="investor",
                failure_reason=str(e),
                response_time_ms=int((time.monotonic() - search_start) * 1000),
            )
            return []

        elapsed_ms = int((time.monotonic() - search_start) * 1000)
        entities_data = response.get("entities", [])

        await self.write_search_record(
            source_searched="crunchbase",
            query_used=f"investor organizations in {city_name}",
            result_found=bool(entities_data),
            entity_type="investor",
            result_count=len(entities_data),
            response_time_ms=elapsed_ms,
        )

        for raw in entities_data:
            props = raw.get("properties", {})
            if not props.get("identifier", {}).get("value"):
                continue

            raw_entity_id = str(uuid.uuid4())
            entity = self._map_crunchbase_org(props, raw_entity_id, run_id, city_name)

            # Write evidence record for each entity
            await self._write_crunchbase_evidence(entity, props, run_id)
            entities.append(entity)

        return entities

    def _map_crunchbase_org(
        self,
        props: dict[str, Any],
        raw_entity_id: str,
        run_id: str,
        city_name: str,
    ) -> dict[str, Any]:
        """Map Crunchbase org properties to our EntityBase + InvestorFields schema."""
        identifier = props.get("identifier", {})
        canonical_name = identifier.get("value", "")

        # Location
        location_ids = props.get("location_identifiers", [])
        location_city = None
        location_state = None
        for loc in location_ids:
            if loc.get("location_type") == "city":
                location_city = loc.get("value")
            elif loc.get("location_type") == "region":
                location_state = loc.get("value")

        # Funding stage → investment stage focus
        last_funding_type = props.get("last_funding_type")
        stage_focus = []
        if last_funding_type and last_funding_type.lower() in CB_STAGE_MAP:
            stage_focus = [CB_STAGE_MAP[last_funding_type.lower()]]

        # Build category fields (stored as JSONB)
        category_fields = {
            "investor_subtype": self._infer_investor_subtype(props),
            "investment_stage_focus": stage_focus,
            "investment_stage_focus_status": "REPORTED" if stage_focus else "NOT_REPORTED",
            "portfolio_count_total": props.get("num_portfolio_organizations"),
            "portfolio_count_total_status": "REPORTED" if props.get("num_portfolio_organizations") else "NOT_COLLECTED",
            "crunchbase_id": identifier.get("permalink"),
        }

        return {
            # EntityBase fields
            "entity_id": None,  # Assigned at resolution
            "canonical_name": canonical_name,
            "entity_type": "investor",
            "entity_subtype": category_fields["investor_subtype"],
            "aliases": [],
            "valid_from": datetime.now(timezone.utc).isoformat(),
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
            "linkedin_url": props.get("linkedin", {}).get("value") if isinstance(props.get("linkedin"), dict) else props.get("linkedin"),
            "linkedin_url_status": "REPORTED" if props.get("linkedin") else "NOT_COLLECTED",
            "twitter_handle": props.get("twitter", {}).get("value") if isinstance(props.get("twitter"), dict) else None,
            "twitter_handle_status": "REPORTED" if props.get("twitter") else "NOT_COLLECTED",

            "description": props.get("short_description"),
            "description_status": "REPORTED" if props.get("short_description") else "NOT_REPORTED",
            "description_source_url": f"https://www.crunchbase.com/organization/{identifier.get('permalink', '')}",

            "external_ids": {
                "crunchbase_id": identifier.get("permalink", ""),
            },

            "source_agent": self.AGENT_NAME,
            "source_run_ids": [run_id],
            "merge_provenance": [],
            "source_urls": [f"https://www.crunchbase.com/organization/{identifier.get('permalink', '')}"],
            "last_seen": datetime.now(timezone.utc).isoformat(),
            "last_verified": None,

            "overall_confidence": "medium",
            "source_count": 1,
            "corroboration_count": 0,

            # Classification flags (zeroed — set by Analysis & Scoring Agent)
            "partner_candidate": False,
            "competitor_candidate": False,
            "blocker_candidate": False,
            "investment_candidate": False,
            "support_candidate": False,
            "recruiter_candidate": False,
            "top_influencer": False,

            # Scores (zeroed — set by Analysis & Scoring Agent)
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

            # Raw entity tracking (pre-resolution)
            "_raw_entity_id": str(uuid.uuid4()),
            "_source": "crunchbase",
        }

    def _infer_investor_subtype(self, props: dict[str, Any]) -> str:
        """Infer investor subtype from Crunchbase category groups."""
        categories = [c.get("value", "").lower() for c in props.get("category_groups", [])]
        if "angel investment" in categories:
            return "angel"
        if "private equity" in categories:
            return "pe"
        if "family office" in " ".join(categories):
            return "family_office"
        if "corporate" in " ".join(categories):
            return "corporate_vc"
        return "vc"  # Default for most investment orgs

    async def _write_crunchbase_evidence(
        self,
        entity: dict[str, Any],
        props: dict[str, Any],
        run_id: str,
    ) -> None:
        """Write evidence records for fields sourced from Crunchbase."""
        identifier = props.get("identifier", {})
        permalink = identifier.get("permalink", "")
        source_url = f"https://www.crunchbase.com/organization/{permalink}"

        evidence_records = []
        base = {
            "entity_id": entity.get("entity_id"),  # May be None pre-resolution — updated later
            "run_id": run_id,
            "source_url": source_url,
            "source_type": "api_response",
            "source_api": "crunchbase",
            "retrieved_at": entity["last_seen"],
            "claim_type": "direct_statement",
            "confidence": "high",
            "agent_name": self.AGENT_NAME,
            "prompt_version": self.AGENT_VERSION,
        }

        if entity.get("canonical_name"):
            evidence_records.append({
                **base,
                "supported_field": "canonical_name",
                "supported_value": entity["canonical_name"],
                "evidence_snippet": f"Crunchbase lists organization name as: {entity['canonical_name']}",
            })
        if entity.get("description"):
            evidence_records.append({
                **base,
                "supported_field": "description",
                "supported_value": entity["description"],
                "evidence_snippet": entity["description"][:500],
            })
        if entity.get("primary_city"):
            evidence_records.append({
                **base,
                "supported_field": "primary_city",
                "supported_value": entity["primary_city"],
                "evidence_snippet": f"Crunchbase location: {entity.get('primary_city')}, {entity.get('primary_state', '')}",
            })

        # Store evidence records on the entity dict for later batch write
        entity["_pending_evidence"] = evidence_records

    # ─────────────────────────────────────────────────────────────────────────
    # SerpAPI collection
    # ─────────────────────────────────────────────────────────────────────────

    async def _collect_from_serpapi(
        self,
        city_name: str,
        country_or_region: str,
        run_id: str,
        targeted_queries: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Query SerpAPI and use LLM extraction for unstructured results."""
        if not settings.serpapi_api_key:
            log.info("InvestorAgent: SerpAPI key not set — skipping")
            await self.write_search_record(
                source_searched="serpapi",
                query_used=f"investors in {city_name}",
                result_found=False,
                entity_type="investor",
                failure_reason="SERPAPI_API_KEY not set",
            )
            return []

        queries = targeted_queries if targeted_queries else [
            f"venture capital firms {city_name} {country_or_region}",
            f"angel investors {city_name} startup ecosystem",
            f"family office investments {city_name}",
        ]

        entities = []
        for query in queries[:3]:  # Max 3 queries per pass to manage budget
            search_start = time.monotonic()
            try:
                response = await self._serpapi.search(query, num=10)
            except Exception as e:
                log.warning("InvestorAgent: SerpAPI search failed for query '%s': %s", query, e)
                await self.write_search_record(
                    source_searched="serpapi",
                    query_used=query,
                    result_found=False,
                    entity_type="investor",
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
                entity_type="investor",
                result_count=len(organic_results),
                response_time_ms=elapsed_ms,
            )

            # Extract entities from each search result using LLM
            for result in organic_results[:5]:  # Top 5 results per query
                extracted = await self._extract_from_serp_result(
                    result, city_name, run_id
                )
                if extracted:
                    entities.append(extracted)

        return entities

    async def _extract_from_serp_result(
        self,
        result: dict[str, Any],
        city_name: str,
        run_id: str,
    ) -> dict[str, Any] | None:
        """Use LLM (qwen3:7b) to extract investor entity from a SerpAPI result."""
        # Build text from result
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
            log.debug("InvestorAgent: LLM extraction failed for result '%s': %s",
                      result.get("title", ""), e)
            return None

        # Validate: must have a name and evidence snippet
        name = extracted_json.get("name")
        evidence_snippet = extracted_json.get("evidence_snippet")
        if not name or not evidence_snippet:
            return None

        # Reject list-article artifacts that slipped through the prompt instruction
        if self.is_garbage_entity_name(name):
            log.debug(
                "InvestorAgent: dropped garbage name from SerpAPI extraction: %r", name
            )
            return None

        source_url = result.get("link", "")
        raw_entity_id = str(uuid.uuid4())

        entity: dict[str, Any] = {
            "entity_id": None,
            "canonical_name": name,
            "entity_type": "investor",
            "entity_subtype": extracted_json.get("entity_subtype") or "vc",
            "aliases": [],
            "valid_from": datetime.now(timezone.utc).isoformat(),
            "valid_to": None,
            "superseded_by": None,

            "primary_city": city_name,
            "primary_city_status": "NOT_COLLECTED",  # Not confirmed from this source
            "primary_state": None,
            "primary_state_status": "NOT_COLLECTED",
            "primary_country": "United States",
            "primary_country_status": "NOT_COLLECTED",

            "website_url": extracted_json.get("website_url"),
            "website_url_status": "REPORTED" if extracted_json.get("website_url") else "NOT_COLLECTED",
            "linkedin_url": extracted_json.get("linkedin_url"),
            "linkedin_url_status": "REPORTED" if extracted_json.get("linkedin_url") else "NOT_COLLECTED",
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
            "last_seen": datetime.now(timezone.utc).isoformat(),
            "last_verified": None,

            "overall_confidence": "low",  # SerpAPI extraction is less reliable
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

            "category_fields": {
                "investor_subtype": extracted_json.get("entity_subtype") or "vc",
                "investment_stage_focus": extracted_json.get("investment_stage_focus", []),
                "investment_stage_focus_status": "REPORTED" if extracted_json.get("investment_stage_focus") else "NOT_COLLECTED",
                "sector_focus": extracted_json.get("sector_focus", []),
                "sector_focus_status": "REPORTED" if extracted_json.get("sector_focus") else "NOT_COLLECTED",
                "managing_partner": extracted_json.get("managing_partner"),
                "managing_partner_status": "REPORTED" if extracted_json.get("managing_partner") else "NOT_COLLECTED",
            },

            "_raw_entity_id": raw_entity_id,
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
                    "retrieved_at": datetime.now(timezone.utc).isoformat(),
                    "evidence_snippet": evidence_snippet[:1000],
                    "claim_type": "inferred",
                    "confidence": "low",
                    "agent_name": self.AGENT_NAME,
                    "prompt_version": self.AGENT_VERSION,
                }
            ],
        }
        return entity

    # ─────────────────────────────────────────────────────────────────────────
    # EDGAR collection (SEC 13F institutional investors)
    # ─────────────────────────────────────────────────────────────────────────

    async def _collect_from_edgar(
        self,
        city_name: str,
        run_id: str,
    ) -> list[dict[str, Any]]:
        """
        Search EDGAR for institutional investment advisers in the city.
        Uses Form ADV filings (Investment Adviser registration).
        """
        search_start = time.monotonic()
        try:
            response = await self._edgar.search_company_name(
                name=f"capital advisors {city_name}",
                city=city_name,
            )
        except Exception as e:
            log.warning("InvestorAgent: EDGAR search failed: %s", e)
            await self.write_search_record(
                source_searched="sec_edgar",
                query_used=f"capital advisors investment {city_name}",
                result_found=False,
                entity_type="investor",
                failure_reason=str(e),
                response_time_ms=int((time.monotonic() - search_start) * 1000),
            )
            return []

        elapsed_ms = int((time.monotonic() - search_start) * 1000)
        hits = response.get("hits", {}).get("hits", [])

        await self.write_search_record(
            source_searched="sec_edgar",
            query_used=f"investment advisers {city_name}",
            result_found=bool(hits),
            entity_type="investor",
            result_count=len(hits),
            response_time_ms=elapsed_ms,
        )

        entities = []
        for hit in hits[:10]:
            source = hit.get("_source", {})
            entity_name = source.get("display_names", [None])[0]
            if not entity_name:
                continue

            cik = source.get("entity_id", "")
            source_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=ADV&dateb=&owner=include&count=10"

            entity: dict[str, Any] = {
                "entity_id": None,
                "canonical_name": entity_name,
                "entity_type": "investor",
                "entity_subtype": "vc",
                "aliases": [],
                "valid_from": datetime.now(timezone.utc).isoformat(),
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
                "last_seen": datetime.now(timezone.utc).isoformat(),
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

                "category_fields": {
                    "investor_subtype": "vc",
                    "sec_crd_number": source.get("crd_number"),
                    "sec_13f_filer": source.get("is_13f_filer", False),
                },

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
                        "retrieved_at": datetime.now(timezone.utc).isoformat(),
                        "evidence_snippet": f"SEC EDGAR lists {entity_name} as a registered investment adviser",
                        "claim_type": "direct_statement",
                        "confidence": "high",
                        "agent_name": self.AGENT_NAME,
                        "prompt_version": self.AGENT_VERSION,
                    }
                ],
            }
            entities.append(entity)

        return entities
