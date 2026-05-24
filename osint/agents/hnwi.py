"""
osint/agents/hnwi.py

High-Net-Worth Individual (HNWI) Agent — Phase 1 collection agent.

Collects: Wealthy individuals who are NOT primarily classified as investors,
executives, or philanthropists. This includes old-money families, real estate
magnates, sports team owners, prominent attorneys, heir/heiresses, and other
wealth-holders with ecosystem influence but no clear business role.

This agent specifically targets HNWIs where wealth is the primary signal,
rather than a role-based identity.

Data sources:
1. EDGAR EFTS    — Insider transactions (Form 4) reveal wealthy shareholders
2. SerpAPI       — Searches for wealthy residents, real estate owners, etc.
3. People Data Labs — Enrichment for identified HNWIs

Output:
- Appends to state["raw_entities"] — raw pre-resolution entity dicts
- Writes evidence records for every sourced field
- Writes search records for every search attempt
- Does NOT resolve or deduplicate

Entity type: "hnwi"
Subtypes: real_estate | old_money | sports_owner | attorney | heiress_heir |
          family_wealth | media_owner | tech_wealth | resource_wealth

Notes:
- EDGAR Form 4 insider transactions are the most reliable public signal of
  wealth — they disclose stock holdings that are often worth millions
- SerpAPI searches target wealth-signaling terms: real estate portfolio,
  family trust, philanthropist (crossover), net worth
- People Data Labs enrichment is called for high-confidence names only
  (confirmed from EDGAR or multiple SerpAPI sources)
- All HNWI entities start as needs_review=False but the Verification Agent
  may upgrade this for sensitive wealth claims
"""

from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from osint.agents.base import BaseAgent
from osint.clients.edgar import EdgarClient
from osint.clients.serpapi import SerpApiClient
from osint.clients.people_data_labs import PeopleDataLabsClient
from osint.core.config import settings
from osint.llm.routing import TaskType

log = logging.getLogger(__name__)

SERPAPI_EXTRACTION_SYSTEM = """You are a data extraction agent for an OSINT pipeline.
Extract high-net-worth individual information from search result text.
Return ONLY valid JSON. Do not include explanation or commentary.
If a field cannot be determined from the text, use null.
Never fabricate information — only extract what is explicitly stated in the source text.
HNWIs are wealthy individuals whose primary identity is wealth-based, not a specific job role."""

SERPAPI_EXTRACTION_PROMPT = """Extract high-net-worth individual information from this search result.

City context: {city_name}

Search result:
---
{search_text}
---

Return a JSON object:
{{
  "name": "<person's full name, or null>",
  "entity_subtype": "<real_estate|old_money|sports_owner|attorney|heiress_heir|family_wealth|media_owner|tech_wealth|resource_wealth|null>",
  "wealth_source": "<primary source of wealth, e.g. 'real estate portfolio', 'tech exits', or null>",
  "estimated_net_worth": "<net worth as string if mentioned, e.g. '$200M', or null>",
  "primary_holdings": ["<major assets or holdings mentioned, or empty list>"],
  "affiliated_entities": ["<companies, families, foundations mentioned, or empty list>"],
  "real_estate_focus": "<true if primarily real estate wealth, false otherwise>",
  "evidence_snippet": "<exact quote from the text that demonstrates this person's wealth or assets in {city_name}>"
}}"""


class HNWIAgent(BaseAgent):
    """
    Phase 1 collection agent for high-net-worth individuals.
    """

    AGENT_NAME = "hnwi_agent"
    AGENT_VERSION = "1.0"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._edgar = EdgarClient(self._rl)
        self._serpapi = SerpApiClient(self._rl)
        self._pdl = PeopleDataLabsClient(self._rl)

    async def run(self, state: dict[str, Any]) -> dict[str, Any]:
        city_name = state["city_name"]
        country_or_region = state.get("country_or_region", "United States")
        run_id = state["run_id"]
        pass_number = state.get("pass_number", 1)
        pass2_targets = state.get("pass2_targets", [])

        log.info("HNWIAgent: collecting for %s (pass %d)", city_name, pass_number)

        new_raw_entities: list[dict[str, Any]] = []

        # ── Pass 2 targeting ──────────────────────────────────────────────────
        targeted_queries: list[str] = []
        if pass_number == 2:
            for target in pass2_targets:
                if target.get("entity_type") == "hnwi":
                    targeted_queries.extend(target.get("suggested_queries", []))
            targeted_queries = [q for q in targeted_queries if q]

        # ── Source 1: EDGAR Form 4 insider transactions ───────────────────────
        edgar_entities = await self._collect_from_edgar(city_name, run_id)
        new_raw_entities.extend(edgar_entities)
        log.info("HNWIAgent: EDGAR yielded %d raw entities", len(edgar_entities))

        # ── Source 2: SerpAPI ─────────────────────────────────────────────────
        serp_entities = await self._collect_from_serpapi(
            city_name, run_id, targeted_queries=targeted_queries
        )
        new_raw_entities.extend(serp_entities)
        log.info("HNWIAgent: SerpAPI yielded %d raw entities", len(serp_entities))

        log.info("HNWIAgent: %d total raw entities collected", len(new_raw_entities))

        patch: dict[str, Any] = {
            "raw_entities": new_raw_entities,          # delta only
            **self.agent_status_patch("success"),
            **self.token_count_patch(),
            **self.entity_count_patch(),
        }
        return patch

    # ─────────────────────────────────────────────────────────────────────────
    # EDGAR Form 4 — insider transactions
    # ─────────────────────────────────────────────────────────────────────────

    async def _collect_from_edgar(
        self,
        city_name: str,
        run_id: str,
    ) -> list[dict[str, Any]]:
        """
        Search EDGAR for recent Form 4 filings (insider transactions) from
        individuals with addresses in the city. Form 4 filers hold significant
        company stock — they are definitionally high-net-worth.

        EDGAR EFTS doesn't directly filter Form 4 by city, so we search
        for filers with the city in their name/address fields.
        """
        search_start = time.monotonic()
        try:
            # Search for Form 4 filers with city name
            response = await self._edgar.search_filings(
                query=city_name,
                form_type="4",  # Form 4 = insider transaction
            )
        except Exception as e:
            log.warning("HNWIAgent: EDGAR Form 4 search failed: %s", e)
            await self.write_search_record(
                source_searched="sec_edgar",
                query_used=f"Form 4 insider transactions {city_name}",
                result_found=False,
                entity_type="hnwi",
                failure_reason=str(e),
                response_time_ms=int((time.monotonic() - search_start) * 1000),
            )
            return []

        elapsed_ms = int((time.monotonic() - search_start) * 1000)
        hits = response.get("hits", {}).get("hits", [])

        await self.write_search_record(
            source_searched="sec_edgar",
            query_used=f"Form 4 insider transactions in {city_name}",
            result_found=bool(hits),
            entity_type="hnwi",
            result_count=len(hits),
            response_time_ms=elapsed_ms,
        )

        entities = []
        seen_names: set[str] = set()
        now_iso = datetime.now(timezone.utc).isoformat()

        for hit in hits[:20]:
            source = hit.get("_source", {})
            # Form 4 _source contains: entity_name (company), display_names (filer)
            display_names = source.get("display_names", [])
            entity_name = display_names[0] if display_names else source.get("entity_name", "")

            if not entity_name or entity_name in seen_names:
                continue

            # Skip if entity looks like a company (contains Inc, Corp, LLC, etc.)
            name_lower = entity_name.lower()
            if any(sfx in name_lower for sfx in [" inc", " corp", " llc", " ltd", " lp ",
                                                   "fund", "trust ", "management"]):
                continue

            seen_names.add(entity_name)

            cik = source.get("entity_id", "")
            filing_date = source.get("period_of_report", "")
            source_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=4&dateb=&owner=include&count=10" if cik else ""

            # Infer HNWI subtype from filing context
            # We don't have enough data yet to subtype accurately — default to tech_wealth
            # (most Form 4 filers are tech executives/major shareholders)
            subtype = "tech_wealth"

            category_fields: dict[str, Any] = {
                "hnwi_subtype": subtype,
                "hnwi_subtype_status": "NOT_COLLECTED",  # Cannot confirm from Form 4 alone
                "sec_cik": cik,
                "sec_cik_status": "REPORTED" if cik else "NOT_COLLECTED",
                "insider_transaction_filer": True,
                "last_form4_date": filing_date,
            }

            entity: dict[str, Any] = {
                "entity_id": None,
                "canonical_name": entity_name,
                "entity_type": "hnwi",
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
                        "evidence_snippet": (
                            f"SEC EDGAR Form 4: {entity_name} filed an insider transaction report"
                            f"{f' on {filing_date}' if filing_date else ''}"
                            f", indicating significant stock ownership (CIK: {cik})"
                        ),
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
    # SerpAPI
    # ─────────────────────────────────────────────────────────────────────────

    async def _collect_from_serpapi(
        self,
        city_name: str,
        run_id: str,
        targeted_queries: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Search for HNWIs via wealth-focused queries."""
        if not settings.serpapi_api_key:
            log.info("HNWIAgent: SerpAPI key not set — skipping")
            await self.write_search_record(
                source_searched="serpapi",
                query_used=f"wealthy individuals {city_name}",
                result_found=False,
                entity_type="hnwi",
                failure_reason="SERPAPI_API_KEY not set",
            )
            return []

        queries = targeted_queries if targeted_queries else [
            f"wealthiest residents billionaires millionaires {city_name}",
            f"real estate developer owner {city_name} wealthy",
            f"family office old money wealth {city_name}",
        ]

        entities = []
        for query in queries[:3]:
            search_start = time.monotonic()
            try:
                response = await self._serpapi.search(query, num=10)
            except Exception as e:
                log.warning("HNWIAgent: SerpAPI '%s' failed: %s", query[:50], e)
                await self.write_search_record(
                    source_searched="serpapi",
                    query_used=query,
                    result_found=False,
                    entity_type="hnwi",
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
                entity_type="hnwi",
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
        """LLM extraction for SerpAPI HNWI results."""
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
            log.debug("HNWIAgent: LLM extraction failed: %s", e)
            return None

        name = extracted_json.get("name")
        evidence_snippet = extracted_json.get("evidence_snippet")
        if not name or not evidence_snippet:
            return None

        source_url = result.get("link", "")
        now_iso = datetime.now(timezone.utc).isoformat()
        subtype = extracted_json.get("entity_subtype") or "family_wealth"

        category_fields: dict[str, Any] = {
            "hnwi_subtype": subtype,
            "hnwi_subtype_status": "REPORTED",
            "wealth_source": extracted_json.get("wealth_source"),
            "wealth_source_status": "REPORTED" if extracted_json.get("wealth_source") else "NOT_COLLECTED",
            "estimated_net_worth_text": extracted_json.get("estimated_net_worth"),
            "estimated_net_worth_text_status": "REPORTED" if extracted_json.get("estimated_net_worth") else "NOT_COLLECTED",
            "primary_holdings": extracted_json.get("primary_holdings", []),
            "affiliated_entities": extracted_json.get("affiliated_entities", []),
            "real_estate_focus": extracted_json.get("real_estate_focus", False),
        }

        return {
            "entity_id": None,
            "canonical_name": name,
            "entity_type": "hnwi",
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
