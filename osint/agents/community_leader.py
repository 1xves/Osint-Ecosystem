"""
osint/agents/community_leader.py

Community Leader Intelligence Agent — Phase 1 collection agent.

Collects: Civic connectors, faith leaders, neighborhood organizers, cultural
leaders, informal power brokers, community foundation officers, HBCU/university
presidents, local media personalities with civic influence, and others who hold
social capital without formal corporate or government titles.

This is the most qualitative agent — there is no structured source that catalogs
community leaders. Everything comes from SerpAPI + GDELT.

Data sources:
1. SerpAPI — Web search for community leaders, influencers, civic figures
2. GDELT   — News mentions of local leaders in civic/community context

Output:
- Appends to state["raw_entities"] — raw pre-resolution entity dicts
- Writes evidence records for every sourced field
- Writes search records for every search attempt
- Does NOT resolve or deduplicate

Entity type: "community_leader"
Subtypes: civic_connector | faith_leader | neighborhood_organizer | cultural_leader |
          media_personality | university_president | informal_broker | labor_leader |
          community_foundation_officer

CRITICAL SCHEMA NOTE:
community_leader entities have confidence_override = "low" hardcoded in the schema.
This agent MUST set overall_confidence="low" for ALL entities regardless of source
quality. This is by design — community leaders are the most subjective category.

Notes:
- GDELT queries use the GKG (Global Knowledge Graph) which tags articles with
  persons and organizations. We filter for city-relevant people with civic themes.
- SerpAPI queries use multiple framing angles to capture different types of leaders
- The orchestrator's framings (especially "practitioner" framing) should inform
  what angles to search from — but we use static queries for Phase 1
"""

from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from osint.agents.base import BaseAgent
from osint.clients.serpapi import SerpApiClient
from osint.clients.gdelt import GDELTClient
from osint.core.config import settings
from osint.llm.routing import TaskType

log = logging.getLogger(__name__)

# Search query templates for different leader types
SERP_QUERY_TEMPLATES = [
    "{city_name} community leader civic activist organizer",
    "{city_name} faith leader pastor church community influential",
    "{city_name} neighborhood association president community board",
    "{city_name} HBCU president university community civic engagement",
    "{city_name} cultural leader arts community foundation officer",
]

# GDELT query templates — searches news for community-relevant mentions
GDELT_QUERY_TEMPLATES = [
    "{city_name} community leader nonprofit civic",
    "{city_name} neighborhood organizer activist",
]

SERPAPI_EXTRACTION_SYSTEM = """You are a data extraction agent for an OSINT pipeline.
Extract community leader information from search result text.
Return ONLY valid JSON. Do not include explanation or commentary.
If a field cannot be determined from the text, use null.
Never fabricate information — only extract what is explicitly stated in the source text.
Community leaders are people with informal civic influence — not politicians, executives, or philanthropists."""

SERPAPI_EXTRACTION_PROMPT = """Extract community leader information from this search result.

City context: {city_name}

Search result:
---
{search_text}
---

Return a JSON object:
{{
  "name": "<person's full name, or null>",
  "entity_subtype": "<civic_connector|faith_leader|neighborhood_organizer|cultural_leader|media_personality|university_president|informal_broker|labor_leader|community_foundation_officer|null>",
  "title_or_role": "<their title or role in the community, or null>",
  "affiliated_organization": "<primary organization they are associated with, or null>",
  "neighborhood_or_district": "<specific neighborhood, ward, or district if mentioned, or null>",
  "cause_or_focus": "<the community cause or issue they champion, or null>",
  "influence_type": "<how they exercise influence: organizing|media|faith|culture|labor|education|null>",
  "evidence_snippet": "<exact quote from the text that demonstrates this person's community influence in {city_name}>"
}}"""


class CommunityLeaderAgent(BaseAgent):
    """
    Phase 1 collection agent for community leaders and civic influencers.
    All entities produced have overall_confidence='low' (confidence_override).
    """

    AGENT_NAME = "community_leader_agent"
    AGENT_VERSION = "1.0"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._serpapi = SerpApiClient(self._rl)
        self._gdelt = GDELTClient(self._rl)

    async def run(self, state: dict[str, Any]) -> dict[str, Any]:
        city_name = state["city_name"]
        country_or_region = state.get("country_or_region", "United States")
        run_id = state["run_id"]
        pass_number = state.get("pass_number", 1)
        pass2_targets = state.get("pass2_targets", [])
        scope_parameters = state.get("scope_parameters", {})
        framings = state.get("framings", [])

        log.info("CommunityLeaderAgent: collecting for %s (pass %d)", city_name, pass_number)

        new_raw_entities: list[dict[str, Any]] = []

        # ── Pass 2 targeting ──────────────────────────────────────────────────
        targeted_queries: list[str] = []
        if pass_number == 2:
            for target in pass2_targets:
                if target.get("entity_type") == "community_leader":
                    targeted_queries.extend(target.get("suggested_queries", []))
            targeted_queries = [q for q in targeted_queries if q]

        # Extract "practitioner" framing search angle if available
        # This gives city-specific angles from the orchestrator's analysis
        practitioner_angle = self._extract_practitioner_angle(framings)

        # ── Source 1: SerpAPI ─────────────────────────────────────────────────
        serp_entities = await self._collect_from_serpapi(
            city_name, run_id,
            targeted_queries=targeted_queries,
            extra_query=practitioner_angle,
        )
        new_raw_entities.extend(serp_entities)
        log.info("CommunityLeaderAgent: SerpAPI yielded %d raw entities", len(serp_entities))

        # ── Source 2: GDELT ───────────────────────────────────────────────────
        gdelt_entities = await self._collect_from_gdelt(city_name, run_id)
        new_raw_entities.extend(gdelt_entities)
        log.info("CommunityLeaderAgent: GDELT yielded %d raw entities", len(gdelt_entities))

        log.info("CommunityLeaderAgent: %d total raw entities collected", len(new_raw_entities))

        patch: dict[str, Any] = {
            "raw_entities": new_raw_entities,          # delta only
            **self.agent_status_patch("success"),
            **self.token_count_patch(),
            **self.entity_count_patch(),
        }
        return patch

    def _extract_practitioner_angle(self, framings: list[dict[str, Any]]) -> str | None:
        """
        Extract the 'practitioner' framing's search_angle to use as an extra query.
        This grounds the search in the orchestrator's city-specific analysis.
        """
        for framing in framings:
            if framing.get("framing_type") == "practitioner":
                angle = framing.get("search_angle", "")
                if angle:
                    log.info("CommunityLeaderAgent: using practitioner angle: %s", angle[:100])
                    return angle
        return None

    # ─────────────────────────────────────────────────────────────────────────
    # SerpAPI
    # ─────────────────────────────────────────────────────────────────────────

    async def _collect_from_serpapi(
        self,
        city_name: str,
        run_id: str,
        targeted_queries: list[str] | None = None,
        extra_query: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Run multiple search angles to discover community leaders.
        Uses more queries than other agents (5 templates) because community
        leaders are harder to find and require diverse search angles.
        """
        if not settings.serpapi_api_key:
            log.info("CommunityLeaderAgent: SerpAPI key not set — skipping")
            await self.write_search_record(
                source_searched="serpapi",
                query_used=f"community leaders {city_name}",
                result_found=False,
                entity_type="community_leader",
                failure_reason="SERPAPI_API_KEY not set",
            )
            return []

        if targeted_queries:
            queries = targeted_queries[:3]
        else:
            queries = [
                t.format(city_name=city_name)
                for t in SERP_QUERY_TEMPLATES
            ]
            if extra_query:
                # Prepend practitioner-framed query as highest priority
                queries = [extra_query] + queries
            queries = queries[:4]  # Cap at 4 — budget conservation

        entities = []
        for query in queries:
            search_start = time.monotonic()
            try:
                response = await self._serpapi.search(query, num=10)
            except Exception as e:
                log.warning("CommunityLeaderAgent: SerpAPI '%s' failed: %s", query[:50], e)
                await self.write_search_record(
                    source_searched="serpapi",
                    query_used=query,
                    result_found=False,
                    entity_type="community_leader",
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
                entity_type="community_leader",
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
        """LLM extraction for SerpAPI community leader results."""
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
            log.debug("CommunityLeaderAgent: LLM extraction failed: %s", e)
            return None

        name = extracted_json.get("name")
        evidence_snippet = extracted_json.get("evidence_snippet")
        if not name or not evidence_snippet:
            return None

        source_url = result.get("link", "")
        now_iso = datetime.now(timezone.utc).isoformat()
        subtype = extracted_json.get("entity_subtype") or "civic_connector"

        category_fields: dict[str, Any] = {
            "community_leader_subtype": subtype,
            "community_leader_subtype_status": "REPORTED",
            "title_or_role": extracted_json.get("title_or_role"),
            "title_or_role_status": "REPORTED" if extracted_json.get("title_or_role") else "NOT_COLLECTED",
            "affiliated_organization": extracted_json.get("affiliated_organization"),
            "affiliated_organization_status": "REPORTED" if extracted_json.get("affiliated_organization") else "NOT_COLLECTED",
            "neighborhood_or_district": extracted_json.get("neighborhood_or_district"),
            "neighborhood_or_district_status": "REPORTED" if extracted_json.get("neighborhood_or_district") else "NOT_COLLECTED",
            "cause_or_focus": extracted_json.get("cause_or_focus"),
            "cause_or_focus_status": "REPORTED" if extracted_json.get("cause_or_focus") else "NOT_COLLECTED",
            "influence_type": extracted_json.get("influence_type"),
            "influence_type_status": "REPORTED" if extracted_json.get("influence_type") else "NOT_COLLECTED",
            # confidence_override: always low for community_leader type
            "confidence_override": "low",
        }

        return {
            "entity_id": None,
            "canonical_name": name,
            "entity_type": "community_leader",
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

            # confidence_override = "low" is mandatory for community_leader type
            "overall_confidence": "low",
            "source_count": 1,
            "corroboration_count": 0,

            "partner_candidate": False,
            "competitor_candidate": False,
            "blocker_candidate": False,
            "investment_candidate": False,
            "support_candidate": True,  # Community leaders are often support/ally targets
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

    # ─────────────────────────────────────────────────────────────────────────
    # GDELT — news-based discovery
    # ─────────────────────────────────────────────────────────────────────────

    async def _collect_from_gdelt(
        self,
        city_name: str,
        run_id: str,
    ) -> list[dict[str, Any]]:
        """
        Search GDELT for news articles mentioning community leaders in the city.
        GDELT covers a vast range of news sources and is good for discovering
        less prominent figures who appear in local media.
        """
        entities = []

        for query_template in GDELT_QUERY_TEMPLATES:
            query = query_template.format(city_name=city_name)
            search_start = time.monotonic()

            try:
                response = await self._gdelt.search_articles(
                    query=query,
                    mode="artlist",
                    maxrecords=10,
                    sort="HybridRel",
                )
            except Exception as e:
                log.warning("CommunityLeaderAgent: GDELT query '%s' failed: %s", query[:50], e)
                await self.write_search_record(
                    source_searched="gdelt",
                    query_used=query,
                    result_found=False,
                    entity_type="community_leader",
                    failure_reason=str(e),
                    response_time_ms=int((time.monotonic() - search_start) * 1000),
                )
                continue

            elapsed_ms = int((time.monotonic() - search_start) * 1000)
            articles = response.get("articles", [])

            await self.write_search_record(
                source_searched="gdelt",
                query_used=query,
                result_found=bool(articles),
                entity_type="community_leader",
                result_count=len(articles),
                response_time_ms=elapsed_ms,
            )

            # Extract community leaders from article titles/snippets via LLM
            for article in articles[:5]:
                extracted = await self._extract_from_gdelt_article(
                    article, city_name, run_id
                )
                if extracted:
                    entities.append(extracted)

        return entities

    async def _extract_from_gdelt_article(
        self,
        article: dict[str, Any],
        city_name: str,
        run_id: str,
    ) -> dict[str, Any] | None:
        """LLM extraction for GDELT articles."""
        # GDELT article format: url, title, seendate, domain, language, sourcecountry
        title = article.get("title", "")
        url = article.get("url", "")

        if not title:
            return None

        search_text = (
            f"Title: {title}\n"
            f"URL: {url}\n"
            f"Source: {article.get('domain', '')}\n"
            f"Date: {article.get('seendate', '')}\n"
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
            log.debug("CommunityLeaderAgent: GDELT LLM extraction failed: %s", e)
            return None

        name = extracted_json.get("name")
        evidence_snippet = extracted_json.get("evidence_snippet")
        if not name or not evidence_snippet:
            return None

        now_iso = datetime.now(timezone.utc).isoformat()
        subtype = extracted_json.get("entity_subtype") or "civic_connector"

        category_fields: dict[str, Any] = {
            "community_leader_subtype": subtype,
            "community_leader_subtype_status": "REPORTED",
            "title_or_role": extracted_json.get("title_or_role"),
            "title_or_role_status": "REPORTED" if extracted_json.get("title_or_role") else "NOT_COLLECTED",
            "affiliated_organization": extracted_json.get("affiliated_organization"),
            "affiliated_organization_status": "REPORTED" if extracted_json.get("affiliated_organization") else "NOT_COLLECTED",
            "cause_or_focus": extracted_json.get("cause_or_focus"),
            "cause_or_focus_status": "REPORTED" if extracted_json.get("cause_or_focus") else "NOT_COLLECTED",
            "confidence_override": "low",
            "gdelt_article_date": article.get("seendate"),
        }

        return {
            "entity_id": None,
            "canonical_name": name,
            "entity_type": "community_leader",
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
            "description_source_url": url,

            "external_ids": {},
            "source_agent": self.AGENT_NAME,
            "source_run_ids": [run_id],
            "merge_provenance": [],
            "source_urls": [url] if url else [],
            "last_seen": now_iso,
            "last_verified": None,

            "overall_confidence": "low",
            "source_count": 1,
            "corroboration_count": 0,

            "partner_candidate": False,
            "competitor_candidate": False,
            "blocker_candidate": False,
            "investment_candidate": False,
            "support_candidate": True,
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
            "_source": "gdelt",
            "_pending_evidence": [
                {
                    "entity_id": None,
                    "run_id": run_id,
                    "supported_field": "canonical_name",
                    "supported_value": name,
                    "source_url": url,
                    "source_type": "news_article",
                    "source_api": "gdelt",
                    "retrieved_at": now_iso,
                    "evidence_snippet": evidence_snippet[:1000],
                    "claim_type": "inferred",
                    "confidence": "low",
                    "agent_name": self.AGENT_NAME,
                    "prompt_version": self.AGENT_VERSION,
                }
            ],
        }
