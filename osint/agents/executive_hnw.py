"""
osint/agents/executive_hnw.py

Executive / High-Net-Worth Individual Agent — Phase 1 collection agent.

Collects: C-suite executives, serial founders, prominent operators, board members
of major companies and nonprofits, and high-net-worth individuals with business
roles in the startup ecosystem.

This agent covers INDIVIDUALS with business/operator roles. Elected officials
are covered by politician_agent. Pure philanthropists by philanthropic_agent.

Data sources (in priority order):
1. Crunchbase People   — Founders, executives, investors (person search)
2. Proxycurl LinkedIn  — Enriches high-priority executives with LinkedIn data
                         CAUTION: $0.01/call — must check budget before each call
3. SerpAPI             — Fills gaps for executives not in Crunchbase
4. People Data Labs    — Person enrichment for LinkedIn-confirmed individuals

Output:
- Appends to state["raw_entities"] — raw pre-resolution entity dicts
- Writes evidence records for every sourced field
- Writes search records for every search attempt
- Does NOT resolve or deduplicate

Entity type: "executive_hnw"
Subtypes: ceo | cto | cfo | coo | founder | serial_founder | board_member |
          managing_director | partner | president | vp | angel_operator

Notes:
- Crunchbase People returns individuals who are listed as founders/executives
- We ONLY call Proxycurl for individuals who have a confirmed LinkedIn URL
  from Crunchbase — never call Proxycurl cold without a URL
- confidence_override applies to this type: confidence defaults to "medium"
  even for structured sources (individuals move roles frequently)
- Proxycurl budget is tracked per-run in Redis via RateLimiter
"""

from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from osint.agents.base import BaseAgent
from osint.clients.crunchbase import CrunchbaseClient
from osint.clients.proxycurl import ProxycurlClient
from osint.clients.serpapi import SerpApiClient
from osint.clients.people_data_labs import PeopleDataLabsClient
from osint.core.config import settings
from osint.core.rate_limiter import BudgetExceeded
from osint.llm.routing import TaskType

log = logging.getLogger(__name__)

# Max Proxycurl calls per agent run — budget protection
# At $0.01/call, this caps executive_hnw at $0.50/city
PROXYCURL_MAX_CALLS_PER_RUN = 50

# Crunchbase primary_job_title patterns → our subtypes
TITLE_SUBTYPE_MAP: list[tuple[list[str], str]] = [
    (["ceo", "chief executive", "co-ceo"], "ceo"),
    (["cto", "chief technology", "vp engineering", "head of engineering"], "cto"),
    (["cfo", "chief financial", "chief finance"], "cfo"),
    (["coo", "chief operating", "chief operations"], "coo"),
    (["founder", "co-founder", "cofounder"], "founder"),
    (["managing director", "managing partner"], "managing_director"),
    (["general partner", "gp", "limited partner"], "partner"),
    (["board member", "board director", "chairman", "chairwoman"], "board_member"),
    (["president"], "president"),
    (["vp ", "vice president", "svp", "evp"], "vp"),
    (["angel", "operator angel", "advisor"], "angel_operator"),
]

SERPAPI_EXTRACTION_SYSTEM = """You are a data extraction agent for an OSINT pipeline.
Extract executive or high-net-worth individual information from search result text.
Return ONLY valid JSON. Do not include explanation or commentary.
If a field cannot be determined from the text, use null.
Never fabricate information — only extract what is explicitly stated in the source text."""

SERPAPI_EXTRACTION_PROMPT = """Extract executive or high-net-worth individual information from this search result.

City context: {city_name}

Search result:
---
{search_text}
---

Return a JSON object:
{{
  "name": "<person's full name, or null>",
  "entity_subtype": "<ceo|cto|cfo|coo|founder|serial_founder|board_member|managing_director|partner|president|vp|angel_operator|null>",
  "current_title": "<current job title, or null>",
  "current_company": "<current employer/organization, or null>",
  "description": "<brief factual description from source, or null>",
  "linkedin_url": "<LinkedIn profile URL if present, or null>",
  "net_worth_mention": "<net worth mentioned as string, e.g. '$50M', or null>",
  "notable_companies_founded": ["<companies founded, or empty list>"],
  "board_memberships": ["<board memberships mentioned, or empty list>"],
  "evidence_snippet": "<exact quote supporting this person being an executive in {city_name}>"
}}"""


class ExecutiveHNWAgent(BaseAgent):
    """
    Phase 1 collection agent for executives and HNW operators.
    """

    AGENT_NAME = "executive_hnw_agent"
    AGENT_VERSION = "1.0"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._crunchbase = CrunchbaseClient(self._rl)
        self._proxycurl = ProxycurlClient(self._rl)
        self._serpapi = SerpApiClient(self._rl)
        self._pdl = PeopleDataLabsClient(self._rl)
        self._proxycurl_calls_this_run = 0

    async def run(self, state: dict[str, Any]) -> dict[str, Any]:
        city_name = state["city_name"]
        country_or_region = state.get("country_or_region", "United States")
        run_id = state["run_id"]
        pass_number = state.get("pass_number", 1)
        pass2_targets = state.get("pass2_targets", [])

        log.info("ExecutiveHNWAgent: collecting for %s (pass %d)", city_name, pass_number)

        new_raw_entities: list[dict[str, Any]] = []

        # ── Pass 2 targeting ──────────────────────────────────────────────────
        targeted_queries: list[str] = []
        if pass_number == 2:
            for target in pass2_targets:
                if target.get("entity_type") == "executive_hnw":
                    targeted_queries.extend(target.get("suggested_queries", []))
            targeted_queries = [q for q in targeted_queries if q]

        # ── Source 1: Crunchbase People ───────────────────────────────────────
        cb_entities = await self._collect_from_crunchbase(
            city_name, country_or_region, run_id
        )
        new_raw_entities.extend(cb_entities)
        log.info("ExecutiveHNWAgent: Crunchbase yielded %d raw entities", len(cb_entities))

        # ── Source 2: Proxycurl — enrich LinkedIn-confirmed executives ─────────
        # Only enrich top entities (those from Crunchbase with LinkedIn URLs)
        enriched = await self._enrich_with_proxycurl(cb_entities, run_id)
        log.info("ExecutiveHNWAgent: Proxycurl enriched %d entities", enriched)

        # ── Source 3: SerpAPI ─────────────────────────────────────────────────
        serp_entities = await self._collect_from_serpapi(
            city_name, country_or_region, run_id, targeted_queries=targeted_queries
        )
        new_raw_entities.extend(serp_entities)
        log.info("ExecutiveHNWAgent: SerpAPI yielded %d raw entities", len(serp_entities))

        log.info(
            "ExecutiveHNWAgent: %d total raw entities, %d Proxycurl calls used",
            len(new_raw_entities), self._proxycurl_calls_this_run
        )

        patch: dict[str, Any] = {
            "raw_entities": new_raw_entities,          # delta only
            **self.agent_status_patch("success"),
            **self.token_count_patch(),
            **self.entity_count_patch(),
        }
        return patch

    # ─────────────────────────────────────────────────────────────────────────
    # Crunchbase People
    # ─────────────────────────────────────────────────────────────────────────

    async def _collect_from_crunchbase(
        self,
        city_name: str,
        country_or_region: str,
        run_id: str,
    ) -> list[dict[str, Any]]:
        """
        Search Crunchbase for founders and executives in the city.
        Uses /searches/people endpoint with location filter.
        """
        if not settings.crunchbase_api_key:
            log.info("ExecutiveHNWAgent: Crunchbase key not set — skipping")
            await self.write_search_record(
                source_searched="crunchbase",
                query_used=f"people in {city_name}",
                result_found=False,
                entity_type="executive_hnw",
                failure_reason="CRUNCHBASE_API_KEY not set",
            )
            return []

        search_start = time.monotonic()
        try:
            response = await self._crunchbase.search_people(
                city=city_name,
                country=country_or_region,
                limit=25,
            )
        except Exception as e:
            log.warning("ExecutiveHNWAgent: Crunchbase people search failed: %s", e)
            await self.write_search_record(
                source_searched="crunchbase",
                query_used=f"executives and founders in {city_name}",
                result_found=False,
                entity_type="executive_hnw",
                failure_reason=str(e),
                response_time_ms=int((time.monotonic() - search_start) * 1000),
            )
            return []

        elapsed_ms = int((time.monotonic() - search_start) * 1000)
        people_data = response.get("entities", [])

        await self.write_search_record(
            source_searched="crunchbase",
            query_used=f"executives and founders in {city_name}",
            result_found=bool(people_data),
            entity_type="executive_hnw",
            result_count=len(people_data),
            response_time_ms=elapsed_ms,
        )

        entities = []
        now_iso = datetime.now(timezone.utc).isoformat()

        for raw in people_data:
            props = raw.get("properties", {})
            identifier = props.get("identifier", {})
            first_name = props.get("first_name", "")
            last_name = props.get("last_name", "")
            canonical_name = f"{first_name} {last_name}".strip()

            if not canonical_name:
                canonical_name = identifier.get("value", "")
            if not canonical_name:
                continue

            permalink = identifier.get("permalink", "")
            source_url = f"https://www.crunchbase.com/person/{permalink}"

            primary_job_title = props.get("primary_job_title", "")
            primary_org = props.get("primary_organization", {})
            primary_org_name = primary_org.get("value", "") if isinstance(primary_org, dict) else ""

            subtype = self._infer_executive_subtype(primary_job_title)
            linkedin_url = props.get("linkedin", {}).get("value") if isinstance(props.get("linkedin"), dict) else props.get("linkedin")

            location_ids = props.get("location_identifiers", [])
            location_city = None
            location_state = None
            for loc in location_ids:
                if loc.get("location_type") == "city":
                    location_city = loc.get("value")
                elif loc.get("location_type") == "region":
                    location_state = loc.get("value")

            category_fields: dict[str, Any] = {
                "executive_subtype": subtype,
                "executive_subtype_status": "REPORTED",
                "crunchbase_id": permalink,
                "current_title": primary_job_title,
                "current_title_status": "REPORTED" if primary_job_title else "NOT_COLLECTED",
                "current_company": primary_org_name,
                "current_company_status": "REPORTED" if primary_org_name else "NOT_COLLECTED",
                "num_founded_companies": props.get("num_founded_organizations"),
                "num_portfolio_companies": props.get("num_portfolio_organizations"),
                "num_investments": props.get("num_investments"),
                "gender": props.get("gender"),
                "gender_status": "REPORTED" if props.get("gender") else "NOT_COLLECTED",
            }

            entity: dict[str, Any] = {
                "entity_id": None,
                "canonical_name": canonical_name,
                "entity_type": "executive_hnw",
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

                "website_url": None,
                "website_url_status": "NOT_COLLECTED",
                "linkedin_url": linkedin_url,
                "linkedin_url_status": "REPORTED" if linkedin_url else "NOT_COLLECTED",
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

                # confidence_override: executive_hnw defaults to medium
                # (individuals change roles; CB data can be stale)
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
                        "evidence_snippet": (
                            f"Crunchbase: {canonical_name}"
                            f"{f', {primary_job_title}' if primary_job_title else ''}"
                            f"{f' at {primary_org_name}' if primary_org_name else ''}"
                            f" in {location_city or city_name}"
                        ),
                        "claim_type": "direct_statement",
                        "confidence": "medium",  # CB person data can be stale
                        "agent_name": self.AGENT_NAME,
                        "prompt_version": self.AGENT_VERSION,
                    }
                ],
            }
            entities.append(entity)

        return entities

    def _infer_executive_subtype(self, title: str) -> str:
        """Map job title string to our executive subtype vocabulary."""
        title_lower = title.lower()
        for keywords, subtype in TITLE_SUBTYPE_MAP:
            if any(kw in title_lower for kw in keywords):
                return subtype
        return "ceo"  # Default for unrecognized executive titles

    # ─────────────────────────────────────────────────────────────────────────
    # Proxycurl — LinkedIn enrichment (budget-gated)
    # ─────────────────────────────────────────────────────────────────────────

    async def _enrich_with_proxycurl(
        self,
        entities: list[dict[str, Any]],
        run_id: str,
    ) -> int:
        """
        Enrich entities that have LinkedIn URLs with Proxycurl data.
        Mutates entities in-place — adds enriched fields to category_fields.
        Returns count of entities actually enriched.

        IMPORTANT: Only calls Proxycurl when:
        1. LinkedIn URL is confirmed (from Crunchbase)
        2. Budget is available (checked via RateLimiter)
        3. Per-run cap not exceeded (PROXYCURL_MAX_CALLS_PER_RUN)
        """
        enriched = 0

        for entity in entities:
            if self._proxycurl_calls_this_run >= PROXYCURL_MAX_CALLS_PER_RUN:
                log.info("ExecutiveHNWAgent: Proxycurl per-run cap reached (%d)", PROXYCURL_MAX_CALLS_PER_RUN)
                break

            linkedin_url = entity.get("linkedin_url")
            if not linkedin_url:
                continue

            search_start = time.monotonic()
            try:
                profile = await self._proxycurl.get_person_profile(linkedin_url)
            except BudgetExceeded:
                log.warning("ExecutiveHNWAgent: Proxycurl budget exceeded — stopping enrichment")
                await self.write_search_record(
                    source_searched="proxycurl",
                    query_used=linkedin_url,
                    result_found=False,
                    entity_type="executive_hnw",
                    failure_reason="Budget exceeded",
                    response_time_ms=int((time.monotonic() - search_start) * 1000),
                )
                break
            except Exception as e:
                log.warning("ExecutiveHNWAgent: Proxycurl failed for %s: %s", linkedin_url, e)
                await self.write_search_record(
                    source_searched="proxycurl",
                    query_used=linkedin_url,
                    result_found=False,
                    entity_type="executive_hnw",
                    failure_reason=str(e),
                    response_time_ms=int((time.monotonic() - search_start) * 1000),
                )
                continue

            elapsed_ms = int((time.monotonic() - search_start) * 1000)
            self._proxycurl_calls_this_run += 1

            if not profile:
                await self.write_search_record(
                    source_searched="proxycurl",
                    query_used=linkedin_url,
                    result_found=False,
                    entity_type="executive_hnw",
                    result_count=0,
                    response_time_ms=elapsed_ms,
                )
                continue

            await self.write_search_record(
                source_searched="proxycurl",
                query_used=linkedin_url,
                result_found=True,
                entity_type="executive_hnw",
                result_count=1,
                response_time_ms=elapsed_ms,
            )

            # Merge enriched data into entity's category_fields
            category_fields = entity.get("category_fields", {})

            # LinkedIn-confirmed data upgrades confidence
            if profile.get("full_name"):
                # Confirm canonical name from LinkedIn
                category_fields["linkedin_confirmed_name"] = profile.get("full_name")

            if profile.get("headline"):
                entity["description"] = profile["headline"]
                entity["description_status"] = "REPORTED"
                category_fields["current_title"] = profile.get("headline")
                category_fields["current_title_status"] = "REPORTED"

            if profile.get("experiences"):
                current_exp = next(
                    (e for e in profile["experiences"] if e.get("ends_at") is None), None
                )
                if current_exp:
                    category_fields["current_company"] = current_exp.get("company", category_fields.get("current_company"))
                    category_fields["current_company_status"] = "REPORTED"

            if profile.get("connections"):
                category_fields["linkedin_connections"] = profile["connections"]

            if profile.get("city"):
                if entity.get("primary_city_status") == "NOT_COLLECTED":
                    entity["primary_city"] = profile["city"]
                    entity["primary_city_status"] = "REPORTED"

            # Overall confidence upgrade: LinkedIn-confirmed = high
            entity["overall_confidence"] = "high"
            entity["category_fields"] = category_fields

            # Add Proxycurl as evidence source
            evidence = entity.get("_pending_evidence", [])
            evidence.append({
                "entity_id": None,
                "run_id": run_id,
                "supported_field": "linkedin_url",
                "supported_value": linkedin_url,
                "source_url": linkedin_url,
                "source_type": "api_response",
                "source_api": "proxycurl",
                "retrieved_at": datetime.now(timezone.utc).isoformat(),
                "evidence_snippet": (
                    f"Proxycurl LinkedIn profile: {profile.get('full_name', entity['canonical_name'])}"
                    + (f", {profile.get('headline', '')}" if profile.get('headline') else "")
                ),
                "claim_type": "direct_statement",
                "confidence": "high",
                "agent_name": self.AGENT_NAME,
                "prompt_version": self.AGENT_VERSION,
            })
            entity["_pending_evidence"] = evidence

            enriched += 1

        return enriched

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
        """Search for prominent executives and operators not in Crunchbase."""
        if not settings.serpapi_api_key:
            log.info("ExecutiveHNWAgent: SerpAPI key not set — skipping")
            await self.write_search_record(
                source_searched="serpapi",
                query_used=f"executives {city_name}",
                result_found=False,
                entity_type="executive_hnw",
                failure_reason="SERPAPI_API_KEY not set",
            )
            return []

        queries = targeted_queries if targeted_queries else [
            f"top CEOs founders executives {city_name} startup tech",
            f"serial entrepreneur high net worth {city_name}",
            f"prominent business leaders {city_name}",
        ]

        entities = []
        for query in queries[:3]:
            search_start = time.monotonic()
            try:
                response = await self._serpapi.search(query, num=10)
            except Exception as e:
                log.warning("ExecutiveHNWAgent: SerpAPI '%s' failed: %s", query, e)
                await self.write_search_record(
                    source_searched="serpapi",
                    query_used=query,
                    result_found=False,
                    entity_type="executive_hnw",
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
                entity_type="executive_hnw",
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
        """LLM extraction for SerpAPI executive results."""
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
            log.debug("ExecutiveHNWAgent: LLM extraction failed: %s", e)
            return None

        name = extracted_json.get("name")
        evidence_snippet = extracted_json.get("evidence_snippet")
        if not name or not evidence_snippet:
            return None

        source_url = result.get("link", "")
        now_iso = datetime.now(timezone.utc).isoformat()
        subtype = extracted_json.get("entity_subtype") or "ceo"

        category_fields: dict[str, Any] = {
            "executive_subtype": subtype,
            "executive_subtype_status": "REPORTED",
            "current_title": extracted_json.get("current_title"),
            "current_title_status": "REPORTED" if extracted_json.get("current_title") else "NOT_COLLECTED",
            "current_company": extracted_json.get("current_company"),
            "current_company_status": "REPORTED" if extracted_json.get("current_company") else "NOT_COLLECTED",
            "net_worth_mention": extracted_json.get("net_worth_mention"),
            "notable_companies_founded": extracted_json.get("notable_companies_founded", []),
            "board_memberships": extracted_json.get("board_memberships", []),
        }

        linkedin_url = extracted_json.get("linkedin_url")

        return {
            "entity_id": None,
            "canonical_name": name,
            "entity_type": "executive_hnw",
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
            "linkedin_url": linkedin_url,
            "linkedin_url_status": "REPORTED" if linkedin_url else "NOT_COLLECTED",
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
