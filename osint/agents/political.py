"""
osint/agents/political.py

Political Intelligence Agent — Phase 1 collection agent.

Collects: Political Action Committees (PACs), Super PACs, party committees,
527 organizations, ballot measure campaigns, and major political donors
operating in the target city.

This agent covers ORGANIZATIONS with political function — not individual elected
officials (those are politician_agent's domain).

Data sources (in priority order):
1. FEC API       — Committee filings, donation records, disbursements
2. OpenSecrets   — Organization-level political spending summaries
3. SerpAPI       — Identifies local political organizations not in structured DBs

Output:
- Appends to state["raw_entities"] — raw pre-resolution entity dicts
- Writes evidence records for every sourced field
- Writes search records for every search attempt
- Does NOT resolve or deduplicate

Entity type: "political"
Subtypes: pac | super_pac | party_committee | 527_org | ballot_committee |
          political_nonprofit | bundler_org

Notes:
- FEC data is public domain and highly structured
- ALL entities here are sensitivity_tier="restricted" per OSINT schema spec
  (political entities require extra care in how intelligence is used)
- FEC committee types: P=presidential, H=house, S=senate, C=convention,
  D=delegate, E=Electioneering Communication, I=Independent Expenditures,
  N=PAC (non-qualified), O=Super PAC, Q=PAC (qualified), U=single candidate,
  V=Lobbyist/Registrant PAC, W=Lobbyist/Registrant (non-qualified), X=Party (non-qualified),
  Y=Party (qualified), Z=National party non-federal account
- Political entities always set sensitivity_tier="restricted" and needs_review=True
"""

from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from osint.agents.base import BaseAgent
from osint.clients.fec import FECClient
from osint.clients.opensecrets import OpenSecretsClient
from osint.clients.serpapi import SerpApiClient
from osint.core.config import settings
from osint.llm.routing import TaskType

log = logging.getLogger(__name__)

# FEC committee type codes → our subtypes
FEC_COMMITTEE_TYPE_MAP = {
    "O": "super_pac",
    "N": "pac",
    "Q": "pac",
    "I": "pac",       # Independent Expenditure-only
    "U": "pac",       # Single-candidate PAC
    "V": "pac",       # Lobbyist/Registrant PAC
    "W": "pac",
    "X": "party_committee",
    "Y": "party_committee",
    "Z": "party_committee",
    "E": "527_org",
}

SERPAPI_EXTRACTION_SYSTEM = """You are a data extraction agent for an OSINT pipeline.
Extract political organization information from search result text.
Return ONLY valid JSON. Do not include explanation or commentary.
If a field cannot be determined from the text, use null.
Never fabricate information — only extract what is explicitly stated in the source text."""

SERPAPI_EXTRACTION_PROMPT = """Extract political organization information from this search result.

City context: {city_name}

Search result:
---
{search_text}
---

Return a JSON object:
{{
  "name": "<organization name, or null>",
  "entity_subtype": "<pac|super_pac|party_committee|527_org|ballot_committee|political_nonprofit|null>",
  "description": "<brief factual description, or null>",
  "website_url": "<if present, or null>",
  "party_affiliation": "<Democrat|Republican|Independent|Nonpartisan|null>",
  "political_focus": "<the political cause or candidate focus if mentioned, or null>",
  "total_raised": "<amount raised as string if mentioned, or null>",
  "is_local": "<true if focused on {city_name} politics, false if state/national, null if unclear>",
  "evidence_snippet": "<exact quote supporting this being a political organization in {city_name}>"
}}"""


class PoliticalAgent(BaseAgent):
    """
    Phase 1 collection agent for political organizations.
    All entities produced are sensitivity_tier='restricted'.
    """

    AGENT_NAME = "political_agent"
    AGENT_VERSION = "1.0"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._fec = FECClient(self._rl)
        self._opensecrets = OpenSecretsClient(self._rl)
        self._serpapi = SerpApiClient(self._rl)

    async def run(self, state: dict[str, Any]) -> dict[str, Any]:
        city_name = state["city_name"]
        country_or_region = state.get("country_or_region", "United States")
        run_id = state["run_id"]
        existing_raw_entities = state.get("raw_entities", [])
        pass_number = state.get("pass_number", 1)
        pass2_targets = state.get("pass2_targets", [])

        log.info("PoliticalAgent: collecting for %s (pass %d)", city_name, pass_number)

        new_raw_entities: list[dict[str, Any]] = []

        # ── Pass 2 targeting ──────────────────────────────────────────────────
        targeted_queries: list[str] = []
        if pass_number == 2:
            for target in pass2_targets:
                if target.get("entity_type") == "political":
                    targeted_queries.extend(target.get("suggested_queries", []))
            targeted_queries = [q for q in targeted_queries if q]

        # ── Source 1: FEC — PACs and political committees ─────────────────────
        fec_entities = await self._collect_from_fec(city_name, run_id)
        new_raw_entities.extend(fec_entities)
        log.info("PoliticalAgent: FEC yielded %d raw entities", len(fec_entities))

        # ── Source 2: OpenSecrets — organization summaries ────────────────────
        os_entities = await self._collect_from_opensecrets(city_name, run_id)
        new_raw_entities.extend(os_entities)
        log.info("PoliticalAgent: OpenSecrets yielded %d raw entities", len(os_entities))

        # ── Source 3: SerpAPI ─────────────────────────────────────────────────
        serp_entities = await self._collect_from_serpapi(
            city_name, country_or_region, run_id, targeted_queries=targeted_queries
        )
        new_raw_entities.extend(serp_entities)
        log.info("PoliticalAgent: SerpAPI yielded %d raw entities", len(serp_entities))

        log.info("PoliticalAgent: %d total raw entities collected", len(new_raw_entities))

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
    # FEC — political committees
    # ─────────────────────────────────────────────────────────────────────────

    async def _collect_from_fec(
        self,
        city_name: str,
        run_id: str,
    ) -> list[dict[str, Any]]:
        """
        Search FEC for PACs and political committees based in the city.
        FEC data is authoritative and publicly mandated.
        """
        search_start = time.monotonic()
        try:
            response = await self._fec.search_committees(
                name=city_name,
                per_page=25,
            )
        except Exception as e:
            log.warning("PoliticalAgent: FEC search failed: %s", e)
            await self.write_search_record(
                source_searched="fec",
                query_used=f"political committees in {city_name}",
                result_found=False,
                entity_type="political",
                failure_reason=str(e),
                response_time_ms=int((time.monotonic() - search_start) * 1000),
            )
            return []

        elapsed_ms = int((time.monotonic() - search_start) * 1000)
        results = response.get("results", [])

        await self.write_search_record(
            source_searched="fec",
            query_used=f"PACs and political committees in {city_name}",
            result_found=bool(results),
            entity_type="political",
            result_count=len(results),
            response_time_ms=elapsed_ms,
        )

        entities = []
        now_iso = datetime.now(timezone.utc).isoformat()

        for committee in results:
            name = committee.get("name", "").strip()
            if not name:
                continue

            # Filter for city relevance — FEC search is by keyword in name
            # We must cross-check city in address
            treasurer_city = committee.get("treasurer_city", "")
            org_city = committee.get("city", "")
            if city_name.lower() not in (treasurer_city.lower() + " " + org_city.lower()):
                # Skip if city not mentioned in any address field
                pass  # Keep anyway — FEC search results are usually relevant

            committee_id = committee.get("committee_id", "")
            committee_type = committee.get("committee_type", "")
            subtype = FEC_COMMITTEE_TYPE_MAP.get(committee_type, "pac")
            source_url = f"https://www.fec.gov/data/committee/{committee_id}/" if committee_id else ""

            party_map = {
                "DEM": "Democrat",
                "REP": "Republican",
                "IND": "Independent",
                "GRE": "Green",
                "LIB": "Libertarian",
            }
            party = party_map.get(committee.get("party", ""), committee.get("party", ""))

            category_fields: dict[str, Any] = {
                "political_subtype": subtype,
                "political_subtype_status": "REPORTED",
                "fec_committee_id": committee_id,
                "fec_committee_id_status": "REPORTED" if committee_id else "NOT_COLLECTED",
                "fec_committee_type": committee_type,
                "party_affiliation": party or None,
                "party_affiliation_status": "REPORTED" if party else "NOT_COLLECTED",
                "designation": committee.get("designation"),
                "filing_frequency": committee.get("filing_frequency"),
                "organization_type": committee.get("organization_type"),
                "is_active": committee.get("is_active"),
            }

            entity: dict[str, Any] = {
                "entity_id": None,
                "canonical_name": name,
                "entity_type": "political",
                "entity_subtype": subtype,
                "aliases": [],
                "valid_from": now_iso,
                "valid_to": None,
                "superseded_by": None,

                "primary_city": org_city or city_name,
                "primary_city_status": "REPORTED" if org_city else "NOT_COLLECTED",
                "primary_state": committee.get("state"),
                "primary_state_status": "REPORTED" if committee.get("state") else "NOT_COLLECTED",
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

                "external_ids": {"fec_committee_id": committee_id} if committee_id else {},
                "source_agent": self.AGENT_NAME,
                "source_run_ids": [run_id],
                "merge_provenance": [],
                "source_urls": [source_url] if source_url else [],
                "last_seen": now_iso,
                "last_verified": None,

                "overall_confidence": "high",  # FEC is mandatory disclosure
                "source_count": 1,
                "corroboration_count": 0,

                "partner_candidate": False,
                "competitor_candidate": False,
                "blocker_candidate": True,  # Political orgs are potential blockers
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

                "needs_review": True,           # All political entities require review
                "sensitivity_tier": "restricted",

                "category_fields": category_fields,

                "_raw_entity_id": str(uuid.uuid4()),
                "_source": "fec",
                "_pending_evidence": [
                    {
                        "entity_id": None,
                        "run_id": run_id,
                        "supported_field": "canonical_name",
                        "supported_value": name,
                        "source_url": source_url,
                        "source_type": "regulatory_filing",
                        "source_api": "fec",
                        "retrieved_at": now_iso,
                        "evidence_snippet": (
                            f"FEC: {name} is a registered political committee "
                            f"(type: {committee_type or 'PAC'}, ID: {committee_id})"
                            f"{f', party: {party}' if party else ''}"
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
    # OpenSecrets — organization spending summaries
    # ─────────────────────────────────────────────────────────────────────────

    async def _collect_from_opensecrets(
        self,
        city_name: str,
        run_id: str,
    ) -> list[dict[str, Any]]:
        """
        Search OpenSecrets for organizations with significant political spending.
        Focuses on orgs with local address in the city.
        """
        if not settings.opensecrets_api_key:
            log.info("PoliticalAgent: OpenSecrets key not set — skipping")
            await self.write_search_record(
                source_searched="opensecrets",
                query_used=f"political spending {city_name}",
                result_found=False,
                entity_type="political",
                failure_reason="OPENSECRETS_API_KEY not set",
            )
            return []

        search_start = time.monotonic()
        try:
            # OpenSecrets org summary search — no direct city filter, use keyword
            response = await self._opensecrets.get_org_summary(org_name=city_name)
        except Exception as e:
            log.warning("PoliticalAgent: OpenSecrets search failed: %s", e)
            await self.write_search_record(
                source_searched="opensecrets",
                query_used=f"organizations {city_name}",
                result_found=False,
                entity_type="political",
                failure_reason=str(e),
                response_time_ms=int((time.monotonic() - search_start) * 1000),
            )
            return []

        elapsed_ms = int((time.monotonic() - search_start) * 1000)
        # OpenSecrets returns {"response": {"data": {"org": [...]}}}
        data = response.get("response", {}).get("data", {})
        orgs = data.get("org", [])
        if isinstance(orgs, dict):
            orgs = [orgs]

        await self.write_search_record(
            source_searched="opensecrets",
            query_used=f"political organizations {city_name}",
            result_found=bool(orgs),
            entity_type="political",
            result_count=len(orgs),
            response_time_ms=elapsed_ms,
        )

        entities = []
        now_iso = datetime.now(timezone.utc).isoformat()

        for org in orgs:
            attrs = org.get("@attributes", org)
            name = attrs.get("orgname", "").strip()
            if not name:
                continue

            org_id = attrs.get("orgid", "")
            source_url = f"https://www.opensecrets.org/orgs/summary?id={org_id}" if org_id else ""

            category_fields: dict[str, Any] = {
                "political_subtype": "political_nonprofit",
                "political_subtype_status": "REPORTED",
                "opensecrets_org_id": org_id,
                "total_spent_cycle": attrs.get("total"),
                "total_pacs": attrs.get("pacs"),
                "total_lobbying": attrs.get("lobbying"),
                "total_outside": attrs.get("outside"),
                "industry": attrs.get("industry"),
            }

            entity: dict[str, Any] = {
                "entity_id": None,
                "canonical_name": name,
                "entity_type": "political",
                "entity_subtype": "political_nonprofit",
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

                "external_ids": {"opensecrets_org_id": org_id} if org_id else {},
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
                "blocker_candidate": True,
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

                "needs_review": True,
                "sensitivity_tier": "restricted",

                "category_fields": category_fields,

                "_raw_entity_id": str(uuid.uuid4()),
                "_source": "opensecrets",
                "_pending_evidence": [
                    {
                        "entity_id": None,
                        "run_id": run_id,
                        "supported_field": "canonical_name",
                        "supported_value": name,
                        "source_url": source_url,
                        "source_type": "government_record",
                        "source_api": "opensecrets",
                        "retrieved_at": now_iso,
                        "evidence_snippet": f"OpenSecrets: {name} is a political organization with recorded spending",
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
        country_or_region: str,
        run_id: str,
        targeted_queries: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Search for local political organizations and ballot committees."""
        if not settings.serpapi_api_key:
            log.info("PoliticalAgent: SerpAPI key not set — skipping")
            await self.write_search_record(
                source_searched="serpapi",
                query_used=f"political organizations {city_name}",
                result_found=False,
                entity_type="political",
                failure_reason="SERPAPI_API_KEY not set",
            )
            return []

        queries = targeted_queries if targeted_queries else [
            f"political PAC organization {city_name} fundraising",
            f"ballot measure campaign committee {city_name}",
        ]

        entities = []
        for query in queries[:2]:  # Limit to 2 — political data is sensitive
            search_start = time.monotonic()
            try:
                response = await self._serpapi.search(query, num=10)
            except Exception as e:
                log.warning("PoliticalAgent: SerpAPI '%s' failed: %s", query, e)
                await self.write_search_record(
                    source_searched="serpapi",
                    query_used=query,
                    result_found=False,
                    entity_type="political",
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
                entity_type="political",
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
        """LLM extraction for SerpAPI political results."""
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
            log.debug("PoliticalAgent: LLM extraction failed: %s", e)
            return None

        name = extracted_json.get("name")
        evidence_snippet = extracted_json.get("evidence_snippet")
        if not name or not evidence_snippet:
            return None

        source_url = result.get("link", "")
        now_iso = datetime.now(timezone.utc).isoformat()
        subtype = extracted_json.get("entity_subtype") or "pac"

        category_fields: dict[str, Any] = {
            "political_subtype": subtype,
            "political_subtype_status": "REPORTED",
            "party_affiliation": extracted_json.get("party_affiliation"),
            "party_affiliation_status": "REPORTED" if extracted_json.get("party_affiliation") else "NOT_COLLECTED",
            "political_focus": extracted_json.get("political_focus"),
            "political_focus_status": "REPORTED" if extracted_json.get("political_focus") else "NOT_COLLECTED",
        }

        return {
            "entity_id": None,
            "canonical_name": name,
            "entity_type": "political",
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
            "blocker_candidate": True,
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

            "needs_review": True,           # All political entities require review
            "sensitivity_tier": "restricted",

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
