"""
osint/agents/politician.py

Politician Intelligence Agent — Phase 1 collection agent.

Collects: Elected and appointed officials — city council members, mayors,
county commissioners, state legislators, US Representatives and Senators —
with jurisdiction over or significant influence in the target city.

This agent covers INDIVIDUALS with elected/appointed government roles.
Political organizations (PACs, committees) are covered by political_agent.

Data sources (in priority order):
1. ProPublica Congress API  — Federal legislators (House + Senate)
2. FEC Candidate API        — Candidate filings and financial data
3. OpenSecrets              — Politician fundraising summaries
4. SerpAPI                  — City/county officials not in federal databases

Output:
- Appends to state["raw_entities"] — raw pre-resolution entity dicts
- Writes evidence records for every sourced field
- Writes search records for every search attempt
- Does NOT resolve or deduplicate

Entity type: "politician"
Subtypes: mayor | city_council | county_official | state_legislator |
          us_representative | us_senator | appointed_official | former_official

Notes:
- ProPublica Congress covers only federal legislators — city/county must be SerpAPI
- FEC candidate search reveals anyone who has ever filed as a federal candidate
- All politicians are sensitivity_tier="restricted" per schema spec
- needs_review=True for all politician entities
- party affiliation is always collected when available (FEC/ProPublica)
- For city context: we search by state derived from city, then filter results
  to include only those likely to cover the city
"""

from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from osint.agents.base import BaseAgent
from osint.clients.propublica import ProPublicaClient
from osint.clients.fec import FECClient
from osint.clients.opensecrets import OpenSecretsClient
from osint.clients.serpapi import SerpApiClient
from osint.core.config import settings
from osint.llm.routing import TaskType

log = logging.getLogger(__name__)

# ProPublica chamber values
CHAMBERS = ["house", "senate"]

# FEC office codes → our subtypes
FEC_OFFICE_MAP = {
    "H": "us_representative",
    "S": "us_senator",
    "P": "us_representative",  # Presidential — unlikely for city-level work
}

# Party abbreviations → full names
PARTY_FULL_NAME = {
    "D": "Democrat",
    "R": "Republican",
    "I": "Independent",
    "L": "Libertarian",
    "G": "Green",
}

SERPAPI_EXTRACTION_SYSTEM = """You are a data extraction agent for an OSINT pipeline.
Extract elected/appointed official information from search result text.
Return ONLY valid JSON. Do not include explanation or commentary.
If a field cannot be determined from the text, use null.
Never fabricate information — only extract what is explicitly stated in the source text."""

SERPAPI_EXTRACTION_PROMPT = """Extract elected or appointed official information from this search result.

City context: {city_name}

Search result:
---
{search_text}
---

Return a JSON object:
{{
  "name": "<official's full name, or null>",
  "entity_subtype": "<mayor|city_council|county_official|state_legislator|us_representative|us_senator|appointed_official|former_official|null>",
  "title": "<current title or office, or null>",
  "party_affiliation": "<Democrat|Republican|Independent|Nonpartisan|null>",
  "district": "<district number or name if applicable, or null>",
  "years_in_office": "<number of years if mentioned, or null>",
  "committee_memberships": ["<committees they serve on, or empty list>"],
  "is_current": "<true if currently in office, false if former, null if unclear>",
  "evidence_snippet": "<exact quote supporting this person being an official covering {city_name}>"
}}"""


class PoliticianAgent(BaseAgent):
    """
    Phase 1 collection agent for elected and appointed officials.
    All entities produced are sensitivity_tier='restricted'.
    """

    AGENT_NAME = "politician_agent"
    AGENT_VERSION = "1.0"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._propublica = ProPublicaClient(self._rl)
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
        scope_parameters = state.get("scope_parameters", {})

        # Attempt to extract state abbreviation from scope_parameters
        # (set by orchestrator — may have key_search_terms with state info)
        state_abbr = self._infer_state_abbr(city_name, scope_parameters)

        log.info("PoliticianAgent: collecting for %s (pass %d)", city_name, pass_number)

        new_raw_entities: list[dict[str, Any]] = []

        # ── Pass 2 targeting ──────────────────────────────────────────────────
        targeted_queries: list[str] = []
        if pass_number == 2:
            for target in pass2_targets:
                if target.get("entity_type") == "politician":
                    targeted_queries.extend(target.get("suggested_queries", []))
            targeted_queries = [q for q in targeted_queries if q]

        # ── Source 1: ProPublica Congress ──────────────────────────────────────
        pp_entities = await self._collect_from_propublica(
            city_name, state_abbr, run_id
        )
        new_raw_entities.extend(pp_entities)
        log.info("PoliticianAgent: ProPublica yielded %d raw entities", len(pp_entities))

        # ── Source 2: FEC candidates ───────────────────────────────────────────
        fec_entities = await self._collect_from_fec(city_name, state_abbr, run_id)
        new_raw_entities.extend(fec_entities)
        log.info("PoliticianAgent: FEC yielded %d raw entities", len(fec_entities))

        # ── Source 3: SerpAPI — local officials ────────────────────────────────
        serp_entities = await self._collect_from_serpapi(
            city_name, country_or_region, run_id, targeted_queries=targeted_queries
        )
        new_raw_entities.extend(serp_entities)
        log.info("PoliticianAgent: SerpAPI yielded %d raw entities", len(serp_entities))

        log.info("PoliticianAgent: %d total raw entities collected", len(new_raw_entities))

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

    def _infer_state_abbr(
        self, city_name: str, scope_parameters: dict[str, Any]
    ) -> str | None:
        """
        Try to extract two-letter state abbreviation from scope data.
        Returns None if it cannot be confidently determined.
        This is a best-effort heuristic — the orchestrator doesn't output state.
        """
        # Common city→state mappings for disambiguation
        KNOWN_CITY_STATES: dict[str, str] = {
            "new york": "NY", "los angeles": "CA", "chicago": "IL",
            "houston": "TX", "phoenix": "AZ", "philadelphia": "PA",
            "san antonio": "TX", "san diego": "CA", "dallas": "TX",
            "san jose": "CA", "austin": "TX", "jacksonville": "FL",
            "fort worth": "TX", "columbus": "OH", "san francisco": "CA",
            "charlotte": "NC", "indianapolis": "IN", "seattle": "WA",
            "denver": "CO", "washington": "DC", "nashville": "TN",
            "oklahoma city": "OK", "el paso": "TX", "boston": "MA",
            "portland": "OR", "las vegas": "NV", "memphis": "TN",
            "louisville": "KY", "baltimore": "MD", "milwaukee": "WI",
            "albuquerque": "NM", "tucson": "AZ", "fresno": "CA",
            "sacramento": "CA", "mesa": "AZ", "atlanta": "GA",
            "miami": "FL", "minneapolis": "MN", "tulsa": "OK",
            "raleigh": "NC", "omaha": "NE", "cleveland": "OH",
            "pittsburgh": "PA", "tampa": "FL", "new orleans": "LA",
            "richmond": "VA", "st. louis": "MO", "detroit": "MI",
        }
        return KNOWN_CITY_STATES.get(city_name.lower())

    # ─────────────────────────────────────────────────────────────────────────
    # ProPublica Congress API — federal legislators
    # ─────────────────────────────────────────────────────────────────────────

    async def _collect_from_propublica(
        self,
        city_name: str,
        state_abbr: str | None,
        run_id: str,
    ) -> list[dict[str, Any]]:
        """
        Fetch current Congress members for the state and both chambers.
        Cannot filter by city at this level — state is the finest granularity.
        All members of Congress for the state are relevant (they all affect the city).
        """
        if not state_abbr:
            log.info("PoliticianAgent: state abbr unknown — skipping ProPublica Congress")
            await self.write_search_record(
                source_searched="propublica_congress",
                query_used=f"Congress members for {city_name}",
                result_found=False,
                entity_type="politician",
                failure_reason="Could not determine state abbreviation for city",
            )
            return []

        entities = []
        now_iso = datetime.now(timezone.utc).isoformat()

        for chamber in CHAMBERS:
            search_start = time.monotonic()
            try:
                response = await self._propublica.get_current_members(
                    chamber=chamber,
                    state=state_abbr,
                    congress=118,  # 118th Congress (2023-2024) — update this periodically
                )
            except Exception as e:
                log.warning("PoliticianAgent: ProPublica %s failed: %s", chamber, e)
                await self.write_search_record(
                    source_searched="propublica_congress",
                    query_used=f"{chamber} members for {state_abbr}",
                    result_found=False,
                    entity_type="politician",
                    failure_reason=str(e),
                    response_time_ms=int((time.monotonic() - search_start) * 1000),
                )
                continue

            elapsed_ms = int((time.monotonic() - search_start) * 1000)
            results = response.get("results", [])
            members = results[0].get("members", []) if results else []

            await self.write_search_record(
                source_searched="propublica_congress",
                query_used=f"118th Congress {chamber} for {state_abbr}",
                result_found=bool(members),
                entity_type="politician",
                result_count=len(members),
                response_time_ms=elapsed_ms,
            )

            for member in members:
                first_name = member.get("first_name", "")
                last_name = member.get("last_name", "")
                name = f"{first_name} {last_name}".strip()
                if not name:
                    continue

                member_id = member.get("id", "")
                subtype = "us_senator" if chamber == "senate" else "us_representative"
                source_url = f"https://projects.propublica.org/api-docs/congress-api/members/#{member_id}" if member_id else ""

                party_abbr = member.get("party", "")
                party = PARTY_FULL_NAME.get(party_abbr, party_abbr)

                category_fields: dict[str, Any] = {
                    "politician_subtype": subtype,
                    "politician_subtype_status": "REPORTED",
                    "propublica_id": member_id,
                    "party_affiliation": party,
                    "party_affiliation_status": "REPORTED" if party else "NOT_COLLECTED",
                    "chamber": chamber,
                    "state": state_abbr,
                    "district": member.get("district"),
                    "district_status": "REPORTED" if member.get("district") else "NOT_COLLECTED",
                    "seniority": member.get("seniority"),
                    "in_office": member.get("in_office"),
                    "total_votes": member.get("total_votes"),
                    "missed_votes_pct": member.get("missed_votes_pct"),
                    "bills_sponsored": member.get("bills_sponsored"),
                    "bills_cosponsored": member.get("bills_cosponsored"),
                    "office_phone": member.get("office"),
                    "votes_with_party_pct": member.get("votes_with_party_pct"),
                }

                entity: dict[str, Any] = {
                    "entity_id": None,
                    "canonical_name": name,
                    "entity_type": "politician",
                    "entity_subtype": subtype,
                    "aliases": [f"Rep. {last_name}" if chamber == "house" else f"Sen. {last_name}"],
                    "valid_from": now_iso,
                    "valid_to": None,
                    "superseded_by": None,

                    "primary_city": city_name,
                    "primary_city_status": "NOT_COLLECTED",
                    "primary_state": state_abbr,
                    "primary_state_status": "REPORTED",
                    "primary_country": "United States",
                    "primary_country_status": "REPORTED",

                    "website_url": member.get("url"),
                    "website_url_status": "REPORTED" if member.get("url") else "NOT_COLLECTED",
                    "linkedin_url": None,
                    "linkedin_url_status": "NOT_COLLECTED",
                    "twitter_handle": member.get("twitter_account"),
                    "twitter_handle_status": "REPORTED" if member.get("twitter_account") else "NOT_COLLECTED",

                    "description": None,
                    "description_status": "NOT_COLLECTED",
                    "description_source_url": source_url,

                    "external_ids": {
                        "propublica_member_id": member_id,
                        "bioguide_id": member.get("id", ""),
                        "fec_candidate_id": member.get("fec_candidate_id", ""),
                        "govtrack_id": str(member.get("govtrack_id", "")),
                    },
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
                    "blocker_candidate": True,
                    "investment_candidate": False,
                    "support_candidate": True,  # Legislators can be useful allies
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
                    "_source": "propublica_congress",
                    "_pending_evidence": [
                        {
                            "entity_id": None,
                            "run_id": run_id,
                            "supported_field": "canonical_name",
                            "supported_value": name,
                            "source_url": source_url,
                            "source_type": "government_record",
                            "source_api": "propublica_congress",
                            "retrieved_at": now_iso,
                            "evidence_snippet": (
                                f"ProPublica Congress API: {name} is a {party} "
                                f"{'Senator' if chamber == 'senate' else 'Representative'} "
                                f"for {state_abbr}"
                                + (f", District {member.get('district')}" if member.get('district') else "")
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
    # FEC — candidate filings
    # ─────────────────────────────────────────────────────────────────────────

    async def _collect_from_fec(
        self,
        city_name: str,
        state_abbr: str | None,
        run_id: str,
    ) -> list[dict[str, Any]]:
        """
        Search FEC for candidates who have filed — current and recent cycles.
        This reveals who is politically active and their fundraising capacity.
        """
        if not state_abbr:
            log.info("PoliticianAgent: state abbr unknown — skipping FEC candidates")
            await self.write_search_record(
                source_searched="fec",
                query_used=f"candidates for {city_name}",
                result_found=False,
                entity_type="politician",
                failure_reason="Could not determine state abbreviation",
            )
            return []

        search_start = time.monotonic()
        try:
            response = await self._fec.search_candidates(
                state=state_abbr,
                cycle=2024,
                per_page=20,
            )
        except Exception as e:
            log.warning("PoliticianAgent: FEC candidates failed: %s", e)
            await self.write_search_record(
                source_searched="fec",
                query_used=f"candidates for {state_abbr} 2024 cycle",
                result_found=False,
                entity_type="politician",
                failure_reason=str(e),
                response_time_ms=int((time.monotonic() - search_start) * 1000),
            )
            return []

        elapsed_ms = int((time.monotonic() - search_start) * 1000)
        results = response.get("results", [])

        await self.write_search_record(
            source_searched="fec",
            query_used=f"federal candidates {state_abbr} 2024 cycle",
            result_found=bool(results),
            entity_type="politician",
            result_count=len(results),
            response_time_ms=elapsed_ms,
        )

        entities = []
        now_iso = datetime.now(timezone.utc).isoformat()

        for candidate in results:
            name = candidate.get("name", "").strip()
            if not name:
                continue

            candidate_id = candidate.get("candidate_id", "")
            office = candidate.get("office", "")
            subtype = FEC_OFFICE_MAP.get(office, "us_representative")
            source_url = f"https://www.fec.gov/data/candidate/{candidate_id}/" if candidate_id else ""

            party_abbr = candidate.get("party", "")
            party = PARTY_FULL_NAME.get(party_abbr, party_abbr)

            # Convert FEC name format (LAST, FIRST) to First Last
            if "," in name:
                parts = name.split(",", 1)
                canonical_name = f"{parts[1].strip()} {parts[0].strip()}"
            else:
                canonical_name = name

            category_fields: dict[str, Any] = {
                "politician_subtype": subtype,
                "politician_subtype_status": "REPORTED",
                "fec_candidate_id": candidate_id,
                "fec_candidate_id_status": "REPORTED" if candidate_id else "NOT_COLLECTED",
                "party_affiliation": party,
                "party_affiliation_status": "REPORTED" if party else "NOT_COLLECTED",
                "office_sought": office,
                "state": candidate.get("state"),
                "district": candidate.get("district"),
                "incumbent_challenger": candidate.get("incumbent_challenger_status"),
                "election_years": candidate.get("election_years", []),
                "active_through": candidate.get("active_through"),
            }

            entity: dict[str, Any] = {
                "entity_id": None,
                "canonical_name": canonical_name,
                "entity_type": "politician",
                "entity_subtype": subtype,
                "aliases": [name],  # Original FEC name as alias
                "valid_from": now_iso,
                "valid_to": None,
                "superseded_by": None,

                "primary_city": city_name,
                "primary_city_status": "NOT_COLLECTED",
                "primary_state": candidate.get("state") or state_abbr,
                "primary_state_status": "REPORTED",
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

                "external_ids": {"fec_candidate_id": candidate_id} if candidate_id else {},
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
                "blocker_candidate": True,
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

                "needs_review": True,
                "sensitivity_tier": "restricted",

                "category_fields": category_fields,

                "_raw_entity_id": str(uuid.uuid4()),
                "_source": "fec",
                "_pending_evidence": [
                    {
                        "entity_id": None,
                        "run_id": run_id,
                        "supported_field": "canonical_name",
                        "supported_value": canonical_name,
                        "source_url": source_url,
                        "source_type": "regulatory_filing",
                        "source_api": "fec",
                        "retrieved_at": now_iso,
                        "evidence_snippet": (
                            f"FEC: {canonical_name} filed as a {party or ''} candidate "
                            f"for {office or 'federal office'} in {state_abbr}"
                            f" (candidate ID: {candidate_id})"
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
    # SerpAPI — local officials (mayor, city council, county)
    # ─────────────────────────────────────────────────────────────────────────

    async def _collect_from_serpapi(
        self,
        city_name: str,
        country_or_region: str,
        run_id: str,
        targeted_queries: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Search for local officials — mayors, city council, county commissioners.
        These are NOT in federal databases like ProPublica Congress or FEC.
        """
        if not settings.serpapi_api_key:
            log.info("PoliticianAgent: SerpAPI key not set — skipping")
            await self.write_search_record(
                source_searched="serpapi",
                query_used=f"local officials {city_name}",
                result_found=False,
                entity_type="politician",
                failure_reason="SERPAPI_API_KEY not set",
            )
            return []

        queries = targeted_queries if targeted_queries else [
            f"mayor city council members {city_name} government officials",
            f"county commissioner supervisor {city_name}",
        ]

        entities = []
        for query in queries[:2]:  # Limit — political data is sensitive
            search_start = time.monotonic()
            try:
                response = await self._serpapi.search(query, num=10)
            except Exception as e:
                log.warning("PoliticianAgent: SerpAPI '%s' failed: %s", query, e)
                await self.write_search_record(
                    source_searched="serpapi",
                    query_used=query,
                    result_found=False,
                    entity_type="politician",
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
                entity_type="politician",
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
        """LLM extraction for SerpAPI politician results."""
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
            log.debug("PoliticianAgent: LLM extraction failed: %s", e)
            return None

        name = extracted_json.get("name")
        evidence_snippet = extracted_json.get("evidence_snippet")
        if not name or not evidence_snippet:
            return None

        source_url = result.get("link", "")
        now_iso = datetime.now(timezone.utc).isoformat()
        subtype = extracted_json.get("entity_subtype") or "city_council"

        category_fields: dict[str, Any] = {
            "politician_subtype": subtype,
            "politician_subtype_status": "REPORTED",
            "title": extracted_json.get("title"),
            "title_status": "REPORTED" if extracted_json.get("title") else "NOT_COLLECTED",
            "party_affiliation": extracted_json.get("party_affiliation"),
            "party_affiliation_status": "REPORTED" if extracted_json.get("party_affiliation") else "NOT_COLLECTED",
            "district": extracted_json.get("district"),
            "district_status": "REPORTED" if extracted_json.get("district") else "NOT_COLLECTED",
            "is_current": extracted_json.get("is_current"),
            "committee_memberships": extracted_json.get("committee_memberships", []),
        }

        return {
            "entity_id": None,
            "canonical_name": name,
            "entity_type": "politician",
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
            "blocker_candidate": True,
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

            "needs_review": True,
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
