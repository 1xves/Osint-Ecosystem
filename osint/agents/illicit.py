"""
osint/agents/illicit.py

Illicit Network Intelligence Agent — Phase 1 collection agent.

Collects: OFAC-sanctioned entities, federal criminal defendants with business
connections to the target city, organized crime figures, known money launderers,
and other bad actors who may have connections to the startup ecosystem.

*** HIGHEST SENSITIVITY AGENT IN THE PIPELINE ***

MANDATORY SCHEMA CONSTRAINTS (enforced, not optional):
- needs_review = True           (ALWAYS)
- sensitivity_tier = "restricted" (ALWAYS)
- confidence_required = "high"  (ALWAYS — low-confidence illicit claims are harmful)
- overall_confidence must be "high" for any entity to be included

Data sources:
1. OFAC SDN List   — Treasury Department Specially Designated Nationals list
2. CourtListener   — Federal court case search for financial crimes
3. SerpAPI         — Cross-reference search ONLY (not primary discovery)
                     SerpAPI is NOT used for primary discovery of illicit entities
                     It is ONLY used to cross-reference already-found OFAC/court names

Output:
- Appends to state["raw_entities"] — raw pre-resolution entity dicts
- Writes evidence records for every sourced field
- Writes search records for every search attempt
- Does NOT resolve or deduplicate

Entity type: "illicit"
Subtypes: ofac_sanctioned | federal_defendant | organized_crime | money_laundering |
          fraud | corruption | human_trafficking | drug_trafficking

CRITICAL OPERATIONAL NOTES:
1. This agent will produce FEW entities (typically 0-5 per city) — that is correct.
2. LOW coverage from this agent is expected and NOT a gap-fill trigger.
3. Every entity here requires human review before any action is taken.
4. Evidence requirements are higher: must have source_url from authoritative source.
5. Claims must be current (within last 5 years for court cases) — old cleared cases
   should be noted in evidence_snippet but entity still flagged.
6. NEVER lower confidence to inflate entity count — it is better to produce 0
   entities than to produce a false illicit designation.
7. Entities discovered here must NEVER be mentioned in automated outreach or
   briefings without explicit human review and sign-off.
"""

from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from osint.agents.base import BaseAgent
from osint.clients.ofac import OFACClient
from osint.clients.courtlistener import CourtListenerClient
from osint.core.config import settings
from osint.llm.routing import TaskType

log = logging.getLogger(__name__)

# OFAC SDN minimum match score to include entity
# OFAC returns scores 0-100; we require >=90 for inclusion
OFAC_MIN_SCORE = 90

# Court case types that indicate illicit financial activity
FINANCIAL_CRIME_KEYWORDS = [
    "wire fraud", "bank fraud", "securities fraud", "money laundering",
    "racketeering", "RICO", "embezzlement", "bribery", "corruption",
    "tax evasion", "ponzi", "pyramid scheme", "insider trading",
    "financial fraud", "cryptocurrency fraud", "crypto fraud",
]

# Maximum age of court case to include (years)
MAX_CASE_AGE_YEARS = 7

CROSS_REF_EXTRACTION_SYSTEM = """You are an intelligence analyst cross-referencing
a known bad actor against web sources. You are NOT identifying new subjects.
Given a name and context, determine if the web source corroborates illicit activity.
Return ONLY valid JSON. Never extrapolate or speculate beyond what is in the source text."""

CROSS_REF_EXTRACTION_PROMPT = """Cross-reference this known bad actor against the search result.

Subject: {subject_name}
Known context: {known_context}
City: {city_name}

Search result:
---
{search_text}
---

Return a JSON object:
{{
  "corroborates": "<true if this source confirms illicit activity for this person, false otherwise>",
  "corroboration_type": "<what kind of corroboration: criminal_case|sanction|news_report|government_record|null>",
  "additional_aliases": ["<any additional name variations found, or empty list>"],
  "city_connection": "<description of the person's connection to {city_name} if found, or null>",
  "evidence_snippet": "<exact quote from the text that corroborates illicit activity>"
}}"""


class IllicitAgent(BaseAgent):
    """
    Phase 1 collection agent for illicit network entities.
    Highest-sensitivity agent — all entities require human review.
    """

    AGENT_NAME = "illicit_agent"
    AGENT_VERSION = "1.0"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._ofac = OFACClient(self._rl)
        self._court = CourtListenerClient(self._rl)

    async def run(self, state: dict[str, Any]) -> dict[str, Any]:
        city_name = state["city_name"]
        run_id = state["run_id"]
        pass_number = state.get("pass_number", 1)

        log.info(
            "IllicitAgent: collecting for %s (pass %d) — HIGH SENSITIVITY",
            city_name, pass_number
        )

        new_raw_entities: list[dict[str, Any]] = []

        # ── Source 1: OFAC SDN List ────────────────────────────────────────────
        ofac_entities = await self._collect_from_ofac(city_name, run_id)
        new_raw_entities.extend(ofac_entities)
        log.info("IllicitAgent: OFAC yielded %d entities (all high-confidence)", len(ofac_entities))

        # ── Source 2: CourtListener — federal financial crime cases ────────────
        court_entities = await self._collect_from_courtlistener(city_name, run_id)
        new_raw_entities.extend(court_entities)
        log.info("IllicitAgent: CourtListener yielded %d entities", len(court_entities))

        # Log summary — low count is expected and correct
        log.info(
            "IllicitAgent: %d total illicit entities for %s. "
            "Low count expected — all require human review before use.",
            len(new_raw_entities), city_name
        )

        patch: dict[str, Any] = {
            "raw_entities": new_raw_entities,          # delta only
            **self.agent_status_patch("success"),
            **self.token_count_patch(),
            **self.entity_count_patch(),
        }
        return patch

    # ─────────────────────────────────────────────────────────────────────────
    # OFAC SDN List
    # ─────────────────────────────────────────────────────────────────────────

    async def _collect_from_ofac(
        self,
        city_name: str,
        run_id: str,
    ) -> list[dict[str, Any]]:
        """
        Search OFAC SDN list for sanctioned individuals and entities with
        addresses or aliases that reference the target city.

        OFAC entities are ALWAYS high confidence by definition — they have been
        officially designated by the US Treasury Department.
        """
        search_start = time.monotonic()
        try:
            response = await self._ofac.search(
                name=city_name,
                min_score=OFAC_MIN_SCORE,
            )
        except Exception as e:
            log.warning("IllicitAgent: OFAC search failed: %s", e)
            await self.write_search_record(
                source_searched="ofac_sdn",
                query_used=f"SDN list search for {city_name}",
                result_found=False,
                entity_type="illicit",
                failure_reason=str(e),
                response_time_ms=int((time.monotonic() - search_start) * 1000),
            )
            return []

        elapsed_ms = int((time.monotonic() - search_start) * 1000)
        matches = self._ofac.extract_matches(response)

        await self.write_search_record(
            source_searched="ofac_sdn",
            query_used=f"OFAC SDN list: {city_name} (min_score={OFAC_MIN_SCORE})",
            result_found=bool(matches),
            entity_type="illicit",
            result_count=len(matches),
            response_time_ms=elapsed_ms,
        )

        entities = []
        now_iso = datetime.now(timezone.utc).isoformat()

        for match in matches:
            name = match.get("name", "").strip()
            if not name:
                continue

            score = match.get("score", 0)
            # Double-check score threshold — belt-and-suspenders
            if score < OFAC_MIN_SCORE:
                log.debug("IllicitAgent: OFAC match '%s' score %d below threshold — skipping", name, score)
                continue

            ofac_id = match.get("uid", "")
            sdn_type = match.get("sdn_type", "")  # "Individual" or "Entity"
            programs = match.get("programs", [])
            addresses = match.get("addresses", [])

            # Infer subtype from OFAC program names
            subtype = self._infer_illicit_subtype_from_ofac(programs)

            # Build address string for evidence
            address_str = "; ".join(
                f"{a.get('city', '')}, {a.get('country', '')}".strip(", ")
                for a in addresses[:3]
            )

            source_url = f"https://sanctionssearch.ofac.treas.gov/Details.aspx?id={ofac_id}" if ofac_id else "https://sanctionssearch.ofac.treas.gov/"

            category_fields: dict[str, Any] = {
                "illicit_subtype": subtype,
                "illicit_subtype_status": "REPORTED",
                "ofac_uid": ofac_id,
                "ofac_uid_status": "REPORTED" if ofac_id else "NOT_COLLECTED",
                "ofac_sdn_type": sdn_type,
                "ofac_programs": programs,
                "ofac_programs_status": "REPORTED" if programs else "NOT_COLLECTED",
                "ofac_match_score": score,
                "ofac_aliases": match.get("aka_list", []),
                "known_addresses": addresses,
                # Mandatory high-sensitivity fields
                "confidence_required": "high",
                "review_required_reason": f"OFAC SDN designation ({', '.join(programs[:2]) if programs else 'unspecified program'})",
            }

            entity: dict[str, Any] = {
                "entity_id": None,
                "canonical_name": name,
                "entity_type": "illicit",
                "entity_subtype": subtype,
                "aliases": match.get("aka_list", []),
                "valid_from": now_iso,
                "valid_to": None,
                "superseded_by": None,

                "primary_city": city_name,
                "primary_city_status": "NOT_COLLECTED",
                "primary_state": None,
                "primary_state_status": "NOT_COLLECTED",
                "primary_country": match.get("nationality", "Unknown"),
                "primary_country_status": "REPORTED" if match.get("nationality") else "NOT_COLLECTED",

                "website_url": None,
                "website_url_status": "NOT_COLLECTED",
                "linkedin_url": None,
                "linkedin_url_status": "NOT_COLLECTED",
                "twitter_handle": None,
                "twitter_handle_status": "NOT_COLLECTED",

                "description": f"OFAC SDN designation: {', '.join(programs[:3]) if programs else 'Specially Designated National'}",
                "description_status": "REPORTED",
                "description_source_url": source_url,

                "external_ids": {"ofac_uid": ofac_id} if ofac_id else {},
                "source_agent": self.AGENT_NAME,
                "source_run_ids": [run_id],
                "merge_provenance": [],
                "source_urls": [source_url],
                "last_seen": now_iso,
                "last_verified": now_iso,  # OFAC search is real-time

                # OFAC designations are ALWAYS high confidence
                "overall_confidence": "high",
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

                # MANDATORY — enforced for illicit type
                "needs_review": True,
                "sensitivity_tier": "restricted",

                "category_fields": category_fields,

                "_raw_entity_id": str(uuid.uuid4()),
                "_source": "ofac_sdn",
                "_pending_evidence": [
                    {
                        "entity_id": None,
                        "run_id": run_id,
                        "supported_field": "canonical_name",
                        "supported_value": name,
                        "source_url": source_url,
                        "source_type": "government_record",
                        "source_api": "ofac_sdn",
                        "retrieved_at": now_iso,
                        "evidence_snippet": (
                            f"OFAC SDN List: {name} is designated as a Specially Designated National "
                            f"under program(s): {', '.join(programs) if programs else 'unspecified'}"
                            f" (match score: {score}%)"
                            f"{f'. Known addresses: {address_str}' if address_str else ''}"
                        ),
                        "claim_type": "direct_statement",
                        "confidence": "high",
                        "agent_name": self.AGENT_NAME,
                        "prompt_version": self.AGENT_VERSION,
                        # Extra sensitive evidence field
                        "sensitive_claim": True,
                    }
                ],
            }
            entities.append(entity)

        return entities

    def _infer_illicit_subtype_from_ofac(self, programs: list[str]) -> str:
        """Infer illicit subtype from OFAC sanction program codes."""
        programs_str = " ".join(programs).upper()
        if "NARCO" in programs_str or "DRUG" in programs_str:
            return "drug_trafficking"
        if "GLOMAG" in programs_str or "CORRUPT" in programs_str:
            return "corruption"
        if "TRAN2" in programs_str or "HUMAN" in programs_str:
            return "human_trafficking"
        if "TCO" in programs_str or "MAFIA" in programs_str:
            return "organized_crime"
        return "ofac_sanctioned"  # Generic for unrecognized programs

    # ─────────────────────────────────────────────────────────────────────────
    # CourtListener — federal financial crime cases
    # ─────────────────────────────────────────────────────────────────────────

    async def _collect_from_courtlistener(
        self,
        city_name: str,
        run_id: str,
    ) -> list[dict[str, Any]]:
        """
        Search CourtListener for federal financial crime cases involving the city.
        Only includes cases meeting the confidence_required="high" threshold:
        - Case must be in the criminal_complaint or indictment stage (not just civil)
        - Financial crime keywords must be present
        - Case must be recent enough (within MAX_CASE_AGE_YEARS)
        """
        # Build a financial crime + city search query
        query = f"({' OR '.join(f'\"{ kw}\"' for kw in FINANCIAL_CRIME_KEYWORDS[:6])}) {city_name}"

        search_start = time.monotonic()
        try:
            response = await self._court.search_cases(
                query=query,
                case_type="r",  # "r" = RECAP (federal cases) — CourtListener v3 API type codes
            )
        except Exception as e:
            log.warning("IllicitAgent: CourtListener search failed: %s", e)
            await self.write_search_record(
                source_searched="courtlistener",
                query_used=f"federal financial crimes in {city_name}",
                result_found=False,
                entity_type="illicit",
                failure_reason=str(e),
                response_time_ms=int((time.monotonic() - search_start) * 1000),
            )
            return []

        elapsed_ms = int((time.monotonic() - search_start) * 1000)
        results = response.get("results", [])

        await self.write_search_record(
            source_searched="courtlistener",
            query_used=f"federal financial crimes in {city_name}",
            result_found=bool(results),
            entity_type="illicit",
            result_count=len(results),
            response_time_ms=elapsed_ms,
        )

        entities = []
        seen_names: set[str] = set()
        now_iso = datetime.now(timezone.utc).isoformat()
        current_year = datetime.now(timezone.utc).year

        for case in results[:15]:
            # Validate case type — only criminal cases
            case_name = case.get("caseName", "")
            docket_number = case.get("docketNumber", "")
            court = case.get("court", "")
            date_filed = case.get("dateFiled", "")

            if not case_name:
                continue

            # Check case age
            if date_filed:
                try:
                    case_year = int(date_filed[:4])
                    if current_year - case_year > MAX_CASE_AGE_YEARS:
                        log.debug("IllicitAgent: case '%s' too old (%s) — skipping", case_name[:50], date_filed)
                        continue
                except (ValueError, IndexError):
                    pass

            # Extract defendant name from case_name
            # Federal criminal case format: "United States v. John Doe"
            defendant_name = self._extract_defendant_name(case_name)
            if not defendant_name or defendant_name in seen_names:
                continue

            # Skip case names that look like organizations (not individuals or companies)
            name_lower = defendant_name.lower()
            if "united states" in name_lower or "u.s." in name_lower:
                continue

            seen_names.add(defendant_name)

            # Infer subtype from case description
            case_text = " ".join([
                case.get("caseName", ""),
                case.get("suitNature", ""),
            ]).lower()
            subtype = self._infer_illicit_subtype_from_case(case_text)

            absolute_url = case.get("absolute_url", "")
            source_url = f"https://www.courtlistener.com{absolute_url}" if absolute_url else ""

            category_fields: dict[str, Any] = {
                "illicit_subtype": subtype,
                "illicit_subtype_status": "REPORTED",
                "court": court,
                "court_status": "REPORTED" if court else "NOT_COLLECTED",
                "docket_number": docket_number,
                "docket_number_status": "REPORTED" if docket_number else "NOT_COLLECTED",
                "case_name": case_name,
                "date_filed": date_filed,
                "date_filed_status": "REPORTED" if date_filed else "NOT_COLLECTED",
                "confidence_required": "high",
                "review_required_reason": f"Federal criminal case: {case_name[:100]}",
            }

            entity: dict[str, Any] = {
                "entity_id": None,
                "canonical_name": defendant_name,
                "entity_type": "illicit",
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

                "description": f"Federal defendant: {case_name}",
                "description_status": "REPORTED",
                "description_source_url": source_url,

                "external_ids": {"courtlistener_docket": docket_number} if docket_number else {},
                "source_agent": self.AGENT_NAME,
                "source_run_ids": [run_id],
                "merge_provenance": [],
                "source_urls": [source_url] if source_url else [],
                "last_seen": now_iso,
                "last_verified": None,

                # Court records are medium confidence — we don't know outcome
                # (indicted ≠ convicted); Verification Agent must upgrade
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

                # MANDATORY — enforced for illicit type
                "needs_review": True,
                "sensitivity_tier": "restricted",

                "category_fields": category_fields,

                "_raw_entity_id": str(uuid.uuid4()),
                "_source": "courtlistener",
                "_pending_evidence": [
                    {
                        "entity_id": None,
                        "run_id": run_id,
                        "supported_field": "canonical_name",
                        "supported_value": defendant_name,
                        "source_url": source_url,
                        "source_type": "court_record",
                        "source_api": "courtlistener",
                        "retrieved_at": now_iso,
                        "evidence_snippet": (
                            f"CourtListener: {case_name}"
                            f" ({court})"
                            f"{f', filed: {date_filed}' if date_filed else ''}"
                            f", docket: {docket_number}"
                        ),
                        "claim_type": "direct_statement",
                        "confidence": "medium",  # Charged ≠ convicted
                        "agent_name": self.AGENT_NAME,
                        "prompt_version": self.AGENT_VERSION,
                        "sensitive_claim": True,
                    }
                ],
            }
            entities.append(entity)

        return entities

    def _extract_defendant_name(self, case_name: str) -> str | None:
        """
        Extract defendant name from federal case name.
        Standard format: "United States v. John Doe" or "United States v. Acme Corp"
        Returns None if can't parse.
        """
        case_lower = case_name.lower()
        for prefix in ["united states v. ", "united states vs. ", "u.s. v. ",
                        "usa v. ", "united states of america v. "]:
            if prefix in case_lower:
                idx = case_lower.index(prefix) + len(prefix)
                defendant = case_name[idx:].strip()
                # Clean up common suffixes
                for suffix in [", et al.", " et al", ", et al"]:
                    if defendant.lower().endswith(suffix):
                        defendant = defendant[:-len(suffix)]
                return defendant.strip() if defendant else None
        return None

    def _infer_illicit_subtype_from_case(self, case_text: str) -> str:
        """Infer illicit subtype from case text keywords."""
        if any(kw in case_text for kw in ["money laundering", "laundering"]):
            return "money_laundering"
        if any(kw in case_text for kw in ["fraud", "ponzi", "pyramid"]):
            return "fraud"
        if any(kw in case_text for kw in ["bribery", "corruption", "kickback"]):
            return "corruption"
        if any(kw in case_text for kw in ["racketeering", "rico", "organized"]):
            return "organized_crime"
        if any(kw in case_text for kw in ["drug", "narco", "trafficking"]):
            return "drug_trafficking"
        return "federal_defendant"  # Generic if no specific keyword matches
