"""
osint/agents/enrichment.py

Enrichment Agent — runs after entity resolution, before relationship mapping.

Responsibilities:
    1. OFAC sanctions check — MANDATORY for every canonical entity.
       Any entity with a match score ≥ 90 is flagged with needs_review=True
       and sensitivity_tier="restricted", and a DB record is written.
       Entities clean on OFAC get a search record confirming the check.

    2. Proxycurl LinkedIn enrichment — for executive_hnw and hnwi entities
       where proxycurl_retrieved=False and linkedin_url is known.
       Budget: respects the run-level spend cap (shared with collection phase).

    3. last_verified update — all entities get last_verified set in state.

Input state fields consumed:
    canonical_entities — post-resolution entity list

Output state fields set:
    enriched_entities   — all canonical entities, with OFAC flags and Proxycurl
                          data merged in where applicable
    enrichment_targets  — list of entity_ids that received new data

DB writes (per entity):
    entity_evidence — one evidence record per enrichment source per field
    rejected_items  — OFAC matches flagged for human review
    entities        — expire + reinsert for OFAC-matched entities only
                      (needs_review must be persisted for the review queue API)

Design note on temporal versioning:
    Within a single run, only OFAC-matched entities undergo expire+reinsert.
    Non-OFAC enrichment data (Proxycurl fields) is written as entity_evidence
    and held in enriched_entities state — the DB entity record is updated in
    future delta runs. This avoids 200 unnecessary temporal version pairs for
    a typical run of ~100 canonical entities.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from osint.agents.base import BaseAgent
from osint.clients.ofac import OFACClient
from osint.clients.proxycurl import ProxycurlClient, BudgetExceeded

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

AGENT_NAME    = "enrichment_agent"
AGENT_VERSION = "1.0"

# OFAC match threshold — same as IllicitAgent.OFAC_MIN_SCORE
# Entities scoring below this on OFAC fuzzy match are considered clean.
OFAC_MIN_SCORE = 90

# Max additional Proxycurl calls in enrichment phase (on top of collection phase).
# At $0.01/call this is an additional max $0.30 spend.
PROXYCURL_MAX_ENRICHMENT_CALLS = 30

# Entity types eligible for Proxycurl enrichment in this phase
PROXYCURL_ELIGIBLE_TYPES = {"executive_hnw", "hnwi"}

# Proxycurl fields to extract from person profile response
PROXYCURL_FIELD_MAP = {
    "headline":    "headline",
    "summary":     "bio",
    "city":        "primary_city",
    "state":       "primary_state",
    "country":     "primary_country",
    "full_name":   "canonical_name",
    "profile_pic_url": "photo_url",
    "public_identifier": "linkedin_handle",
}


# ─────────────────────────────────────────────────────────────────────────────
# Agent
# ─────────────────────────────────────────────────────────────────────────────

class EnrichmentAgent(BaseAgent):
    """
    Post-resolution enrichment: OFAC checks + Proxycurl LinkedIn data.
    """

    AGENT_NAME = AGENT_NAME
    AGENT_VERSION = AGENT_VERSION

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._ofac     = OFACClient(self._rl)
        self._proxycurl = ProxycurlClient(self._rl)
        self._proxycurl_calls_this_run = 0

    async def run(self, state: dict[str, Any]) -> dict[str, Any]:
        run_id    = state["run_id"]
        city_key  = state.get("city_key", "")
        canonical_entities: list[dict[str, Any]] = state.get("canonical_entities", [])

        log.info(
            "enrichment_agent: starting — %d canonical entities to process",
            len(canonical_entities),
        )

        if not canonical_entities:
            log.warning("enrichment_agent: no canonical_entities — returning empty")
            return self._empty_patch(state)

        enriched_entities:  list[dict[str, Any]] = []
        enrichment_targets: list[str]            = []

        for entity in canonical_entities:
            entity_id   = entity.get("entity_id", "")
            entity_type = entity.get("entity_type", "")
            name        = entity.get("canonical_name", entity.get("name", ""))
            now         = datetime.now(timezone.utc).isoformat()

            # Start with a copy — we mutate this, not the state original
            enriched = dict(entity)
            enriched["last_verified"] = now
            was_enriched = False

            # ── OFAC check (mandatory for every entity) ────────────────────────
            ofac_flagged = await self._run_ofac_check(
                entity=enriched,
                entity_id=entity_id,
                name=name,
                run_id=run_id,
                city_key=city_key,
            )
            if ofac_flagged:
                was_enriched = True

            # ── Proxycurl enrichment (eligible types only) ─────────────────────
            if entity_type in PROXYCURL_ELIGIBLE_TYPES:
                proxycurl_enriched = await self._run_proxycurl_enrichment(
                    entity=enriched,
                    entity_id=entity_id,
                    name=name,
                    run_id=run_id,
                )
                if proxycurl_enriched:
                    was_enriched = True

            if was_enriched and entity_id:
                enrichment_targets.append(entity_id)

            enriched_entities.append(enriched)

        log.info(
            "enrichment_agent: complete — %d/%d entities enriched "
            "(%d Proxycurl calls, OFAC screened all)",
            len(enrichment_targets), len(canonical_entities),
            self._proxycurl_calls_this_run,
        )

        return {
            "enriched_entities":  enriched_entities,
            "enrichment_targets": enrichment_targets,
            "current_phase":      "RELATIONSHIP",
            **self.agent_status_patch(
                "success",
                state.get("agent_statuses", {}),
            ),
            **self.token_count_patch(
                state.get("total_tokens_in", 0),
                state.get("total_tokens_out", 0),
                state.get("agent_token_counts", {}),
            ),
            **self.entity_count_patch(state.get("agent_entity_counts", {})),
        }

    # ─────────────────────────────────────────────────────────────────────────
    # OFAC check
    # ─────────────────────────────────────────────────────────────────────────

    async def _run_ofac_check(
        self,
        entity: dict[str, Any],
        entity_id: str,
        name: str,
        run_id: str,
        city_key: str,
    ) -> bool:
        """
        Check a single entity against the OFAC SDN list.
        Mutates `entity` in-place if a match is found.
        Returns True if entity was modified (OFAC match found).
        """
        if not name:
            return False

        t_start = datetime.now(timezone.utc).timestamp()

        try:
            result = await self._ofac.search(
                name=name,
                min_score=OFAC_MIN_SCORE,
                source="sdn",
            )
        except Exception as exc:
            log.warning(
                "enrichment_agent: OFAC check failed for '%s': %s", name, exc
            )
            # Write search record noting failure
            await self.write_search_record(
                source_searched="ofac_sdn",
                query_used=name,
                result_found=False,
                entity_id=entity_id,
                failure_reason=str(exc),
                response_time_ms=_elapsed_ms(t_start),
            )
            return False

        response_ms = _elapsed_ms(t_start)
        matches = OFACClient.extract_matches(result)
        result_found = len(matches) > 0

        # Write search record for audit trail — every entity must have one
        await self.write_search_record(
            source_searched="ofac_sdn",
            query_used=name,
            result_found=result_found,
            entity_id=entity_id,
            result_count=len(matches),
            response_time_ms=response_ms,
        )

        if not result_found:
            log.debug("enrichment_agent: OFAC clean — '%s'", name)
            return False

        # ── OFAC match found ───────────────────────────────────────────────────
        log.warning(
            "enrichment_agent: OFAC HIT for '%s' — %d matches (top score: %s)",
            name, len(matches), matches[0].get("score") if matches else "?",
        )

        # Update entity flags
        entity["needs_review"]     = True
        entity["sensitivity_tier"] = "restricted"
        entity["blocker_candidate"] = True

        # Store OFAC match data in category_fields
        cat = entity.setdefault("category_fields", {})
        cat["ofac_match_found"]   = True
        cat["ofac_matches"]       = matches
        cat["ofac_screened_at"]   = datetime.now(timezone.utc).isoformat()

        # Write evidence record for the OFAC match
        evidence_text = "; ".join(
            f"{m.get('name')} (score={m.get('score')}, programs={m.get('programs')})"
            for m in matches[:3]
        )
        if entity_id:
            try:
                await self.write_evidence({
                    "link_id":         str(uuid.uuid4()),
                    "entity_id":       entity_id,
                    "run_id":          run_id,
                    "field_name":      "ofac_designation",
                    "claim_type":      "direct_statement",
                    "source_type":     "government_database",
                    "source_name":     "OFAC SDN List",
                    "source_url":      "https://ofac.treasury.gov",
                    "evidence_snippet": evidence_text[:1000],
                    "confidence":      "high",
                    "sensitive_claim": True,
                    "created_at":      datetime.now(timezone.utc).isoformat(),
                })
            except Exception as exc:
                log.warning("enrichment_agent: failed to write OFAC evidence: %s", exc)

            # Write to rejected_items so the review queue API surfaces it
            try:
                await self.write_rejected_item(
                    stage="enrichment_ofac_screen",
                    item_type="ofac_match",
                    item_snapshot={
                        "entity_id":     entity_id,
                        "entity_name":   name,
                        "entity_type":   entity.get("entity_type"),
                        "ofac_matches":  matches,
                    },
                    rejection_reason="ofac_match",
                    rejection_detail=f"{len(matches)} OFAC SDN match(es). Requires human verification.",
                    item_id=entity_id,
                )
            except Exception as exc:
                log.warning("enrichment_agent: failed to write OFAC rejected_item: %s", exc)

            # Expire old entity record and reinsert with needs_review=True
            # This ensures the review queue API (reads from DB entities table) sees the flag
            try:
                new_entity_id = str(uuid.uuid4())
                enriched_for_db = dict(entity)
                enriched_for_db["entity_id"]   = new_entity_id
                enriched_for_db["superseded_by"] = None
                enriched_for_db.pop("_pending_evidence", None)

                await self._db.expire_entity(entity_id, new_entity_id)
                await self._db.write_entity(enriched_for_db)

                # Update in-memory entity with new ID
                entity["entity_id"] = new_entity_id
                log.info(
                    "enrichment_agent: entity '%s' versioned (OFAC flag) — "
                    "old=%s new=%s", name, entity_id, new_entity_id,
                )
            except Exception as exc:
                log.error(
                    "enrichment_agent: temporal versioning failed for OFAC entity '%s': %s",
                    name, exc,
                )
                # Revert entity_id change if write failed
                entity["entity_id"] = entity_id

        return True

    # ─────────────────────────────────────────────────────────────────────────
    # Proxycurl enrichment
    # ─────────────────────────────────────────────────────────────────────────

    async def _run_proxycurl_enrichment(
        self,
        entity: dict[str, Any],
        entity_id: str,
        name: str,
        run_id: str,
    ) -> bool:
        """
        Enrich an individual entity with Proxycurl LinkedIn data.
        Only called if entity is in PROXYCURL_ELIGIBLE_TYPES.
        Mutates `entity` in-place. Returns True if enrichment data was added.
        """
        # Check if already enriched during collection phase
        cat = entity.get("category_fields", {})
        if cat.get("proxycurl_retrieved") or entity.get("proxycurl_retrieved"):
            log.debug("enrichment_agent: '%s' already Proxycurl-enriched — skipping", name)
            return False

        # Need a LinkedIn URL to proceed
        linkedin_url = entity.get("linkedin_url")
        if not linkedin_url:
            log.debug(
                "enrichment_agent: '%s' has no linkedin_url — skipping Proxycurl", name
            )
            return False

        # Per-agent call cap
        if self._proxycurl_calls_this_run >= PROXYCURL_MAX_ENRICHMENT_CALLS:
            log.info(
                "enrichment_agent: Proxycurl cap reached (%d calls) — skipping '%s'",
                PROXYCURL_MAX_ENRICHMENT_CALLS, name,
            )
            return False

        t_start = datetime.now(timezone.utc).timestamp()

        try:
            profile = await self._proxycurl.get_person_profile(
                linkedin_url=linkedin_url,
                run_id=run_id,
            )
            self._proxycurl_calls_this_run += 1
        except BudgetExceeded:
            log.warning(
                "enrichment_agent: Proxycurl budget exceeded — halting further calls"
            )
            self._proxycurl_calls_this_run = PROXYCURL_MAX_ENRICHMENT_CALLS  # stop future attempts
            return False
        except Exception as exc:
            log.warning(
                "enrichment_agent: Proxycurl fetch failed for '%s': %s", name, exc
            )
            await self.write_search_record(
                source_searched="proxycurl_linkedin",
                query_used=linkedin_url,
                result_found=False,
                entity_id=entity_id,
                failure_reason=str(exc),
                response_time_ms=_elapsed_ms(t_start),
            )
            return False

        response_ms = _elapsed_ms(t_start)

        if not profile or not profile.get("full_name"):
            await self.write_search_record(
                source_searched="proxycurl_linkedin",
                query_used=linkedin_url,
                result_found=False,
                entity_id=entity_id,
                failure_reason="empty_response",
                response_time_ms=response_ms,
            )
            return False

        await self.write_search_record(
            source_searched="proxycurl_linkedin",
            query_used=linkedin_url,
            result_found=True,
            entity_id=entity_id,
            result_count=1,
            response_time_ms=response_ms,
        )

        # ── Extract and merge Proxycurl fields ─────────────────────────────────
        enriched_fields: dict[str, Any] = {}

        # Core profile fields
        if profile.get("headline"):
            enriched_fields["headline"] = profile["headline"]
        if profile.get("summary"):
            enriched_fields["bio"] = profile["summary"][:2000]
        if profile.get("city") and not entity.get("primary_city"):
            enriched_fields["primary_city"] = profile["city"]
        if profile.get("state") and not entity.get("primary_state"):
            enriched_fields["primary_state"] = profile["state"]

        # Current experience → role and employer
        experiences = profile.get("experiences") or []
        current_exp = _find_current_experience(experiences)
        if current_exp:
            if current_exp.get("title"):
                enriched_fields["current_title"]    = current_exp["title"]
                enriched_fields["current_employer"]  = current_exp.get("company", "")
            if current_exp.get("company"):
                enriched_fields["primary_employer"] = current_exp["company"]

        # Education → highest degree
        educations = profile.get("education") or []
        if educations:
            enriched_fields["education_history"] = [
                {
                    "institution": e.get("school"),
                    "degree":      e.get("degree_name"),
                    "field":       e.get("field_of_study"),
                    "end_year":    e.get("ends_at", {}).get("year") if e.get("ends_at") else None,
                }
                for e in educations[:5]
            ]

        # Mark proxycurl as retrieved
        enriched_fields["proxycurl_retrieved"] = True
        enriched_fields["proxycurl_retrieved_at"] = datetime.now(timezone.utc).isoformat()

        if not enriched_fields:
            return False

        # Merge into category_fields
        entity_cat = entity.setdefault("category_fields", {})
        entity_cat.update(enriched_fields)

        # Upgrade overall_confidence to "high" (LinkedIn is direct source)
        entity["overall_confidence"] = "high"

        # Write evidence records for Proxycurl-sourced fields
        evidence_records: list[dict[str, Any]] = []
        for field_name, value in enriched_fields.items():
            if field_name.startswith("proxycurl_") or not value:
                continue
            evidence_records.append({
                "link_id":         str(uuid.uuid4()),
                "entity_id":       entity_id,
                "run_id":          run_id,
                "field_name":      field_name,
                "claim_type":      "direct_statement",
                "source_type":     "professional_network",
                "source_name":     "LinkedIn (via Proxycurl)",
                "source_url":      linkedin_url,
                "evidence_snippet": str(value)[:500],
                "confidence":      "high",
                "created_at":      datetime.now(timezone.utc).isoformat(),
            })

        if evidence_records and entity_id:
            try:
                await self.write_evidence_batch(evidence_records)
            except Exception as exc:
                log.warning(
                    "enrichment_agent: evidence write failed for '%s': %s", name, exc
                )

        log.info(
            "enrichment_agent: Proxycurl enriched '%s' — %d new fields",
            name, len(enriched_fields),
        )
        return True

    # ─────────────────────────────────────────────────────────────────────────
    # Utility
    # ─────────────────────────────────────────────────────────────────────────

    def _empty_patch(self, state: dict[str, Any]) -> dict[str, Any]:
        return {
            "enriched_entities":  [],
            "enrichment_targets": [],
            "current_phase":      "RELATIONSHIP",
            **self.agent_status_patch("success", state.get("agent_statuses", {})),
            **self.token_count_patch(
                state.get("total_tokens_in", 0),
                state.get("total_tokens_out", 0),
                state.get("agent_token_counts", {}),
            ),
            **self.entity_count_patch(state.get("agent_entity_counts", {})),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Module helpers
# ─────────────────────────────────────────────────────────────────────────────

def _elapsed_ms(start_ts: float) -> int:
    return int((datetime.now(timezone.utc).timestamp() - start_ts) * 1000)


def _find_current_experience(experiences: list[dict[str, Any]]) -> dict[str, Any] | None:
    """
    Return the most recent (or currently active) experience entry.
    Proxycurl lists experiences newest-first; current jobs have no ends_at.
    """
    # First: look for an entry with no end date (still current)
    for exp in experiences:
        if exp and not exp.get("ends_at"):
            return exp
    # Fallback: most recent (first in list)
    return experiences[0] if experiences else None
