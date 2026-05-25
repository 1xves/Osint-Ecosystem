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
from osint.clients.eventbrite import EventbriteClient
from osint.clients.followthemoney import FollowTheMoneyClient
from osint.clients.gdelt import GDELTClient
from osint.clients.littlesis import LittleSisClient
from osint.clients.meetup import MeetupClient
from osint.clients.ofac import OFACClient
from osint.clients.opencorporates import OpenCorporatesClient
from osint.clients.patent_view import PatentViewClient
from osint.clients.proxycurl import ProxycurlClient, BudgetExceeded
from osint.clients.scraper_base import RobotsDisallowedError
from osint.clients.scrapers.bizapedia import BizapediaScraper
from osint.clients.scrapers.wayback_scraper import WaybackScraper
from osint.clients.scrapers.sos.sos_pa import PASoSScraper
from osint.clients.scrapers.sos.sos_de import DESoSScraper
from osint.clients.wayback import WaybackClient
from osint.clients.edgar import EdgarClient
from osint.clients.fincen import FinCENClient
from osint.clients.hud import HUDClient
from osint.clients.icij import ICIJClient
from osint.clients.pacer import CourtListenerClient
from osint.llm.extractors import DocumentExtractor
from osint.llm.routing import LLMRouter
from osint.utils.document_fetcher import DocumentFetcher

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

# Min name similarity to accept a LittleSis entity as a match for a pipeline entity.
# Slightly below resolution threshold since we're enriching, not merging.
LITTLESIS_MIN_NAME_SIMILARITY = 0.82

# Max LittleSis searches per enrichment run. Each search = 1 API call;
# relationship fetches are additional calls (up to 4 pages each).
# 60 entities × avg 3 calls each = ~180 calls → well within 30 req/min across the run.
LITTLESIS_MAX_ENTITIES = 60

# Max GDELT queries for description enrichment per run.
# GDELT is fast but verbose — cap to avoid overwhelming context.
GDELT_MAX_DESCRIPTION_TARGETS = 50

# Max PatentView enrichment calls per run (corporate entities only)
PATENTVIEW_MAX_ENTITIES = 30

# Max OpenCorporates enrichment calls per run (corporate entities)
OPENCORPORATES_MAX_ENTITIES = 40

# Max FollowTheMoney searches per run (politician + executive_hnw)
FTM_MAX_ENTITIES = 30

# Max event enrichment calls per run (community_leader + corporate)
EVENT_MAX_ENTITIES = 20

# Entity types eligible for patent enrichment
PATENTVIEW_ELIGIBLE_TYPES = {"corporate", "investor"}

# Entity types eligible for FollowTheMoney enrichment
FTM_ELIGIBLE_TYPES = {"politician", "political", "executive_hnw", "hnwi"}

# Entity types eligible for event enrichment
EVENT_ELIGIBLE_TYPES = {"community_leader", "corporate"}

# Max Bizapedia scrapes per run (corporate entities)
BIZAPEDIA_MAX_ENTITIES = 30

# Max Wayback historical scrapes per run (corporate / investor entities with thin data)
WAYBACK_MAX_ENTITIES = 20

# Max SoS scrapes per run (PA + DE combined)
SOS_MAX_ENTITIES = 40

# Entity types eligible for Bizapedia / SoS enrichment
SCRAPER_ELIGIBLE_TYPES = {"corporate", "investor"}

# Min founding year for Wayback historical scrape trigger
# Only scrape historical pages for companies founded before 2015
WAYBACK_FOUNDED_BEFORE = 2015

# Max EDGAR proxy/10-K document extraction calls per run (corporate entities with CIK)
EDGAR_DOC_MAX_ENTITIES = 20

# Max CourtListener litigation lookups per run
COURTLISTENER_MAX_ENTITIES = 40

# Entity types eligible for EDGAR document extraction (need a CIK)
EDGAR_DOC_ELIGIBLE_TYPES = {"corporate", "investor"}

# Entity types eligible for CourtListener litigation search
LITIGATION_ELIGIBLE_TYPES = {"corporate", "investor", "executive_hnw", "illicit"}

# Phase 8 — ETL bulk data constants
# Max entities to query per ETL-backed client per run
# (DuckDB is local/fast — limits are purely for pipeline throughput control)
FINCEN_MAX_ENTITIES = 60
HUD_MAX_ENTITIES    = 60

# Entity types eligible for FinCEN CTR enrichment
# (financial institutions only — too noisy for general corporate)
FINCEN_ELIGIBLE_TYPES = {"corporate"}

# Entity types eligible for HUD multifamily property lookup
HUD_ELIGIBLE_TYPES = {"corporate", "real_estate", "investor"}

# Phase 9 — ICIJ Offshore Leaks (Neo4j)
# Max entities to screen per run. Each entity = 1 Neo4j CONTAINS query + optional
# shell chain traversal. Neo4j is local; cap is for pipeline throughput control.
ICIJ_MAX_ENTITIES = 50

# Entity types eligible for ICIJ offshore screening.
# illicit is always screened. corporate and investor capture shell company usage.
ICIJ_ELIGIBLE_TYPES = {"corporate", "investor", "illicit"}

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
        self._ofac             = OFACClient(self._rl)
        self._proxycurl        = ProxycurlClient(self._rl)
        self._littlesis        = LittleSisClient(self._rl)
        self._gdelt            = GDELTClient(self._rl)
        self._patent_view      = PatentViewClient(self._rl)
        self._opencorporates   = OpenCorporatesClient(self._rl)
        self._followthemoney   = FollowTheMoneyClient(self._rl)
        self._eventbrite       = EventbriteClient(self._rl)
        self._meetup           = MeetupClient(self._rl)
        self._wayback_client   = WaybackClient(self._rl)
        self._doc_fetcher      = DocumentFetcher()
        self._bizapedia        = BizapediaScraper(self._rl)
        self._wayback_scraper  = WaybackScraper(self._wayback_client, self._doc_fetcher)
        self._sos_pa           = PASoSScraper(self._rl)
        self._sos_de           = DESoSScraper(self._rl)
        self._edgar            = EdgarClient(self._rl)
        self._courtlistener    = CourtListenerClient(self._rl)
        self._doc_extractor    = DocumentExtractor(self._llm)  # LLM-powered extraction
        # Phase 8 — ETL-backed local DuckDB clients (no network calls at runtime)
        self._fincen           = FinCENClient()
        self._hud              = HUDClient()
        # Phase 9 — ICIJ Neo4j client (lazy connect; silent if Neo4j not running)
        self._icij             = ICIJClient()
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
        littlesis_count    = 0
        gdelt_count        = 0
        patentview_count   = 0
        opencorp_count     = 0
        ftm_count          = 0
        event_count        = 0
        bizapedia_count    = 0
        wayback_count      = 0
        sos_count          = 0
        edgar_doc_count    = 0
        litigation_count   = 0
        fincen_count       = 0
        hud_count          = 0
        icij_count         = 0

        city_name    = state.get("city_key", "").split(",")[0].strip()
        state_abbr   = state.get("state_abbr", "")

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

            # ── LittleSis relationship enrichment ─────────────────────────────
            # Fetch pre-built relationship edges from LittleSis for known entities.
            # Stores raw relationship list in category_fields["littlesis_relationships"]
            # so relationship_agent can create edges without re-calling the API.
            if littlesis_count < LITTLESIS_MAX_ENTITIES and name:
                ls_enriched = await self._run_littlesis_enrichment(
                    entity=enriched,
                    entity_id=entity_id,
                    name=name,
                    run_id=run_id,
                )
                if ls_enriched:
                    was_enriched = True
                    littlesis_count += 1

            # ── GDELT description enrichment ───────────────────────────────────
            # Populate description for entities that have no description yet.
            # GDELT provides news-based entity descriptions that bootstrap
            # LLM relationship inference (which requires non-empty descriptions).
            cat = enriched.get("category_fields", {})
            needs_description = (
                not enriched.get("description")
                and enriched.get("description_status") in (None, "NOT_COLLECTED", "NOT_REPORTED")
            )
            if (
                gdelt_count < GDELT_MAX_DESCRIPTION_TARGETS
                and needs_description
                and name
            ):
                gdelt_enriched = await self._run_gdelt_description_enrichment(
                    entity=enriched,
                    entity_id=entity_id,
                    name=name,
                    run_id=run_id,
                )
                if gdelt_enriched:
                    was_enriched = True
                    gdelt_count += 1

            # ── PatentView enrichment (corporate / investor) ───────────────────
            if (
                patentview_count < PATENTVIEW_MAX_ENTITIES
                and entity_type in PATENTVIEW_ELIGIBLE_TYPES
                and name
            ):
                pv_enriched = await self._run_patentview_enrichment(
                    entity=enriched,
                    entity_id=entity_id,
                    name=name,
                    run_id=run_id,
                    state_abbr=state_abbr,
                )
                if pv_enriched:
                    was_enriched = True
                    patentview_count += 1

            # ── OpenCorporates enrichment (corporate entities) ─────────────────
            if (
                opencorp_count < OPENCORPORATES_MAX_ENTITIES
                and entity_type in ("corporate", "investor")
                and name
            ):
                oc_enriched = await self._run_opencorporates_enrichment(
                    entity=enriched,
                    entity_id=entity_id,
                    name=name,
                    run_id=run_id,
                )
                if oc_enriched:
                    was_enriched = True
                    opencorp_count += 1

            # ── FollowTheMoney enrichment (politician / political / exec) ──────
            if (
                ftm_count < FTM_MAX_ENTITIES
                and entity_type in FTM_ELIGIBLE_TYPES
                and name
            ):
                ftm_enriched = await self._run_followthemoney_enrichment(
                    entity=enriched,
                    entity_id=entity_id,
                    name=name,
                    run_id=run_id,
                    state_abbr=state_abbr,
                )
                if ftm_enriched:
                    was_enriched = True
                    ftm_count += 1

            # ── Event enrichment (community_leader / corporate) ────────────────
            if (
                event_count < EVENT_MAX_ENTITIES
                and entity_type in EVENT_ELIGIBLE_TYPES
                and name
                and city_name
            ):
                ev_enriched = await self._run_event_enrichment(
                    entity=enriched,
                    entity_id=entity_id,
                    name=name,
                    run_id=run_id,
                    city_name=city_name,
                    state_abbr=state_abbr,
                )
                if ev_enriched:
                    was_enriched = True
                    event_count += 1

            # ── Bizapedia scraper (corporate / investor) ───────────────────────
            if (
                bizapedia_count < BIZAPEDIA_MAX_ENTITIES
                and entity_type in SCRAPER_ELIGIBLE_TYPES
                and name
            ):
                biz_enriched = await self._run_bizapedia_enrichment(
                    entity=enriched,
                    entity_id=entity_id,
                    name=name,
                    run_id=run_id,
                    city=city_name,
                    state_abbr=state_abbr,
                )
                if biz_enriched:
                    was_enriched = True
                    bizapedia_count += 1

            # ── SoS scrapers — PA and DE (corporate / investor) ───────────────
            if (
                sos_count < SOS_MAX_ENTITIES
                and entity_type in SCRAPER_ELIGIBLE_TYPES
                and name
            ):
                sos_enriched = await self._run_sos_enrichment(
                    entity=enriched,
                    entity_id=entity_id,
                    name=name,
                    run_id=run_id,
                    state_abbr=state_abbr,
                )
                if sos_enriched:
                    was_enriched = True
                    sos_count += 1

            # ── Wayback historical scraper (thin corporate data, old companies) ─
            cat_fields = enriched.get("category_fields", {})
            founded_year = cat_fields.get("founded_year") or 9999
            thin_execs   = len(cat_fields.get("executives", [])) < 3
            website      = cat_fields.get("website") or enriched.get("website", "")
            if (
                wayback_count < WAYBACK_MAX_ENTITIES
                and entity_type in SCRAPER_ELIGIBLE_TYPES
                and founded_year < WAYBACK_FOUNDED_BEFORE
                and thin_execs
                and website
            ):
                wb_enriched = await self._run_wayback_enrichment(
                    entity=enriched,
                    entity_id=entity_id,
                    name=name,
                    run_id=run_id,
                    website=website,
                )
                if wb_enriched:
                    was_enriched = True
                    wayback_count += 1

            # ── EDGAR proxy/10-K document extraction (corporate w/ CIK) ────────
            cik = cat_fields.get("cik") or enriched.get("cik", "")
            if (
                edgar_doc_count < EDGAR_DOC_MAX_ENTITIES
                and entity_type in EDGAR_DOC_ELIGIBLE_TYPES
                and cik
                and name
            ):
                ed_enriched = await self._run_edgar_doc_enrichment(
                    entity=enriched,
                    entity_id=entity_id,
                    name=name,
                    cik=str(cik),
                    run_id=run_id,
                )
                if ed_enriched:
                    was_enriched = True
                    edgar_doc_count += 1

            # ── CourtListener litigation search (all types) ───────────────────
            if (
                litigation_count < COURTLISTENER_MAX_ENTITIES
                and entity_type in LITIGATION_ELIGIBLE_TYPES
                and name
            ):
                lit_enriched = await self._run_litigation_enrichment(
                    entity=enriched,
                    entity_id=entity_id,
                    name=name,
                    run_id=run_id,
                )
                if lit_enriched:
                    was_enriched = True
                    litigation_count += 1

            # ── FinCEN CTR enrichment (Phase 8 — financial institutions) ─────
            # DuckDB local — no network call; skips silently if ETL not run
            if (
                fincen_count < FINCEN_MAX_ENTITIES
                and entity_type in FINCEN_ELIGIBLE_TYPES
                and name
            ):
                fin_enriched = self._run_fincen_enrichment(
                    entity=enriched,
                    name=name,
                )
                if fin_enriched:
                    was_enriched = True
                    fincen_count += 1

            # ── HUD multifamily property enrichment (Phase 8) ────────────────
            # DuckDB local — skips silently if ETL not run
            if (
                hud_count < HUD_MAX_ENTITIES
                and entity_type in HUD_ELIGIBLE_TYPES
                and name
            ):
                hud_enriched = self._run_hud_enrichment(
                    entity=enriched,
                    name=name,
                    city=enriched.get("primary_city", ""),
                    state=enriched.get("primary_state", ""),
                )
                if hud_enriched:
                    was_enriched = True
                    hud_count += 1

            # ── ICIJ Offshore Leaks screening (Phase 9 — Neo4j) ─────────────
            # Silently skips if Neo4j is unavailable or ICIJ ETL has not been run.
            # Only screens entities not already flagged offshore by OFAC.
            if (
                icij_count < ICIJ_MAX_ENTITIES
                and entity_type in ICIJ_ELIGIBLE_TYPES
                and name
                and not enriched.get("category_fields", {}).get("icij_nodes")
            ):
                icij_enriched = await self._run_icij_enrichment(
                    entity=enriched,
                    entity_id=entity_id,
                    name=name,
                    run_id=run_id,
                    country_code=enriched.get("primary_country_code", ""),
                )
                if icij_enriched:
                    was_enriched = True
                    icij_count += 1

            if was_enriched and entity_id:
                enrichment_targets.append(entity_id)

            enriched_entities.append(enriched)

        log.info(
            "enrichment_agent: complete — %d/%d entities enriched "
            "(%d Proxycurl, %d LittleSis, %d GDELT, %d PatentView, "
            "%d OpenCorporates, %d FTM, %d Events, "
            "%d Bizapedia, %d SoS, %d Wayback, "
            "%d EdgarDocs, %d Litigation, "
            "%d FinCEN, %d HUD, %d ICIJ, OFAC screened all)",
            len(enrichment_targets), len(canonical_entities),
            self._proxycurl_calls_this_run, littlesis_count, gdelt_count,
            patentview_count, opencorp_count, ftm_count, event_count,
            bizapedia_count, sos_count, wayback_count,
            edgar_doc_count, litigation_count,
            fincen_count, hud_count, icij_count,
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
    # LittleSis enrichment
    # ─────────────────────────────────────────────────────────────────────────

    async def _run_littlesis_enrichment(
        self,
        entity: dict[str, Any],
        entity_id: str,
        name: str,
        run_id: str,
    ) -> bool:
        """
        Search LittleSis for this entity and fetch all relationships.

        Stores results in entity["category_fields"]["littlesis_relationships"]
        as a list of raw relationship attribute dicts. The relationship_agent
        reads this field during its `_extract_littlesis_relationships()` pass.

        Also stores the LittleSis entity ID in category_fields["littlesis_id"]
        for future direct lookups and stores the entity's blurb in `description`
        if the entity doesn't already have one.

        Returns True if at least one relationship was found and stored.
        """
        from difflib import SequenceMatcher

        # Skip if already enriched by LittleSis this run
        cat = entity.setdefault("category_fields", {})
        if cat.get("littlesis_retrieved"):
            return False

        t_start = datetime.now(timezone.utc).timestamp()

        try:
            response = await self._littlesis.search(query=name, num=5)
        except Exception as exc:
            log.debug("enrichment_agent: LittleSis search failed for '%s': %s", name, exc)
            return False

        response_ms = _elapsed_ms(t_start)
        search_results = LittleSisClient.extract_entities(response)

        if not search_results:
            await self.write_search_record(
                source_searched="littlesis",
                query_used=name,
                result_found=False,
                entity_id=entity_id,
                result_count=0,
                response_time_ms=response_ms,
            )
            cat["littlesis_retrieved"] = True     # Mark as attempted — don't retry
            return False

        # Find best name match using same similarity threshold as resolution
        best_match = None
        best_score = 0.0
        for candidate in search_results:
            candidate_name = candidate.get("name", "")
            if not candidate_name:
                continue
            score = SequenceMatcher(None, name.lower(), candidate_name.lower()).ratio()
            if score > best_score:
                best_score = score
                best_match = candidate

        if not best_match or best_score < LITTLESIS_MIN_NAME_SIMILARITY:
            log.debug(
                "enrichment_agent: LittleSis no confident match for '%s' "
                "(best_score=%.2f, best_name='%s')",
                name, best_score, best_match.get("name", "") if best_match else "",
            )
            await self.write_search_record(
                source_searched="littlesis",
                query_used=name,
                result_found=False,
                entity_id=entity_id,
                result_count=len(search_results),
                failure_reason=f"no_confident_match (best={best_score:.2f})",
                response_time_ms=response_ms,
            )
            cat["littlesis_retrieved"] = True
            return False

        littlesis_id = best_match.get("id")
        littlesis_url = best_match.get("littlesis_url", "")

        await self.write_search_record(
            source_searched="littlesis",
            query_used=name,
            result_found=True,
            entity_id=entity_id,
            result_count=len(search_results),
            response_time_ms=response_ms,
        )

        # ── Populate description from LittleSis blurb ─────────────────────────
        blurb = best_match.get("blurb")
        if blurb and not entity.get("description"):
            entity["description"]        = blurb
            entity["description_status"] = "REPORTED"
            entity["description_source_url"] = littlesis_url
            if entity_id:
                try:
                    await self.write_evidence({
                        "link_id":          str(uuid.uuid4()),
                        "entity_id":        entity_id,
                        "run_id":           run_id,
                        "field_name":       "description",
                        "claim_type":       "direct_statement",
                        "source_type":      "structured_data",
                        "source_name":      "LittleSis",
                        "source_url":       littlesis_url,
                        "evidence_snippet": blurb[:500],
                        "confidence":       "medium",
                        "created_at":       datetime.now(timezone.utc).isoformat(),
                    })
                except Exception as exc:
                    log.debug("enrichment_agent: LittleSis description evidence write failed: %s", exc)

        # ── Fetch relationships ────────────────────────────────────────────────
        t_rel_start = datetime.now(timezone.utc).timestamp()
        try:
            relationships = await self._littlesis.get_all_relationships(
                entity_id=littlesis_id,
                max_pages=4,
                per_page=25,
            )
        except Exception as exc:
            log.debug(
                "enrichment_agent: LittleSis relationships fetch failed for '%s': %s",
                name, exc,
            )
            cat["littlesis_id"]        = littlesis_id
            cat["littlesis_url"]       = littlesis_url
            cat["littlesis_retrieved"] = True
            return bool(blurb)

        # ── Store raw relationships in category_fields ─────────────────────────
        # relationship_agent reads this in _extract_littlesis_relationships()
        cat["littlesis_id"]            = littlesis_id
        cat["littlesis_url"]           = littlesis_url
        cat["littlesis_relationships"] = relationships
        cat["littlesis_retrieved"]     = True

        log.info(
            "enrichment_agent: LittleSis enriched '%s' — %d relationships (sim=%.2f)",
            name, len(relationships), best_score,
        )
        return True

    # ─────────────────────────────────────────────────────────────────────────
    # GDELT description enrichment
    # ─────────────────────────────────────────────────────────────────────────

    async def _run_gdelt_description_enrichment(
        self,
        entity: dict[str, Any],
        entity_id: str,
        name: str,
        run_id: str,
    ) -> bool:
        """
        Use GDELT to populate description for entities that have none.

        GDELT's ArticleSearch endpoint returns news articles mentioning the
        entity. We extract the top article's description snippet and store
        it as the entity's description. This is a "good enough" description
        for bootstrapping LLM relationship inference.

        Description-first enrichment is critical:
          relationship_agent._llm_inference_pass() only fires for entities with
          non-empty descriptions. Entities with description=None get zero LLM
          relationship inference, leaving their relationship edges empty.

        Returns True if a description was added.
        """
        t_start = datetime.now(timezone.utc).timestamp()

        from datetime import timedelta
        one_year_ago = (datetime.now(timezone.utc) - timedelta(days=365)).strftime("%Y%m%d%H%M%S")

        try:
            response = await self._gdelt.search_articles(
                query=f'"{name}"',
                max_records=5,
                start_date=one_year_ago,
                sort="DateDesc",
            )
        except Exception as exc:
            log.debug(
                "enrichment_agent: GDELT description search failed for '%s': %s",
                name, exc,
            )
            return False

        response_ms = _elapsed_ms(t_start)
        articles = response.get("articles", [])

        if not articles:
            await self.write_search_record(
                source_searched="gdelt",
                query_used=f'"{name}"',
                result_found=False,
                entity_id=entity_id,
                result_count=0,
                response_time_ms=response_ms,
            )
            return False

        # Use the first article's seendate and title+description for the entity blurb
        top_article = articles[0]
        title    = top_article.get("title", "")
        snippet  = top_article.get("seendesc", top_article.get("url", ""))
        source_url = top_article.get("url", "https://gdelt.googledocs.org")

        # Build a concise description from the article title + snippet
        if title:
            description = title[:300]
            if snippet and len(description) < 200:
                description = f"{description}. {snippet[:200]}"
        else:
            description = snippet[:400] if snippet else None

        if not description:
            return False

        await self.write_search_record(
            source_searched="gdelt",
            query_used=f'"{name}"',
            result_found=True,
            entity_id=entity_id,
            result_count=len(articles),
            response_time_ms=response_ms,
        )

        entity["description"]        = description
        entity["description_status"] = "REPORTED"
        entity["description_source_url"] = source_url

        if entity_id:
            try:
                await self.write_evidence({
                    "link_id":          str(uuid.uuid4()),
                    "entity_id":        entity_id,
                    "run_id":           run_id,
                    "field_name":       "description",
                    "claim_type":       "direct_statement",
                    "source_type":      "news_article",
                    "source_name":      "GDELT",
                    "source_url":       source_url,
                    "evidence_snippet": description[:500],
                    "confidence":       "low",           # News-derived — lower confidence
                    "created_at":       datetime.now(timezone.utc).isoformat(),
                })
            except Exception as exc:
                log.debug("enrichment_agent: GDELT evidence write failed: %s", exc)

        log.debug(
            "enrichment_agent: GDELT description set for '%s' (%d chars)",
            name, len(description),
        )
        return True

    # ─────────────────────────────────────────────────────────────────────────
    # PatentView enrichment
    # ─────────────────────────────────────────────────────────────────────────

    async def _run_patentview_enrichment(
        self,
        entity: dict[str, Any],
        entity_id: str,
        name: str,
        run_id: str,
        state_abbr: str = "",
    ) -> bool:
        """
        Enrich a corporate/investor entity with USPTO patent data via PatentsView.

        Stores in category_fields:
            patents:           list of {patent_id, title, date, assignee}
            patent_count:      integer total
            patent_co_inventors: list of co-inventor full names (for INVENTED_BY edges)
            patent_cpc_domains: list of technology domain strings

        Returns True if at least one patent was found.
        """
        cat = entity.setdefault("category_fields", {})
        if cat.get("patentview_retrieved"):
            return False

        t_start = datetime.now(timezone.utc).timestamp()
        try:
            response = await self._patent_view.search_by_assignee(
                company_name=name,
                state=state_abbr or None,
                limit=25,
            )
        except Exception as exc:
            log.debug("enrichment_agent: PatentView failed for '%s': %s", name, exc)
            cat["patentview_retrieved"] = True
            return False

        response_ms = _elapsed_ms(t_start)
        patents = self._patent_view.extract_patents(response)

        await self.write_search_record(
            source_searched="patent_view",
            query_used=name,
            result_found=bool(patents),
            entity_id=entity_id,
            result_count=len(patents),
            response_time_ms=response_ms,
        )

        cat["patentview_retrieved"] = True

        if not patents:
            return False

        # Extract structured patent data
        patent_list = []
        for p in patents[:25]:
            patent_list.append({
                "patent_id":  p.get("patent_number") or p.get("patent_id", ""),
                "title":      p.get("patent_title", ""),
                "date":       p.get("patent_date", ""),
                "assignee":   name,
            })

        co_inventors = self._patent_view.extract_co_inventors(patents, name)
        cpc_domains  = self._patent_view.extract_cpc_domains(patents)

        cat["patents"]              = patent_list
        cat["patent_count"]         = len(patent_list)
        cat["patent_co_inventors"]  = co_inventors[:20]   # Cap to avoid noise
        cat["patent_cpc_domains"]   = cpc_domains[:10]

        log.info(
            "enrichment_agent: PatentView found %d patents for '%s' "
            "(%d co-inventors, %d CPC domains)",
            len(patent_list), name, len(co_inventors), len(cpc_domains),
        )
        return True

    # ─────────────────────────────────────────────────────────────────────────
    # OpenCorporates enrichment
    # ─────────────────────────────────────────────────────────────────────────

    async def _run_opencorporates_enrichment(
        self,
        entity: dict[str, Any],
        entity_id: str,
        name: str,
        run_id: str,
    ) -> bool:
        """
        Enrich a corporate entity with OpenCorporates registration data.

        Cross-validates domestic data and fills gaps:
            - Registered jurisdiction
            - Registration status (active / dissolved)
            - Registered agent name
            - Officer list (directors, registered agents)
            - Incorporation date

        Stores in category_fields:
            opencorporates_data:       {jurisdiction, company_number, status, incorporation_date}
            opencorporates_officers:   list of {name, position, start_date}

        Returns True if a match was found.
        """
        cat = entity.setdefault("category_fields", {})
        if cat.get("opencorporates_retrieved"):
            return False

        t_start = datetime.now(timezone.utc).timestamp()
        try:
            response = await self._opencorporates.search_companies(
                name=name,
                per_page=5,
            )
        except Exception as exc:
            log.debug("enrichment_agent: OpenCorporates failed for '%s': %s", name, exc)
            cat["opencorporates_retrieved"] = True
            return False

        response_ms = _elapsed_ms(t_start)
        cat["opencorporates_retrieved"] = True

        companies = (
            response.get("results", {}).get("companies", [])
            if isinstance(response.get("results"), dict)
            else []
        )

        await self.write_search_record(
            source_searched="opencorporates",
            query_used=name,
            result_found=bool(companies),
            entity_id=entity_id,
            result_count=len(companies),
            response_time_ms=response_ms,
        )

        if not companies:
            return False

        # Use first result (OpenCorporates lists most likely match first)
        top = companies[0].get("company") if isinstance(companies[0], dict) else {}
        if not top:
            return False

        jurisdiction = top.get("jurisdiction_code", "")
        company_number = top.get("company_number", "")
        status = top.get("current_status", "")
        incorporation_date = top.get("incorporation_date", "")
        registered_address = top.get("registered_address_in_full", "")
        company_url = top.get("opencorporates_url", "https://opencorporates.com")

        cat["opencorporates_data"] = {
            "jurisdiction":        jurisdiction,
            "company_number":      company_number,
            "status":              status,
            "incorporation_date":  incorporation_date,
            "registered_address":  registered_address,
            "opencorporates_url":  company_url,
        }

        # Populate entity fields if not already set
        if incorporation_date and not entity.get("founded_year"):
            try:
                entity["founded_year"] = int(incorporation_date[:4])
            except (ValueError, TypeError):
                pass

        # Fetch officers if we have jurisdiction + company_number
        if jurisdiction and company_number:
            try:
                detail = await self._opencorporates.get_company(
                    jurisdiction_code=jurisdiction,
                    company_number=company_number,
                )
                company_detail = detail.get("results", {}).get("company", {})
                officers_raw = company_detail.get("officers", [])
                officers = []
                for o in officers_raw[:15]:
                    off = o.get("officer") if isinstance(o, dict) else {}
                    if off and off.get("name"):
                        officers.append({
                            "name":       off["name"],
                            "position":   off.get("position", ""),
                            "start_date": off.get("start_date", ""),
                        })
                if officers:
                    cat["opencorporates_officers"] = officers
                    log.debug(
                        "enrichment_agent: OpenCorporates found %d officers for '%s'",
                        len(officers), name,
                    )
            except Exception as exc:
                log.debug(
                    "enrichment_agent: OpenCorporates officer fetch failed for '%s': %s",
                    name, exc,
                )

        if entity_id:
            try:
                await self.write_evidence({
                    "link_id":          str(uuid.uuid4()),
                    "entity_id":        entity_id,
                    "run_id":           run_id,
                    "field_name":       "registration_data",
                    "claim_type":       "direct_statement",
                    "source_type":      "government_database",
                    "source_name":      "OpenCorporates",
                    "source_url":       company_url,
                    "evidence_snippet": f"{name}: {jurisdiction}, {status}, inc. {incorporation_date}",
                    "confidence":       "high",
                    "created_at":       datetime.now(timezone.utc).isoformat(),
                })
            except Exception as exc:
                log.debug("enrichment_agent: OpenCorporates evidence write failed: %s", exc)

        log.info(
            "enrichment_agent: OpenCorporates matched '%s' → %s (%s, %s)",
            name, jurisdiction, company_number, status,
        )
        return True

    # ─────────────────────────────────────────────────────────────────────────
    # FollowTheMoney enrichment
    # ─────────────────────────────────────────────────────────────────────────

    async def _run_followthemoney_enrichment(
        self,
        entity: dict[str, Any],
        entity_id: str,
        name: str,
        run_id: str,
        state_abbr: str = "",
    ) -> bool:
        """
        Enrich politician/political/executive_hnw entities with FollowTheMoney
        state campaign finance data.

        For politicians/political entities: retrieves top donors who contributed
        to them. Stores in category_fields["ftm_contributions"] as a list of
        {donor_name, amount, employer, state, year} dicts for relationship_agent.

        For executive_hnw/hnwi: searches as a donor to find which candidates
        they contributed to. Stores in category_fields["ftm_donation_targets"].

        Returns True if contribution data was found.
        """
        cat = entity.setdefault("category_fields", {})
        if cat.get("ftm_retrieved"):
            return False

        entity_type = entity.get("entity_type", "")
        t_start = datetime.now(timezone.utc).timestamp()

        try:
            response = await self._followthemoney.search(
                query=name,
                state=state_abbr or None,
            )
        except Exception as exc:
            log.debug("enrichment_agent: FollowTheMoney search failed for '%s': %s", name, exc)
            cat["ftm_retrieved"] = True
            return False

        response_ms = _elapsed_ms(t_start)
        records = self._followthemoney.extract_records(response)
        cat["ftm_retrieved"] = True

        await self.write_search_record(
            source_searched="followthemoney",
            query_used=name,
            result_found=bool(records),
            entity_id=entity_id,
            result_count=len(records),
            response_time_ms=response_ms,
        )

        if not records:
            return False

        # For political/politician entities: these are candidates receiving donations
        if entity_type in ("politician", "political"):
            # Try to get top donors using the first matched candidate ID
            top_record = records[0] if records else {}
            candidate_id = (
                top_record.get("can_id")
                or top_record.get("candidate_id")
                or top_record.get("id")
            )

            donors: list[dict[str, Any]] = []
            if candidate_id:
                try:
                    donors_response = await self._followthemoney.top_donors(
                        candidate_id=str(candidate_id),
                        limit=20,
                    )
                    donors_raw = self._followthemoney.extract_records(donors_response)
                    for d in donors_raw[:20]:
                        donors.append({
                            "donor_name": d.get("contributor_name") or d.get("name", ""),
                            "amount":     d.get("amount") or d.get("total", 0),
                            "employer":   d.get("employer_name") or d.get("employer", ""),
                            "state":      d.get("contributor_state") or d.get("state", ""),
                            "year":       d.get("year") or d.get("election_year", ""),
                        })
                except Exception as exc:
                    log.debug(
                        "enrichment_agent: FTM top_donors failed for '%s': %s", name, exc
                    )

            if donors:
                cat["ftm_contributions"] = donors
                log.info(
                    "enrichment_agent: FollowTheMoney found %d donors for politician '%s'",
                    len(donors), name,
                )
                return True

        # For executive_hnw/hnwi: records represent their donation activity
        elif entity_type in ("executive_hnw", "hnwi"):
            donation_targets = []
            for r in records[:20]:
                target = r.get("candidate_name") or r.get("committee_name") or r.get("name", "")
                amount = r.get("amount") or r.get("total", 0)
                year   = r.get("year") or r.get("election_year", "")
                state  = r.get("state") or r.get("jurisdiction", "")
                if target:
                    donation_targets.append({
                        "target_name": target,
                        "amount":      amount,
                        "year":        year,
                        "state":       state,
                    })
            if donation_targets:
                cat["ftm_donation_targets"] = donation_targets
                log.info(
                    "enrichment_agent: FollowTheMoney found %d donation targets for '%s'",
                    len(donation_targets), name,
                )
                return True

        return False

    # ─────────────────────────────────────────────────────────────────────────
    # Event enrichment (Eventbrite + Meetup)
    # ─────────────────────────────────────────────────────────────────────────

    async def _run_event_enrichment(
        self,
        entity: dict[str, Any],
        entity_id: str,
        name: str,
        run_id: str,
        city_name: str = "",
        state_abbr: str = "",
    ) -> bool:
        """
        Enrich community_leader and corporate entities with event network data
        from Eventbrite and Meetup.

        Both sources are tertiary (low confidence) — event co-attendance/co-
        organization is a weak signal. The primary value is:
          - `events_organized_count`: quantitative signal for influence scoring
          - `event_co_organizers`: names that may map to other pipeline entities

        Stores in category_fields:
            eventbrite_events:     list of {event_name, date, organizer_name, attendee_count}
            meetup_groups:         list of {group_name, topic, member_count, organizer_name}
            event_co_organizers:   list of co-organizer/co-host names (deduped)
            events_organized_count: integer total across both sources

        Returns True if at least one event or group was found.
        """
        cat = entity.setdefault("category_fields", {})
        if cat.get("event_enrichment_retrieved"):
            return False

        cat["event_enrichment_retrieved"] = True
        found = False

        # ── Eventbrite ────────────────────────────────────────────────────────
        if city_name:
            t_start = datetime.now(timezone.utc).timestamp()
            try:
                eb_response = await self._eventbrite.search_events(
                    city=city_name,
                    state_code=state_abbr or None,
                    keywords=name,
                    max_results=20,
                )
                eb_events_raw = self._eventbrite.extract_events(eb_response)
                response_ms = _elapsed_ms(t_start)

                await self.write_search_record(
                    source_searched="eventbrite",
                    query_used=f"{name} @ {city_name}",
                    result_found=bool(eb_events_raw),
                    entity_id=entity_id,
                    result_count=len(eb_events_raw),
                    response_time_ms=response_ms,
                )

                eb_events = []
                for ev in eb_events_raw[:10]:
                    organizer = self._eventbrite.extract_organizer(ev)
                    eb_events.append({
                        "event_name":     ev.get("name", {}).get("text", "") if isinstance(ev.get("name"), dict) else ev.get("name", ""),
                        "date":           ev.get("start", {}).get("local", "") if isinstance(ev.get("start"), dict) else "",
                        "organizer_name": organizer.get("name", "") if organizer else "",
                        "is_online":      ev.get("is_online_event", False),
                    })
                if eb_events:
                    cat["eventbrite_events"] = eb_events
                    found = True

            except Exception as exc:
                log.debug("enrichment_agent: Eventbrite failed for '%s': %s", name, exc)

        # ── Meetup ────────────────────────────────────────────────────────────
        # Meetup GraphQL requires lat/lon — look up from city name.
        # Skip silently if city not in the coordinates map.
        coords = _CITY_COORDS.get(city_name.lower()) if city_name else None
        if coords is not None:
            t_meetup = datetime.now(timezone.utc).timestamp()
            try:
                mu_response = await self._meetup.search_groups(
                    query=name,
                    lat=coords[0],
                    lon=coords[1],
                    radius_miles=25,
                )
                groups_raw = self._meetup.extract_groups_from_search(mu_response)
                meetup_ms = _elapsed_ms(t_meetup)

                await self.write_search_record(
                    source_searched="meetup",
                    query_used=name,
                    result_found=bool(groups_raw),
                    entity_id=entity_id,
                    result_count=len(groups_raw),
                    response_time_ms=meetup_ms,
                )

                mu_groups = []
                for g in groups_raw[:10]:
                    organizer = g.get("organizer")
                    mu_groups.append({
                        "group_name":     g.get("name", ""),
                        "member_count":   g.get("members", 0) or g.get("membersCount", 0),
                        "organizer_name": organizer.get("name", "") if isinstance(organizer, dict) else "",
                    })
                if mu_groups:
                    cat["meetup_groups"] = mu_groups
                    found = True

            except Exception as exc:
                log.debug("enrichment_agent: Meetup failed for '%s': %s", name, exc)
        else:
            log.debug(
                "enrichment_agent: Meetup skipped for '%s' — no coordinates for city '%s'",
                name, city_name,
            )

        if not found:
            return False

        # ── Aggregate co-organizer names ──────────────────────────────────────
        co_organizers: set[str] = set()
        for ev in cat.get("eventbrite_events", []):
            org_name = ev.get("organizer_name", "")
            if org_name and org_name.lower() != name.lower():
                co_organizers.add(org_name)
        for grp in cat.get("meetup_groups", []):
            org_name = grp.get("organizer_name", "")
            if org_name and org_name.lower() != name.lower():
                co_organizers.add(org_name)

        cat["event_co_organizers"] = sorted(co_organizers)[:15]
        cat["events_organized_count"] = (
            len(cat.get("eventbrite_events", []))
            + len(cat.get("meetup_groups", []))
        )

        log.info(
            "enrichment_agent: Events found for '%s' — %d Eventbrite, %d Meetup, %d co-organizers",
            name,
            len(cat.get("eventbrite_events", [])),
            len(cat.get("meetup_groups", [])),
            len(co_organizers),
        )
        return True

    # ─────────────────────────────────────────────────────────────────────────
    # Bizapedia enrichment
    # ─────────────────────────────────────────────────────────────────────────

    async def _run_bizapedia_enrichment(
        self,
        entity: dict[str, Any],
        entity_id: str,
        name: str,
        run_id: str,
        city: str = "",
        state_abbr: str = "",
    ) -> bool:
        """
        Scrape Bizapedia for corporate registration data (registered agent,
        incorporation date, officers) and store in category_fields.

        Returns True if useful data was retrieved and stored.
        """
        log.debug("enrichment_agent: Bizapedia scrape for '%s'", name)

        try:
            data = await self._bizapedia.scrape(
                company_name=name,
                city=city,
                state_abbr=state_abbr,
            )
        except Exception as exc:
            log.debug("enrichment_agent: Bizapedia failed for '%s': %s", name, exc)
            return False

        if not data:
            return False

        # Only store if we got at least one meaningful field
        meaningful = bool(
            data.get("registered_agent")
            or data.get("incorporation_date")
            or data.get("officers")
            or data.get("status")
        )
        if not meaningful:
            return False

        cat = entity.setdefault("category_fields", {})
        cat["bizapedia_data"] = data

        # Promote incorporation date to top-level if missing
        if not cat.get("founded_year") and data.get("incorporation_date"):
            raw_date = data["incorporation_date"]
            year_match = __import__("re").search(r"\b(19|20)\d{2}\b", raw_date)
            if year_match:
                cat["founded_year"] = int(year_match.group(0))

        # Promote registered agent name
        if data.get("registered_agent"):
            cat["registered_agent"] = data["registered_agent"]

        log.info(
            "enrichment_agent: Bizapedia enriched '%s' — agent=%s officers=%d",
            name,
            data.get("registered_agent", "—"),
            len(data.get("officers", [])),
        )
        return True

    # ─────────────────────────────────────────────────────────────────────────
    # SoS enrichment (PA + DE)
    # ─────────────────────────────────────────────────────────────────────────

    async def _run_sos_enrichment(
        self,
        entity: dict[str, Any],
        entity_id: str,
        name: str,
        run_id: str,
        state_abbr: str = "",
    ) -> bool:
        """
        Query PA and/or DE Secretary of State portals for corporate registration
        data. PA is tried if state_abbr == "PA"; DE is always tried (most corps
        are registered in DE regardless of operating state).

        Returns True if at least one SoS returned useful data.
        """
        cat = entity.setdefault("category_fields", {})
        found_any = False

        # ── Pennsylvania SoS ──────────────────────────────────────────────────
        if state_abbr.upper() == "PA":
            log.debug("enrichment_agent: SoS PA scrape for '%s'", name)
            try:
                pa_data = await self._sos_pa.scrape(company_name=name)
            except Exception as exc:
                log.debug("enrichment_agent: SoS PA failed for '%s': %s", name, exc)
                pa_data = {}

            if pa_data and (pa_data.get("status") or pa_data.get("incorporation_date")):
                cat["sos_pa_data"] = pa_data
                if not cat.get("registered_agent") and pa_data.get("registered_agent"):
                    cat["registered_agent"] = pa_data["registered_agent"]
                found_any = True
                log.info(
                    "enrichment_agent: SoS PA enriched '%s' — status=%s",
                    name, pa_data.get("status", "—"),
                )

        # ── Delaware SoS (always — most corps are DE-incorporated) ────────────
        log.debug("enrichment_agent: SoS DE scrape for '%s'", name)
        try:
            de_data = await self._sos_de.scrape(company_name=name)
        except Exception as exc:
            log.debug("enrichment_agent: SoS DE failed for '%s': %s", name, exc)
            de_data = {}

        if de_data and (de_data.get("status") or de_data.get("file_number")):
            # Enrich registered agent detail from DE entity detail page
            if de_data.get("detail_url") and not de_data.get("registered_agent"):
                de_data = await self._sos_de.enrich_with_detail(de_data)

            cat["sos_de_data"] = de_data
            # DE registered agent is especially valuable for shell company analysis
            if de_data.get("registered_agent"):
                cat.setdefault("registered_agent", de_data["registered_agent"])
                cat["registered_agent_de"] = de_data["registered_agent"]
            found_any = True
            log.info(
                "enrichment_agent: SoS DE enriched '%s' — status=%s file=%s",
                name,
                de_data.get("status", "—"),
                de_data.get("file_number", "—"),
            )

        return found_any

    # ─────────────────────────────────────────────────────────────────────────
    # Wayback historical executive enrichment
    # ─────────────────────────────────────────────────────────────────────────

    async def _run_wayback_enrichment(
        self,
        entity: dict[str, Any],
        entity_id: str,
        name: str,
        run_id: str,
        website: str,
    ) -> bool:
        """
        Fetch archived versions of the company's team/about pages from the
        Wayback Machine to extract historical executive names.

        Only runs when:
          - Entity is corporate/investor type
          - founded_year < 2015
          - < 3 executives already known
          - website URL is available

        Returns True if at least one historical executive was found.
        """
        log.debug(
            "enrichment_agent: Wayback historical scrape for '%s' at '%s'",
            name, website,
        )

        try:
            executives = await self._wayback_scraper.scrape_historical_executives(
                company_website=website,
                min_year="2010",
                max_pages=3,
            )
        except Exception as exc:
            log.debug(
                "enrichment_agent: Wayback scrape failed for '%s': %s", name, exc
            )
            return False

        if not executives:
            return False

        cat = entity.setdefault("category_fields", {})
        cat["wayback_executives"] = executives

        log.info(
            "enrichment_agent: Wayback found %d historical executives for '%s'",
            len(executives), name,
        )
        return True

    # ─────────────────────────────────────────────────────────────────────────
    # EDGAR document extraction (Phase 7)
    # ─────────────────────────────────────────────────────────────────────────

    async def _run_edgar_doc_enrichment(
        self,
        entity: dict[str, Any],
        entity_id: str,
        name: str,
        cik: str,
        run_id: str,
    ) -> bool:
        """
        Fetch and extract EDGAR DEF 14A (proxy) and 10-K (annual report) for
        a corporate entity. Stores compensation data and officer list.

        Trigger condition: entity_type in {corporate, investor} AND cik is set.

        Stores:
            category_fields["exec_compensation"]:
                [{name, title, base_salary, bonus, total_compensation, fiscal_year}]
            category_fields["annual_report_officers"]:
                [{name, age, title, bio_summary, tenure_start_year}]
            category_fields["annual_report_directors"]:
                [{name, age, title, independence, bio_summary}]

        Returns True if at least one document was successfully extracted.
        """
        cat = entity.setdefault("category_fields", {})
        found_any = False

        # ── DEF 14A proxy statement — executive compensation ──────────────────
        log.debug("enrichment_agent: EDGAR DEF 14A extraction for '%s' (CIK %s)", name, cik)
        try:
            proxy_data = await self._edgar.get_proxy_executive_compensation(
                cik=cik,
                company_name=name,
                extractor=self._doc_extractor,
            )
        except Exception as exc:
            log.debug("enrichment_agent: DEF 14A extraction failed for '%s': %s", name, exc)
            proxy_data = {}

        if proxy_data and proxy_data.get("executives"):
            cat["exec_compensation"] = proxy_data["executives"]
            if proxy_data.get("directors"):
                cat.setdefault("proxy_directors", proxy_data["directors"])
            found_any = True
            log.info(
                "enrichment_agent: DEF 14A extracted %d executives for '%s'",
                len(proxy_data["executives"]), name,
            )

        # ── 10-K annual report — officer/director list ────────────────────────
        log.debug("enrichment_agent: EDGAR 10-K extraction for '%s' (CIK %s)", name, cik)
        try:
            annual_data = await self._edgar.get_annual_report_officers(
                cik=cik,
                company_name=name,
                extractor=self._doc_extractor,
            )
        except Exception as exc:
            log.debug("enrichment_agent: 10-K extraction failed for '%s': %s", name, exc)
            annual_data = {}

        if annual_data:
            if annual_data.get("officers"):
                cat["annual_report_officers"] = annual_data["officers"]
                found_any = True
            if annual_data.get("directors"):
                cat["annual_report_directors"] = annual_data["directors"]
                found_any = True
            if found_any:
                log.info(
                    "enrichment_agent: 10-K extracted %d officers, %d directors for '%s'",
                    len(annual_data.get("officers", [])),
                    len(annual_data.get("directors", [])),
                    name,
                )

        return found_any

    # ─────────────────────────────────────────────────────────────────────────
    # CourtListener litigation enrichment (Phase 7)
    # ─────────────────────────────────────────────────────────────────────────

    async def _run_litigation_enrichment(
        self,
        entity: dict[str, Any],
        entity_id: str,
        name: str,
        run_id: str,
    ) -> bool:
        """
        Search CourtListener for cases involving this entity and extract
        structured litigation data via DocumentExtractor.

        Stores in category_fields["litigation"]:
            [
                {
                    case_name, case_number, court, filing_date,
                    case_type, outcome, monetary_judgment, summary,
                    docket_url,
                }
            ]

        The relationship agent reads this list → LITIGATION_AGAINST edges.

        Returns True if at least one case was found.
        """
        log.debug("enrichment_agent: CourtListener search for '%s'", name)

        try:
            cases = await self._courtlistener.search_cases(
                party_name=name,
                max_results=10,
            )
        except Exception as exc:
            log.debug("enrichment_agent: CourtListener search failed for '%s': %s", name, exc)
            return False

        if not cases:
            return False

        cat = entity.setdefault("category_fields", {})
        litigation_records: list[dict[str, Any]] = []

        for case in cases[:5]:  # Limit LLM calls — process top 5 cases
            docket_id = case.get("docket_id")
            if not docket_id:
                continue

            # Fetch docket text for LLM extraction
            try:
                docket_text = await self._courtlistener.get_docket_text(docket_id)
            except Exception as exc:
                log.debug(
                    "enrichment_agent: docket text fetch failed for %s: %s",
                    docket_id, exc,
                )
                docket_text = ""

            if not docket_text:
                # Use search-level metadata only (no LLM extraction)
                litigation_records.append({
                    "case_name":         case.get("case_name", ""),
                    "case_number":       case.get("case_number", ""),
                    "court":             case.get("court", ""),
                    "filing_date":       case.get("date_filed"),
                    "case_type":         "civil",
                    "outcome":           None,
                    "monetary_judgment": None,
                    "summary":           "",
                    "docket_url":        case.get("docket_url", ""),
                })
                continue

            # LLM extraction from docket text
            try:
                extracted = await self._doc_extractor.extract_court_filing(
                    text=docket_text,
                    party_name=name,
                )
            except Exception as exc:
                log.debug(
                    "enrichment_agent: court extraction failed for docket %s: %s",
                    docket_id, exc,
                )
                extracted = {}

            if extracted:
                extracted["docket_url"] = case.get("docket_url", "")
                litigation_records.append(extracted)
            else:
                litigation_records.append({
                    "case_name":         case.get("case_name", ""),
                    "case_number":       case.get("case_number", ""),
                    "court":             case.get("court", ""),
                    "filing_date":       case.get("date_filed"),
                    "case_type":         "civil",
                    "outcome":           None,
                    "monetary_judgment": None,
                    "summary":           "",
                    "docket_url":        case.get("docket_url", ""),
                })

        if not litigation_records:
            return False

        cat["litigation"] = litigation_records
        log.info(
            "enrichment_agent: CourtListener found %d cases for '%s'",
            len(litigation_records), name,
        )
        return True

    # ─────────────────────────────────────────────────────────────────────────
    # FinCEN CTR enrichment (Phase 8)
    # ─────────────────────────────────────────────────────────────────────────

    def _run_fincen_enrichment(
        self,
        entity: dict[str, Any],
        name: str,
    ) -> bool:
        """
        Look up FinCEN CTR filing history for a financial institution entity.

        Uses synchronous DuckDB query — no async needed (local file read).
        Silently returns False if the ETL database has not been populated.

        Trigger condition: entity_type in {"corporate"} and name is set.

        Stores in category_fields["fincen_ctr"]:
            {
                "institution":  str,
                "state":        str,
                "history":      [{year: int, ctr_count: int}],
                "total_ctrs":   int,
                "peak_year":    int,
                "peak_count":   int,
            }

        Returns True if CTR data was found.
        """
        state = entity.get("primary_state", "")
        try:
            result = self._fincen.get_institution_ctr_history(
                institution_name=name,
                state=state or None,
            )
        except Exception as exc:
            log.debug("enrichment_agent: FinCEN CTR lookup failed for '%s': %s", name, exc)
            return False

        if not result:
            return False

        cat = entity.setdefault("category_fields", {})
        cat["fincen_ctr"] = result
        log.info(
            "enrichment_agent: FinCEN CTR found %d total CTRs for '%s' (peak %d in %d)",
            result.get("total_ctrs", 0), name,
            result.get("peak_count", 0), result.get("peak_year", 0),
        )
        return True

    # ─────────────────────────────────────────────────────────────────────────
    # HUD multifamily enrichment (Phase 8)
    # ─────────────────────────────────────────────────────────────────────────

    def _run_hud_enrichment(
        self,
        entity: dict[str, Any],
        name: str,
        city: str = "",
        state: str = "",
    ) -> bool:
        """
        Look up HUD FHA-insured multifamily properties for an entity.

        Uses synchronous DuckDB query. Silently skips if ETL not populated.

        Trigger condition: entity_type in {"corporate", "real_estate", "investor"}
            and name is set.

        Stores in category_fields:
            "hud_properties":      list[dict] — individual property records
            "hud_portfolio_value": float — sum of loan_amount values (USD)

        The relationship agent reads hud_properties → OWNS edges.

        Returns True if any properties were found.
        """
        try:
            properties = self._hud.get_properties_by_owner(
                owner_name=name,
                city=city or None,
                state=state or None,
            )
        except Exception as exc:
            log.debug("enrichment_agent: HUD lookup failed for '%s': %s", name, exc)
            return False

        if not properties:
            return False

        cat = entity.setdefault("category_fields", {})
        cat["hud_properties"] = properties

        portfolio_value = sum(
            p["loan_amount"] for p in properties if p.get("loan_amount")
        )
        if portfolio_value:
            cat["hud_portfolio_value"] = portfolio_value

        # Update net worth floor if HUD portfolio value is significant
        existing_floor = entity.get("net_worth_floor") or 0
        if portfolio_value and portfolio_value > existing_floor:
            entity["net_worth_floor"] = portfolio_value
            log.debug(
                "enrichment_agent: HUD net worth floor updated for '%s': $%,.0f",
                name, portfolio_value,
            )

        log.info(
            "enrichment_agent: HUD found %d properties for '%s' (portfolio value: $%,.0f)",
            len(properties), name, portfolio_value,
        )
        return True

    # ─────────────────────────────────────────────────────────────────────────
    # Phase 9 — ICIJ Offshore Leaks enrichment
    # ─────────────────────────────────────────────────────────────────────────

    async def _run_icij_enrichment(
        self,
        entity: dict[str, Any],
        entity_id: str,
        name: str,
        run_id: str,
        country_code: str = "",
    ) -> bool:
        """
        Screen an entity against the ICIJ Offshore Leaks Neo4j subgraph.

        Silently skips if:
            - Neo4j is unreachable
            - ICIJ ETL has not been run (no ICIJNode data in Neo4j)
            - No match found above the fuzzy threshold

        Trigger condition: entity_type in {"corporate", "investor", "illicit"}
            and icij_nodes not already populated.

        On match, stores in category_fields:
            "icij_nodes":       list[dict] — matched ICIJ node summaries
                                Each: {icij_id, name, icij_type, source_dataset,
                                       countries, country_codes, similarity}
            "icij_shell_chain": list[dict] — connected nodes (shell chain)
                                Each: {icij_id, name, icij_type, source_dataset,
                                       countries, rel_types}
            "offshore_flag":    bool — True if any match found

        Also flags the entity:
            needs_review      = True   (ICIJ match requires human verification)
            sensitivity_tier  = "restricted"

        Returns True if any ICIJ match was found.
        """
        try:
            matches = await self._icij.find_entity_matches(
                name=name,
                country=country_code or None,
            )
        except Exception as exc:
            log.debug("enrichment_agent: ICIJ search failed for '%s': %s", name, exc)
            return False

        if not matches:
            return False

        cat = entity.setdefault("category_fields", {})
        cat["icij_nodes"]    = matches
        cat["offshore_flag"] = True

        # Flag entity for human review — ICIJ match is a sensitive claim
        entity["needs_review"]     = True
        entity["sensitivity_tier"] = "restricted"

        log.info(
            "enrichment_agent: ICIJ match found for '%s' — %d node(s), "
            "top match: '%s' (%s, similarity=%.2f)",
            name, len(matches),
            matches[0].get("name"), matches[0].get("source_dataset"),
            matches[0].get("similarity", 0),
        )

        # Fetch shell chain from the best-scoring match
        best_icij_id = matches[0].get("icij_id", "")
        if best_icij_id:
            try:
                shell_chain = await self._icij.get_shell_chain(
                    icij_node_id=best_icij_id,
                    max_depth=4,
                )
                if shell_chain:
                    cat["icij_shell_chain"] = shell_chain
                    log.debug(
                        "enrichment_agent: ICIJ shell chain for '%s': %d connected nodes",
                        name, len(shell_chain),
                    )
            except Exception as exc:
                log.debug(
                    "enrichment_agent: ICIJ shell chain failed for '%s': %s", name, exc
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


# City coordinates for Meetup GraphQL search (requires lat/lon, no city-name search).
# Covers the same cities as _CITY_STATE_MAP in state.py.
# Format: city_name_lowercase → (lat, lon)
_CITY_COORDS: dict[str, tuple[float, float]] = {
    "philadelphia":     (39.9526, -75.1652),
    "new york":         (40.7128, -74.0060),
    "new york city":    (40.7128, -74.0060),
    "nyc":              (40.7128, -74.0060),
    "los angeles":      (34.0522, -118.2437),
    "chicago":          (41.8781, -87.6298),
    "houston":          (29.7604, -95.3698),
    "phoenix":          (33.4484, -112.0740),
    "san antonio":      (29.4241, -98.4936),
    "san diego":        (32.7157, -117.1611),
    "dallas":           (32.7767, -96.7970),
    "san jose":         (37.3382, -121.8863),
    "austin":           (30.2672, -97.7431),
    "jacksonville":     (30.3322, -81.6557),
    "fort worth":       (32.7555, -97.3308),
    "columbus":         (39.9612, -82.9988),
    "charlotte":        (35.2271, -80.8431),
    "san francisco":    (37.7749, -122.4194),
    "indianapolis":     (39.7684, -86.1581),
    "seattle":          (47.6062, -122.3321),
    "denver":           (39.7392, -104.9903),
    "washington":       (38.9072, -77.0369),
    "washington dc":    (38.9072, -77.0369),
    "nashville":        (36.1627, -86.7816),
    "oklahoma city":    (35.4676, -97.5164),
    "el paso":          (31.7619, -106.4850),
    "boston":           (42.3601, -71.0589),
    "portland":         (45.5231, -122.6765),
    "las vegas":        (36.1699, -115.1398),
    "memphis":          (35.1495, -90.0490),
    "louisville":       (38.2527, -85.7585),
    "baltimore":        (39.2904, -76.6122),
    "milwaukee":        (43.0389, -87.9065),
    "albuquerque":      (35.0844, -106.6504),
    "tucson":           (32.2226, -110.9747),
    "fresno":           (36.7378, -119.7871),
    "sacramento":       (38.5816, -121.4944),
    "mesa":             (33.4152, -111.8315),
    "kansas city":      (39.0997, -94.5786),
    "atlanta":          (33.7490, -84.3880),
    "omaha":            (41.2565, -95.9345),
    "colorado springs": (38.8339, -104.8214),
    "raleigh":          (35.7796, -78.6382),
    "virginia beach":   (36.8529, -75.9780),
    "long beach":       (33.7701, -118.1937),
    "minneapolis":      (44.9778, -93.2650),
    "tampa":            (27.9506, -82.4572),
    "new orleans":      (29.9511, -90.0715),
    "arlington":        (32.7357, -97.1081),
    "miami":            (25.7617, -80.1918),
    "pittsburgh":       (40.4406, -79.9959),
    "cincinnati":       (39.1031, -84.5120),
    "cleveland":        (41.4993, -81.6944),
    "detroit":          (42.3314, -83.0458),
    "richmond":         (37.5407, -77.4360),
    "st louis":         (38.6270, -90.1994),
    "saint louis":      (38.6270, -90.1994),
}


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
