"""
osint/db/supabase.py

Supabase / PostgreSQL client wrapper for the OSINT system.

All database writes for the canonical data store go through this class.
Every method validates inputs against the SQL schema in 001_initial_schema.sql.

Usage:
    db = SupabaseClient()
    await db.upsert_run(run_record)
    entity_id = await db.write_entity(entity_dict)
    await db.write_evidence(evidence_record)

Connection:
    Uses asyncpg for raw async PostgreSQL access.
    Supabase is used as the Postgres host — not the Supabase Python client,
    which has too much overhead for bulk writes.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

import asyncpg

from osint.core.config import settings

log = logging.getLogger(__name__)


class SupabaseClientError(Exception):
    """Raised when a DB write fails validation or PostgreSQL returns an error."""
    pass


class SupabaseClient:
    """
    Async PostgreSQL client backed by asyncpg.
    Manages a connection pool shared across all agents in a worker process.
    """

    def __init__(self) -> None:
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        """
        Initialize the asyncpg connection pool.
        Call once at worker startup. Raises if DATABASE_URL is not set.
        """
        if not settings.database_url:
            raise SupabaseClientError(
                "DATABASE_URL is not set. Add it to .env — get it from your Supabase project settings."
            )
        self._pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=2,
            max_size=10,
            command_timeout=30,
            # Supabase's pooler drops idle connections after ~5 minutes.
            # Setting max_inactive_connection_lifetime to 300s ensures asyncpg
            # recycles connections before the pooler kills them, preventing
            # "connection closed unexpectedly" errors mid-pipeline.
            max_inactive_connection_lifetime=300,
        )
        log.info("SupabaseClient: connection pool created (min=2, max=10)")

    async def disconnect(self) -> None:
        """Close the connection pool. Call at worker shutdown."""
        if self._pool:
            await self._pool.close()
            self._pool = None

    def _pool_required(self) -> asyncpg.Pool:
        if self._pool is None:
            raise SupabaseClientError(
                "SupabaseClient not connected. Call await client.connect() first."
            )
        return self._pool

    # ─────────────────────────────────────────────────────────────────────────
    # agent_runs table
    # ─────────────────────────────────────────────────────────────────────────

    async def upsert_run(self, run: dict[str, Any]) -> str:
        """
        Insert or update an agent_runs record.
        Returns run_id (UUID string).

        On conflict (run_id already exists), updates status and timing fields.
        """
        pool = self._pool_required()
        run_id = run.get("run_id") or str(uuid.uuid4())

        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO agent_runs (
                    run_id, city_name, country_or_region, city_key,
                    run_status, model_default, model_escalation,
                    triggered_by, trigger_type, is_delta_run, previous_run_id,
                    started_at
                ) VALUES (
                    $1::uuid, $2, $3, $4,
                    $5, $6, $7,
                    $8, $9, $10, $11::uuid,
                    NOW()
                )
                ON CONFLICT (run_id) DO UPDATE SET
                    run_status      = EXCLUDED.run_status,
                    completed_at    = EXCLUDED.completed_at,
                    duration_seconds = EXCLUDED.duration_seconds,
                    total_entities_found      = COALESCE(EXCLUDED.total_entities_found, agent_runs.total_entities_found),
                    total_relationships_found = COALESCE(EXCLUDED.total_relationships_found, agent_runs.total_relationships_found),
                    total_items_rejected      = COALESCE(EXCLUDED.total_items_rejected, agent_runs.total_items_rejected),
                    failure_reason  = EXCLUDED.failure_reason,
                    overall_confidence = EXCLUDED.overall_confidence
                """,
                run_id,
                run["city_name"],
                run.get("country_or_region", "United States"),
                run["city_key"],
                run.get("run_status", "pending"),
                run.get("model_default", settings.ollama_default_model),
                run.get("model_escalation", settings.ollama_escalation_model),
                run.get("triggered_by"),
                run.get("trigger_type", "manual"),
                run.get("is_delta_run", False),
                run.get("previous_run_id"),
            )

        log.debug("upsert_run: %s (%s)", run_id, run.get("run_status"))
        return run_id

    async def complete_run(
        self,
        run_id: str,
        status: str,
        summary: dict[str, Any],
        failure_reason: str | None = None,
    ) -> None:
        """
        Mark a run as complete/failed and write the result summary.
        """
        pool = self._pool_required()
        started_at_raw = await self._get_run_started_at(run_id)
        duration = None
        if started_at_raw:
            duration = int((datetime.now(timezone.utc) - started_at_raw).total_seconds())

        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE agent_runs SET
                    run_status              = $2,
                    completed_at            = NOW(),
                    duration_seconds        = $3,
                    failure_reason          = $4,
                    total_entities_found    = $5,
                    total_relationships_found = $6,
                    total_claims_verified   = $7,
                    total_claims_failed     = $8,
                    total_items_rejected    = $9,
                    overall_confidence      = $10,
                    pass_count              = $11,
                    gap_fill_triggered      = $12,
                    categories_thin         = $13
                WHERE run_id = $1::uuid
                """,
                run_id,
                status,
                duration,
                failure_reason,
                summary.get("entities_total", 0),
                summary.get("relationships_total", 0),
                summary.get("claims_verified", 0),
                summary.get("claims_failed", 0),
                summary.get("items_rejected", 0),
                summary.get("overall_confidence"),
                summary.get("pass_count", 1),
                summary.get("gap_fill_triggered", False),
                summary.get("categories_thin", []),
            )

    async def _get_run_started_at(self, run_id: str) -> datetime | None:
        pool = self._pool_required()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT started_at FROM agent_runs WHERE run_id = $1::uuid", run_id
            )
            return row["started_at"] if row else None

    # ─────────────────────────────────────────────────────────────────────────
    # entities table
    # ─────────────────────────────────────────────────────────────────────────

    async def write_entity(self, entity: dict[str, Any]) -> str:
        """
        Insert a new canonical entity record.
        Returns entity_id (UUID string).

        This is an INSERT, not upsert. Temporal versioning means we never UPDATE
        an entity — we insert a new record. The caller is responsible for setting
        valid_to on the old record before calling this.

        Raises SupabaseClientError if required fields are missing.
        """
        pool = self._pool_required()
        self._validate_entity(entity)

        entity_id = entity.get("entity_id") or str(uuid.uuid4())

        # Separate category_fields from base fields — category_fields goes in JSONB column
        category_fields = entity.get("category_fields", {})

        # Collect external IDs from category_fields or entity dict
        ext = entity.get("external_ids", {})

        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO entities (
                    entity_id, canonical_name, entity_type, entity_subtype, aliases,
                    valid_from, valid_to, superseded_by,
                    crunchbase_id, ein, fec_candidate_id, fec_committee_id,
                    sec_crd_number, sec_cik, opencorporates_id, bioguide_id,
                    opensecrets_id, wikidata_id,
                    primary_city, primary_city_status,
                    primary_state, primary_state_status,
                    primary_country, primary_country_status,
                    website_url, website_url_status,
                    linkedin_url, linkedin_url_status,
                    description, description_status,
                    source_agent, source_run_ids, merge_provenance, source_urls,
                    last_seen, last_verified,
                    overall_confidence, source_count, corroboration_count,
                    partner_candidate, competitor_candidate, blocker_candidate,
                    investment_candidate, support_candidate, recruiter_candidate, top_influencer,
                    score_influence, score_startup_relevance, score_partner_potential,
                    score_supporter_potential, score_competitor_potential, score_blocker_risk,
                    score_investment_potential, score_support_target, score_recruiting_potential,
                    needs_review, sensitivity_tier,
                    category_fields, proxycurl_retrieved, proxycurl_retrieved_at
                ) VALUES (
                    $1::uuid, $2, $3, $4, $5,
                    $6, $7, $8::uuid,
                    $9, $10, $11, $12,
                    $13, $14, $15, $16,
                    $17, $18,
                    $19, $20,
                    $21, $22,
                    $23, $24,
                    $25, $26,
                    $27, $28,
                    $29, $30,
                    $31, $32::uuid[], $33::jsonb, $34,
                    $35, $36,
                    $37, $38, $39,
                    $40, $41, $42,
                    $43, $44, $45, $46,
                    $47, $48, $49,
                    $50, $51, $52,
                    $53, $54, $55,
                    $56, $57,
                    $58::jsonb, $59, $60
                )
                """,
                entity_id,
                entity["canonical_name"],
                entity["entity_type"],
                entity.get("entity_subtype"),
                entity.get("aliases", []),
                self._coerce_dt(entity.get("valid_from")),
                self._coerce_dt(entity.get("valid_to")),
                entity.get("superseded_by"),
                # External IDs
                ext.get("crunchbase_id") or entity.get("crunchbase_id"),
                ext.get("ein") or entity.get("ein"),
                ext.get("fec_candidate_id") or entity.get("fec_candidate_id"),
                ext.get("fec_committee_id") or entity.get("fec_committee_id"),
                ext.get("sec_crd_number") or entity.get("sec_crd_number"),
                ext.get("sec_cik") or entity.get("sec_cik"),
                ext.get("opencorporates_id") or entity.get("opencorporates_id"),
                ext.get("bioguide_id") or entity.get("bioguide_id"),
                ext.get("opensecrets_id") or entity.get("opensecrets_id"),
                ext.get("wikidata_id") or entity.get("wikidata_id"),
                # Location
                entity.get("primary_city"),
                entity.get("primary_city_status", "NOT_COLLECTED"),
                entity.get("primary_state"),
                entity.get("primary_state_status", "NOT_COLLECTED"),
                entity.get("primary_country", "United States"),
                entity.get("primary_country_status", "NOT_COLLECTED"),
                # Web
                entity.get("website_url"),
                entity.get("website_url_status", "NOT_COLLECTED"),
                entity.get("linkedin_url"),
                entity.get("linkedin_url_status", "NOT_COLLECTED"),
                # Description
                entity.get("description"),
                entity.get("description_status", "NOT_COLLECTED"),
                # Provenance
                entity["source_agent"],
                entity.get("source_run_ids", []),
                json.dumps(entity.get("merge_provenance", [])),
                entity.get("source_urls", []),
                self._coerce_dt(entity.get("last_seen")),
                self._coerce_dt(entity.get("last_verified")),
                # Confidence
                entity.get("overall_confidence"),
                entity.get("source_count", 0),
                entity.get("corroboration_count", 0),
                # Classification flags
                entity.get("partner_candidate", False),
                entity.get("competitor_candidate", False),
                entity.get("blocker_candidate", False),
                entity.get("investment_candidate", False),
                entity.get("support_candidate", False),
                entity.get("recruiter_candidate", False),
                entity.get("top_influencer", False),
                # Scores
                entity.get("score_influence", 0),
                entity.get("score_startup_relevance", 0),
                entity.get("score_partner_potential", 0),
                entity.get("score_supporter_potential", 0),
                entity.get("score_competitor_potential", 0),
                entity.get("score_blocker_risk", 0),
                entity.get("score_investment_potential", 0),
                entity.get("score_support_target", 0),
                entity.get("score_recruiting_potential", 0),
                # Sensitivity
                entity.get("needs_review", False),
                entity.get("sensitivity_tier", "standard"),
                # Category fields (JSONB)
                json.dumps(category_fields),
                entity.get("proxycurl_retrieved", False),
                self._coerce_dt(entity.get("proxycurl_retrieved_at")),
            )

        log.debug(
            "write_entity: %s (%s / %s)",
            entity_id, entity["entity_type"], entity["canonical_name"]
        )
        return entity_id

    async def expire_entity(self, entity_id: str, superseded_by: str) -> None:
        """
        Set valid_to = NOW() on an entity record being superseded.
        Call before write_entity when updating an existing entity.
        """
        pool = self._pool_required()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE entities SET
                    valid_to        = NOW(),
                    superseded_by   = $2::uuid
                WHERE entity_id = $1::uuid AND valid_to IS NULL
                """,
                entity_id,
                superseded_by,
            )

    async def update_entity_scores(
        self,
        entity_id: str,
        scores: dict[str, int],
        classification_flags: dict[str, bool],
    ) -> None:
        """
        Update the 9-dimension score columns and classification flags on an existing
        entity record in-place.

        Called by the Scoring Agent after LLM scoring — scores are computed values
        that do NOT require temporal versioning (they are derived from the entity data
        in this run, not new source data).

        Args:
            entity_id: The entity to update.
            scores: Dict of score_* column names → integer values (0–100).
            classification_flags: Dict of flag column names → bool values.
        """
        pool = self._pool_required()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE entities SET
                    score_influence              = $2,
                    score_startup_relevance      = $3,
                    score_partner_potential      = $4,
                    score_supporter_potential    = $5,
                    score_competitor_potential   = $6,
                    score_blocker_risk           = $7,
                    score_investment_potential   = $8,
                    score_support_target         = $9,
                    score_recruiting_potential   = $10,
                    partner_candidate            = $11,
                    competitor_candidate         = $12,
                    blocker_candidate            = $13,
                    investment_candidate         = $14,
                    support_candidate            = $15,
                    recruiter_candidate          = $16,
                    top_influencer               = $17
                WHERE entity_id = $1::uuid AND valid_to IS NULL
                """,
                entity_id,
                scores.get("score_influence", 0),
                scores.get("score_startup_relevance", 0),
                scores.get("score_partner_potential", 0),
                scores.get("score_supporter_potential", 0),
                scores.get("score_competitor_potential", 0),
                scores.get("score_blocker_risk", 0),
                scores.get("score_investment_potential", 0),
                scores.get("score_support_target", 0),
                scores.get("score_recruiting_potential", 0),
                classification_flags.get("partner_candidate", False),
                classification_flags.get("competitor_candidate", False),
                classification_flags.get("blocker_candidate", False),
                classification_flags.get("investment_candidate", False),
                classification_flags.get("support_candidate", False),
                classification_flags.get("recruiter_candidate", False),
                classification_flags.get("top_influencer", False),
            )

    async def update_entity_category_fields(
        self,
        entity_id: str,
        category_fields: dict,
    ) -> None:
        """
        Persist enriched category_fields JSONB back to the current entity record.

        Called by the Scoring Agent after enrichment is complete. category_fields
        is populated during the enrichment phase (HUD properties, FinCEN CTR,
        CourtListener cases, EDGAR compensation, etc.) but write_entity() is only
        called at entity creation time (resolution.py) and for OFAC temporal
        versioning. This method closes that gap for all non-OFAC entities.

        Not a temporal versioning event — category_fields are derived enrichment
        data, not new source identity data. UPDATE in-place on the current record.

        Args:
            entity_id:        The entity UUID to update.
            category_fields:  Dict of enrichment data keyed by source name.
        """
        if not category_fields:
            return
        pool = self._pool_required()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE entities
                SET category_fields = $2::jsonb
                WHERE entity_id = $1::uuid AND valid_to IS NULL
                """,
                entity_id,
                json.dumps(category_fields),
            )

    async def flag_entities_for_review(self, entity_ids: list[str]) -> None:
        """
        Set needs_review = true for a list of entities via a single targeted UPDATE.

        Called by the Verification Agent for entities that have at least one
        high-confidence "fail" verdict.  This is a non-temporal update — we are
        setting a review flag on the current canonical record, not superseding it.

        Args:
            entity_ids: List of entity_id UUID strings to flag.
        """
        if not entity_ids:
            return
        pool = self._pool_required()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE entities
                SET needs_review = true
                WHERE entity_id = ANY($1::uuid[]) AND valid_to IS NULL
                """,
                entity_ids,
            )
        log.debug("flag_entities_for_review: flagged %d entities", len(entity_ids))

    async def get_entity(self, entity_id: str) -> dict[str, Any] | None:
        """Fetch a single entity by ID. Returns None if not found."""
        pool = self._pool_required()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM entities WHERE entity_id = $1::uuid AND valid_to IS NULL",
                entity_id,
            )
            return dict(row) if row else None

    async def get_relationships_by_run(
        self, run_id: str, verified_only: bool = False
    ) -> list[dict[str, Any]]:
        """Fetch all relationships produced by a run."""
        pool = self._pool_required()
        async with pool.acquire() as conn:
            if verified_only:
                rows = await conn.fetch(
                    "SELECT * FROM relationships WHERE run_id = $1::uuid AND verified = TRUE",
                    run_id,
                )
            else:
                rows = await conn.fetch(
                    "SELECT * FROM relationships WHERE run_id = $1::uuid",
                    run_id,
                )
            return [dict(r) for r in rows]

    async def get_entities_by_run(
        self, run_id: str, entity_type: str | None = None
    ) -> list[dict[str, Any]]:
        """Fetch all entities produced by a run, optionally filtered by type."""
        pool = self._pool_required()
        async with pool.acquire() as conn:
            if entity_type:
                rows = await conn.fetch(
                    """
                    SELECT * FROM entities
                    WHERE $1::uuid = ANY(source_run_ids)
                    AND entity_type = $2
                    AND valid_to IS NULL
                    ORDER BY score_influence DESC
                    """,
                    run_id, entity_type,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT * FROM entities
                    WHERE $1::uuid = ANY(source_run_ids)
                    AND valid_to IS NULL
                    ORDER BY entity_type, score_influence DESC
                    """,
                    run_id,
                )
            return [dict(r) for r in rows]

    # ─────────────────────────────────────────────────────────────────────────
    # entity_evidence table
    # ─────────────────────────────────────────────────────────────────────────

    async def write_evidence(self, evidence: dict[str, Any]) -> str:
        """
        Insert an entity_evidence record.
        Returns link_id (UUID string).

        source_url and evidence_snippet are MANDATORY — raises if absent.
        """
        pool = self._pool_required()
        if not evidence.get("source_url"):
            raise SupabaseClientError(
                f"write_evidence: source_url is required. entity_id={evidence.get('entity_id')}"
            )
        if not evidence.get("evidence_snippet"):
            raise SupabaseClientError(
                f"write_evidence: evidence_snippet is required. entity_id={evidence.get('entity_id')}"
            )

        link_id = evidence.get("link_id") or str(uuid.uuid4())

        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO entity_evidence (
                    link_id, entity_id, run_id,
                    supported_field, supported_value,
                    source_url, source_type, source_api, archived_url, sha256_hash,
                    retrieved_at, evidence_snippet, claim_type, confidence,
                    agent_name, prompt_version
                ) VALUES (
                    $1::uuid, $2::uuid, $3::uuid,
                    $4, $5,
                    $6, $7, $8, $9, $10,
                    $11, $12, $13, $14,
                    $15, $16
                )
                """,
                link_id,
                evidence["entity_id"],
                evidence["run_id"],
                evidence["supported_field"],
                evidence.get("supported_value"),
                evidence["source_url"],
                evidence.get("source_type", "api_response"),
                evidence.get("source_api"),
                evidence.get("archived_url"),
                evidence.get("sha256_hash"),
                self._coerce_dt(evidence.get("retrieved_at")),
                evidence["evidence_snippet"],
                evidence.get("claim_type", "direct_statement"),
                evidence.get("confidence", "medium"),
                evidence["agent_name"],
                evidence.get("prompt_version"),
            )

        return link_id

    async def write_evidence_batch(self, records: list[dict[str, Any]]) -> list[str]:
        """
        Batch insert multiple evidence records. More efficient than calling
        write_evidence in a loop for bulk collection output.
        Returns list of link_ids in order.
        """
        link_ids = []
        pool = self._pool_required()
        async with pool.acquire() as conn:
            async with conn.transaction():
                for evidence in records:
                    if not evidence.get("source_url") or not evidence.get("evidence_snippet"):
                        log.warning(
                            "write_evidence_batch: skipping record missing source_url or snippet "
                            "(entity_id=%s, field=%s)",
                            evidence.get("entity_id"), evidence.get("supported_field")
                        )
                        continue
                    link_id = evidence.get("link_id") or str(uuid.uuid4())
                    await conn.execute(
                        """
                        INSERT INTO entity_evidence (
                            link_id, entity_id, run_id,
                            supported_field, supported_value,
                            source_url, source_type, source_api, archived_url, sha256_hash,
                            retrieved_at, evidence_snippet, claim_type, confidence,
                            agent_name, prompt_version
                        ) VALUES (
                            $1::uuid, $2::uuid, $3::uuid,
                            $4, $5, $6, $7, $8, $9, $10,
                            $11, $12, $13, $14, $15, $16
                        )
                        """,
                        link_id,
                        evidence["entity_id"],
                        evidence["run_id"],
                        evidence["supported_field"],
                        evidence.get("supported_value"),
                        evidence["source_url"],
                        evidence.get("source_type", "api_response"),
                        evidence.get("source_api"),
                        evidence.get("archived_url"),
                        evidence.get("sha256_hash"),
                        self._coerce_dt(evidence.get("retrieved_at")),
                        evidence["evidence_snippet"],
                        evidence.get("claim_type", "direct_statement"),
                        evidence.get("confidence", "medium"),
                        evidence["agent_name"],
                        evidence.get("prompt_version"),
                    )
                    link_ids.append(link_id)
        return link_ids

    # ─────────────────────────────────────────────────────────────────────────
    # analytical_assessments table
    # ─────────────────────────────────────────────────────────────────────────

    async def write_assessment(self, assessment: dict[str, Any]) -> str:
        """
        Insert an analytical_assessments record.
        Returns assessment_id (UUID string).

        claim_text, framework_name, framework_version, model_used, prompt_version are required.
        """
        pool = self._pool_required()
        required = ["claim_text", "framework_name", "framework_version", "model_used", "prompt_version"]
        missing = [f for f in required if not assessment.get(f)]
        if missing:
            raise SupabaseClientError(
                f"write_assessment: missing required fields: {missing}"
            )

        assessment_id = assessment.get("assessment_id") or str(uuid.uuid4())

        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO analytical_assessments (
                    assessment_id, entity_id, run_id,
                    assessment_type, claim_text, claim_json,
                    framework_name, framework_version, derived_from,
                    model_used, prompt_version,
                    confidence, needs_review,
                    superseded_by, is_current
                ) VALUES (
                    $1::uuid, $2::uuid, $3::uuid,
                    $4, $5, $6::jsonb,
                    $7, $8, $9::uuid[],
                    $10, $11,
                    $12, $13,
                    $14::uuid, $15
                )
                """,
                assessment_id,
                assessment.get("entity_id"),
                assessment["run_id"],
                assessment.get("assessment_type", "briefing_claim"),
                assessment["claim_text"],
                json.dumps(assessment.get("claim_json")) if assessment.get("claim_json") else None,
                assessment["framework_name"],
                assessment["framework_version"],
                assessment.get("derived_from", []),
                assessment["model_used"],
                assessment["prompt_version"],
                assessment.get("confidence"),
                assessment.get("needs_review", False),
                assessment.get("superseded_by"),
                assessment.get("is_current", True),
            )

        return assessment_id

    # ─────────────────────────────────────────────────────────────────────────
    # Read methods — API layer queries
    # ─────────────────────────────────────────────────────────────────────────

    async def list_runs(
        self,
        city_key: str | None = None,
        status: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """
        Paginated list of agent_runs, newest first.

        Args:
            city_key: Normalized city identifier e.g. "philadelphia_us"
            status:   Filter by run_status: pending|running|complete|partial|failed
            limit:    Max records (capped at 100)
            offset:   Pagination offset
        """
        pool = self._pool_required()
        limit = min(limit, 100)

        conditions: list[str] = []
        params: list[Any] = []
        idx = 1

        if city_key:
            conditions.append(f"city_key = ${idx}")
            params.append(city_key)
            idx += 1

        if status:
            conditions.append(f"run_status = ${idx}")
            params.append(status)
            idx += 1

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.extend([limit, offset])

        query = f"""
            SELECT run_id, city_name, country_or_region, city_key,
                   run_status, started_at, completed_at, duration_seconds,
                   total_entities_found, total_relationships_found,
                   total_claims_verified, gap_fill_triggered,
                   overall_confidence, failure_reason
            FROM agent_runs
            {where}
            ORDER BY started_at DESC NULLS LAST
            LIMIT ${idx} OFFSET ${idx + 1}
        """

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            return [dict(r) for r in rows]

    async def count_runs(
        self,
        city_key: str | None = None,
        status: str | None = None,
    ) -> int:
        """Count runs matching optional filters."""
        pool = self._pool_required()
        conditions: list[str] = []
        params: list[Any] = []
        idx = 1

        if city_key:
            conditions.append(f"city_key = ${idx}")
            params.append(city_key)
            idx += 1
        if status:
            conditions.append(f"run_status = ${idx}")
            params.append(status)
            idx += 1

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        async with pool.acquire() as conn:
            row = await conn.fetchrow(f"SELECT COUNT(*) AS n FROM agent_runs {where}", *params)
            return row["n"] if row else 0

    async def get_run(self, run_id: str) -> dict[str, Any] | None:
        """
        Fetch a single agent_runs record by run_id.
        Returns None if not found.
        """
        pool = self._pool_required()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM agent_runs WHERE run_id = $1::uuid",
                run_id,
            )
            return dict(row) if row else None

    async def get_briefing(self, run_id: str) -> dict[str, Any] | None:
        """
        Fetch the final briefing for a run from analytical_assessments.
        Prefers 'final_briefing_full' (complete content) over 'final_briefing' (summary).
        Returns the claim_json dict, or None if not generated yet.
        """
        pool = self._pool_required()
        async with pool.acquire() as conn:
            # Try full briefing first (written by briefing agent after pipeline completes)
            row = await conn.fetchrow(
                """
                SELECT claim_json FROM analytical_assessments
                WHERE run_id = $1::uuid
                  AND assessment_type = 'final_briefing_full'
                  AND is_current = true
                ORDER BY created_at DESC
                LIMIT 1
                """,
                run_id,
            )
            if not row or not row["claim_json"]:
                # Fall back to summary assessment
                row = await conn.fetchrow(
                    """
                    SELECT claim_json FROM analytical_assessments
                    WHERE run_id = $1::uuid
                      AND assessment_type = 'final_briefing'
                      AND is_current = true
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    run_id,
                )
            if not row or not row["claim_json"]:
                return None
            data = row["claim_json"]
            return json.loads(data) if isinstance(data, str) else data

    async def search_entities(
        self,
        city_key: str | None = None,
        entity_type: str | None = None,
        run_id: str | None = None,
        needs_review: bool | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """
        Paginated entity search with optional filters.
        Filters: city_key, entity_type, run_id (must appear in source_run_ids), needs_review.
        Always excludes expired records (valid_to IS NULL).
        Sorted by score_influence DESC then canonical_name ASC.

        Args:
            city_key:     Normalized city identifier, e.g. "austin_us"
            entity_type:  One of the 10 collection types
            run_id:       Filter to entities produced by a specific run
            needs_review: If provided, filter by review flag
            limit:        Max records to return (capped at 200)
            offset:       Pagination offset
        """
        pool = self._pool_required()
        limit = min(limit, 200)

        conditions = ["valid_to IS NULL"]
        params: list[Any] = []
        param_idx = 1

        if city_key:
            # entities have no city_key column — filter via agent_runs join
            conditions.append(
                f"EXISTS (SELECT 1 FROM agent_runs ar "
                f"WHERE ar.run_id = ANY(source_run_ids) AND ar.city_key = ${param_idx})"
            )
            params.append(city_key)
            param_idx += 1

        if entity_type:
            conditions.append(f"entity_type = ${param_idx}")
            params.append(entity_type)
            param_idx += 1

        if run_id:
            conditions.append(f"${param_idx}::uuid = ANY(source_run_ids)")
            params.append(run_id)
            param_idx += 1

        if needs_review is not None:
            conditions.append(f"needs_review = ${param_idx}")
            params.append(needs_review)
            param_idx += 1

        where_clause = " AND ".join(conditions)
        params.extend([limit, offset])

        query = f"""
            SELECT
                entity_id, canonical_name, entity_type, entity_subtype,
                primary_city, primary_state,
                overall_confidence, needs_review,
                score_influence, score_partner_potential, score_startup_relevance,
                score_blocker_risk, score_competitor_potential,
                score_investment_potential, score_recruiting_potential,
                score_supporter_potential,
                partner_candidate, blocker_candidate, competitor_candidate,
                investment_candidate, top_influencer,
                description, source_run_ids, created_at
            FROM entities
            WHERE {where_clause}
            ORDER BY score_influence DESC NULLS LAST, canonical_name ASC
            LIMIT ${param_idx} OFFSET ${param_idx + 1}
        """

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            return [dict(r) for r in rows]

    async def count_entities(
        self,
        city_key: str | None = None,
        entity_type: str | None = None,
        run_id: str | None = None,
    ) -> int:
        """Count entities matching the given filters (for pagination total)."""
        pool = self._pool_required()
        conditions = ["valid_to IS NULL"]
        params: list[Any] = []
        param_idx = 1

        if city_key:
            conditions.append(
                f"EXISTS (SELECT 1 FROM agent_runs ar "
                f"WHERE ar.run_id = ANY(source_run_ids) AND ar.city_key = ${param_idx})"
            )
            params.append(city_key)
            param_idx += 1
        if entity_type:
            conditions.append(f"entity_type = ${param_idx}")
            params.append(entity_type)
            param_idx += 1
        if run_id:
            conditions.append(f"${param_idx}::uuid = ANY(source_run_ids)")
            params.append(run_id)
            param_idx += 1

        where_clause = " AND ".join(conditions)
        async with pool.acquire() as conn:
            return await conn.fetchval(
                f"SELECT COUNT(*) FROM entities WHERE {where_clause}",
                *params,
            )

    # ─────────────────────────────────────────────────────────────────────────
    # osint_search_records table
    # ─────────────────────────────────────────────────────────────────────────

    async def write_search_record(self, record: dict[str, Any]) -> str:
        """
        Insert an osint_search_records record.
        Returns search_id (UUID string).

        Written immediately at search time — do not batch.
        """
        pool = self._pool_required()
        search_id = record.get("search_id") or str(uuid.uuid4())

        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO osint_search_records (
                    search_id, run_id, agent_name,
                    entity_type, entity_id, raw_entity_id,
                    source_searched, query_used, search_framing,
                    result_found, result_count, failure_reason,
                    http_status_code, response_time_ms,
                    served_from_cache, cache_key,
                    timestamp
                ) VALUES (
                    $1::uuid, $2::uuid, $3,
                    $4, $5::uuid, $6,
                    $7, $8, $9,
                    $10, $11, $12,
                    $13, $14,
                    $15, $16,
                    $17
                )
                """,
                search_id,
                record["run_id"],
                record["agent_name"],
                record.get("entity_type"),
                record.get("entity_id"),
                record.get("raw_entity_id"),
                record["source_searched"],
                record["query_used"],
                record.get("search_framing"),
                record.get("result_found", False),
                record.get("result_count"),
                record.get("failure_reason"),
                record.get("http_status_code"),
                record.get("response_time_ms"),
                record.get("served_from_cache", False),
                record.get("cache_key"),
                self._coerce_dt(record.get("timestamp")),
            )

        return search_id

    # ─────────────────────────────────────────────────────────────────────────
    # relationships table
    # ─────────────────────────────────────────────────────────────────────────

    async def write_relationship(self, edge: dict[str, Any]) -> str:
        """
        Insert a relationships record.
        Returns relationship_id (UUID string).

        evidence_ids MUST be non-empty — raises if empty.
        """
        pool = self._pool_required()
        if not edge.get("evidence_ids"):
            raise SupabaseClientError(
                f"write_relationship: evidence_ids cannot be empty. "
                f"source={edge.get('source_entity_id')} → target={edge.get('target_entity_id')} "
                f"type={edge.get('relationship_type')}"
            )
        if edge.get("source_entity_id") == edge.get("target_entity_id"):
            raise SupabaseClientError(
                f"write_relationship: self-loops are not permitted. "
                f"entity_id={edge.get('source_entity_id')}"
            )

        relationship_id = edge.get("relationship_id") or str(uuid.uuid4())

        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO relationships (
                    relationship_id, run_id,
                    source_entity_id, target_entity_id,
                    relationship_type, direction,
                    evidence_ids, evidence_snippets,
                    confidence, confidence_score, relationship_strength,
                    sensitive_claim, verified, verified_at,
                    valid_from, valid_to,
                    neo4j_synced
                ) VALUES (
                    $1::uuid, $2::uuid,
                    $3::uuid, $4::uuid,
                    $5, $6,
                    $7::uuid[], $8,
                    $9, $10, $11,
                    $12, $13, $14,
                    $15::date, $16::date,
                    FALSE
                )
                ON CONFLICT (source_entity_id, target_entity_id, relationship_type, run_id)
                DO NOTHING
                """,
                relationship_id,
                edge["run_id"],
                edge["source_entity_id"],
                edge["target_entity_id"],
                edge["relationship_type"],
                edge.get("direction", "directed"),
                edge["evidence_ids"],
                edge.get("evidence_snippets", []),
                edge.get("confidence", "medium"),
                edge.get("confidence_score"),           # Phase 11.2 — float (may be NULL for old rows)
                edge.get("relationship_strength"),
                edge.get("sensitive_claim", False),
                edge.get("verified", False),
                self._coerce_dt(edge.get("verified_at")),
                self._coerce_dt(edge.get("valid_from")),
                self._coerce_dt(edge.get("valid_to")),
            )

        return relationship_id

    async def mark_relationship_verified(self, relationship_id: str) -> None:
        """Mark a relationship as verified after Verification Agent sign-off."""
        pool = self._pool_required()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE relationships SET verified = TRUE, verified_at = NOW()
                WHERE relationship_id = $1::uuid
                """,
                relationship_id,
            )

    async def mark_neo4j_synced(self, relationship_ids: list[str]) -> None:
        """Mark a batch of relationships as synced to Neo4j."""
        pool = self._pool_required()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE relationships SET neo4j_synced = TRUE, neo4j_synced_at = NOW()
                WHERE relationship_id = ANY($1::uuid[])
                """,
                relationship_ids,
            )

    # ─────────────────────────────────────────────────────────────────────────
    # agent_outputs table
    # ─────────────────────────────────────────────────────────────────────────

    async def write_agent_output(self, output: dict[str, Any]) -> str:
        """
        Insert or update an agent_outputs record.
        Returns output_id (UUID string).
        """
        pool = self._pool_required()
        output_id = output.get("output_id") or str(uuid.uuid4())

        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO agent_outputs (
                    output_id, run_id, agent_name,
                    agent_status, error_message,
                    model_used, prompt_version,
                    tokens_in, tokens_out, llm_call_count, latency_ms,
                    api_calls_made, api_calls_cached,
                    entities_produced, relationships_produced, items_rejected,
                    output_snapshot_path, started_at, completed_at
                ) VALUES (
                    $1::uuid, $2::uuid, $3,
                    $4, $5,
                    $6, $7,
                    $8, $9, $10, $11,
                    $12, $13,
                    $14, $15, $16,
                    $17, $18, $19
                )
                ON CONFLICT (output_id) DO UPDATE SET
                    agent_status     = EXCLUDED.agent_status,
                    error_message    = EXCLUDED.error_message,
                    tokens_in        = EXCLUDED.tokens_in,
                    tokens_out       = EXCLUDED.tokens_out,
                    llm_call_count   = EXCLUDED.llm_call_count,
                    latency_ms       = EXCLUDED.latency_ms,
                    api_calls_made   = EXCLUDED.api_calls_made,
                    api_calls_cached = EXCLUDED.api_calls_cached,
                    entities_produced = EXCLUDED.entities_produced,
                    relationships_produced = EXCLUDED.relationships_produced,
                    items_rejected   = EXCLUDED.items_rejected,
                    completed_at     = EXCLUDED.completed_at
                """,
                output_id,
                output["run_id"],
                output["agent_name"],
                output["agent_status"],
                output.get("error_message"),
                output.get("model_used"),
                output.get("prompt_version"),
                output.get("tokens_in", 0),
                output.get("tokens_out", 0),
                output.get("llm_call_count", 0),
                output.get("latency_ms"),
                output.get("api_calls_made", 0),
                output.get("api_calls_cached", 0),
                output.get("entities_produced", 0),
                output.get("relationships_produced", 0),
                output.get("items_rejected", 0),
                output.get("output_snapshot_path"),
                self._coerce_dt(output.get("started_at")),
                self._coerce_dt(output.get("completed_at")),
            )

        return output_id

    # ─────────────────────────────────────────────────────────────────────────
    # rejected_items table
    # ─────────────────────────────────────────────────────────────────────────

    async def write_rejected_item(self, item: dict[str, Any]) -> str:
        """
        Insert a rejected_items record.
        Returns rejection_id (UUID string).

        item_snapshot is MANDATORY — raises if absent.
        """
        pool = self._pool_required()
        if not item.get("item_snapshot"):
            raise SupabaseClientError(
                f"write_rejected_item: item_snapshot is required. "
                f"stage={item.get('stage')} type={item.get('item_type')}"
            )
        if not item.get("rejection_reason"):
            raise SupabaseClientError(
                f"write_rejected_item: rejection_reason is required."
            )

        rejection_id = item.get("rejection_id") or str(uuid.uuid4())

        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO rejected_items (
                    rejection_id, run_id, agent_name,
                    stage, item_type, item_id,
                    item_snapshot, rejection_reason, rejection_detail,
                    timestamp
                ) VALUES (
                    $1::uuid, $2::uuid, $3,
                    $4, $5, $6,
                    $7::jsonb, $8, $9,
                    $10
                )
                """,
                rejection_id,
                item["run_id"],
                item["agent_name"],
                item["stage"],
                item["item_type"],
                item.get("item_id"),
                json.dumps(item["item_snapshot"]),
                item["rejection_reason"],
                item.get("rejection_detail"),
                self._coerce_dt(item.get("timestamp")),
            )

        return rejection_id

    # ─────────────────────────────────────────────────────────────────────────
    # Type coercion helpers
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _coerce_dt(value: Any) -> datetime | None:
        """
        Coerce a timestamp value to a datetime object for asyncpg.

        asyncpg requires datetime objects for TIMESTAMPTZ columns; it rejects
        ISO 8601 strings. Agents frequently call datetime.now(...).isoformat()
        for convenience — this method normalizes at the DB boundary.

        Accepts: None, datetime object, ISO 8601 string.
        Returns: datetime (UTC-aware) or None.
        """
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        raise SupabaseClientError(
            f"_coerce_dt: cannot coerce type {type(value).__name__!r} to datetime"
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Validation helpers
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _validate_entity(entity: dict[str, Any]) -> None:
        """Raise SupabaseClientError if required entity fields are missing."""
        required = ["canonical_name", "entity_type", "source_agent"]
        missing = [f for f in required if not entity.get(f)]
        if missing:
            raise SupabaseClientError(
                f"write_entity: missing required fields: {missing}"
            )
        valid_types = {
            "investor", "philanthropic", "corporate", "political",
            "nonprofit", "executive_hnw", "community_leader",
            "politician", "hnwi", "illicit",
        }
        if entity["entity_type"] not in valid_types:
            raise SupabaseClientError(
                f"write_entity: invalid entity_type '{entity['entity_type']}'. "
                f"Must be one of: {sorted(valid_types)}"
            )
