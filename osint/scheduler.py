"""
osint/scheduler.py

Continuous pipeline refresh scheduler.

Provides two ARQ-compatible async task functions that run on cron schedules
wired into WorkerSettings.cron_jobs in osint/workers/worker.py:

  1. run_enrichment_refresh (every 6 hours)
     — Loads all known entities for a city from the DB
     — Re-runs enrichment on stale entities (last_verified older than 12h)
     — Re-infers relationships from updated category_fields
     — Stamps last_verified on refreshed entities
     — Produces no new entities — only updates existing ones

  2. run_weekly_discovery (Mondays 2 AM)
     — Enqueues a fresh full-pipeline run for each city in continuous_schedule
     — The full run includes collection, resolution, enrichment, relationships, briefing
     — Uses the existing run_osint_pipeline ARQ task

Design rationale:
  The enrichment refresh bypasses the LangGraph graph entirely and calls
  EnrichmentAgent and RelationshipAgent directly. This is intentional:
    - LangGraph adds overhead and state for the full pipeline
    - A refresh run does not need collection, resolution, or gap analysis
    - Direct agent calls are simpler to implement and easier to debug
  Agents are designed as callables that accept a state dict, so direct
  invocation is safe and supported.

  The weekly discovery uses the full LangGraph pipeline (run_osint_pipeline)
  via ARQ job enqueue — it needs collection and resolution.

Cron cadences (configurable in continuous_schedule DB table):
  Enrichment refresh:  every 6 hours (default)
  Weekly discovery:    Mondays 2:00 AM server time (hardcoded in WorkerSettings)

Usage:
  These functions are registered as ARQ cron jobs in WorkerSettings.
  They receive an ARQ context dict as their first argument (standard ARQ convention).
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from osint.core.config import settings
from osint.core.rate_limiter import RateLimiter
from osint.db.chromadb import ChromaDBClient
from osint.db.neo4j import Neo4jClient
from osint.db.redis import RedisClient
from osint.db.supabase import SupabaseClient
from osint.llm.ollama import OllamaClient
from osint.llm.routing import LLMRouter

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Default scheduled cities
# The scheduler reads from continuous_schedule in the DB if available,
# falling back to this list. Override via DB to add/remove cities without
# a code deploy.
# ─────────────────────────────────────────────────────────────────────────────

_DEFAULT_CITIES: list[dict[str, str]] = [
    {
        "city_key":          "philadelphia_us",
        "city_name":         "Philadelphia",
        "country_or_region": "United States",
    },
]

# How many hours old an entity's last_verified timestamp must be before
# it is included in a refresh run. Entities verified more recently are skipped.
_STALE_THRESHOLD_HOURS = 12


# ─────────────────────────────────────────────────────────────────────────────
# Enrichment refresh — runs every 6 hours
# ─────────────────────────────────────────────────────────────────────────────

async def run_enrichment_refresh(ctx: dict[str, Any]) -> dict[str, Any]:
    """
    ARQ cron task: re-enrich stale entities + re-infer relationships.

    Iterates over all cities in continuous_schedule (or _DEFAULT_CITIES)
    and runs the lightweight refresh pipeline for each.

    Returns a summary dict for ARQ result storage.
    """
    log.info("scheduler: run_enrichment_refresh starting")

    db     = SupabaseClient()
    neo4j  = Neo4jClient()
    chroma = ChromaDBClient()
    redis  = RedisClient()
    ollama = OllamaClient()

    try:
        await db.connect()
        await neo4j.connect()
        await chroma.connect()
        await redis.connect()
        await ollama.connect()
    except Exception as exc:
        log.error("scheduler: enrichment_refresh client connection failed: %s", exc)
        await _disconnect_all(db, neo4j, chroma, redis, ollama)
        return {"status": "failed", "error": str(exc)}

    rate_limiter = RateLimiter(redis.get_raw_client())
    llm          = LLMRouter(ollama)

    # ── Fetch scheduled cities ────────────────────────────────────────────────
    cities = await _load_scheduled_cities(db)

    total_entities_refreshed = 0
    total_relationships      = 0
    city_results: list[dict[str, Any]] = []

    for city in cities:
        city_name         = city["city_name"]
        country_or_region = city["country_or_region"]
        city_key          = city["city_key"]

        result = await _refresh_city(
            db=db,
            neo4j=neo4j,
            chroma=chroma,
            redis=redis,
            llm=llm,
            rate_limiter=rate_limiter,
            city_name=city_name,
            country_or_region=country_or_region,
            city_key=city_key,
        )
        total_entities_refreshed += result.get("entities_refreshed", 0)
        total_relationships      += result.get("relationships_found", 0)
        city_results.append({"city_key": city_key, **result})

    await _disconnect_all(db, neo4j, chroma, redis, ollama)

    summary = {
        "status":              "complete",
        "cities_processed":    len(city_results),
        "total_entities":      total_entities_refreshed,
        "total_relationships": total_relationships,
        "cities":              city_results,
        "completed_at":        datetime.now(timezone.utc).isoformat(),
    }
    log.info(
        "scheduler: enrichment_refresh DONE — %d cities, %d entities, %d relationships",
        len(city_results), total_entities_refreshed, total_relationships,
    )
    return summary


async def _refresh_city(
    db: SupabaseClient,
    neo4j: Neo4jClient,
    chroma: ChromaDBClient,
    redis: RedisClient,
    llm: LLMRouter,
    rate_limiter: RateLimiter,
    city_name: str,
    country_or_region: str,
    city_key: str,
) -> dict[str, Any]:
    """
    Run enrichment + relationship refresh for one city.

    Calls EnrichmentAgent and RelationshipAgent directly (outside LangGraph)
    because this is a targeted refresh that does not need collection/resolution.
    """
    from osint.agents.enrichment import EnrichmentAgent
    from osint.agents.relationship import RelationshipAgent

    run_id = str(uuid.uuid4())
    now    = datetime.now(timezone.utc).isoformat()

    log.info(
        "scheduler: refreshing city=%s run_id=%s", city_name, run_id
    )

    # ── Register the refresh run in DB ────────────────────────────────────────
    try:
        await db.upsert_run({
            "run_id":            run_id,
            "city_name":         city_name,
            "country_or_region": country_or_region,
            "city_key":          city_key,
            "run_status":        "running",
            "run_mode":          "enrichment_refresh",
            "run_type":          "enrichment_refresh",
            "scheduled":         True,
        })
    except Exception as exc:
        log.warning("scheduler: failed to upsert refresh run record: %s", exc)

    # ── Load stale entities from DB ───────────────────────────────────────────
    try:
        entities = await db.get_entities_by_city(
            city_name=city_name,
            stale_only=True,
            stale_hours=_STALE_THRESHOLD_HOURS,
        )
    except Exception as exc:
        log.error(
            "scheduler: failed to load entities for city=%s: %s", city_name, exc
        )
        await _mark_run_failed(db, run_id, str(exc))
        return {"entities_refreshed": 0, "relationships_found": 0, "status": "failed"}

    if not entities:
        log.info(
            "scheduler: no stale entities for city=%s (all verified within %dh)",
            city_name, _STALE_THRESHOLD_HOURS,
        )
        await _mark_run_complete(db, run_id, {})
        return {"entities_refreshed": 0, "relationships_found": 0, "status": "skipped"}

    log.info(
        "scheduler: %d stale entities to refresh for city=%s",
        len(entities), city_name,
    )

    # ── Reset was_enriched so the enrichment agent re-runs ────────────────────
    # Enrichment agent checks `was_enriched` and skips already-enriched entities.
    # For a refresh run we explicitly clear this so every entity is re-processed.
    for entity in entities:
        entity["was_enriched"] = False
        entity["source_run_ids"] = entity.get("source_run_ids") or [run_id]

    # ── Build minimal state for direct agent calls ────────────────────────────
    deps = (db, neo4j, chroma, redis, llm, rate_limiter)
    state: dict[str, Any] = {
        "run_id":             run_id,
        "city_name":          city_name,
        "country_or_region":  country_or_region,
        "city_key":           city_key,
        "run_status":         "running",
        "run_mode":           "enrichment_refresh",
        "current_phase":      "ENRICHMENT",
        "pass_number":        1,
        "gate_cleared":       True,
        # canonical_entities is what the enrichment agent reads
        "canonical_entities": entities,
        "enrichment_targets": [
            str(e["entity_id"]) for e in entities if e.get("entity_id")
        ],
        # These are needed by the relationship agent
        "enriched_entities":  [],
        "relationships_draft":  [],
        "relationships_verified": [],
        "relationships_rejected": [],
        # Operational zeroes
        "agent_statuses":       {},
        "agent_errors":         {},
        "agent_entity_counts":  {},
        "agent_token_counts":   {},
        "raw_entities":         [],
        "raw_search_records":   [],
        "warnings":             [],
        "errors":               [],
        "total_tokens_in":      0,
        "total_tokens_out":     0,
        "redis_cache_hits":     0,
        "proxycurl_spend_usd":  0.0,
        "operator_id":          "scheduler",
        "triggered_at":         now,
    }

    # ── Phase: Enrichment ─────────────────────────────────────────────────────
    enrichment_agent = EnrichmentAgent(*deps)
    try:
        enrichment_patch = await enrichment_agent(state)
        # Merge the patch back into state (simulates LangGraph reducer)
        state = _merge_state(state, enrichment_patch)
        entities_refreshed = len(state.get("enriched_entities", []))
        log.info(
            "scheduler: enrichment complete for city=%s — %d entities",
            city_name, entities_refreshed,
        )
    except Exception as exc:
        log.error(
            "scheduler: enrichment failed for city=%s run_id=%s: %s",
            city_name, run_id, exc, exc_info=True,
        )
        await _mark_run_failed(db, run_id, f"Enrichment failed: {exc}")
        return {"entities_refreshed": 0, "relationships_found": 0, "status": "failed"}

    # ── Phase: Relationship inference ─────────────────────────────────────────
    relationship_agent = RelationshipAgent(*deps)
    try:
        relationship_patch = await relationship_agent(state)
        state = _merge_state(state, relationship_patch)
        relationships_found = len(state.get("relationships_verified", []))
        log.info(
            "scheduler: relationship inference complete for city=%s — %d relationships",
            city_name, relationships_found,
        )
    except Exception as exc:
        log.error(
            "scheduler: relationship inference failed for city=%s run_id=%s: %s",
            city_name, run_id, exc, exc_info=True,
        )
        # Non-fatal — enrichment succeeded, log and continue
        relationships_found = 0

    # ── Stamp last_verified on all refreshed entities ─────────────────────────
    refreshed_ids = [
        str(e["entity_id"])
        for e in state.get("enriched_entities", [])
        if e.get("entity_id")
    ]
    if refreshed_ids:
        try:
            await asyncio.gather(*[
                db.update_entity_last_verified(eid, run_id)
                for eid in refreshed_ids
            ], return_exceptions=True)
        except Exception as exc:
            log.warning(
                "scheduler: failed to stamp last_verified for city=%s: %s",
                city_name, exc,
            )

    # ── Complete the run record ───────────────────────────────────────────────
    summary = {
        "entities_refreshed": entities_refreshed,
        "relationships_found": relationships_found,
    }
    await _mark_run_complete(db, run_id, summary)

    # ── Update continuous_schedule bookkeeping ────────────────────────────────
    try:
        pool = db._pool_required()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE continuous_schedule
                SET    last_enrichment_refresh_at = NOW(),
                       last_enrichment_run_id = $1::uuid,
                       updated_at = NOW()
                WHERE  city_key = $2
                """,
                run_id, city_key,
            )
    except Exception as exc:
        log.warning(
            "scheduler: failed to update continuous_schedule for city=%s: %s",
            city_key, exc,
        )

    return {
        "entities_refreshed": entities_refreshed,
        "relationships_found": relationships_found,
        "status": "complete",
        "run_id": run_id,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Weekly discovery pass — runs Mondays 2 AM
# ─────────────────────────────────────────────────────────────────────────────

async def run_weekly_discovery(ctx: dict[str, Any]) -> dict[str, Any]:
    """
    ARQ cron task: enqueue a full pipeline run for each scheduled city.

    Does not run the pipeline directly — enqueues an ARQ job using the
    existing run_osint_pipeline function so the full LangGraph pipeline
    executes with collection, resolution, enrichment, and briefing.

    Returns a summary of which cities were queued.
    """
    log.info("scheduler: run_weekly_discovery starting")

    db    = SupabaseClient()
    redis = RedisClient()

    try:
        await db.connect()
        await redis.connect()
    except Exception as exc:
        log.error("scheduler: weekly_discovery connection failed: %s", exc)
        await db.disconnect()
        await redis.disconnect()
        return {"status": "failed", "error": str(exc)}

    cities = await _load_scheduled_cities(db)

    queued: list[str] = []
    failed: list[str] = []

    for city in cities:
        city_key          = city["city_key"]
        city_name         = city["city_name"]
        country_or_region = city["country_or_region"]
        run_id            = str(uuid.uuid4())

        try:
            # Pre-register the run in DB so the status is visible immediately
            await db.upsert_run({
                "run_id":            run_id,
                "city_name":         city_name,
                "country_or_region": country_or_region,
                "city_key":          city_key,
                "run_status":        "pending",
                "run_mode":          "discovery_pass",
                "run_type":          "weekly_discovery",
                "scheduled":         True,
            })

            # Enqueue the full pipeline job via ARQ
            arq_pool = ctx.get("redis")  # ARQ passes the redis pool in ctx
            if arq_pool:
                await arq_pool.enqueue_job(
                    "run_osint_pipeline",
                    run_id=run_id,
                    city_name=city_name,
                    country_or_region=country_or_region,
                    operator_id="scheduler",
                )
                queued.append(city_key)
                log.info(
                    "scheduler: weekly_discovery enqueued run_id=%s for city=%s",
                    run_id, city_name,
                )

                # Update schedule bookkeeping
                try:
                    pool = db._pool_required()
                    async with pool.acquire() as conn:
                        await conn.execute(
                            """
                            UPDATE continuous_schedule
                            SET    last_discovery_pass_at  = NOW(),
                                   last_discovery_run_id   = $1::uuid,
                                   updated_at              = NOW()
                            WHERE  city_key = $2
                            """,
                            run_id, city_key,
                        )
                except Exception as upd_exc:
                    log.warning(
                        "scheduler: failed to update schedule bookkeeping: %s", upd_exc
                    )
            else:
                log.warning(
                    "scheduler: no arq redis pool in ctx — cannot enqueue for city=%s",
                    city_name,
                )
                failed.append(city_key)

        except Exception as exc:
            log.error(
                "scheduler: failed to enqueue discovery for city=%s: %s",
                city_name, exc,
            )
            failed.append(city_key)

    await db.disconnect()
    await redis.disconnect()

    summary = {
        "status":           "complete" if not failed else "partial",
        "queued":           queued,
        "failed":           failed,
        "completed_at":     datetime.now(timezone.utc).isoformat(),
    }
    log.info(
        "scheduler: weekly_discovery DONE — queued=%d failed=%d",
        len(queued), len(failed),
    )
    return summary


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

async def _load_scheduled_cities(db: SupabaseClient) -> list[dict[str, str]]:
    """
    Load cities from the continuous_schedule table.
    Falls back to _DEFAULT_CITIES if the table is empty or unreachable.
    """
    try:
        pool = db._pool_required()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT city_key, city_name, country_or_region
                FROM   continuous_schedule
                WHERE  (enrichment_refresh_enabled = TRUE
                        OR discovery_pass_enabled  = TRUE)
                ORDER BY city_key
                """
            )
            if rows:
                return [dict(r) for r in rows]
    except Exception as exc:
        log.warning(
            "scheduler: could not read continuous_schedule from DB (%s) "
            "— falling back to defaults",
            exc,
        )
    return list(_DEFAULT_CITIES)


def _merge_state(
    base: dict[str, Any],
    patch: dict[str, Any],
) -> dict[str, Any]:
    """
    Merge an agent's state patch into the base state.

    For list fields that use operator.add reducers in OSINTRunState,
    concatenate rather than overwrite. For dict fields that use _merge_dicts,
    shallow-merge. For all other fields, the patch value wins.

    This simulates what LangGraph's reducer framework does automatically.
    """
    # Fields that accumulate (operator.add reducers in state.py)
    _ADDITIVE_LIST_FIELDS = {
        "raw_entities", "raw_search_records", "enriched_entities",
        "relationships_draft", "relationships_verified", "relationships_rejected",
        "merge_decisions", "ambiguous_merges", "dedup_merges",
        "scored_entities", "verified_entity_ids", "flagged_entity_ids",
        "warnings", "errors",
    }
    # Fields that shallow-merge (dict reducers)
    _MERGE_DICT_FIELDS = {
        "agent_statuses", "agent_errors", "agent_entity_counts", "agent_token_counts",
    }
    # Integer accumulators
    _ADD_INT_FIELDS = {
        "total_tokens_in", "total_tokens_out", "redis_cache_hits",
    }

    result = dict(base)
    for key, value in patch.items():
        if key in _ADDITIVE_LIST_FIELDS:
            existing = result.get(key, [])
            result[key] = (existing or []) + (value or [])
        elif key in _MERGE_DICT_FIELDS:
            existing = result.get(key, {})
            result[key] = {**(existing or {}), **(value or {})}
        elif key in _ADD_INT_FIELDS:
            result[key] = (result.get(key, 0) or 0) + (value or 0)
        else:
            result[key] = value
    return result


async def _mark_run_complete(
    db: SupabaseClient,
    run_id: str,
    summary: dict[str, Any],
) -> None:
    try:
        await db.complete_run(
            run_id=run_id,
            status="complete",
            summary=summary,
        )
    except Exception as exc:
        log.warning("scheduler: failed to mark run %s complete: %s", run_id, exc)


async def _mark_run_failed(
    db: SupabaseClient,
    run_id: str,
    reason: str,
) -> None:
    try:
        await db.complete_run(
            run_id=run_id,
            status="failed",
            summary={},
            failure_reason=reason[:500],
        )
    except Exception as exc:
        log.warning("scheduler: failed to mark run %s failed: %s", run_id, exc)


async def _disconnect_all(
    db: SupabaseClient,
    neo4j: Neo4jClient,
    chroma: ChromaDBClient,
    redis: RedisClient,
    ollama: OllamaClient | None = None,
) -> None:
    clients = [(db, "db"), (neo4j, "neo4j"), (chroma, "chroma"), (redis, "redis")]
    if ollama is not None:
        clients.append((ollama, "ollama"))
    for client, name in clients:
        try:
            await client.disconnect()
        except Exception as exc:
            log.warning("scheduler: failed to disconnect %s: %s", name, exc)
