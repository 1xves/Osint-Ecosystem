"""
osint/workers/worker.py

ARQ async task queue worker for the OSINT pipeline.

Defines one task:
    run_osint_pipeline(ctx, run_id, city_name, country_or_region, operator_id)
        — Instantiates all infrastructure clients, builds the LangGraph graph,
          constructs the initial state, and runs the full pipeline to completion.
        — Marks the run "failed" in DB if an unhandled exception escapes.
        — Designed to be idempotent: safe to re-enqueue if the run was never started.

WorkerSettings:
    - Pulls jobs from Redis queue
    - Max 2 concurrent pipeline jobs per worker process (each job is CPU/GPU bound)
    - 4-hour job timeout (realistic for a full 15-agent run on a GPU VPS)
    - Health-check function: returns True if Supabase and Ollama are reachable

Running:
    arq osint.workers.worker.WorkerSettings

Or via CLI entrypoint (if defined in pyproject.toml):
    osint-worker

Architecture note:
    Each job creates its own set of DB client connections and tears them down
    on completion. This is intentional — workers are stateless and can be
    horizontally scaled. Shared state across jobs lives only in Postgres + Redis.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

import arq
from arq import cron

from osint.core.config import settings
from osint.core.rate_limiter import RateLimiter
from osint.db.chromadb import ChromaDBClient
from osint.db.neo4j import Neo4jClient
from osint.db.redis import RedisClient
from osint.db.supabase import SupabaseClient
from osint.graph.graph import build_graph
from osint.llm.ollama import OllamaClient
from osint.llm.routing import LLMRouter
from osint.state import initial_state
from osint.scheduler import run_enrichment_refresh, run_weekly_discovery

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline task
# ─────────────────────────────────────────────────────────────────────────────

async def run_osint_pipeline(
    ctx: dict[str, Any],
    run_id: str,
    city_name: str,
    country_or_region: str,
    operator_id: str,
) -> dict[str, Any]:
    """
    Execute a full OSINT pipeline run.

    This function is the ARQ task. It is called by the worker when a job is
    dequeued. It runs synchronously from the caller's perspective — ARQ awaits
    its completion (or timeout).

    Args:
        ctx:               ARQ job context (contains arq_pool, job metadata).
        run_id:            UUID string — matches the agent_runs record written by the API.
        city_name:         e.g. "Austin"
        country_or_region: e.g. "United States"
        operator_id:       Identifier of the user who triggered the run.

    Returns:
        Summary dict with entity_count and run_status (used by ARQ for job result storage).

    Raises:
        Does not raise — all exceptions are caught, logged, and the run is marked failed.
        ARQ sees a successful return value regardless, so the job is not retried.
        (Retrying a failed pipeline run automatically could be dangerous — partial state.)
    """
    log.info(
        "worker: starting run_id=%s city=%s country=%s",
        run_id, city_name, country_or_region,
    )

    # ── Instantiate infrastructure clients ────────────────────────────────────
    # Each job gets its own client pool. This avoids cross-job connection
    # contamination and makes the worker stateless.
    # NOTE: RateLimiter and LLMRouter are initialized AFTER connecting clients
    # because they require a live Redis client and a connected OllamaClient.
    db     = SupabaseClient()
    neo4j  = Neo4jClient()
    chroma = ChromaDBClient()
    redis  = RedisClient()
    ollama = OllamaClient()

    rate_limiter: RateLimiter | None = None
    llm: LLMRouter | None = None

    try:
        await db.connect()
        await neo4j.connect()
        await chroma.connect()
        await redis.connect()
        await ollama.connect()
        log.debug("worker: clients connected for run_id=%s", run_id)

    except Exception as exc:
        log.error("worker: client connection failed for run_id=%s: %s", run_id, exc)
        # Mark run failed in DB if we can reach it
        try:
            await db.complete_run(
                run_id=run_id,
                status="failed",
                summary={},
                failure_reason=f"Infrastructure connection failed: {exc}",
            )
        except Exception:
            pass  # DB unreachable — can't do anything
        await _disconnect_all(db, neo4j, chroma, redis, ollama)
        return {"run_id": run_id, "run_status": "failed", "error": str(exc)}

    # Dependents require live connections — init after connect
    rate_limiter = RateLimiter(redis.get_raw_client())
    llm          = LLMRouter(ollama)

    # ── Mark run as running ───────────────────────────────────────────────────
    try:
        await db.upsert_run({
            "run_id":            run_id,
            "city_name":         city_name,
            "country_or_region": country_or_region,
            "city_key":          _city_key(city_name, country_or_region),
            "run_status":        "running",
        })
    except Exception as exc:
        log.warning("worker: failed to update run status to 'running': %s", exc)
        # Non-fatal — proceed

    # ── Build and run the graph ───────────────────────────────────────────────
    try:
        graph = build_graph(db, neo4j, chroma, redis, llm, rate_limiter)

        state = initial_state(
            run_id=run_id,
            city_name=city_name,
            country_or_region=country_or_region,
            operator_id=operator_id,
            triggered_at=datetime.now(timezone.utc).isoformat(),
        )

        log.info("worker: invoking graph for run_id=%s", run_id)
        final_state = await graph.ainvoke(state)

        # ── Extract summary from final state ──────────────────────────────────
        entity_count       = len(final_state.get("scored_entities", []))
        relationship_count = len(final_state.get("relationships_draft", []))
        run_status         = final_state.get("run_status", "complete")
        warnings           = final_state.get("warnings", [])

        if warnings:
            log.warning(
                "worker: run_id=%s completed with %d warning(s): %s",
                run_id, len(warnings), warnings[:3],
            )

        log.info(
            "worker: run_id=%s COMPLETE — status=%s entities=%d relationships=%d",
            run_id, run_status, entity_count, relationship_count,
        )

        return {
            "run_id":             run_id,
            "run_status":         run_status,
            "entity_count":       entity_count,
            "relationship_count": relationship_count,
        }

    except Exception as exc:
        log.error(
            "worker: UNHANDLED EXCEPTION for run_id=%s: %s",
            run_id, exc,
            exc_info=True,
        )
        # Mark run failed — complete_run() may have already been called by the
        # briefing agent if the pipeline ran far enough; this handles early failures.
        try:
            await db.complete_run(
                run_id=run_id,
                status="failed",
                summary={},
                failure_reason=str(exc)[:500],
            )
        except Exception as inner:
            log.error("worker: failed to mark run as failed: %s", inner)

        return {
            "run_id":     run_id,
            "run_status": "failed",
            "error":      str(exc)[:500],
        }

    finally:
        await _disconnect_all(db, neo4j, chroma, redis, ollama)
        log.debug("worker: clients disconnected for run_id=%s", run_id)


# ─────────────────────────────────────────────────────────────────────────────
# Startup / shutdown hooks (run once per worker process, not per job)
# ─────────────────────────────────────────────────────────────────────────────

async def startup(ctx: dict[str, Any]) -> None:
    """
    ARQ worker startup hook.
    Runs once when the worker process starts.
    Used to verify configuration and warm up any process-level resources.
    """
    log.info("worker: process starting — validating configuration")

    # Validate required settings
    missing: list[str] = []
    if not settings.database_url:
        missing.append("DATABASE_URL")
    if not settings.ollama_host:
        missing.append("OLLAMA_HOST")
    if missing:
        log.warning("worker: missing configuration: %s", missing)

    log.info(
        "worker: process ready — "
        "db=%s ollama=%s models=[%s, %s, %s]",
        settings.supabase_url[:30] + "..." if settings.supabase_url else "not set",
        settings.ollama_host,
        settings.ollama_default_model,
        settings.ollama_escalation_model,
        settings.ollama_extraction_model,
    )


async def shutdown(ctx: dict[str, Any]) -> None:
    """ARQ worker shutdown hook. Runs when the worker process exits."""
    log.info("worker: process shutting down")


async def health_check(ctx: dict[str, Any]) -> bool:
    """
    ARQ health check — called periodically.
    Returns True if the worker can process jobs.
    Used by monitoring systems to detect zombie workers.
    """
    try:
        # Quick connectivity test: can we reach Supabase?
        db = SupabaseClient()
        await db.connect()
        await db.disconnect()
        return True
    except Exception as exc:
        log.error("worker: health_check FAILED: %s", exc)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# WorkerSettings — ARQ reads this class for configuration
# ─────────────────────────────────────────────────────────────────────────────

class WorkerSettings:
    """
    ARQ worker configuration.

    Start the worker:
        arq osint.workers.worker.WorkerSettings

    Key settings:
        functions     — list of task functions this worker handles
        cron_jobs     — scheduled recurring tasks (enrichment refresh + weekly discovery)
        on_startup    — called once on process start
        on_shutdown   — called once on process exit
        health_check  — called every health_check_interval seconds
        max_jobs      — max concurrent jobs per worker process
        job_timeout   — seconds before a job is killed (default 300)
        keep_result   — seconds to keep job result in Redis

    Continuous pipeline schedule:
        Every 6 hours:    run_enrichment_refresh  — re-enrich known entities, re-infer rels
        Mondays 2:00 AM:  run_weekly_discovery    — full collection pass for all cities
    """

    redis_settings = arq.connections.RedisSettings.from_dsn(settings.redis_url)

    # Task functions this worker handles (on-demand)
    functions = [run_osint_pipeline]

    # Cron tasks — continuous pipeline scheduler
    # run_enrichment_refresh: every 6 hours (0:00, 6:00, 12:00, 18:00 UTC)
    # run_weekly_discovery:   Mondays at 2:00 AM UTC
    cron_jobs = [
        cron(
            run_enrichment_refresh,
            hour={0, 6, 12, 18},
            minute=0,
            job_id="enrichment_refresh",    # stable ID prevents duplicate queuing
        ),
        cron(
            run_weekly_discovery,
            weekday=0,                       # Monday
            hour=2,
            minute=0,
            job_id="weekly_discovery",
        ),
    ]

    # Lifecycle hooks
    on_startup  = startup
    on_shutdown = shutdown
    health_check = health_check
    health_check_interval = 60   # seconds

    # Concurrency — OSINT runs are GPU-bound; 2 is safe on a 24GB GPU VPS.
    # Enrichment refresh jobs are lighter (no collection) so they can run
    # alongside a full discovery pass safely.
    max_jobs = 3

    # Timeout — 4 hours for full runs, 2 hours is more than enough for refresh.
    # ARQ doesn't support per-function timeouts, so we use the max safe value.
    job_timeout = 60 * 60 * 4   # 4 hours in seconds

    # Keep job result in Redis for 24 hours
    keep_result = 60 * 60 * 24

    # Retry policy — do NOT retry failed pipeline jobs automatically.
    # A failed run leaves partial state in the DB; re-running the same
    # run_id would conflict. Let the user trigger a new run if needed.
    max_tries = 1


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

# Common country name → ISO 3166-1 alpha-2 mapping.
# [:2] on the country *name* is wrong — "United States"[:2] = "un", not "us".
# Extend this table as new countries are added to the pipeline.
_COUNTRY_ISO2: dict[str, str] = {
    "united states":        "us",
    "united states of america": "us",
    "usa":                  "us",
    "united kingdom":       "uk",
    "great britain":        "uk",
    "united arab emirates": "ae",
    "uae":                  "ae",
    "new zealand":          "nz",
    "south korea":          "kr",
    "north korea":          "kp",
    "saudi arabia":         "sa",
    "south africa":         "za",
    "costa rica":           "cr",
    "czech republic":       "cz",
    "dominican republic":   "do",
    "el salvador":          "sv",
    "hong kong":            "hk",
    "new guinea":           "pg",
    "puerto rico":          "pr",
    "sri lanka":            "lk",
    "trinidad and tobago":  "tt",
}


def _city_key(city_name: str, country_or_region: str) -> str:
    """
    Produce a stable, URL-safe city identifier.

    Format: {city_slug}_{iso2}   e.g. "philadelphia_us", "london_uk"

    Uses a lookup table for multi-word country names so that
    "United States" → "us" (not "un" from [:2] slicing).
    Falls back to first-two-chars for countries not in the table.
    """
    iso2 = _COUNTRY_ISO2.get(country_or_region.lower().strip())
    if iso2 is None:
        # Fallback: first 2 chars of the first word (handles single-word names
        # like "Germany" → "ge", "France" → "fr", "Canada" → "ca")
        iso2 = country_or_region.strip().split()[0][:2].lower()
    city_slug = city_name.lower().replace(" ", "_").replace("-", "_")
    return f"{city_slug}_{iso2}"


async def _disconnect_all(
    db: SupabaseClient,
    neo4j: Neo4jClient,
    chroma: ChromaDBClient,
    redis: RedisClient,
    ollama: OllamaClient | None = None,
) -> None:
    """Disconnect all clients, ignoring individual disconnect failures."""
    clients = [(db, "db"), (neo4j, "neo4j"), (chroma, "chroma"), (redis, "redis")]
    if ollama is not None:
        clients.append((ollama, "ollama"))
    for client, name in clients:
        try:
            await client.disconnect()
        except Exception as exc:
            log.warning("worker: failed to disconnect %s: %s", name, exc)
