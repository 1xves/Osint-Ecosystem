"""
osint/api/app.py

FastAPI application for the OSINT Startup Ecosystem Intelligence System.

Endpoints:
    GET  /health                         — liveness check (no auth)
    POST /runs                           — trigger a new pipeline run → 202 Accepted
    GET  /runs                           — paginated list of runs (city_key, status filters)
    GET  /runs/{run_id}                  — get run status + aggregate stats
    GET  /runs/{run_id}/briefing         — get final briefing (JSON or markdown)
    GET  /runs/{run_id}/relationships    — get all relationships produced by a run
    GET  /entities                       — paginated entity search
    GET  /entities/{entity_id}           — get a single entity by ID

Auth:
    All endpoints except /health require X-API-Key header matching settings.secret_key.
    Returns 401 if header missing, 403 if key invalid.

Execution model:
    POST /runs enqueues a job to ARQ (Redis task queue) and returns 202 immediately.
    The ARQ worker (osint/workers/worker.py) picks up the job and runs the pipeline.
    Clients poll GET /runs/{run_id} for status; pipeline writes progress to DB.
    If ARQ is unavailable on startup (Redis not reachable), the app still boots but
    POST /runs returns 503 until the queue is available.

Startup/shutdown (lifespan):
    On startup: connect Supabase, Neo4j, ChromaDB, Redis; build LLMRouter + RateLimiter.
    All clients stored in app.state for use by route handlers.
    On shutdown: graceful disconnect of all clients.

Running:
    uvicorn osint.api.app:app --host 0.0.0.0 --port 8000 --reload
"""

from __future__ import annotations

import asyncio
import logging
import re
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import orjson
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

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
# Custom JSON response using orjson (handles datetime, UUID, asyncpg Record)
# ─────────────────────────────────────────────────────────────────────────────

class ORJSONResponse(JSONResponse):
    media_type = "application/json"

    def render(self, content: Any) -> bytes:
        return orjson.dumps(
            content,
            option=orjson.OPT_NON_STR_KEYS | orjson.OPT_SERIALIZE_UUID,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Pydantic models
# ─────────────────────────────────────────────────────────────────────────────

class RunCreateRequest(BaseModel):
    city_name: str = Field(..., min_length=2, max_length=100, examples=["Austin"])
    country_or_region: str = Field(
        default="United States", max_length=100, examples=["United States"]
    )


class RunCreateResponse(BaseModel):
    run_id: str
    city_name: str
    country_or_region: str
    status: str
    triggered_at: str
    message: str


class RunStatusResponse(BaseModel):
    run_id: str
    city_name: str
    country_or_region: str
    city_key: str
    status: str
    current_phase: str | None = None
    entity_count: int | None = None
    relationship_count: int | None = None
    claims_verified: int | None = None
    started_at: str | None = None
    completed_at: str | None = None
    duration_seconds: int | None = None
    gap_fill_triggered: bool | None = None
    overall_confidence: str | None = None
    failure_reason: str | None = None


class EntitySummary(BaseModel):
    entity_id: str
    canonical_name: str | None = None
    entity_type: str
    city_key: str | None = None
    overall_confidence: str | None = None
    needs_review: bool = False
    score_influence: int | None = None
    score_partner_potential: int | None = None
    score_blocker_risk: int | None = None
    partner_candidate: bool = False
    blocker_candidate: bool = False
    top_influencer: bool = False


class EntityListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    entities: list[dict[str, Any]]


# ─────────────────────────────────────────────────────────────────────────────
# Lifespan — startup / shutdown
# ─────────────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Connect all infrastructure clients on startup; disconnect on shutdown."""
    log.info("API startup: connecting clients")

    db = SupabaseClient()
    neo4j = Neo4jClient()
    chroma = ChromaDBClient()
    redis = RedisClient()
    ollama = OllamaClient()

    # Supabase — fatal: all read endpoints require it
    await db.connect()

    # Neo4j — non-fatal: only used by pipeline agents, not by API read endpoints
    try:
        await neo4j.connect()
        log.info("API startup: Neo4j connected")
    except Exception as exc:
        log.warning("API startup: Neo4j unavailable (%s) — graph features will degrade gracefully", exc)

    # Redis — non-fatal: used for rate limiting and ARQ queue; API read endpoints work without it
    try:
        await redis.connect()
        log.info("API startup: Redis connected")
    except Exception as exc:
        log.warning("API startup: Redis unavailable (%s) — rate limiting and job queue will be unavailable", exc)

    # Ollama — non-fatal: used by LLM routing, not by API read endpoints
    try:
        await ollama.connect()
        log.info("API startup: Ollama connected")
    except Exception as exc:
        log.warning("API startup: Ollama unavailable (%s) — LLM features will degrade gracefully", exc)

    # ChromaDB — non-fatal: only used by the resolution agent
    try:
        await chroma.connect()
        log.info("API startup: ChromaDB connected")
    except Exception as exc:
        log.warning("API startup: ChromaDB unavailable (%s) — resolution agent will degrade gracefully", exc)

    # Dependents — non-fatal if their backing service is unavailable
    try:
        rate_limiter = RateLimiter(redis.get_raw_client())
    except Exception as exc:
        log.warning("API startup: RateLimiter unavailable (%s)", exc)
        rate_limiter = None

    try:
        llm = LLMRouter(ollama)
    except Exception as exc:
        log.warning("API startup: LLMRouter unavailable (%s)", exc)
        llm = None

    # Store on app.state for access in route handlers
    app.state.db           = db
    app.state.neo4j        = neo4j
    app.state.chroma       = chroma
    app.state.redis        = redis
    app.state.rate_limiter = rate_limiter
    app.state.llm          = llm

    # ARQ queue pool — used to enqueue pipeline jobs
    # If Redis is unavailable, queue_pool stays None and POST /runs returns 503
    try:
        import arq
        redis_settings = arq.connections.RedisSettings.from_dsn(settings.redis_url)
        app.state.arq_pool = await arq.create_pool(redis_settings)
        log.info("API startup: ARQ queue pool connected")
    except Exception as exc:
        log.warning("API startup: ARQ pool unavailable — POST /runs will return 503: %s", exc)
        app.state.arq_pool = None

    log.info("API startup: all clients ready")
    yield

    # Shutdown
    log.info("API shutdown: disconnecting clients")
    if app.state.arq_pool:
        await app.state.arq_pool.aclose()
    await db.disconnect()
    await neo4j.disconnect()
    await chroma.disconnect()
    await redis.disconnect()
    await ollama.disconnect()
    log.info("API shutdown: clean")


# ─────────────────────────────────────────────────────────────────────────────
# App
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="OSINT Ecosystem Intelligence API",
    description=(
        "Trigger and retrieve startup ecosystem intelligence runs. "
        "Each run maps investors, corporates, nonprofits, politicians, "
        "executives, and illicit actors for a given city."
    ),
    version="0.1.0",
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://finitebuilds.com",
        "https://www.finitebuilds.com",
        "https://vk-osint-frontend.pages.dev",
        "https://vk-osint.com",
        "https://www.vk-osint.com",
        # Local dev
        "http://localhost:3000",
        "http://localhost:8080",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["X-API-Key", "Accept", "Content-Type"],
)

# ─────────────────────────────────────────────────────────────────────────────
# Static files — dashboard frontend
# Resolved relative to this file: osint/api/app.py → ../../.. → project root
# ─────────────────────────────────────────────────────────────────────────────

_STATIC_DIR = Path(__file__).resolve().parent.parent.parent / "static"

if _STATIC_DIR.exists():
    # Mount /static/... so the HTML can reference relative asset paths if needed.
    # The root GET / handler below serves index.html directly.
    app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")


# ─────────────────────────────────────────────────────────────────────────────
# Auth dependency
# ─────────────────────────────────────────────────────────────────────────────

async def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    """Validate X-API-Key header against settings.secret_key."""
    if x_api_key is None:
        raise HTTPException(status_code=401, detail="X-API-Key header required")
    if x_api_key != settings.secret_key:
        raise HTTPException(status_code=403, detail="Invalid API key")


def _db(request_state: Any = None) -> SupabaseClient:
    """Helper to extract db from app.state via request."""
    # Used via dependency injection pattern below
    return app.state.db


# ─────────────────────────────────────────────────────────────────────────────
# Health
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["meta"])
async def health() -> dict[str, Any]:
    """Liveness check — no auth required. Returns DB connectivity status."""
    db_ok    = app.state.db._pool is not None
    redis_ok = app.state.redis._redis is not None
    return {
        "status":     "ok" if (db_ok and redis_ok) else "degraded",
        "db":         "connected" if db_ok else "disconnected",
        "redis":      "connected" if redis_ok else "disconnected",
        "arq_queue":  "connected" if app.state.arq_pool else "unavailable",
        "timestamp":  datetime.now(timezone.utc).isoformat(),
    }


@app.get("/", include_in_schema=False)
async def serve_frontend() -> FileResponse:
    """
    Serve the dashboard frontend (static/index.html). No auth required —
    the page is public; the API calls it makes require X-API-Key.
    """
    index = _STATIC_DIR / "index.html"
    if not index.exists():
        raise HTTPException(
            status_code=404,
            detail="Frontend not found — ensure static/index.html exists in the project root.",
        )
    return FileResponse(index, media_type="text/html")


# ─────────────────────────────────────────────────────────────────────────────
# Runs
# ─────────────────────────────────────────────────────────────────────────────

@app.post(
    "/runs",
    status_code=202,
    tags=["runs"],
    dependencies=[Depends(require_api_key)],
    response_model=RunCreateResponse,
)
async def trigger_run(request: RunCreateRequest) -> RunCreateResponse:
    """
    Trigger a new OSINT pipeline run for a city.

    Returns 202 Accepted immediately with the run_id.
    Poll GET /runs/{run_id} for status. When status=="complete",
    fetch the briefing from GET /runs/{run_id}/briefing.
    """
    if not app.state.arq_pool:
        raise HTTPException(
            status_code=503,
            detail="Task queue unavailable — Redis not connected. Check worker status.",
        )

    run_id       = str(uuid.uuid4())
    triggered_at = datetime.now(timezone.utc).isoformat()
    city_key     = (
        f"{request.city_name.lower().replace(' ', '_')}_"
        f"{request.country_or_region[:2].lower()}"
    )
    operator_id  = "api"  # in a real system: extracted from auth token

    db: SupabaseClient = app.state.db

    # Write initial run record to DB
    try:
        await db.upsert_run({
            "run_id":            run_id,
            "city_name":         request.city_name,
            "country_or_region": request.country_or_region,
            "city_key":          city_key,
            "run_status":        "pending",
            "model_default":     settings.ollama_default_model,
            "model_escalation":  settings.ollama_escalation_model,
            "triggered_by":      operator_id,
            "trigger_type":      "manual",
            "is_delta_run":      False,
        })
    except Exception as exc:
        log.error("trigger_run: failed to write run record: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to initialize run record")

    # Enqueue to ARQ worker
    try:
        job = await app.state.arq_pool.enqueue_job(
            "run_osint_pipeline",
            run_id,
            request.city_name,
            request.country_or_region,
            operator_id,
            _job_id=run_id,  # Use run_id as job_id — makes deduplication explicit
        )
    except Exception as exc:
        log.error("trigger_run: failed to enqueue job: %s", exc)
        # Run record is written — mark it failed so it doesn't hang in "pending"
        await db.complete_run(
            run_id=run_id,
            status="failed",
            summary={},
            failure_reason=f"Enqueue failed: {exc}",
        )
        raise HTTPException(status_code=500, detail="Failed to enqueue pipeline job")

    if job is None:
        # ARQ returns None when a job with the same _job_id already exists.
        # This should not happen since run_id is a fresh UUID, but guard explicitly.
        log.error(
            "trigger_run: enqueue_job returned None for run_id=%s — "
            "possible duplicate job_id or Redis key collision",
            run_id,
        )
        await db.complete_run(
            run_id=run_id,
            status="failed",
            summary={},
            failure_reason="enqueue_job returned None — duplicate job_id",
        )
        raise HTTPException(
            status_code=500,
            detail="Job enqueue returned None — possible duplicate job_id. "
                   "This run_id was already enqueued.",
        )

    log.info(
        "trigger_run: run_id=%s city=%s enqueued as arq_job_id=%s",
        run_id, request.city_name, job.job_id,
    )

    return RunCreateResponse(
        run_id=run_id,
        city_name=request.city_name,
        country_or_region=request.country_or_region,
        status="pending",
        triggered_at=triggered_at,
        message="Run queued. Poll GET /runs/{run_id} for status updates.",
    )


@app.get(
    "/runs",
    tags=["runs"],
    dependencies=[Depends(require_api_key)],
)
async def list_runs(
    city_key: str | None = Query(default=None, description="Normalized city key, e.g. 'philadelphia_us'"),
    status: str | None = Query(
        default=None,
        description="Filter by run status: pending|running|complete|partial|failed",
        pattern="^(pending|running|complete|partial|failed)$",
    ),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    """
    List OSINT pipeline runs, most recent first.

    Query params:
        city_key — filter by city (e.g. 'philadelphia_us')
        status   — filter by run status
        limit    — page size (1–100, default 20)
        offset   — pagination offset

    Returns total count + list of run summaries.
    """
    db: SupabaseClient = app.state.db
    runs  = await db.list_runs(city_key=city_key, status=status, limit=limit, offset=offset)
    total = await db.count_runs(city_key=city_key, status=status)
    return {
        "total":  total,
        "limit":  limit,
        "offset": offset,
        "runs":   [_serialize_run(r) for r in runs],
    }


@app.get(
    "/runs/{run_id}",
    tags=["runs"],
    dependencies=[Depends(require_api_key)],
)
async def get_run_status(run_id: str) -> dict[str, Any]:
    """
    Get the current status and aggregate statistics for a run.

    Status values:
        pending   — queued, not yet started
        running   — pipeline executing
        complete  — all phases done, briefing available
        partial   — pipeline completed with some agent failures
        failed    — pipeline failed before producing output
    """
    _validate_uuid(run_id)
    db: SupabaseClient = app.state.db
    run = await db.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    # Serialize datetime fields
    return _serialize_run(run)


@app.get(
    "/runs/{run_id}/briefing",
    tags=["runs"],
    dependencies=[Depends(require_api_key)],
)
async def get_briefing(
    run_id: str,
    format: str = Query(default="json", pattern="^(json|markdown)$"),
) -> Any:
    """
    Retrieve the final intelligence briefing for a completed run.

    Query params:
        format=json      → returns the full 20-section structured JSON briefing
        format=markdown  → returns the same content as a formatted markdown string

    Returns 404 if the run doesn't exist or briefing not yet generated.
    Returns 409 if the run exists but has not yet reached "complete" status.
    """
    _validate_uuid(run_id)
    db: SupabaseClient = app.state.db

    # Validate run exists + is complete
    run = await db.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    if run.get("run_status") not in ("complete", "partial"):
        raise HTTPException(
            status_code=409,
            detail=(
                f"Run {run_id} is not yet complete (status={run.get('run_status')}). "
                f"Poll GET /runs/{run_id} until status is 'complete'."
            ),
        )

    briefing = await db.get_briefing(run_id)
    if not briefing:
        raise HTTPException(
            status_code=404,
            detail=f"Briefing not found for run {run_id}. "
                   f"Run may have completed without generating a briefing.",
        )

    if format == "markdown":
        # The full briefing_markdown is stored in claim_text of the "final_briefing_full"
        # assessment (truncated at 10000 chars). For full content: render from claim_json.
        from osint.agents.briefing import _render_markdown
        if briefing.get("sections"):
            # briefing is the full briefing_json — render it
            md = _render_markdown(briefing)
        else:
            md = "(Briefing markdown not available — full content not stored. Use format=json.)"
        return PlainTextResponse(content=md, media_type="text/markdown")

    return ORJSONResponse(content=briefing)


@app.get(
    "/runs/{run_id}/relationships",
    tags=["runs"],
    dependencies=[Depends(require_api_key)],
)
async def get_run_relationships(
    run_id: str,
    verified_only: bool = Query(
        default=False,
        description="If true, return only relationships that passed verification",
    ),
) -> dict[str, Any]:
    """
    Return all relationships produced by a run.

    Query params:
        verified_only — return only verified=TRUE edges (default: all edges)

    Returns:
        count — total edges in this response
        relationships — list of relationship records, each with:
            relationship_id, source_entity_id, target_entity_id,
            relationship_type, relationship_strength, confidence_score,
            confidence (low/medium/high), verified, is_inferred,
            and evidence fields
    """
    _validate_uuid(run_id)
    db: SupabaseClient = app.state.db

    run = await db.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    relationships = await db.get_relationships_by_run(run_id, verified_only=verified_only)
    return {
        "run_id":        run_id,
        "verified_only": verified_only,
        "count":         len(relationships),
        "relationships": [_serialize_record(r) for r in relationships],
    }


# ─────────────────────────────────────────────────────────────────────────────
# Entities
# ─────────────────────────────────────────────────────────────────────────────

@app.get(
    "/entities",
    tags=["entities"],
    dependencies=[Depends(require_api_key)],
)
async def list_entities(
    run_id: str | None = Query(default=None, description="Filter to entities from a specific run"),
    city_key: str | None = Query(default=None, description="Normalized city key, e.g. 'austin_us'"),
    entity_type: str | None = Query(
        default=None,
        description="Entity type: investor|corporate|nonprofit|political|philanthropic|"
                    "executive_hnw|community_leader|politician|hnwi|illicit",
    ),
    needs_review: bool | None = Query(default=None, description="Filter to entities flagged for review"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    """
    Paginated entity search.

    At least one of run_id or city_key is recommended for meaningful results.
    Results are sorted by score_influence descending.
    """
    if run_id:
        _validate_uuid(run_id)

    db: SupabaseClient = app.state.db

    entities = await db.search_entities(
        city_key=city_key,
        entity_type=entity_type,
        run_id=run_id,
        needs_review=needs_review,
        limit=limit,
        offset=offset,
    )
    total = await db.count_entities(city_key=city_key, entity_type=entity_type, run_id=run_id)

    return {
        "total":    total,
        "limit":    limit,
        "offset":   offset,
        "entities": [_serialize_record(e) for e in entities],
    }


@app.get(
    "/entities/{entity_id}",
    tags=["entities"],
    dependencies=[Depends(require_api_key)],
)
async def get_entity(entity_id: str) -> dict[str, Any]:
    """Get a single entity by its UUID."""
    _validate_uuid(entity_id)
    db: SupabaseClient = app.state.db
    entity = await db.get_entity(entity_id)
    if not entity:
        raise HTTPException(status_code=404, detail=f"Entity {entity_id} not found")
    return _serialize_record(entity)


# ─────────────────────────────────────────────────────────────────────────────
# Cities — Intelligence API
#
# These routes are the frontend-facing surface. They expose intelligence data
# by city, with no concept of runs, agents, or pipeline internals visible to
# the caller. run_id resolution happens entirely inside SupabaseClient.
#
# All five routes require X-API-Key auth — same as the pipeline API.
# ─────────────────────────────────────────────────────────────────────────────

# Valid entity types — kept here as a constant to avoid drift from the schema.
_ENTITY_TYPES = frozenset({
    "investor", "philanthropic", "corporate", "political",
    "nonprofit", "executive_hnw", "community_leader",
    "politician", "hnwi", "illicit",
})

# Valid classification flags for entity filtering.
_ENTITY_FLAGS = frozenset({
    "partner_candidate", "blocker_candidate", "top_influencer",
    "investment_candidate", "competitor_candidate",
    "recruiter_candidate", "support_candidate",
})


@app.get(
    "/cities",
    tags=["cities"],
    dependencies=[Depends(require_api_key)],
)
async def list_cities() -> dict[str, Any]:
    """
    List all cities that have completed intelligence data.

    Returns one entry per city — its most recently completed run's metadata.
    Cities with only pending or failed runs are NOT included.

    Response fields per city:
        city_key          — normalized identifier, e.g. 'philadelphia_us'
        city_name         — display name, e.g. 'Philadelphia'
        country_or_region
        data_as_of        — ISO timestamp of when the latest run completed
        overall_confidence — high | medium | low | null
        entity_count      — total entities collected
        relationship_count — total relationships mapped
    """
    db: SupabaseClient = app.state.db
    cities = await db.list_cities_with_intelligence()
    return {
        "count":  len(cities),
        "cities": [_serialize_city_meta(c) for c in cities],
    }


@app.get(
    "/cities/{city_key}",
    tags=["cities"],
    dependencies=[Depends(require_api_key)],
)
async def get_city(city_key: str) -> dict[str, Any]:
    """
    Return an intelligence overview for a city.

    Includes entity counts broken down by type, relationship count,
    and counts of actionable classifications (partner candidates,
    blockers, top influencers, investment candidates, competitors).

    Returns 404 if the city has no completed intelligence data.
    """
    _validate_city_key(city_key)
    db: SupabaseClient = app.state.db
    overview = await db.get_city_overview(city_key)
    if not overview:
        raise HTTPException(
            status_code=404,
            detail=f"No completed intelligence found for city '{city_key}'. "
                   f"Run must have status 'complete' or 'partial'.",
        )
    return _serialize_city_overview(overview)


@app.get(
    "/cities/{city_key}/entities",
    tags=["cities"],
    dependencies=[Depends(require_api_key)],
)
async def get_city_entities(
    city_key: str,
    entity_type: str | None = Query(
        default=None,
        description=(
            "Filter by entity type: investor | philanthropic | corporate | political | "
            "nonprofit | executive_hnw | community_leader | politician | hnwi | illicit"
        ),
    ),
    flag: str | None = Query(
        default=None,
        description=(
            "Filter to entities with a specific classification flag set to true: "
            "partner_candidate | blocker_candidate | top_influencer | "
            "investment_candidate | competitor_candidate | recruiter_candidate | "
            "support_candidate"
        ),
    ),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    """
    Return entities for a city, sorted by influence score descending.

    Filters:
        entity_type — restrict to one category of actor
        flag        — restrict to entities flagged as a specific classification

    Both filters are optional and combinable (e.g. investors who are also
    partner candidates).

    Returns 404 if the city has no completed intelligence data.
    Returns 400 if entity_type or flag values are not recognized.

    Pipeline-internal fields (run IDs, agent names, review flags, cost tracking)
    are stripped from all entity records in this response.
    """
    _validate_city_key(city_key)

    if entity_type and entity_type not in _ENTITY_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown entity_type '{entity_type}'. "
                   f"Valid values: {sorted(_ENTITY_TYPES)}",
        )
    if flag and flag not in _ENTITY_FLAGS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown flag '{flag}'. "
                   f"Valid values: {sorted(_ENTITY_FLAGS)}",
        )

    db: SupabaseClient = app.state.db

    # Confirm city exists before running the paginated query.
    run = await db.get_latest_completed_run_for_city(city_key)
    if not run:
        raise HTTPException(
            status_code=404,
            detail=f"No completed intelligence found for city '{city_key}'.",
        )

    entities, total = await asyncio.gather(
        db.get_city_entities(
            city_key=city_key,
            entity_type=entity_type,
            flag=flag,
            limit=limit,
            offset=offset,
        ),
        db.count_city_entities(
            city_key=city_key,
            entity_type=entity_type,
            flag=flag,
        ),
    )

    return {
        "city_key":    city_key,
        "total":       total,
        "limit":       limit,
        "offset":      offset,
        "entity_type": entity_type,
        "flag":        flag,
        "entities":    [_serialize_city_entity(e) for e in entities],
    }


@app.get(
    "/cities/{city_key}/relationships",
    tags=["cities"],
    dependencies=[Depends(require_api_key)],
)
async def get_city_relationships(
    city_key: str,
    verified_only: bool = Query(
        default=False,
        description="If true, return only relationships that passed verification.",
    ),
) -> dict[str, Any]:
    """
    Return the relationship graph for a city.

    Each edge includes the canonical name and type of both endpoints, so the
    graph can be rendered without a secondary entity lookup.

    Fields per relationship:
        relationship_id, source_entity_id, source_name, source_type,
        target_entity_id, target_name, target_type,
        relationship_type, direction, confidence, confidence_score,
        relationship_strength, verified, valid_from, valid_to

    Pipeline-internal fields (run_id, evidence_ids, neo4j sync state) are
    not included.

    Returns 404 if the city has no completed intelligence data.
    """
    _validate_city_key(city_key)
    db: SupabaseClient = app.state.db

    run = await db.get_latest_completed_run_for_city(city_key)
    if not run:
        raise HTTPException(
            status_code=404,
            detail=f"No completed intelligence found for city '{city_key}'.",
        )

    relationships = await db.get_city_relationships(
        city_key=city_key,
        verified_only=verified_only,
    )
    return {
        "city_key":      city_key,
        "verified_only": verified_only,
        "count":         len(relationships),
        "relationships": [_serialize_record(r) for r in relationships],
    }


@app.get(
    "/cities/{city_key}/briefing",
    tags=["cities"],
    dependencies=[Depends(require_api_key)],
)
async def get_city_briefing(
    city_key: str,
    format: str = Query(
        default="json",
        pattern="^(json|markdown)$",
        description="Response format: json (default) or markdown",
    ),
) -> Any:
    """
    Return the intelligence briefing for a city.

    The briefing is the final output of the pipeline — a structured,
    multi-section document covering ecosystem overview, key players,
    capital networks, political landscape, risk signals, and more.

    Query params:
        format=json      → full structured briefing as JSON
        format=markdown  → same content rendered as markdown text

    Returns 404 if the city has no completed intelligence or no briefing
    was generated (possible for partial runs that failed late).
    """
    _validate_city_key(city_key)
    db: SupabaseClient = app.state.db

    run = await db.get_latest_completed_run_for_city(city_key)
    if not run:
        raise HTTPException(
            status_code=404,
            detail=f"No completed intelligence found for city '{city_key}'.",
        )

    briefing = await db.get_city_briefing(city_key)
    if not briefing:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No briefing found for city '{city_key}'. "
                f"The run completed but the briefing agent may not have finished."
            ),
        )

    if format == "markdown":
        try:
            from osint.agents.briefing import _render_markdown
            md = _render_markdown(briefing) if briefing.get("sections") else (
                "(Briefing markdown not available — use format=json.)"
            )
        except Exception:
            md = "(Markdown rendering unavailable — use format=json.)"
        return PlainTextResponse(content=md, media_type="text/markdown")

    return ORJSONResponse(content=briefing)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _validate_uuid(value: str) -> None:
    """Raise 400 if value is not a valid UUID."""
    try:
        uuid.UUID(value)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid UUID: {value!r}")


def _serialize_record(record: dict[str, Any]) -> dict[str, Any]:
    """
    Convert asyncpg Record to a JSON-safe dict.
    Converts datetime → ISO string, UUID → str, bytes → base64 (omitted here).
    """
    out: dict[str, Any] = {}
    for k, v in record.items():
        if v is None:
            out[k] = None
        elif isinstance(v, datetime):
            out[k] = v.isoformat()
        elif hasattr(v, "hex"):  # UUID from asyncpg
            out[k] = str(v)
        elif isinstance(v, (list, tuple)):
            out[k] = [str(i) if hasattr(i, "hex") else i for i in v]
        else:
            out[k] = v
    return out


def _serialize_run(run: dict[str, Any]) -> dict[str, Any]:
    """Serialize a run record to API-safe dict."""
    serialized = _serialize_record(run)
    # Rename run_status → status for cleaner API contract
    serialized["status"] = serialized.pop("run_status", None)
    return serialized


# ── City-specific serializers ─────────────────────────────────────────────────

# city_key format: lowercase alphanumeric + underscores only.
# e.g. "philadelphia_us", "new_york_us", "san_francisco_us"
# Length cap at 64 prevents abuse; min 3 rules out trivial inputs.
_CITY_KEY_RE = re.compile(r"^[a-z0-9_]{3,64}$")


def _validate_city_key(city_key: str) -> None:
    """
    Raise 400 if city_key contains characters outside [a-z0-9_] or is
    outside the length range 3–64.

    city_key is interpolated into SQL via asyncpg parameterization (safe),
    but we validate format here to return a clear error before hitting the DB.
    """
    if not _CITY_KEY_RE.match(city_key):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid city_key '{city_key}'. "
                f"Must be lowercase alphanumeric and underscores only, "
                f"3–64 characters (e.g. 'philadelphia_us')."
            ),
        )


def _serialize_city_meta(city: dict[str, Any]) -> dict[str, Any]:
    """
    Serialize a city list entry from list_cities_with_intelligence().

    Renames DB column names to clean API names and handles datetime → ISO.
    """
    return {
        "city_key":          city.get("city_key"),
        "city_name":         city.get("city_name"),
        "country_or_region": city.get("country_or_region"),
        "data_as_of":        city["data_as_of"].isoformat() if city.get("data_as_of") else None,
        "overall_confidence": city.get("overall_confidence"),
        "entity_count":      city.get("total_entities_found", 0),
        "relationship_count": city.get("total_relationships_found", 0),
    }


def _serialize_city_overview(overview: dict[str, Any]) -> dict[str, Any]:
    """
    Serialize the output of get_city_overview().

    data_as_of is a datetime from asyncpg — convert to ISO string.
    All other fields are already JSON-safe (str, int, dict).
    """
    out = dict(overview)
    if isinstance(out.get("data_as_of"), datetime):
        out["data_as_of"] = out["data_as_of"].isoformat()
    return out


def _serialize_city_entity(entity: dict[str, Any]) -> dict[str, Any]:
    """
    Serialize an entity record from get_city_entities().

    Handles: datetime → ISO string, UUID → str, JSONB string → parsed dict.

    asyncpg returns JSONB columns as raw JSON strings, not Python dicts.
    category_fields must be parsed before the response is serialized to JSON,
    otherwise the client receives a double-encoded string.
    """
    import json as _json

    out: dict[str, Any] = {}
    for k, v in entity.items():
        if v is None:
            out[k] = None
        elif isinstance(v, datetime):
            out[k] = v.isoformat()
        elif hasattr(v, "hex"):  # UUID from asyncpg
            out[k] = str(v)
        elif isinstance(v, (list, tuple)):
            out[k] = [str(i) if hasattr(i, "hex") else i for i in v]
        elif isinstance(v, str) and k == "category_fields":
            # JSONB comes back as a raw JSON string from asyncpg
            try:
                out[k] = _json.loads(v)
            except (ValueError, TypeError):
                out[k] = {}
        else:
            out[k] = v
    return out
