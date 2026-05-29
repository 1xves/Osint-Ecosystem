"""
osint/core/config.py

Central configuration — all thresholds, limits, and model routing.
Single source of truth. Never scatter these values across agent files.

Usage:
    from osint.core.config import settings, RATE_LIMITS, THRESHOLDS
"""

from __future__ import annotations
import os
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):
    """
    Environment-driven settings. Values come from .env file.
    Field names map to env vars case-insensitively (e.g. database_url → DATABASE_URL).
    All fields have sensible defaults for local development.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ─── Supabase ─────────────────────────────────────────────────────────────
    supabase_url: str = ""
    supabase_anon_key: str = ""
    supabase_service_role_key: str = ""
    database_url: str = ""

    # ─── Neo4j ────────────────────────────────────────────────────────────────
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "changeme"

    # ─── ChromaDB ─────────────────────────────────────────────────────────────
    chromadb_host: str = "localhost"
    chromadb_port: int = 8001

    # ─── Redis ────────────────────────────────────────────────────────────────
    redis_url: str = "redis://localhost:6379/0"

    # ─── Ollama ───────────────────────────────────────────────────────────────
    ollama_host: str = "http://localhost:11434"
    ollama_default_model: str = "qwen3:14b"
    ollama_escalation_model: str = "qwen3:22b"
    ollama_extraction_model: str = "qwen3:7b"
    ollama_embed_model: str = "nomic-embed-text"

    # ─── API Keys ─────────────────────────────────────────────────────────────
    crunchbase_api_key: str = ""
    serpapi_api_key: str = ""
    opensecrets_api_key: str = ""
    proxycurl_api_key: str = ""
    pdl_api_key: str = ""
    mediastack_api_key: str = ""
    fec_api_key: str = ""
    courtlistener_api_key: str = ""
    followthemoney_api_key: str = ""    # Optional — unauthenticated allowed at lower rate
    opencorporates_api_key: str = ""    # Optional — increases rate limit

    # ─── Research Pipeline ────────────────────────────────────────────────────
    # PIPELINE_DATABASE_URL: direct asyncpg connection to the pipeline's Supabase project
    # (wuojatgaxkeqpubsvrrg — separate from OSINT project gdiuwayqjrejwosuxmel).
    # The PipelineAgent queries synthesis + extractions tables directly instead of
    # going through the Flask API at localhost:5050.
    pipeline_database_url: str = ""
    research_pipeline_url: str = "http://localhost:5050"   # kept for backward compat / fallback
    research_pipeline_api_key: str = ""

    # ─── Budget Controls ──────────────────────────────────────────────────────
    proxycurl_budget_per_run_usd: float = 25.0

    # ─── Application ──────────────────────────────────────────────────────────
    app_env: str = "development"
    log_level: str = "INFO"
    secret_key: str = "dev-secret-key"


# Singleton — import this everywhere
settings = Settings()


# ─────────────────────────────────────────────────────────────────────────────
# Rate Limit Configuration
# All limits are conservative — better to be slow than to get blocked.
# cache_ttl_seconds: how long to cache API responses in Redis.
# ─────────────────────────────────────────────────────────────────────────────

RATE_LIMITS: dict[str, dict] = {
    "crunchbase": {
        "requests_per_minute": 10,
        "requests_per_day": 1000,
        "retry_backoff_seconds": [2, 5, 15, 30],
        "cache_ttl_seconds": 604800,        # 7 days
    },
    "sec_edgar": {
        "requests_per_second": 10,          # EDGAR policy
        "retry_backoff_seconds": [1, 3, 10],
        "cache_ttl_seconds": 2592000,       # 30 days — filings are stable
    },
    "fec_api": {
        "requests_per_minute": 60,
        "retry_backoff_seconds": [2, 5, 15],
        "cache_ttl_seconds": 2592000,       # 30 days
    },
    "propublica_nonprofit": {
        "requests_per_minute": 60,
        "retry_backoff_seconds": [2, 5, 15],
        "cache_ttl_seconds": 604800,        # 7 days
    },
    "propublica_congress": {
        "requests_per_minute": 60,
        "retry_backoff_seconds": [2, 5, 15],
        "cache_ttl_seconds": 86400,         # 1 day — voting records update more often
    },
    "usaspending": {
        "requests_per_minute": 60,
        "retry_backoff_seconds": [2, 5, 15],
        "cache_ttl_seconds": 604800,        # 7 days
    },
    "serpapi": {
        "requests_per_month": 2000,
        "requests_per_second": 2,
        "retry_backoff_seconds": [2, 5, 15],
        "cache_ttl_seconds": 86400,         # 1 day — search results change
    },
    "proxycurl": {
        "requests_per_second": 5,
        "budget_per_run_usd": settings.proxycurl_budget_per_run_usd,
        "cost_per_call_usd": 0.01,
        "retry_backoff_seconds": [2, 5],
        "cache_ttl_seconds": 2592000,       # 30 days
    },
    "opensecrets": {
        "requests_per_day": 500,
        "retry_backoff_seconds": [2, 5, 15],
        "cache_ttl_seconds": 86400,
    },
    "opencorporates": {
        "requests_per_minute": 30,
        "retry_backoff_seconds": [2],   # 1 retry only — OC frequently returns 401/network errors
        "cache_ttl_seconds": 604800,
    },
    "courtlistener": {
        "requests_per_minute": 30,
        "retry_backoff_seconds": [2, 5, 15],
        "cache_ttl_seconds": 604800,
    },
    "gdelt": {
        "requests_per_minute": 60,
        "retry_backoff_seconds": [1, 3],
        "cache_ttl_seconds": 3600,          # 1 hour — near real-time events
    },
    "people_data_labs": {
        "requests_per_minute": 60,
        "retry_backoff_seconds": [2, 5, 15],
        "cache_ttl_seconds": 2592000,
    },
    "ofac": {
        "requests_per_minute": 60,
        "retry_backoff_seconds": [],        # Fail fast — no retries on DNS/connectivity errors
        "cache_ttl_seconds": 86400,         # 1 day — sanctions list updates daily
    },
    "littlesis": {
        "requests_per_minute": 30,          # Conservative — undocumented limit; API is rate-sensitive
        "retry_backoff_seconds": [3],       # 1 retry only — if API is down, fail fast (was [2,5,15])
        "cache_ttl_seconds": 604800,        # 7 days — power network data is stable
    },
    "followthemoney": {
        "requests_per_minute": 30,          # Unauthenticated limit; 100/min with API key
        "retry_backoff_seconds": [2, 5, 15],
        "cache_ttl_seconds": 86400,         # 1 day — state campaign finance updates frequently
    },
    "icij": {
        "requests_per_minute": 20,          # Undocumented limit; be conservative
        "retry_backoff_seconds": [3, 10, 30],
        "cache_ttl_seconds": 2592000,       # 30 days — ICIJ data is from leaked docs, stable
    },
    "wayback": {
        "requests_per_minute": 10,          # Conservative — IA requests politeness
        "retry_backoff_seconds": [2, 5],
        "cache_ttl_seconds": 86400,         # 1 day — snapshots don't change
    },
    "patent_view": {
        "requests_per_minute": 45,          # USPTO PatentsView published limit
        "retry_backoff_seconds": [2, 5, 15],
        "cache_ttl_seconds": 2592000,       # 30 days — patent records are stable
    },
    "eventbrite": {
        "requests_per_minute": 60,
        "retry_backoff_seconds": [2, 5, 15],
        "cache_ttl_seconds": 3600,          # 1 hour — events change frequently
    },
    "meetup": {
        "requests_per_minute": 30,
        "retry_backoff_seconds": [2, 5, 15],
        "cache_ttl_seconds": 3600,
    },
    "bizapedia": {
        "requests_per_minute": 20,          # 1 req/3s — conservative to avoid blocks
        "retry_backoff_seconds": [5, 15, 60],
        "cache_ttl_seconds": 604800,        # 7 days — corporate records are stable
    },
    "sos_us": {
        "requests_per_minute": 24,          # ~2 req/5s per SoS domain
        "retry_backoff_seconds": [5, 15, 60],
        "cache_ttl_seconds": 604800,
    },
    # Form D uses the "sec_edgar" domain (shared with EdgarClient — same EDGAR servers)
    # Rate limit for XML fetches is enforced by _XML_FETCH_SEMAPHORE in form_d.py,
    # not by RateLimiter, because the XML responses cannot go through RateLimiter.get()
    # (which calls resp.json() and would fail on non-JSON content).
}


# ─────────────────────────────────────────────────────────────────────────────
# Entity Resolution Thresholds
# ─────────────────────────────────────────────────────────────────────────────

RESOLUTION_THRESHOLDS = {
    "exact_id_match": 1.0,              # Any external ID match = definitive merge
    "fuzzy_auto_merge_min": 0.85,       # Fuzzy name match above this = auto-merge
    "embedding_auto_merge_min": 0.85,   # Embedding similarity above this = auto-merge
    "human_review_min": 0.60,           # Below auto-merge, above this = human review queue
    # Below 0.60: automatic rejection — not the same entity
}


# ─────────────────────────────────────────────────────────────────────────────
# Scoring Classification Thresholds
# ─────────────────────────────────────────────────────────────────────────────

CLASSIFICATION_THRESHOLDS = {
    "partner_candidate":        {"dimension": "score_partner_potential",    "min": 70},
    "competitor_candidate":     {"dimension": "score_competitor_potential", "min": 65},
    "blocker_candidate":        {"dimension": "score_blocker_risk",         "min": 65},
    "investment_candidate":     {"dimension": "score_investment_potential", "min": 70},
    "support_candidate":        {"dimension": "score_support_target",       "min": 60},
    "recruiter_candidate":      {"dimension": "score_recruiting_potential", "min": 65},
    "top_influencer":           {"dimension": "score_influence",            "min": 75},
}

RATIONALE_REQUIRED_ABOVE = 70           # Score rationale required for any dimension score >= 70
BLOCKER_EVIDENCE_REQUIRED_ABOVE = 60    # Evidence citation required for blocker_risk >= 60
ILLICIT_CONFIDENCE_THRESHOLD = "high"   # Only high-confidence illicit claims enter report


# ─────────────────────────────────────────────────────────────────────────────
# Coverage Thresholds (for gap analysis → Pass 2 trigger)
# ─────────────────────────────────────────────────────────────────────────────

MIN_ENTITIES_PER_CATEGORY = {
    "investor": 5,
    "philanthropic": 3,
    "corporate": 5,
    "political": 3,
    "nonprofit": 5,
    "executive_hnw": 10,
    "community_leader": 3,
    "politician": 3,
    "hnwi": 2,
    "illicit": 0,           # No minimum — absence is valid intelligence
}

PASS2_TRIGGER_THRESHOLD = 0.6           # Gap-fill if coverage_score < this


# ─────────────────────────────────────────────────────────────────────────────
# Freshness Thresholds (days until a record is considered stale)
# ─────────────────────────────────────────────────────────────────────────────

FRESHNESS_THRESHOLDS_DAYS = {
    "funding_rounds": 90,
    "board_memberships": 180,
    "advisory_roles": 180,
    "executive_profiles": 180,
    "political_contributions": 365,
    "nonprofit_990_data": 365,          # IRS filing cycle
    "government_contracts": 180,
    "news_media_signals": 30,
    "company_registration": 365,
    "patent_filings": 180,
}


# ─────────────────────────────────────────────────────────────────────────────
# Model Routing Table
# Deterministic — task type maps to model. Never decided at runtime by LLM.
# ─────────────────────────────────────────────────────────────────────────────

MODEL_ROUTING: dict[str, str] = {
    # High-volume extraction from clean structured JSON (Crunchbase, USASpending)
    "structured_extraction_clean": settings.ollama_extraction_model,    # qwen3:7b

    # Extraction from semi-structured text (EDGAR, 990 PDFs, news)
    "structured_extraction_text": settings.ollama_default_model,        # qwen3:14b

    # Entity resolution arbitration (score 0.60–0.84)
    "entity_resolution_arbitration": settings.ollama_escalation_model,  # qwen3:22b

    # Framing generation
    "framing_generation": settings.ollama_default_model,

    # Relationship inference
    "relationship_inference": settings.ollama_default_model,

    # 9-dimension scoring
    "entity_scoring": settings.ollama_default_model,

    # Claim verification
    "claim_verification": settings.ollama_default_model,

    # Brief section drafting
    "brief_drafting": settings.ollama_default_model,

    # Brief final polish (triggered if quality gate fails)
    "brief_polish": settings.ollama_escalation_model,

    # Gap analysis
    "gap_analysis": settings.ollama_default_model,

    # Document extraction — semi-structured text from PDFs/HTML
    # All use the default model (qwen3:14b) — these require reasoning over dense text.
    "document_extraction_proxy": settings.ollama_default_model,  # DEF 14A proxy statements
    "document_extraction_10k":   settings.ollama_default_model,  # 10-K annual reports
    "document_extraction_court": settings.ollama_default_model,  # Court filings / dockets
    "document_extraction_990":   settings.ollama_default_model,  # IRS Form 990 XML sections

    # Default fallback
    "default": settings.ollama_default_model,
}


# ─────────────────────────────────────────────────────────────────────────────
# Ollama Model Parameters
# ─────────────────────────────────────────────────────────────────────────────

MODEL_PARAMS: dict[str, dict] = {
    "qwen3:7b": {
        "temperature": 0.1,
        "top_p": 0.9,
        "top_k": 40,
        "repeat_penalty": 1.1,
        "num_ctx": 16384,   # context window (input + output); Ollama default is 4096 — far too small
        "num_predict": 4096,
    },
    "qwen3:14b": {
        "temperature": 0.15,
        "top_p": 0.9,
        "top_k": 40,
        "repeat_penalty": 1.1,
        "num_ctx": 16384,   # 4096 default gets consumed by prompt alone on complex city analysis
        "num_predict": 8192,
    },
    "qwen3:22b": {
        "temperature": 0.2,
        "top_p": 0.9,
        "top_k": 50,
        "repeat_penalty": 1.1,
        "num_ctx": 32768,
        "num_predict": 16384,
    },
    "nomic-embed-text": {
        "temperature": 0.0,
    },
}
