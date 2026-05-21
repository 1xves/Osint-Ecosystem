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

    # ─── Research Pipeline ────────────────────────────────────────────────────
    research_pipeline_url: str = "http://localhost:5050"
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
        "retry_backoff_seconds": [2, 5, 15],
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
        "retry_backoff_seconds": [2, 5],
        "cache_ttl_seconds": 86400,         # 1 day — sanctions list updates daily
    },
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
        "num_predict": 4096,
    },
    "qwen3:14b": {
        "temperature": 0.15,
        "top_p": 0.9,
        "top_k": 40,
        "repeat_penalty": 1.1,
        "num_predict": 8192,
    },
    "qwen3:22b": {
        "temperature": 0.2,
        "top_p": 0.9,
        "top_k": 50,
        "repeat_penalty": 1.1,
        "num_predict": 16384,
    },
    "nomic-embed-text": {
        "temperature": 0.0,
    },
}
