"""
tests/unit/test_config.py

Tests for config constants and settings loading.

Verifies:
- PASS2_TRIGGER_THRESHOLD value and type
- MIN_ENTITIES_PER_CATEGORY covers all collection agent categories
- RESOLUTION_THRESHOLDS ordering invariant (auto-merge > human-review)
- MODEL_ROUTING covers all task types referenced by agents
- RATE_LIMITS covers all 14 API sources
- Settings loads without crashing; field types are correct
"""

import pytest

from osint.core.config import (
    PASS2_TRIGGER_THRESHOLD,
    MIN_ENTITIES_PER_CATEGORY,
    RESOLUTION_THRESHOLDS,
    CLASSIFICATION_THRESHOLDS,
    MODEL_ROUTING,
    RATE_LIMITS,
    settings,
)


# ─────────────────────────────────────────────────────────────────────────────
# PASS2_TRIGGER_THRESHOLD
# ─────────────────────────────────────────────────────────────────────────────

def test_pass2_threshold_value():
    assert PASS2_TRIGGER_THRESHOLD == 0.6


def test_pass2_threshold_is_float():
    assert isinstance(PASS2_TRIGGER_THRESHOLD, float)


def test_pass2_threshold_between_zero_and_one():
    assert 0.0 < PASS2_TRIGGER_THRESHOLD < 1.0


# ─────────────────────────────────────────────────────────────────────────────
# MIN_ENTITIES_PER_CATEGORY
# ─────────────────────────────────────────────────────────────────────────────

EXPECTED_COLLECTION_CATEGORIES = {
    "investor", "philanthropic", "corporate", "political",
    "nonprofit", "executive_hnw", "community_leader",
    "politician", "hnwi", "illicit",
}


def test_min_entities_has_all_categories():
    assert EXPECTED_COLLECTION_CATEGORIES == set(MIN_ENTITIES_PER_CATEGORY.keys())


def test_illicit_minimum_is_zero():
    """Absence of illicit actors is valid intelligence — no minimum."""
    assert MIN_ENTITIES_PER_CATEGORY["illicit"] == 0


def test_all_minimums_non_negative():
    for cat, minimum in MIN_ENTITIES_PER_CATEGORY.items():
        assert minimum >= 0, f"Category '{cat}' has negative minimum: {minimum}"


def test_investor_minimum_reasonable():
    """Investors are the primary target — minimum should be > 0."""
    assert MIN_ENTITIES_PER_CATEGORY["investor"] > 0


# ─────────────────────────────────────────────────────────────────────────────
# RESOLUTION_THRESHOLDS
# ─────────────────────────────────────────────────────────────────────────────

def test_resolution_thresholds_has_required_keys():
    required = {
        "exact_id_match",
        "fuzzy_auto_merge_min",
        "embedding_auto_merge_min",
        "human_review_min",
    }
    assert required == set(RESOLUTION_THRESHOLDS.keys())


def test_exact_id_match_is_one():
    assert RESOLUTION_THRESHOLDS["exact_id_match"] == 1.0


def test_auto_merge_above_human_review():
    """Auto-merge threshold must be strictly above the human review floor."""
    assert RESOLUTION_THRESHOLDS["fuzzy_auto_merge_min"] > RESOLUTION_THRESHOLDS["human_review_min"]
    assert RESOLUTION_THRESHOLDS["embedding_auto_merge_min"] > RESOLUTION_THRESHOLDS["human_review_min"]


def test_human_review_above_zero():
    """Human review range must exist above 0 — otherwise everything is auto-rejected."""
    assert RESOLUTION_THRESHOLDS["human_review_min"] > 0.0


def test_auto_merge_thresholds_below_one():
    """Auto-merge threshold < 1.0 — exact_id_match is the only 1.0 case."""
    assert RESOLUTION_THRESHOLDS["fuzzy_auto_merge_min"] < 1.0
    assert RESOLUTION_THRESHOLDS["embedding_auto_merge_min"] < 1.0


# ─────────────────────────────────────────────────────────────────────────────
# MODEL_ROUTING
# ─────────────────────────────────────────────────────────────────────────────

EXPECTED_TASK_TYPES = {
    "structured_extraction_clean",
    "structured_extraction_text",
    "entity_resolution_arbitration",
    "framing_generation",
    "relationship_inference",
    "entity_scoring",
    "claim_verification",
    "brief_drafting",
    "brief_polish",
    "gap_analysis",
    "default",
}


def test_model_routing_has_all_task_types():
    assert EXPECTED_TASK_TYPES == set(MODEL_ROUTING.keys())


def test_model_routing_values_are_nonempty_strings():
    for task, model in MODEL_ROUTING.items():
        assert isinstance(model, str) and model, f"Task '{task}' has empty/invalid model: {model!r}"


def test_escalation_tasks_use_largest_model():
    """Tasks that need the most reasoning should route to the escalation model."""
    escalation = settings.ollama_escalation_model
    assert MODEL_ROUTING["entity_resolution_arbitration"] == escalation
    assert MODEL_ROUTING["brief_polish"] == escalation


def test_high_volume_extraction_uses_smallest_model():
    """Clean structured extraction should use the fastest/smallest model."""
    extraction = settings.ollama_extraction_model
    assert MODEL_ROUTING["structured_extraction_clean"] == extraction


# ─────────────────────────────────────────────────────────────────────────────
# RATE_LIMITS
# ─────────────────────────────────────────────────────────────────────────────

EXPECTED_API_SOURCES = {
    "crunchbase", "sec_edgar", "fec_api", "propublica_nonprofit",
    "propublica_congress", "usaspending", "serpapi", "proxycurl",
    "opensecrets", "opencorporates", "courtlistener", "gdelt",
    "people_data_labs", "ofac",
}


def test_rate_limits_covers_all_sources():
    assert EXPECTED_API_SOURCES == set(RATE_LIMITS.keys())


def test_all_rate_limits_have_retry_backoff():
    """Every source must define retry backoff — no silent immediate failure."""
    for source, limits in RATE_LIMITS.items():
        assert "retry_backoff_seconds" in limits, (
            f"Source '{source}' missing retry_backoff_seconds"
        )
        assert len(limits["retry_backoff_seconds"]) > 0


def test_all_rate_limits_have_cache_ttl():
    """Every source must define a cache TTL to prevent redundant API calls."""
    for source, limits in RATE_LIMITS.items():
        assert "cache_ttl_seconds" in limits, (
            f"Source '{source}' missing cache_ttl_seconds"
        )
        assert limits["cache_ttl_seconds"] > 0


def test_ofac_cache_ttl_one_day():
    """OFAC sanctions list updates daily — TTL should not exceed 1 day."""
    assert RATE_LIMITS["ofac"]["cache_ttl_seconds"] <= 86400


def test_gdelt_shortest_cache_ttl():
    """GDELT is near real-time — should have the shortest cache TTL."""
    gdelt_ttl = RATE_LIMITS["gdelt"]["cache_ttl_seconds"]
    assert gdelt_ttl <= 3600  # <= 1 hour


# ─────────────────────────────────────────────────────────────────────────────
# Settings loading
# ─────────────────────────────────────────────────────────────────────────────

def test_settings_loads_without_crash():
    """Settings singleton must be importable and not None."""
    assert settings is not None


def test_settings_model_names_are_strings():
    assert isinstance(settings.ollama_default_model, str)
    assert isinstance(settings.ollama_escalation_model, str)
    assert isinstance(settings.ollama_extraction_model, str)
    assert isinstance(settings.ollama_embed_model, str)


def test_settings_model_names_nonempty():
    assert settings.ollama_default_model
    assert settings.ollama_escalation_model
    assert settings.ollama_extraction_model


def test_settings_proxycurl_budget_positive():
    assert settings.proxycurl_budget_per_run_usd > 0


def test_settings_chromadb_port_is_int():
    assert isinstance(settings.chromadb_port, int)


def test_settings_database_url_nonempty():
    """DATABASE_URL must be set — migration requires a real connection."""
    assert settings.database_url, "DATABASE_URL is empty — check .env"
