"""
tests/unit/test_state.py

Tests for OSINTRunState and the initial_state() factory.

Covers:
- All fields present in initial state
- Correct default values for control fields
- city_key normalization
- All list/dict fields initialized as empty containers (not None)
"""

import pytest
from osint.state import initial_state, OSINTRunState


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def base_state() -> OSINTRunState:
    return initial_state(
        run_id="test-run-001",
        city_name="Austin",
        country_or_region="United States",
        operator_id="user-123",
        triggered_at="2026-05-20T12:00:00Z",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Identity fields
# ─────────────────────────────────────────────────────────────────────────────

def test_run_id_stored(base_state):
    assert base_state["run_id"] == "test-run-001"


def test_city_name_stored(base_state):
    assert base_state["city_name"] == "Austin"


def test_country_stored(base_state):
    assert base_state["country_or_region"] == "United States"


def test_operator_id_stored(base_state):
    assert base_state["operator_id"] == "user-123"


def test_triggered_at_stored(base_state):
    assert base_state["triggered_at"] == "2026-05-20T12:00:00Z"


# ─────────────────────────────────────────────────────────────────────────────
# city_key normalization
# ─────────────────────────────────────────────────────────────────────────────

def test_city_key_simple():
    """Single-word city name, two-char country abbreviation."""
    s = initial_state("r1", "Austin", "United States", "u1", "t")
    assert s["city_key"] == "austin_un"


def test_city_key_spaces_replaced():
    """Spaces in city name become underscores."""
    s = initial_state("r1", "New York", "United States", "u1", "t")
    assert s["city_key"] == "new_york_un"


def test_city_key_lowercased():
    """City name is lowercased in the key."""
    s = initial_state("r1", "MIAMI", "United States", "u1", "t")
    assert s["city_key"] == "miami_un"


def test_city_key_country_two_chars():
    """Country code is the first two characters, lowercased."""
    s = initial_state("r1", "Berlin", "Germany", "u1", "t")
    assert s["city_key"] == "berlin_ge"


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline control defaults
# ─────────────────────────────────────────────────────────────────────────────

def test_run_status_default(base_state):
    assert base_state["run_status"] == "pending"


def test_current_phase_default(base_state):
    assert base_state["current_phase"] == "INIT"


def test_pass_number_default(base_state):
    assert base_state["pass_number"] == 1


def test_gate_cleared_default(base_state):
    assert base_state["gate_cleared"] is False


# ─────────────────────────────────────────────────────────────────────────────
# Empty containers — must be mutable empty collections, not None
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("field", [
    "raw_entities",
    "raw_search_records",
    "canonical_entities",
    "merge_decisions",
    "ambiguous_merges",
    "enrichment_targets",
    "enriched_entities",
    "relationships_draft",
    "relationships_verified",
    "relationships_rejected",
    "scored_entities",
    "verified_entity_ids",
    "flagged_entity_ids",
    "warnings",
    "errors",
    "framings",
    "pass2_targets",
])
def test_list_field_empty(base_state, field):
    assert base_state[field] == []
    assert isinstance(base_state[field], list)


@pytest.mark.parametrize("field", [
    "scope_parameters",
    "agent_statuses",
    "agent_errors",
    "agent_entity_counts",
    "agent_token_counts",
    "gap_analysis",
    "ranked_lists",
    "verification_results",
    "verification_summary",
    "pipeline_agent_output",
    "rate_limit_state",
    "briefing_json",
])
def test_dict_field_empty(base_state, field):
    assert base_state[field] == {}
    assert isinstance(base_state[field], dict)


# ─────────────────────────────────────────────────────────────────────────────
# Numeric/string operational defaults
# ─────────────────────────────────────────────────────────────────────────────

def test_redis_cache_hits_zero(base_state):
    assert base_state["redis_cache_hits"] == 0


def test_proxycurl_spend_zero(base_state):
    assert base_state["proxycurl_spend_usd"] == 0.0


def test_total_tokens_in_zero(base_state):
    assert base_state["total_tokens_in"] == 0


def test_total_tokens_out_zero(base_state):
    assert base_state["total_tokens_out"] == 0


def test_briefing_markdown_empty(base_state):
    assert base_state["briefing_markdown"] == ""
