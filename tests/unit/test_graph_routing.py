"""
tests/unit/test_graph_routing.py

Tests for graph routing logic:
- collection_gate: enforces two-phase gate, non-fatal on agent errors
- should_run_pass2: conditional routing based on gap analysis coverage scores
- build_graph: smoke test — graph compiles cleanly with mocked dependencies

No live services required. All tests run entirely in memory.
"""

import pytest
from unittest.mock import MagicMock, AsyncMock

from osint.state import initial_state
from osint.graph.graph import collection_gate, should_run_pass2, build_graph
from osint.core.config import PASS2_TRIGGER_THRESHOLD


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_state(**overrides):
    """Create a base state with optional field overrides."""
    s = initial_state(
        run_id="test-run",
        city_name="Austin",
        country_or_region="United States",
        operator_id="u1",
        triggered_at="2026-05-20T00:00:00Z",
    )
    s.update(overrides)
    return s


# ─────────────────────────────────────────────────────────────────────────────
# collection_gate
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_collection_gate_all_success_clears_gate():
    """All agents succeed → gate_cleared=True."""
    state = _make_state(
        agent_statuses={
            "investor_agent": "success",
            "philanthropic_agent": "success",
            "corporate_agent": "success",
            "political_agent": "success",
            "nonprofit_agent": "success",
            "executive_hnw_agent": "success",
            "community_leader_agent": "success",
            "politician_agent": "success",
            "hnwi_agent": "success",
            "illicit_agent": "success",
            "pipeline_agent": "success",
        }
    )
    result = await collection_gate(state)
    assert result["gate_cleared"] is True


@pytest.mark.asyncio
async def test_collection_gate_sets_phase_to_gap_analysis():
    """Gate always advances current_phase to GAP_ANALYSIS."""
    state = _make_state()
    result = await collection_gate(state)
    assert result["current_phase"] == "GAP_ANALYSIS"


@pytest.mark.asyncio
async def test_collection_gate_non_fatal_on_agent_error():
    """Agent errors are non-blocking — gate still clears, warning added."""
    state = _make_state(
        agent_statuses={
            "investor_agent": "error",
            "philanthropic_agent": "timeout",
        }
    )
    result = await collection_gate(state)
    assert result["gate_cleared"] is True
    assert len(result["warnings"]) == 2


@pytest.mark.asyncio
async def test_collection_gate_error_warning_message():
    """Warning message names the failed agent and its status."""
    state = _make_state(
        agent_statuses={"investor_agent": "error"}
    )
    result = await collection_gate(state)
    warning = result["warnings"][0]
    assert "investor_agent" in warning
    assert "error" in warning


@pytest.mark.asyncio
async def test_collection_gate_preserves_existing_warnings():
    """Pre-existing warnings are not discarded."""
    state = _make_state(
        warnings=["pre-existing warning"],
        agent_statuses={"investor_agent": "error"},
    )
    result = await collection_gate(state)
    assert "pre-existing warning" in result["warnings"]
    assert len(result["warnings"]) == 2


@pytest.mark.asyncio
async def test_collection_gate_no_warnings_on_clean_run():
    """No warnings added when all agents succeed."""
    state = _make_state()
    result = await collection_gate(state)
    assert result["warnings"] == []


@pytest.mark.asyncio
async def test_collection_gate_missing_agents_treated_as_skipped():
    """Agents not in agent_statuses default to 'skipped' — not an error."""
    state = _make_state(agent_statuses={})
    result = await collection_gate(state)
    assert result["gate_cleared"] is True
    assert result["warnings"] == []


# ─────────────────────────────────────────────────────────────────────────────
# should_run_pass2
# ─────────────────────────────────────────────────────────────────────────────

def test_should_run_pass2_empty_gap_analysis_skips():
    """Empty gap_analysis → skip to resolution."""
    state = _make_state(gap_analysis={})
    assert should_run_pass2(state) == "resolution"


def test_should_run_pass2_no_gap_analysis_key_skips():
    """Missing gap_analysis key → skip to resolution."""
    state = _make_state()
    # initial_state sets gap_analysis={}, same as empty
    assert should_run_pass2(state) == "resolution"


def test_should_run_pass2_thin_category_triggers_pass2():
    """Any category below PASS2_TRIGGER_THRESHOLD triggers Pass 2."""
    state = _make_state(
        gap_analysis={
            "investor": {"coverage_score": PASS2_TRIGGER_THRESHOLD - 0.01},
        }
    )
    assert should_run_pass2(state) == "pass2"


def test_should_run_pass2_all_above_threshold_skips():
    """All categories at or above threshold → skip to resolution."""
    state = _make_state(
        gap_analysis={
            "investor":      {"coverage_score": PASS2_TRIGGER_THRESHOLD},
            "philanthropic": {"coverage_score": 0.9},
            "corporate":     {"coverage_score": 1.0},
        }
    )
    assert should_run_pass2(state) == "resolution"


def test_should_run_pass2_exactly_at_threshold_skips():
    """Coverage exactly at threshold is NOT below it — no Pass 2."""
    state = _make_state(
        gap_analysis={
            "investor": {"coverage_score": PASS2_TRIGGER_THRESHOLD},
        }
    )
    assert should_run_pass2(state) == "resolution"


def test_should_run_pass2_zero_coverage_triggers():
    """Zero coverage (no entities found) definitely triggers Pass 2."""
    state = _make_state(
        gap_analysis={
            "nonprofit": {"coverage_score": 0.0},
        }
    )
    assert should_run_pass2(state) == "pass2"


def test_should_run_pass2_mixed_categories_triggers():
    """One thin category among many healthy ones still triggers Pass 2."""
    state = _make_state(
        gap_analysis={
            "investor":      {"coverage_score": 0.9},
            "philanthropic": {"coverage_score": 0.8},
            "corporate":     {"coverage_score": 0.3},   # thin
            "nonprofit":     {"coverage_score": 1.0},
        }
    )
    assert should_run_pass2(state) == "pass2"


def test_should_run_pass2_missing_coverage_score_defaults_to_1():
    """Categories without coverage_score key default to 1.0 → not thin."""
    state = _make_state(
        gap_analysis={
            "investor": {},  # no coverage_score key
        }
    )
    # data.get("coverage_score", 1.0) → 1.0, which is not < threshold
    assert should_run_pass2(state) == "resolution"


# ─────────────────────────────────────────────────────────────────────────────
# build_graph — smoke test
# ─────────────────────────────────────────────────────────────────────────────

def test_build_graph_compiles_without_error():
    """
    build_graph() should compile the LangGraph DAG cleanly.

    Uses MagicMock for all infrastructure deps — no live services required.
    This test catches:
    - Broken imports in any agent module
    - Missing graph nodes referenced in edges
    - Any agent __init__ that crashes with MagicMock deps
    """
    mock_db         = MagicMock()
    mock_neo4j      = MagicMock()
    mock_chroma     = MagicMock()
    mock_redis      = MagicMock()
    mock_llm        = MagicMock()
    mock_rate_limiter = MagicMock()

    graph = build_graph(
        mock_db,
        mock_neo4j,
        mock_chroma,
        mock_redis,
        mock_llm,
        mock_rate_limiter,
    )

    assert graph is not None


def test_build_graph_has_expected_nodes():
    """
    Compiled graph must contain all 22 expected node names.
    Catches accidental omissions when wiring new agents.
    """
    graph = build_graph(
        MagicMock(), MagicMock(), MagicMock(),
        MagicMock(), MagicMock(), MagicMock(),
    )

    expected_nodes = {
        "orchestrator",
        "investor_agent", "philanthropic_agent", "corporate_agent",
        "political_agent", "nonprofit_agent", "executive_hnw_agent",
        "community_leader_agent", "politician_agent", "hnwi_agent",
        "illicit_agent", "pipeline_agent",
        "collection_gate",
        "gap_analysis_agent",
        "pass2_dispatcher",
        "resolution_agent",
        "enrichment_agent",
        "relationship_agent",
        "scoring_agent",
        "verification_agent",
        "briefing_agent",
    }

    # LangGraph compiled graphs expose their nodes via .get_graph().nodes
    graph_nodes = set(graph.get_graph().nodes.keys())
    # Remove internal LangGraph sentinel nodes
    graph_nodes.discard("__start__")
    graph_nodes.discard("__end__")

    assert expected_nodes == graph_nodes
