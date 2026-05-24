"""
osint/graph/graph.py

LangGraph pipeline graph definition.

Architecture:
- StateGraph with OSINTRunState as the shared state
- Two-phase collection gate: collection agents must all succeed before analytical agents run
- Conditional routing for Pass 2 gap-fill
- All agents run concurrently within each phase where possible

Phase topology:
  orchestrator
    ↓
  [PHASE 1: Collection — concurrent fan-out]
  investor_agent | philanthropic_agent | corporate_agent | political_agent |
  nonprofit_agent | executive_hnw_agent | community_leader_agent |
  politician_agent | hnwi_agent | illicit_agent | pipeline_agent
    ↓
  collection_gate  (blocks if any collection agent failed critically)
    ↓
  gap_analysis_agent
    ↓ (conditional: trigger Pass 2 if coverage < threshold)
  [PHASE 2: Gap-fill — targeted re-collection of thin categories]
    ↓
  resolution_agent
    ↓
  enrichment_agent
    ↓
  relationship_agent
    ↓
  scoring_agent
    ↓
  verification_agent
    ↓
  briefing_agent
    ↓
  END

Usage:
    from osint.graph.graph import build_graph

    graph = build_graph(db, neo4j, chroma, redis, llm, rate_limiter)
    result = await graph.ainvoke(initial_state)
"""

from __future__ import annotations

import logging
from typing import Any

from langgraph.graph import StateGraph, END

from osint.state import OSINTRunState
from osint.db.supabase import SupabaseClient
from osint.db.neo4j import Neo4jClient
from osint.db.chromadb import ChromaDBClient
from osint.db.redis import RedisClient
from osint.llm.routing import LLMRouter
from osint.core.rate_limiter import RateLimiter
from osint.core.config import PASS2_TRIGGER_THRESHOLD
from osint.agents.orchestrator import OrchestratorAgent
from osint.agents.investor import InvestorAgent
from osint.agents.philanthropic import PhilanthropicAgent
from osint.agents.nonprofit import NonprofitAgent
from osint.agents.corporate import CorporateAgent
from osint.agents.political import PoliticalAgent
from osint.agents.politician import PoliticianAgent
from osint.agents.executive_hnw import ExecutiveHNWAgent
from osint.agents.community_leader import CommunityLeaderAgent
from osint.agents.hnwi import HNWIAgent
from osint.agents.illicit import IllicitAgent
from osint.agents.pipeline import PipelineAgent
from osint.agents.gap_analysis import GapAnalysisAgent
from osint.agents.resolution import ResolutionAgent
from osint.agents.enrichment import EnrichmentAgent
from osint.agents.relationship import RelationshipAgent
from osint.agents.scoring import ScoringAgent
from osint.agents.verification import VerificationAgent
from osint.agents.briefing import BriefingAgent
from osint.agents.pass2_dispatcher import Pass2Dispatcher

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Collection gate
# ─────────────────────────────────────────────────────────────────────────────

async def collection_gate(state: OSINTRunState) -> OSINTRunState:
    """
    LangGraph node that enforces the two-phase gate.

    Checks that all collection agents completed (success or skipped).
    If any agent is in error/timeout state AND produced zero entities,
    records a warning but does not block (partial data is still useful).

    Sets gate_cleared = True to allow analytical phase to proceed.
    """
    agent_statuses = state.get("agent_statuses", {})
    collection_agents = [
        "investor_agent", "philanthropic_agent", "corporate_agent",
        "political_agent", "nonprofit_agent", "executive_hnw_agent",
        "community_leader_agent", "politician_agent", "hnwi_agent",
        "illicit_agent", "pipeline_agent",
    ]

    new_warnings: list[str] = []

    for agent_name in collection_agents:
        status = agent_statuses.get(agent_name, "skipped")
        if status in ("error", "timeout"):
            # Non-blocking — log as warning, not fatal
            msg = (
                f"Collection agent '{agent_name}' finished with status '{status}'. "
                f"Analytical phase will proceed with partial data."
            )
            new_warnings.append(msg)
            log.warning("collection_gate: %s", msg)

    # Gate is always cleared at this point — collection errors are non-fatal
    log.info(
        "collection_gate: CLEARED — %d raw entities collected across all agents",
        len(state.get("raw_entities", [])),
    )

    # Return DELTA only — do NOT spread **state.
    # Annotated reducer fields (raw_entities, agent_statuses, etc.) are already
    # accumulated by the LangGraph reducers during the parallel collection step.
    # Spreading **state here would feed those complete lists back through the
    # operator.add reducer and double every entry.
    patch: dict[str, Any] = {
        "gate_cleared":  True,
        "current_phase": "GAP_ANALYSIS",
    }
    if new_warnings:
        patch["warnings"] = new_warnings   # reducer appends these to existing warnings
    return patch


# ─────────────────────────────────────────────────────────────────────────────
# Pass 2 routing condition
# ─────────────────────────────────────────────────────────────────────────────

def should_run_pass2(state: OSINTRunState) -> str:
    """
    LangGraph conditional edge function.
    Returns "pass2" if gap analysis found thin categories.
    Returns "resolution" to skip directly to resolution.
    """
    gap_analysis = state.get("gap_analysis", {})
    if not gap_analysis:
        log.info("should_run_pass2: no gap_analysis data — skipping Pass 2")
        return "resolution"

    thin_categories = [
        cat for cat, data in gap_analysis.items()
        if data.get("coverage_score", 1.0) < PASS2_TRIGGER_THRESHOLD
    ]

    if thin_categories:
        log.info(
            "should_run_pass2: triggering Pass 2 for thin categories: %s",
            thin_categories
        )
        return "pass2"
    else:
        log.info("should_run_pass2: all categories above threshold — skipping Pass 2")
        return "resolution"


# ─────────────────────────────────────────────────────────────────────────────
# Graph builder
# ─────────────────────────────────────────────────────────────────────────────

def build_graph(
    db: SupabaseClient,
    neo4j: Neo4jClient,
    chroma: ChromaDBClient,
    redis: RedisClient,
    llm: LLMRouter,
    rate_limiter: RateLimiter,
) -> Any:
    """
    Build and compile the OSINT LangGraph pipeline graph.

    Args:
        db, neo4j, chroma, redis, llm, rate_limiter: Shared infrastructure clients.
            One instance per worker, passed to all agents.

    Returns:
        Compiled LangGraph graph — call with .ainvoke(state) to run the pipeline.
    """
    deps = (db, neo4j, chroma, redis, llm, rate_limiter)

    # ── Instantiate agents ────────────────────────────────────────────────────
    orchestrator           = OrchestratorAgent(*deps)
    investor_agent         = InvestorAgent(*deps)
    philanthropic_agent    = PhilanthropicAgent(*deps)
    nonprofit_agent        = NonprofitAgent(*deps)
    corporate_agent        = CorporateAgent(*deps)
    political_agent        = PoliticalAgent(*deps)
    politician_agent       = PoliticianAgent(*deps)
    executive_hnw_agent    = ExecutiveHNWAgent(*deps)
    community_leader_agent = CommunityLeaderAgent(*deps)
    hnwi_agent             = HNWIAgent(*deps)
    illicit_agent          = IllicitAgent(*deps)
    pipeline_agent         = PipelineAgent(*deps)

    gap_analysis_agent = GapAnalysisAgent(*deps)

    # Pass 2 Dispatcher gets references to the collection agents so it can call
    # them directly with pass_number=2 and the LLM-generated suggested_queries.
    # pipeline_agent is excluded — it calls an internal service that has no
    # mechanism for accepting targeted queries.
    pass2_dispatcher = Pass2Dispatcher(
        *deps,
        collection_agents={
            "investor_agent":          investor_agent,
            "philanthropic_agent":     philanthropic_agent,
            "corporate_agent":         corporate_agent,
            "political_agent":         political_agent,
            "nonprofit_agent":         nonprofit_agent,
            "executive_hnw_agent":     executive_hnw_agent,
            "community_leader_agent":  community_leader_agent,
            "politician_agent":        politician_agent,
            "hnwi_agent":              hnwi_agent,
        },
    )

    resolution_agent = ResolutionAgent(*deps)
    enrichment_agent      = EnrichmentAgent(*deps)
    relationship_agent    = RelationshipAgent(*deps)
    scoring_agent         = ScoringAgent(*deps)
    verification_agent    = VerificationAgent(*deps)
    briefing_agent        = BriefingAgent(*deps)

    # ── Build graph ───────────────────────────────────────────────────────────
    graph = StateGraph(OSINTRunState)

    # Add all nodes
    graph.add_node("orchestrator",          orchestrator)
    graph.add_node("investor_agent",        investor_agent)
    graph.add_node("philanthropic_agent",   philanthropic_agent)
    graph.add_node("corporate_agent",       corporate_agent)
    graph.add_node("political_agent",       political_agent)
    graph.add_node("nonprofit_agent",       nonprofit_agent)
    graph.add_node("executive_hnw_agent",   executive_hnw_agent)
    graph.add_node("community_leader_agent", community_leader_agent)
    graph.add_node("politician_agent",      politician_agent)
    graph.add_node("hnwi_agent",            hnwi_agent)
    graph.add_node("illicit_agent",         illicit_agent)
    graph.add_node("pipeline_agent",        pipeline_agent)
    graph.add_node("collection_gate",       collection_gate)
    graph.add_node("gap_analysis_agent",    gap_analysis_agent)
    graph.add_node("pass2_dispatcher",      pass2_dispatcher)
    graph.add_node("resolution_agent",      resolution_agent)
    graph.add_node("enrichment_agent",      enrichment_agent)
    graph.add_node("relationship_agent",    relationship_agent)
    graph.add_node("scoring_agent",         scoring_agent)
    graph.add_node("verification_agent",    verification_agent)
    graph.add_node("briefing_agent",        briefing_agent)

    # ── Entry point ───────────────────────────────────────────────────────────
    graph.set_entry_point("orchestrator")

    # ── Orchestrator → collection fan-out ────────────────────────────────────
    # All collection agents run after orchestrator
    for collection_agent_name in [
        "investor_agent", "philanthropic_agent", "corporate_agent",
        "political_agent", "nonprofit_agent", "executive_hnw_agent",
        "community_leader_agent", "politician_agent", "hnwi_agent",
        "illicit_agent", "pipeline_agent",
    ]:
        graph.add_edge("orchestrator", collection_agent_name)

    # ── Collection agents → gate ──────────────────────────────────────────────
    # All collection agents must complete before the gate
    for collection_agent_name in [
        "investor_agent", "philanthropic_agent", "corporate_agent",
        "political_agent", "nonprofit_agent", "executive_hnw_agent",
        "community_leader_agent", "politician_agent", "hnwi_agent",
        "illicit_agent", "pipeline_agent",
    ]:
        graph.add_edge(collection_agent_name, "collection_gate")

    # ── Gate → gap analysis ───────────────────────────────────────────────────
    graph.add_edge("collection_gate", "gap_analysis_agent")

    # ── Gap analysis → conditional Pass 2 or resolution ──────────────────────
    graph.add_conditional_edges(
        "gap_analysis_agent",
        should_run_pass2,
        {
            "pass2": "pass2_dispatcher",
            "resolution": "resolution_agent",
        }
    )

    # ── Pass 2 → resolution ────────────────────────────────────────────────────
    # pass2_dispatcher runs targeted collection agents inline (not as graph nodes)
    # and merges their output into raw_entities before routing to resolution.
    graph.add_edge("pass2_dispatcher", "resolution_agent")

    # ── Analytical pipeline (sequential) ─────────────────────────────────────
    graph.add_edge("resolution_agent",  "enrichment_agent")
    graph.add_edge("enrichment_agent",  "relationship_agent")
    graph.add_edge("relationship_agent", "scoring_agent")
    graph.add_edge("scoring_agent",     "verification_agent")
    graph.add_edge("verification_agent", "briefing_agent")
    graph.add_edge("briefing_agent",    END)

    return graph.compile()
