"""
osint/state.py

LangGraph shared state object. Every agent reads from and writes to this.
Lock this before writing any agent. Any field added here must be
documented in OSINT_Schema_Spec.md first.

Usage:
    from osint.state import OSINTRunState
    graph = StateGraph(OSINTRunState)
"""

from __future__ import annotations
from typing import TypedDict, Literal, Optional, Any


class OSINTRunState(TypedDict, total=False):
    """
    Shared in-memory state for a single OSINT pipeline run.
    Lives in LangGraph's state graph — ephemeral, not stored in DB directly.
    All DB writes happen from within agents using the data here.

    total=False means all fields are optional at creation time.
    Agents add fields as the pipeline progresses.
    """

    # ─── Run Identity ─────────────────────────────────────────────────────────
    run_id: str                          # UUID — matches agent_runs.run_id in DB
    city_name: str                       # e.g., "Austin"
    country_or_region: str              # e.g., "United States"
    city_key: str                        # Normalized: "austin_us"
    operator_id: str                     # user_id who triggered this run
    triggered_at: str                    # ISO8601 timestamp

    # ─── Pipeline Control ─────────────────────────────────────────────────────
    run_status: Literal["pending", "running", "complete", "failed", "partial"]
    current_phase: Literal[
        "INIT", "COLLECTION_PASS1", "GAP_ANALYSIS", "COLLECTION_PASS2",
        "RESOLUTION", "ENRICHMENT", "RELATIONSHIP", "SCORING",
        "VERIFICATION", "BRIEFING", "DONE"
    ]
    pass_number: int                     # 1 (initial) or 2 (gap-fill)
    gate_cleared: bool                   # Two-phase gate: True only when ALL collection agents pass

    # ─── Orchestrator Output ──────────────────────────────────────────────────
    scope_parameters: dict[str, Any]     # market_size_estimate, known_characteristics, priority_categories
    framings: list[dict[str, Any]]       # 4 perspective framings — see FramingObject below

    # ─── Agent Status Tracking ────────────────────────────────────────────────
    agent_statuses: dict[str, str]       # agent_name → "pending"|"running"|"success"|"error"|"timeout"|"skipped"
    agent_errors: dict[str, str]         # agent_name → error message (if failed)
    agent_entity_counts: dict[str, int]  # agent_name → entities produced this run
    agent_token_counts: dict[str, int]   # agent_name → total tokens used

    # ─── Phase 1 Output: Raw Collection ───────────────────────────────────────
    # All raw_entities are pre-resolution — multiple agents may have extracted
    # the same entity under different names. Resolution de-duplicates.
    raw_entities: list[dict[str, Any]]          # All extracted entities, all agents
    raw_search_records: list[dict[str, Any]]    # All search attempts (also written to DB immediately)
    pipeline_agent_output: dict[str, Any]       # Structured output from localhost:5050

    # ─── Gap Analysis ─────────────────────────────────────────────────────────
    gap_analysis: dict[str, dict[str, Any]]     # category → {entities_found, expected_min, coverage_score, gaps, agents_to_retry}
    pass2_targets: list[dict[str, Any]]         # Targeted queries for Pass 2

    # ─── Phase 2 Output: Resolution ───────────────────────────────────────────
    canonical_entities: list[dict[str, Any]]    # Post-resolution deduplicated entity set
    merge_decisions: list[dict[str, Any]]       # Every merge and rejection decision (Layer 1-3)
    ambiguous_merges: list[dict[str, Any]]      # Score 0.60–0.84: held for human review queue

    # ─── Phase 2 Output: Enrichment ───────────────────────────────────────────
    enrichment_targets: list[str]               # entity_ids selected for enrichment
    enriched_entities: list[dict[str, Any]]     # Post-enrichment canonical set

    # ─── Phase 3 Output: Analytical ───────────────────────────────────────────
    relationships_draft: list[dict[str, Any]]   # Edge list pre-verification
    relationships_verified: list[dict[str, Any]] # Edge list post-verification
    relationships_rejected: list[dict[str, Any]] # Edges that failed (written to rejected_items)
    scored_entities: list[dict[str, Any]]        # Entities with 9-dimension scores appended
    ranked_lists: dict[str, list[str]]           # dimension → ordered list of entity_ids

    # ─── Phase 4 Output: Synthesis ────────────────────────────────────────────
    verification_results: dict[str, Any]         # entity_id → {claims_verified, overall_verdict, hard_fail, flagged}
    verification_summary: dict[str, Any]         # {total_entities, passed, failed, unverifiable, flagged_entity_ids}
    verified_entity_ids: list[str]               # Entity IDs that passed the hard gate (no high-confidence fails)
    flagged_entity_ids: list[str]                # Entity IDs with high-confidence fails, excluded from briefing
    briefing_json: dict[str, Any]                # Full 20-section brief as structured JSON
    briefing_markdown: str                        # Same brief as human-readable markdown

    # ─── Operational ──────────────────────────────────────────────────────────
    rate_limit_state: dict[str, dict[str, Any]]  # domain → {requests_made, remaining, reset_at, daily_budget}
    redis_cache_hits: int
    proxycurl_spend_usd: float           # Running cost — agent stops if > budget
    total_tokens_in: int                 # Aggregate across all LLM calls this run
    total_tokens_out: int
    warnings: list[str]                  # Non-fatal issues
    errors: list[str]                    # Any error here blocks final report


# ─────────────────────────────────────────────────────────────────────────────
# State factory — creates a clean initial state for a new run
# ─────────────────────────────────────────────────────────────────────────────

def initial_state(
    run_id: str,
    city_name: str,
    country_or_region: str,
    operator_id: str,
    triggered_at: str,
) -> OSINTRunState:
    """
    Returns a fully initialized OSINTRunState with all fields set to
    their correct empty/default values. Pass this to the LangGraph
    graph when starting a new run.
    """
    city_key = f"{city_name.lower().replace(' ', '_')}_{country_or_region[:2].lower()}"

    return OSINTRunState(
        # Identity
        run_id=run_id,
        city_name=city_name,
        country_or_region=country_or_region,
        city_key=city_key,
        operator_id=operator_id,
        triggered_at=triggered_at,

        # Pipeline control
        run_status="pending",
        current_phase="INIT",
        pass_number=1,
        gate_cleared=False,

        # Orchestrator output
        scope_parameters={},
        framings=[],

        # Agent tracking
        agent_statuses={},
        agent_errors={},
        agent_entity_counts={},
        agent_token_counts={},

        # Phase 1 output
        raw_entities=[],
        raw_search_records=[],
        pipeline_agent_output={},

        # Gap analysis
        gap_analysis={},
        pass2_targets=[],

        # Resolution
        canonical_entities=[],
        merge_decisions=[],
        ambiguous_merges=[],

        # Enrichment
        enrichment_targets=[],
        enriched_entities=[],

        # Analytical
        relationships_draft=[],
        relationships_verified=[],
        relationships_rejected=[],
        scored_entities=[],
        ranked_lists={},

        # Synthesis
        verification_results={},
        verification_summary={},
        verified_entity_ids=[],
        flagged_entity_ids=[],
        briefing_json={},
        briefing_markdown="",

        # Operational
        rate_limit_state={},
        redis_cache_hits=0,
        proxycurl_spend_usd=0.0,
        total_tokens_in=0,
        total_tokens_out=0,
        warnings=[],
        errors=[],
    )
