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
import operator
from typing import Annotated, TypedDict, Literal, Optional, Any


# ─────────────────────────────────────────────────────────────────────────────
# Reducer helpers — used by Annotated field declarations below
# ─────────────────────────────────────────────────────────────────────────────

def _merge_dicts(a: dict, b: dict) -> dict:
    """
    LangGraph reducer for dict fields written by concurrent agents.
    b's keys overwrite a's — each agent only writes its own AGENT_NAME key,
    so there is no conflict between parallel nodes.
    """
    return {**a, **b}


def _add_int(a: int, b: int) -> int:
    """LangGraph reducer for integer accumulator fields."""
    return a + b


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
    state_abbr: str                      # Two-letter US state abbreviation, e.g., "TX", "PA"
    operator_id: str                     # user_id who triggered this run
    triggered_at: str                    # ISO8601 timestamp

    # ─── Pipeline Control ─────────────────────────────────────────────────────
    run_status: Literal["pending", "running", "complete", "failed", "partial"]
    run_mode: Literal["full", "enrichment_refresh", "discovery_pass"]
    # run_mode controls graph routing:
    #   "full"               — full collection → analytical pipeline (default)
    #   "enrichment_refresh" — skip collection, load existing DB entities, re-enrich
    #   "discovery_pass"     — full collection (same as "full", aliased for clarity)
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
    # These four dicts are written by ALL parallel collection agents simultaneously.
    # _merge_dicts reducer merges partial dicts ({agent_name: value}) from each
    # concurrent node so LangGraph doesn't raise INVALID_CONCURRENT_GRAPH_UPDATE.
    # Each agent MUST return only its own key (delta), not the full merged dict.
    agent_statuses: Annotated[dict[str, str], _merge_dicts]       # agent_name → status
    agent_errors: Annotated[dict[str, str], _merge_dicts]         # agent_name → error message
    agent_entity_counts: Annotated[dict[str, int], _merge_dicts]  # agent_name → entity count
    agent_token_counts: Annotated[dict[str, int], _merge_dicts]   # agent_name → total tokens

    # ─── Phase 1 Output: Raw Collection ───────────────────────────────────────
    # All raw_entities are pre-resolution — multiple agents may have extracted
    # the same entity under different names. Resolution de-duplicates.
    # operator.add reducer concatenates each agent's contribution list.
    # Agents MUST return only their NEW entities (delta), not existing + new.
    raw_entities: Annotated[list[dict[str, Any]], operator.add]          # All extracted entities
    raw_search_records: Annotated[list[dict[str, Any]], operator.add]    # All search attempts
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

    # ─── Phase 2.5 Output: Deduplication (dedup_agent) ────────────────────────
    dedup_merges: list[dict[str, Any]]          # Audit log: {primary_id, merged_ids, bucket, …}

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
    redis_cache_hits: Annotated[int, _add_int]
    proxycurl_spend_usd: float           # Running cost — agent stops if > budget
    total_tokens_in: Annotated[int, _add_int]    # Aggregate across all LLM calls this run
    total_tokens_out: Annotated[int, _add_int]
    warnings: Annotated[list[str], operator.add]  # Non-fatal issues
    errors: Annotated[list[str], operator.add]    # Any error here blocks final report


# ─────────────────────────────────────────────────────────────────────────────
# State factory — creates a clean initial state for a new run
# ─────────────────────────────────────────────────────────────────────────────

def initial_state(
    run_id: str,
    city_name: str,
    country_or_region: str,
    operator_id: str,
    triggered_at: str,
    state_abbr: str = "",
    run_mode: str = "full",
) -> OSINTRunState:
    """
    Returns a fully initialized OSINTRunState with all fields set to
    their correct empty/default values. Pass this to the LangGraph
    graph when starting a new run.
    """
    # Extract 2-letter country code from full country name.
    # Explicit lookup handles multi-word names ("United States" → "us").
    # Falls back to first 2 chars of first word for single-word countries.
    _COUNTRY_CODE_MAP: dict[str, str] = {
        "united states": "us", "united kingdom": "uk", "united arab emirates": "ae",
        "south korea": "kr",   "new zealand": "nz",    "south africa": "za",
        "canada": "ca",        "australia": "au",       "germany": "de",
        "france": "fr",        "japan": "jp",           "china": "cn",
        "india": "in",         "brazil": "br",          "mexico": "mx",
        "singapore": "sg",     "israel": "il",          "sweden": "se",
        "netherlands": "nl",   "switzerland": "ch",     "spain": "es",
        "italy": "it",         "portugal": "pt",        "denmark": "dk",
        "finland": "fi",       "norway": "no",          "austria": "at",
        "belgium": "be",       "ireland": "ie",         "poland": "pl",
    }
    _country_key  = country_or_region.strip().lower()
    _country_code = _COUNTRY_CODE_MAP.get(_country_key, _country_key[:2])
    city_key = f"{city_name.lower().replace(' ', '_')}_{_country_code}"

    # Auto-detect US state from major city names if state_abbr not provided
    if not state_abbr and _country_code == "us":
        _CITY_STATE_MAP: dict[str, str] = {
            "philadelphia": "PA", "pittsburgh": "PA", "allentown": "PA",
            "new york": "NY", "new york city": "NY", "nyc": "NY", "brooklyn": "NY",
            "los angeles": "CA", "san francisco": "CA", "san jose": "CA",
            "san diego": "CA", "oakland": "CA", "sacramento": "CA",
            "chicago": "IL",    "houston": "TX",   "dallas": "TX",
            "austin": "TX",     "san antonio": "TX",
            "phoenix": "AZ",    "seattle": "WA",   "denver": "CO",
            "boston": "MA",     "atlanta": "GA",   "miami": "FL",
            "orlando": "FL",    "tampa": "FL",     "minneapolis": "MN",
            "detroit": "MI",    "columbus": "OH",  "cleveland": "OH",
            "portland": "OR",   "las vegas": "NV", "nashville": "TN",
            "memphis": "TN",    "baltimore": "MD", "washington": "DC",
            "dc": "DC",         "charlotte": "NC", "raleigh": "NC",
            "indianapolis": "IN", "louisville": "KY", "kansas city": "MO",
            "st. louis": "MO",  "salt lake city": "UT", "richmond": "VA",
            "virginia beach": "VA", "new orleans": "LA", "baton rouge": "LA",
            "oklahoma city": "OK", "albuquerque": "NM", "tucson": "AZ",
            "milwaukee": "WI",  "buffalo": "NY",   "hartford": "CT",
            "new haven": "CT",  "providence": "RI",
        }
        state_abbr = _CITY_STATE_MAP.get(city_name.strip().lower(), "")

    return OSINTRunState(
        # Identity
        run_id=run_id,
        city_name=city_name,
        country_or_region=country_or_region,
        city_key=city_key,
        state_abbr=state_abbr,
        operator_id=operator_id,
        triggered_at=triggered_at,

        # Pipeline control
        run_status="pending",
        run_mode=run_mode,
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

        # Deduplication
        dedup_merges=[],

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
