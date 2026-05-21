"""
osint/schemas/records.py

TypedDicts for the non-entity record types:
- EvidenceRecord     → entity_evidence table
- AnalyticalAssessment → analytical_assessments table
- OsintSearchRecord  → osint_search_records table
- AgentOutput        → agent_outputs table
- RejectedItem       → rejected_items table

These represent the audit trail and operational records.
Evidence and analytical records must never be mixed — see OSINT_Schema_Spec.md Part 1.2.
"""

from __future__ import annotations
from typing import TypedDict, Literal

Confidence = Literal["high", "medium", "low"]


# ─────────────────────────────────────────────────────────────────────────────
# Evidence Record
# INTELLIGENCE_RECORD class: sourced fact traceable to a specific URL.
# No inference permitted — that goes in AnalyticalAssessment.
# Matches entity_evidence table.
# ─────────────────────────────────────────────────────────────────────────────

class EvidenceRecord(TypedDict, total=False):
    link_id: str                        # UUID primary key
    entity_id: str                      # FK → entities.entity_id
    run_id: str                         # FK → agent_runs.run_id

    # What this record supports
    supported_field: str                # Field name on entity (e.g., "aum_usd", "description")
    supported_value: str                # String representation of the supported value

    # Source (ALL required — no evidence without a URL)
    source_url: str                     # Must be non-null. Archived URL preferred.
    source_type: Literal[
        "api_response",
        "pdf_document",
        "web_page",
        "news_article",
        "regulatory_filing",
        "database_record",
    ]
    source_api: str | None              # Which API domain provided this (e.g., "crunchbase")
    archived_url: str | None            # Wayback Machine / archive.ph URL if available
    sha256_hash: str | None             # Hash of source document for tamper detection

    # Retrieval
    retrieved_at: str                   # ISO8601 timestamp of retrieval

    # Snippet (required — what in the source supports this value)
    evidence_snippet: str               # Direct quote or exact excerpt from source

    # Classification
    claim_type: Literal[
        "direct_statement",             # Source directly states this value
        "inferred",                     # LLM extracted from context (use sparingly)
        "computed",                     # Derived from math on other REPORTED values
    ]
    confidence: Confidence

    # Provenance
    agent_name: str                     # Agent that produced this record
    prompt_version: str | None          # Prompt version used for extraction


# ─────────────────────────────────────────────────────────────────────────────
# Analytical Assessment
# ANALYTICAL_ASSESSMENT class: labeled LLM inference.
# Must carry: claim text, framework, derived_from (evidence IDs), model, confidence.
# Cannot contain raw sourced data — that goes in EvidenceRecord.
# Matches analytical_assessments table.
# ─────────────────────────────────────────────────────────────────────────────

class AnalyticalAssessment(TypedDict, total=False):
    assessment_id: str                  # UUID primary key
    entity_id: str                      # FK → entities.entity_id
    run_id: str                         # FK → agent_runs.run_id

    # Type of assessment
    assessment_type: Literal[
        "score_rationale",              # Justification for a dimension score
        "relationship_inference",       # Why a relationship edge was inferred
        "briefing_claim",               # Claim in the intelligence brief
        "entity_resolution_decision",   # Why two entities were/weren't merged
        "gap_analysis",                 # Coverage gap identified for this entity
        "framing",                      # Perspective framing (Orchestrator Agent)
    ]

    # The claim
    claim_text: str                     # Human-readable assertion
    claim_json: dict | None             # Structured form if applicable (e.g., score breakdown)

    # Framework (identifies prompt version producing this)
    framework_name: str                 # e.g., "influence_scoring_v1", "relationship_inference_v2"
    framework_version: str

    # Derivation (what evidence this is based on — mandatory for traceability)
    derived_from: list[str]             # entity_evidence.link_id references

    # Model provenance
    model_used: str                     # e.g., "qwen3:14b"
    prompt_version: str

    # Quality
    confidence: Confidence
    needs_review: bool                  # Flagged for human review

    # Review tracking
    reviewed_by: str | None
    reviewed_at: str | None
    review_outcome: str | None          # "approved" | "rejected" | "revised"

    # Temporal (for score rationales that can change across runs)
    superseded_by: str | None           # assessment_id of newer version
    is_current: bool                    # False if a newer version exists


# ─────────────────────────────────────────────────────────────────────────────
# OSINT Search Record
# Every search attempt logged, regardless of outcome.
# Proof-of-search audit trail.
# Matches osint_search_records table.
# ─────────────────────────────────────────────────────────────────────────────

class OsintSearchRecord(TypedDict, total=False):
    search_id: str                      # UUID primary key
    run_id: str                         # FK → agent_runs.run_id

    # What was searched
    agent_name: str
    entity_type: str                    # EntityType enum value
    entity_id: str | None               # Populated if search was for a known entity
    raw_entity_id: str | None           # Populated if search produced a new candidate

    # Search details
    source_searched: str                # Domain key (e.g., "crunchbase", "sec_edgar")
    query_used: str                     # Exact query string or API params (JSON-serialized)
    search_framing: str | None          # Framing context if Pass 2 targeted search

    # Outcome
    result_found: bool                  # True if any result returned
    result_count: int | None            # How many results
    failure_reason: str | None          # If result_found=False, why

    # HTTP metadata (for debugging and rate limit monitoring)
    http_status_code: int | None
    response_time_ms: int | None
    served_from_cache: bool
    cache_key: str | None

    # Timestamp
    timestamp: str                      # ISO8601


# ─────────────────────────────────────────────────────────────────────────────
# Agent Output
# One record per agent execution per run.
# Matches agent_outputs table.
# ─────────────────────────────────────────────────────────────────────────────

class AgentOutput(TypedDict, total=False):
    output_id: str                      # UUID primary key
    run_id: str                         # FK → agent_runs.run_id

    agent_name: str
    agent_status: Literal[
        "pending", "running", "success", "error", "timeout"
    ]
    error_message: str | None           # If agent_status is "error"

    # Model provenance
    model_used: str | None
    prompt_version: str | None

    # Resource usage
    tokens_in: int
    tokens_out: int
    llm_call_count: int
    latency_ms: int | None

    # API usage
    api_calls_made: int
    api_calls_cached: int               # How many were served from Redis cache

    # Production counts
    entities_produced: int
    relationships_produced: int
    items_rejected: int

    # Debug
    output_snapshot_path: str | None    # Path to full output JSON if archived

    # Timing
    started_at: str                     # ISO8601
    completed_at: str | None            # ISO8601


# ─────────────────────────────────────────────────────────────────────────────
# Rejected Item
# Everything rejected at any pipeline stage, for any reason.
# Never delete data — rejection is itself an audit trail entry.
# Matches rejected_items table.
# ─────────────────────────────────────────────────────────────────────────────

class RejectedItem(TypedDict, total=False):
    rejection_id: str                   # UUID primary key
    run_id: str                         # FK → agent_runs.run_id

    agent_name: str
    stage: Literal[
        "extraction",
        "resolution",
        "enrichment",
        "relationship",
        "scoring",
        "verification",
    ]
    item_type: Literal[
        "entity",
        "relationship",
        "enrichment",
        "claim",
        "merge_decision",
    ]

    item_id: str | None                 # UUID if the item had one before rejection
    item_snapshot: dict                 # Full JSON snapshot at time of rejection (REQUIRED)

    rejection_reason: str               # Short code (e.g., "no_evidence", "below_threshold", "confidence_too_low")
    rejection_detail: str | None        # Human-readable explanation

    timestamp: str                      # ISO8601


# ─────────────────────────────────────────────────────────────────────────────
# Run Record
# Top-level run metadata.
# Matches agent_runs table.
# ─────────────────────────────────────────────────────────────────────────────

class RunRecord(TypedDict, total=False):
    run_id: str                         # UUID primary key
    city_name: str
    country_or_region: str
    city_key: str                       # Normalized: "austin_us"
    operator_id: str                    # User who triggered the run

    run_status: Literal["pending", "running", "complete", "failed", "partial"]
    current_phase: str

    # Models used
    orchestrator_model: str | None
    extraction_model: str | None
    resolution_model: str | None
    briefing_model: str | None

    # Result summary (populated on completion)
    entities_total: int
    entities_by_type: dict[str, int]    # entity_type → count
    relationships_total: int
    items_rejected_total: int
    api_calls_total: int
    api_calls_cached: int
    proxycurl_spend_usd: float
    tokens_in_total: int
    tokens_out_total: int

    # Timing
    triggered_at: str                   # ISO8601
    started_at: str | None
    completed_at: str | None
    duration_seconds: int | None

    # Trigger info
    trigger_source: str | None          # "api" | "scheduler" | "manual"
    previous_run_id: str | None         # For incremental runs
