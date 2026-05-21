"""
osint/schemas/relationships.py

TypedDict definitions for relationship edges and merge decisions.
All 18 relationship types are defined here.

Rules:
- Every edge requires minimum 1 evidence_id. No evidence = rejection.
- No self-loops (source_entity_id != target_entity_id).
- direction must match the relationship_type (see DIRECTION_MAP below).
"""

from __future__ import annotations
from typing import TypedDict, Literal


Confidence = Literal["high", "medium", "low"]


# ─────────────────────────────────────────────────────────────────────────────
# Relationship type literals (all 18 from RelationshipType enum)
# ─────────────────────────────────────────────────────────────────────────────

RelationshipTypeLiteral = Literal[
    "INVESTED_IN",
    "CO_INVESTED_WITH",
    "SITS_ON_BOARD_OF",
    "EMPLOYED_BY",
    "FOUNDED",
    "ADVISED_BY",
    "FUNDED_BY",
    "DONATED_TO",
    "RECEIVED_GRANT_FROM",
    "AWARDED_CONTRACT_TO",
    "POLITICALLY_CONNECTED_TO",
    "ALUMNI_OF",
    "CO_FOUNDED_WITH",
    "SUBSIDIARY_OF",
    "MENTIONED_WITH",
    "REGULATORY_OVERSIGHT",
    "LITIGATION_AGAINST",
    "PEER_INVESTOR_IN",
]

# Canonical direction for each type
# "directed" = asymmetric (source → target matters)
# "undirected" = symmetric (order doesn't matter)
RELATIONSHIP_DIRECTION: dict[str, str] = {
    "INVESTED_IN":              "directed",
    "CO_INVESTED_WITH":         "undirected",
    "SITS_ON_BOARD_OF":         "directed",
    "EMPLOYED_BY":              "directed",
    "FOUNDED":                  "directed",
    "ADVISED_BY":               "directed",
    "FUNDED_BY":                "directed",
    "DONATED_TO":               "directed",
    "RECEIVED_GRANT_FROM":      "directed",
    "AWARDED_CONTRACT_TO":      "directed",
    "POLITICALLY_CONNECTED_TO": "undirected",
    "ALUMNI_OF":                "directed",
    "CO_FOUNDED_WITH":          "undirected",
    "SUBSIDIARY_OF":            "directed",
    "MENTIONED_WITH":           "undirected",
    "REGULATORY_OVERSIGHT":     "directed",
    "LITIGATION_AGAINST":       "directed",
    "PEER_INVESTOR_IN":         "undirected",
}


# ─────────────────────────────────────────────────────────────────────────────
# Relationship Edge
# Matches the `relationships` table in 001_initial_schema.sql
# ─────────────────────────────────────────────────────────────────────────────

class RelationshipEdge(TypedDict, total=False):
    # Identity
    relationship_id: str                # UUID
    run_id: str                         # Run that produced this edge

    # Endpoints
    source_entity_id: str               # UUID of source entity (must exist in entities table)
    target_entity_id: str               # UUID of target entity (must exist in entities table)

    # Type
    relationship_type: RelationshipTypeLiteral
    direction: Literal["directed", "undirected"]

    # Evidence (MANDATORY — minimum 1 item, rejection without)
    evidence_ids: list[str]             # entity_evidence.link_id references
    evidence_snippets: list[str]        # Human-readable summary per evidence item

    # Quality
    confidence: Confidence
    relationship_strength: float        # 0.0–1.0, computed by Relationship Agent

    # Flags
    sensitive_claim: bool               # True if involves illicit entity or sensitive inference
    verified: bool                      # True after Verification Agent sign-off
    verified_at: str | None             # ISO8601

    # Temporal
    valid_from: str | None              # ISO8601 date when relationship began
    valid_to: str | None                # ISO8601 date when relationship ended (None = ongoing)

    # Sync
    neo4j_synced: bool                  # True after Neo4j upsert
    neo4j_synced_at: str | None

    # Provenance
    inference_model: str | None         # Model that inferred this relationship (if not direct)
    inference_prompt_version: str | None
    agent_name: str                     # Which agent produced this


# ─────────────────────────────────────────────────────────────────────────────
# Merge Decision
# Records every entity resolution decision (Layer 1, 2, or 3).
# Matches merge_decisions in OSINTRunState.
# ─────────────────────────────────────────────────────────────────────────────

class MergeDecision(TypedDict, total=False):
    decision_id: str                    # UUID
    run_id: str
    layer: int                          # 1 = exact ID, 2 = fuzzy name, 3 = embedding
    decision: Literal["merge", "reject", "human_review"]
    raw_entity_ids: list[str]           # The candidate entities being considered
    canonical_entity_id: str | None     # The entity they merged into (None if rejected)
    similarity_score: float | None      # Layer 2/3 score (0.0–1.0)
    match_reason: str                   # Human-readable explanation
    model_used: str | None              # Model used for Layer 3 arbitration
    review_note: str | None             # Note for human reviewer (if human_review)
    decided_at: str                     # ISO8601 timestamp


# ─────────────────────────────────────────────────────────────────────────────
# Framing Object
# 4 perspectives generated by Orchestrator Agent before collection begins.
# Matches framings field in OSINTRunState.
# ─────────────────────────────────────────────────────────────────────────────

class FramingObject(TypedDict, total=False):
    framing_id: str                     # UUID
    run_id: str
    framing_type: Literal[
        "mainstream",
        "heterodox",
        "adjacent_domain",
        "practitioner",
    ]
    framing_label: str                  # Short name for this perspective
    framing_description: str            # Narrative — what lens this applies
    entities_to_prioritize: list[str]   # Entity types to weight in this framing
    search_angle: str                   # What to look for under this framing
    model_used: str
    prompt_version: str
    generated_at: str                   # ISO8601
