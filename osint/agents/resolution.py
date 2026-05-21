"""
osint/agents/resolution.py

Entity Resolution Agent — second node in the analytical phase.

Resolves raw_entities (collected by all collection agents, possibly containing
duplicates across agents) into a deduplicated canonical_entities list.

3-Layer resolution per entity-type group:

  Layer 1 — Exact external ID match
    Any two entities sharing a value in any external_ids key
    (crunchbase_id, ein, fec_id, sec_cik, etc.) are auto-merged.
    Score = 1.0. Definitive — no further comparison needed.

  Layer 2 — Fuzzy name match (SequenceMatcher)
    Normalized name comparison. If ratio ≥ FUZZY_AUTO_MERGE_MIN (0.85),
    entities are auto-merged. Only compared within same entity_type.
    City or external ID agreement is used as tie-breaker when ratio is close.

  Layer 3 — Embedding similarity (ChromaDB / cosine)
    Entities not yet merged are embedded using nomic-embed-text.
    In-memory cosine similarity computed for all remaining pairs in the group.
      similarity ≥ 0.85  → auto-merge
      0.60 ≤ sim < 0.85  → ambiguous — held for human review
                            LLM (qwen3:22b) writes a recommendation
      similarity < 0.60  → auto-reject — different entities

Decisions written to:
  - DB rejected_items: all auto-rejects AND ambiguous merges
  - State canonical_entities: deduplicated entity set
  - State merge_decisions: every merge with layer + score
  - State ambiguous_merges: 0.60–0.84 pairs with LLM recommendation

Evidence backfill:
  Raw entities carry _pending_evidence lists (set by collection agents).
  After entity_id is assigned, these are written to entity_evidence table.
  This is the only place evidence DB writes happen — collection agents
  deliberately defer until entity_id is known.

ChromaDB writes:
  Every canonical entity is upserted to ChromaDB after DB write.
  This enables future cross-run resolution (delta runs).
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from difflib import SequenceMatcher
from math import sqrt
from typing import Any

from osint.agents.base import BaseAgent
from osint.core.config import RESOLUTION_THRESHOLDS
from osint.db.chromadb import ChromaDBClient

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

AGENT_NAME = "resolution_agent"
AGENT_VERSION = "1.0"

# Thresholds (sourced from config.RESOLUTION_THRESHOLDS)
FUZZY_AUTO_MERGE_MIN   = RESOLUTION_THRESHOLDS["fuzzy_auto_merge_min"]   # 0.85
EMBED_AUTO_MERGE_MIN   = RESOLUTION_THRESHOLDS["embedding_auto_merge_min"]  # 0.85
EMBED_HUMAN_REVIEW_MIN = RESOLUTION_THRESHOLDS["human_review_min"]       # 0.60
# Below EMBED_HUMAN_REVIEW_MIN = auto-reject

# Confidence ordering (higher index = higher confidence)
CONFIDENCE_RANK: dict[str, int] = {
    "high":   3,
    "medium": 2,
    "low":    1,
}

# Status priority for field merging — higher wins
# REPORTED wins over everything; NOT_COLLECTED is last resort
STATUS_PRIORITY: dict[str, int] = {
    "REPORTED":             5,
    "REPORTED_ABSENT":      4,
    "NOT_REPORTED":         3,
    "COLLECTED_UNREPORTED": 2,
    "NOT_COLLECTED":        1,
}

# External ID keys used for Layer 1 matching
EXTERNAL_ID_KEYS = [
    "crunchbase_id", "ein", "fec_candidate_id", "fec_committee_id",
    "sec_crd_number", "sec_cik", "opencorporates_id", "bioguide_id",
    "opensecrets_id", "wikidata_id",
]

# Boolean classification flags — True if ANY merged entity has True
OR_LOGIC_FLAGS = [
    "partner_candidate", "competitor_candidate", "blocker_candidate",
    "investment_candidate", "support_candidate", "recruiter_candidate",
    "top_influencer", "needs_review",
]

# LLM system prompt for arbitration
ARBITRATION_SYSTEM_PROMPT = (
    "You are an expert at entity disambiguation. "
    "You will receive two OSINT entity profiles and must determine if they "
    "represent the same real-world entity. "
    "Respond with valid JSON only — no markdown, no explanation outside the JSON."
)

ARBITRATION_PROMPT_TEMPLATE = """\
Determine if the following two {entity_type} entities are the same real-world entity.

ENTITY A:
{entity_a}

ENTITY B:
{entity_b}

Embedding similarity score: {similarity:.3f} (scale 0–1, where 1 = identical)

Respond with this JSON:
{{
  "are_same": true or false,
  "confidence": <integer 0-100>,
  "reasoning": "<one or two sentences explaining your reasoning>"
}}"""


# ─────────────────────────────────────────────────────────────────────────────
# Union-Find (path-compressed, index-based)
# ─────────────────────────────────────────────────────────────────────────────

class UnionFind:
    """
    Simple path-compressed union-find for tracking merge clusters.
    Entities are referenced by index (0-based position in entity list).
    """

    def __init__(self, n: int) -> None:
        self._parent = list(range(n))
        self._rank   = [0] * n

    def find(self, x: int) -> int:
        while self._parent[x] != x:
            self._parent[x] = self._parent[self._parent[x]]   # path compression
            x = self._parent[x]
        return x

    def union(self, x: int, y: int) -> bool:
        """Merge clusters of x and y. Returns True if they were in different clusters."""
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return False
        if self._rank[rx] < self._rank[ry]:
            rx, ry = ry, rx
        self._parent[ry] = rx
        if self._rank[rx] == self._rank[ry]:
            self._rank[rx] += 1
        return True

    def clusters(self, n: int) -> dict[int, list[int]]:
        """Return {root → [member_indices]} for all clusters."""
        groups: dict[int, list[int]] = defaultdict(list)
        for i in range(n):
            groups[self.find(i)].append(i)
        return dict(groups)


# ─────────────────────────────────────────────────────────────────────────────
# Helper: cosine similarity for in-memory float vectors
# ─────────────────────────────────────────────────────────────────────────────

def _cosine_similarity(a: list[float], b: list[float]) -> float:
    """Compute cosine similarity between two float vectors. Returns 0.0 on error."""
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    mag_a = sqrt(sum(x * x for x in a))
    mag_b = sqrt(sum(y * y for y in b))
    if mag_a == 0.0 or mag_b == 0.0:
        return 0.0
    return dot / (mag_a * mag_b)


# ─────────────────────────────────────────────────────────────────────────────
# Helper: normalize name for fuzzy comparison
# ─────────────────────────────────────────────────────────────────────────────

_FUZZY_STRIP = str.maketrans("", "", ".,;:'-&/")

def _normalize_name(name: str) -> str:
    """Lowercase, strip punctuation and excess whitespace."""
    return " ".join(name.lower().translate(_FUZZY_STRIP).split())


# ─────────────────────────────────────────────────────────────────────────────
# Helper: extract all external IDs from a raw entity
# ─────────────────────────────────────────────────────────────────────────────

def _get_external_ids(entity: dict[str, Any]) -> dict[str, str]:
    """
    Return a flat {key: value} dict of all non-empty external IDs.
    Reads from both entity["external_ids"] sub-dict and top-level keys.
    """
    ids: dict[str, str] = {}
    for key in EXTERNAL_ID_KEYS:
        # Check sub-dict first
        sub = entity.get("external_ids", {})
        val = sub.get(key) or entity.get(key)
        if val:
            ids[key] = str(val)
    return ids


# ─────────────────────────────────────────────────────────────────────────────
# Helper: merge two external_id dicts
# ─────────────────────────────────────────────────────────────────────────────

def _merge_external_ids(entities: list[dict[str, Any]]) -> dict[str, str]:
    merged: dict[str, str] = {}
    for e in entities:
        for k, v in _get_external_ids(e).items():
            if v and k not in merged:
                merged[k] = v
    return merged


# ─────────────────────────────────────────────────────────────────────────────
# Helper: entity summary for LLM arbitration prompt
# ─────────────────────────────────────────────────────────────────────────────

def _entity_summary(entity: dict[str, Any]) -> str:
    """Build a compact, human-readable summary of an entity for LLM prompting."""
    parts = [
        f"Name: {entity.get('canonical_name', entity.get('name', 'Unknown'))}",
        f"Type: {entity.get('entity_type', '?')} / {entity.get('entity_subtype', '?')}",
        f"City: {entity.get('primary_city', entity.get('city', '?'))}",
        f"Confidence: {entity.get('overall_confidence', '?')}",
    ]
    # Category fields snippet
    cat = entity.get("category_fields", {})
    if isinstance(cat, dict) and cat:
        # First 3 non-status keys
        shown = [(k, v) for k, v in cat.items() if not k.endswith("_status") and v][:3]
        for k, v in shown:
            parts.append(f"{k}: {str(v)[:100]}")
    # External IDs
    ext = _get_external_ids(entity)
    if ext:
        parts.append(f"IDs: {ext}")
    return "\n".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
# Canonical entity merge
# ─────────────────────────────────────────────────────────────────────────────

def _merge_entities(
    entities: list[dict[str, Any]],
    run_id: str,
) -> dict[str, Any]:
    """
    Merge N raw entities into one canonical entity.

    Strategy:
    - Scalar fields: highest-confidence source wins.
    - Boolean OR-logic flags: True if any source has True.
    - external_ids: union.
    - source_urls, aliases, merge_provenance: union.
    - category_fields: status-aware merge (REPORTED > REPORTED_ABSENT > NOT_REPORTED > ...).
    - sensitivity_tier: 'restricted' if any source has 'restricted'.
    - overall_confidence: max of all sources.
    """
    # Sort entities by confidence descending — highest confidence processed last
    # (we overwrite, so last write wins = highest confidence wins)
    sorted_ents = sorted(
        entities,
        key=lambda e: CONFIDENCE_RANK.get(e.get("overall_confidence", "low"), 1),
    )

    entity_id = str(uuid.uuid4())

    # ── Base identity ─────────────────────────────────────────────────────────
    # canonical_name: prefer highest confidence; use longest if tied
    canonical_name = ""
    for e in sorted_ents:
        n = e.get("canonical_name") or e.get("name") or e.get("org_name") or e.get("person_name") or ""
        if n and (not canonical_name or len(n) > len(canonical_name)):
            canonical_name = n

    entity_type    = entities[0]["entity_type"]
    entity_subtype = None
    for e in sorted_ents:
        if e.get("entity_subtype"):
            entity_subtype = e["entity_subtype"]

    # ── Confidence and review flags ───────────────────────────────────────────
    max_confidence = "low"
    for e in sorted_ents:
        ec = e.get("overall_confidence", "low")
        if CONFIDENCE_RANK.get(ec, 1) > CONFIDENCE_RANK.get(max_confidence, 1):
            max_confidence = ec

    # ── OR-logic flags ────────────────────────────────────────────────────────
    flag_values: dict[str, bool] = {f: False for f in OR_LOGIC_FLAGS}
    for e in sorted_ents:
        for flag in OR_LOGIC_FLAGS:
            if e.get(flag):
                flag_values[flag] = True

    # ── sensitivity_tier ──────────────────────────────────────────────────────
    sensitivity_tier = "standard"
    for e in sorted_ents:
        if e.get("sensitivity_tier") == "restricted":
            sensitivity_tier = "restricted"
            break

    # ── Location ──────────────────────────────────────────────────────────────
    # Prefer highest-confidence; status-aware
    primary_city  = _best_field_value(sorted_ents, "primary_city",  "city")
    primary_state = _best_field_value(sorted_ents, "primary_state", "state")
    primary_country = _best_field_value(sorted_ents, "primary_country", "country")

    # ── URLs ──────────────────────────────────────────────────────────────────
    website_url  = _best_field_value(sorted_ents, "website_url")
    linkedin_url = _best_field_value(sorted_ents, "linkedin_url")
    description  = _best_field_value(sorted_ents, "description")

    # ── Provenance ────────────────────────────────────────────────────────────
    all_source_urls: list[str] = []
    all_aliases: list[str] = []
    all_raw_ids: list[str] = []
    all_source_agents: list[str] = []

    for e in sorted_ents:
        urls = e.get("source_urls", [])
        if isinstance(urls, list):
            all_source_urls.extend(u for u in urls if u and u not in all_source_urls)

        als = e.get("aliases", [])
        if isinstance(als, list):
            all_aliases.extend(a for a in als if a and a not in all_aliases)

        raw_id = e.get("raw_entity_id") or e.get("entity_id")
        if raw_id and raw_id not in all_raw_ids:
            all_raw_ids.append(raw_id)

        agent = e.get("source_agent")
        if agent and agent not in all_source_agents:
            all_source_agents.append(agent)

    # ── External IDs ─────────────────────────────────────────────────────────
    merged_external_ids = _merge_external_ids(sorted_ents)

    # ── category_fields: status-aware deep merge ──────────────────────────────
    merged_category_fields = _merge_category_fields(sorted_ents)

    # ── Pending evidence accumulation ─────────────────────────────────────────
    # Each raw entity may have _pending_evidence — collected but not yet written
    # to DB because entity_id was unknown. We carry them forward; the caller
    # will write them after the entity_id is confirmed.
    pending_evidence: list[dict[str, Any]] = []
    for e in sorted_ents:
        pe = e.get("_pending_evidence", [])
        if isinstance(pe, list):
            pending_evidence.extend(pe)

    now = datetime.now(timezone.utc).isoformat()

    canonical: dict[str, Any] = {
        "entity_id":        entity_id,
        "canonical_name":   canonical_name,
        "entity_type":      entity_type,
        "entity_subtype":   entity_subtype,
        "aliases":          all_aliases,

        "primary_city":     primary_city,
        "primary_state":    primary_state,
        "primary_country":  primary_country,
        "website_url":      website_url,
        "linkedin_url":     linkedin_url,
        "description":      description,

        "external_ids":     merged_external_ids,
        "source_agent":     ", ".join(all_source_agents),
        "source_run_ids":   [run_id],
        "merge_provenance": all_raw_ids,
        "source_urls":      all_source_urls,

        "overall_confidence": max_confidence,
        "sensitivity_tier":   sensitivity_tier,
        "source_count":       len(sorted_ents),
        "corroboration_count": max(0, len(sorted_ents) - 1),

        "category_fields":  merged_category_fields,

        # OR-logic flags
        **flag_values,

        # Timestamps
        "valid_from": now,
        "last_seen":  now,

        # Internal — stripped before DB write
        "_pending_evidence": pending_evidence,
        "_raw_entity_count": len(sorted_ents),
    }

    return canonical


def _best_field_value(
    entities_sorted_by_confidence: list[dict[str, Any]],
    *field_names: str,
) -> Any:
    """
    Return the best available value for a field (or list of fallback field names).
    'Best' = REPORTED status first, then higher confidence.
    Entities already sorted ascending by confidence — last REPORTED value wins.
    """
    best = None
    for e in entities_sorted_by_confidence:
        for fn in field_names:
            val = e.get(fn)
            if val:
                status_key = f"{fn}_status"
                status = e.get(status_key, "NOT_COLLECTED")
                if status in ("REPORTED", "COLLECTED_UNREPORTED"):
                    best = val  # overwrite — ascending sort means higher conf overwrites
    return best


def _merge_category_fields(
    entities_sorted_by_confidence: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Merge category_fields across all raw entities in a cluster.

    For each key in category_fields:
    - A value with status REPORTED wins over NOT_COLLECTED
    - When both have REPORTED, higher-confidence entity's value wins
      (entities sorted ascending, so last REPORTED write wins)
    """
    merged: dict[str, Any] = {}

    for e in entities_sorted_by_confidence:
        cat = e.get("category_fields", {})
        if not isinstance(cat, dict):
            continue

        # Group keys into value keys and _status keys
        status_keys = {k for k in cat if k.endswith("_status")}
        value_keys  = {k for k in cat if not k.endswith("_status")}

        for k in value_keys:
            new_val    = cat.get(k)
            new_status = cat.get(f"{k}_status", "NOT_COLLECTED")
            new_prio   = STATUS_PRIORITY.get(new_status, 1)

            if k not in merged:
                merged[k] = new_val
                merged[f"{k}_status"] = new_status
            else:
                existing_status = merged.get(f"{k}_status", "NOT_COLLECTED")
                existing_prio   = STATUS_PRIORITY.get(existing_status, 1)

                if new_prio > existing_prio:
                    merged[k] = new_val
                    merged[f"{k}_status"] = new_status
                elif new_prio == existing_prio and new_val is not None:
                    # Same status priority — higher confidence entity (later in sort) wins
                    merged[k] = new_val

        # Preserve any _status-only keys that have no matching value key
        for sk in status_keys:
            base = sk[:-len("_status")]
            if base not in value_keys and sk not in merged:
                merged[sk] = cat[sk]

    return merged


# ─────────────────────────────────────────────────────────────────────────────
# Agent
# ─────────────────────────────────────────────────────────────────────────────

class ResolutionAgent(BaseAgent):
    """
    Entity Resolution Agent — deduplicates raw_entities into canonical_entities.
    Implements 3-layer resolution with union-find clustering.
    """

    AGENT_NAME = AGENT_NAME
    AGENT_VERSION = AGENT_VERSION

    async def run(self, state: dict[str, Any]) -> dict[str, Any]:
        run_id     = state["run_id"]
        city_key   = state.get("city_key", "")
        raw_entities: list[dict[str, Any]] = state.get("raw_entities", [])

        log.info(
            "resolution_agent: starting — %d raw entities to resolve",
            len(raw_entities),
        )

        if not raw_entities:
            log.warning("resolution_agent: no raw_entities found — returning empty canonical set")
            return self._empty_patch(state)

        # ── Assign raw_entity_id to any entity that lacks one ─────────────────
        for i, e in enumerate(raw_entities):
            if not e.get("raw_entity_id"):
                e["raw_entity_id"] = str(uuid.uuid4())

        # ── Group by entity_type ──────────────────────────────────────────────
        by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for e in raw_entities:
            etype = e.get("entity_type")
            if not etype:
                log.warning("resolution_agent: skipping entity with no entity_type: %s",
                            e.get("canonical_name", e.get("name", "<unnamed>")))
                continue
            by_type[etype].append(e)

        # ── Resolve each type group ───────────────────────────────────────────
        all_canonical:  list[dict[str, Any]] = []
        all_decisions:  list[dict[str, Any]] = []
        all_ambiguous:  list[dict[str, Any]] = []

        for entity_type, group in by_type.items():
            log.info(
                "resolution_agent: resolving '%s' group (%d entities)",
                entity_type, len(group),
            )
            canonicals, decisions, ambiguous = await self._resolve_group(
                group=group,
                entity_type=entity_type,
                run_id=run_id,
                city_key=city_key,
            )
            all_canonical.extend(canonicals)
            all_decisions.extend(decisions)
            all_ambiguous.extend(ambiguous)

        log.info(
            "resolution_agent: resolved %d raw → %d canonical (%d merges, %d ambiguous)",
            len(raw_entities), len(all_canonical), len(all_decisions), len(all_ambiguous),
        )

        # ── Write canonical entities to DB + backfill evidence ────────────────
        written_entities: list[dict[str, Any]] = []
        for canonical in all_canonical:
            pending_evidence = canonical.pop("_pending_evidence", [])
            canonical.pop("_raw_entity_count", None)

            try:
                entity_id = await self.write_entity(canonical)
                canonical["entity_id"] = entity_id
                written_entities.append(canonical)
            except Exception as exc:
                log.error(
                    "resolution_agent: failed to write canonical entity '%s': %s",
                    canonical.get("canonical_name"), exc,
                )
                continue

            # Backfill pending evidence with resolved entity_id
            if pending_evidence:
                await self._backfill_evidence(
                    entity_id=entity_id,
                    run_id=run_id,
                    pending_evidence=pending_evidence,
                )

        # ── Upsert canonical entity embeddings to ChromaDB ────────────────────
        await self._upsert_canonical_embeddings(
            canonicals=written_entities,
            run_id=run_id,
            city_key=city_key,
        )

        # ── Write ambiguous merges to DB as rejected_items ────────────────────
        for amb in all_ambiguous:
            try:
                await self.write_rejected_item(
                    stage="entity_resolution_layer3",
                    item_type="ambiguous_entity_pair",
                    item_snapshot=amb,
                    rejection_reason="ambiguous_merge_pending_review",
                    rejection_detail=(
                        f"Similarity={amb.get('similarity', 0):.3f} "
                        f"(0.60–0.84 range). LLM recommendation: "
                        f"{amb.get('llm_recommendation', {}).get('are_same', 'unknown')}"
                    ),
                )
            except Exception as exc:
                log.warning("resolution_agent: failed to write ambiguous merge record: %s", exc)

        # ── Write auto-merge decisions to DB as assessments ───────────────────
        for decision in all_decisions:
            try:
                await self.write_assessment({
                    "run_id":           run_id,
                    "agent_name":       self.AGENT_NAME,
                    "assessment_type":  "entity_resolution_decision",
                    "content":          decision,
                    "created_at":       self.now_iso(),
                })
            except Exception as exc:
                log.warning("resolution_agent: failed to write merge decision assessment: %s", exc)

        # ── Return state patch ────────────────────────────────────────────────
        return {
            "canonical_entities": written_entities,
            "merge_decisions":    all_decisions,
            "ambiguous_merges":   all_ambiguous,
            "current_phase":      "ENRICHMENT",
            **self.agent_status_patch(
                "success",
                state.get("agent_statuses", {}),
            ),
            **self.token_count_patch(
                state.get("total_tokens_in", 0),
                state.get("total_tokens_out", 0),
                state.get("agent_token_counts", {}),
            ),
            **self.entity_count_patch(state.get("agent_entity_counts", {})),
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Core: resolve one entity-type group
    # ─────────────────────────────────────────────────────────────────────────

    async def _resolve_group(
        self,
        group: list[dict[str, Any]],
        entity_type: str,
        run_id: str,
        city_key: str,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        """
        Run 3-layer resolution on a single entity-type group.

        Returns:
            (canonical_entities, merge_decisions, ambiguous_merges)
        """
        n = len(group)
        if n == 0:
            return [], [], []
        if n == 1:
            # Singleton — promote directly, no comparison needed
            canonical = _merge_entities(group, run_id)
            return [canonical], [], []

        uf = UnionFind(n)
        decisions: list[dict[str, Any]] = []
        ambiguous: list[dict[str, Any]] = []

        # ── Layer 1: Exact external ID match ──────────────────────────────────
        # Build inverted index: id_value → list of entity indices
        id_index: dict[str, list[int]] = defaultdict(list)
        for i, entity in enumerate(group):
            for key, val in _get_external_ids(entity).items():
                if val:
                    composite_key = f"{key}:{val}"
                    id_index[composite_key].append(i)

        for indices in id_index.values():
            if len(indices) < 2:
                continue
            # All entities sharing this ID value are the same entity
            for j in range(1, len(indices)):
                if uf.union(indices[0], indices[j]):
                    decisions.append({
                        "layer":       1,
                        "method":      "exact_id_match",
                        "score":       1.0,
                        "merged":      True,
                        "entity_a_id": group[indices[0]].get("raw_entity_id"),
                        "entity_b_id": group[indices[j]].get("raw_entity_id"),
                        "entity_a_name": _get_name(group[indices[0]]),
                        "entity_b_name": _get_name(group[indices[j]]),
                        "entity_type": entity_type,
                    })
                    log.debug(
                        "L1 MERGE: '%s' + '%s' (shared external ID)",
                        _get_name(group[indices[0]]),
                        _get_name(group[indices[j]]),
                    )

        # ── Layer 2: Fuzzy name match ──────────────────────────────────────────
        normalized = [_normalize_name(_get_name(e)) for e in group]

        for i in range(n):
            for j in range(i + 1, n):
                if uf.find(i) == uf.find(j):
                    continue  # Already in same cluster

                ratio = SequenceMatcher(None, normalized[i], normalized[j]).ratio()

                if ratio >= FUZZY_AUTO_MERGE_MIN:
                    uf.union(i, j)
                    decisions.append({
                        "layer":       2,
                        "method":      "fuzzy_name_match",
                        "score":       round(ratio, 4),
                        "merged":      True,
                        "entity_a_id": group[i].get("raw_entity_id"),
                        "entity_b_id": group[j].get("raw_entity_id"),
                        "entity_a_name": _get_name(group[i]),
                        "entity_b_name": _get_name(group[j]),
                        "entity_type": entity_type,
                    })
                    log.debug(
                        "L2 MERGE: '%s' + '%s' (ratio=%.3f)",
                        _get_name(group[i]), _get_name(group[j]), ratio,
                    )

        # ── Layer 3: Embedding similarity ─────────────────────────────────────
        # Only embed entities that have at least one unresolved pair to compare.
        # Pairs where both are already in the same cluster are skipped.
        unresolved_pairs = [
            (i, j)
            for i in range(n)
            for j in range(i + 1, n)
            if uf.find(i) != uf.find(j)
        ]

        if unresolved_pairs:
            # Generate embeddings for all entities in this group
            embeddings = await self._embed_all(group)

            for i, j in unresolved_pairs:
                if uf.find(i) == uf.find(j):
                    continue  # May have been merged by an earlier pair in this loop

                sim = _cosine_similarity(embeddings[i], embeddings[j])

                if sim >= EMBED_AUTO_MERGE_MIN:
                    uf.union(i, j)
                    decisions.append({
                        "layer":       3,
                        "method":      "embedding_similarity",
                        "score":       round(sim, 4),
                        "merged":      True,
                        "entity_a_id": group[i].get("raw_entity_id"),
                        "entity_b_id": group[j].get("raw_entity_id"),
                        "entity_a_name": _get_name(group[i]),
                        "entity_b_name": _get_name(group[j]),
                        "entity_type": entity_type,
                    })
                    log.debug(
                        "L3 MERGE: '%s' + '%s' (sim=%.3f)",
                        _get_name(group[i]), _get_name(group[j]), sim,
                    )

                elif sim >= EMBED_HUMAN_REVIEW_MIN:
                    # Ambiguous — get LLM recommendation
                    llm_rec = await self._arbitrate_ambiguous(
                        entity_a=group[i],
                        entity_b=group[j],
                        similarity=sim,
                        entity_type=entity_type,
                    )
                    ambiguous_record = {
                        "layer":       3,
                        "method":      "embedding_similarity",
                        "score":       round(sim, 4),
                        "merged":      False,
                        "entity_a_id": group[i].get("raw_entity_id"),
                        "entity_b_id": group[j].get("raw_entity_id"),
                        "entity_a_name": _get_name(group[i]),
                        "entity_b_name": _get_name(group[j]),
                        "entity_type": entity_type,
                        "entity_a_snapshot": _slim_snapshot(group[i]),
                        "entity_b_snapshot": _slim_snapshot(group[j]),
                        "llm_recommendation": llm_rec,
                        "run_id": run_id,
                        "flagged_at": datetime.now(timezone.utc).isoformat(),
                    }
                    ambiguous.append(ambiguous_record)
                    log.info(
                        "L3 AMBIGUOUS: '%s' vs '%s' (sim=%.3f) — held for human review",
                        _get_name(group[i]), _get_name(group[j]), sim,
                    )

                else:
                    # Auto-reject — similarity too low, definitely different entities
                    log.debug(
                        "L3 REJECT: '%s' vs '%s' (sim=%.3f) — different entities",
                        _get_name(group[i]), _get_name(group[j]), sim,
                    )

        # ── Build canonical entities from clusters ────────────────────────────
        clusters = uf.clusters(n)
        canonicals: list[dict[str, Any]] = []

        for _root, member_indices in clusters.items():
            cluster_entities = [group[i] for i in member_indices]
            canonical = _merge_entities(cluster_entities, run_id)
            canonicals.append(canonical)

        log.info(
            "resolution_agent: '%s' → %d clusters from %d raw "
            "(%d L1 merges, %d L2 merges, %d ambiguous)",
            entity_type,
            len(canonicals),
            n,
            sum(1 for d in decisions if d["layer"] == 1),
            sum(1 for d in decisions if d["layer"] == 2),
            len(ambiguous),
        )

        return canonicals, decisions, ambiguous

    # ─────────────────────────────────────────────────────────────────────────
    # Embedding helpers
    # ─────────────────────────────────────────────────────────────────────────

    async def _embed_all(
        self, entities: list[dict[str, Any]]
    ) -> list[list[float]]:
        """
        Generate embeddings for all entities concurrently.
        Returns list of float vectors in same order as input.
        Falls back to empty vector on per-entity failure.
        """
        embed_texts = [ChromaDBClient.build_embed_text(e) for e in entities]
        tasks = [self.embed(text) for text in embed_texts]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        embeddings: list[list[float]] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                log.warning(
                    "resolution_agent: embed failed for '%s': %s",
                    _get_name(entities[i]), result,
                )
                embeddings.append([])
            else:
                embeddings.append(result)

        return embeddings

    async def _upsert_canonical_embeddings(
        self,
        canonicals: list[dict[str, Any]],
        run_id: str,
        city_key: str,
    ) -> None:
        """
        Upsert canonical entity embeddings to ChromaDB for future cross-run resolution.
        Runs as a batch. Failures are logged but non-fatal.
        """
        if not canonicals:
            return

        entity_ids: list[str] = []
        embed_texts: list[str] = []
        embeddings:  list[list[float]] = []
        metadatas:   list[dict[str, Any]] = []

        # Embed all canonicals concurrently
        texts = [ChromaDBClient.build_embed_text(e) for e in canonicals]
        embed_results = await asyncio.gather(
            *[self.embed(t) for t in texts], return_exceptions=True
        )

        for i, canonical in enumerate(canonicals):
            entity_id = canonical.get("entity_id")
            if not entity_id:
                continue
            result = embed_results[i]
            if isinstance(result, Exception):
                log.warning(
                    "resolution_agent: failed to embed canonical '%s' for ChromaDB: %s",
                    canonical.get("canonical_name"), result,
                )
                continue

            entity_ids.append(entity_id)
            embed_texts.append(texts[i])
            embeddings.append(result)
            metadatas.append({
                "entity_id":     entity_id,
                "entity_type":   canonical.get("entity_type", ""),
                "city_key":      city_key,
                "run_id":        run_id,
                "canonical_name": canonical.get("canonical_name", ""),
            })

        if entity_ids:
            try:
                await self._chroma.upsert_entities_batch(
                    entity_ids=entity_ids,
                    embed_texts=embed_texts,
                    embeddings=embeddings,
                    metadatas=metadatas,
                )
                log.info(
                    "resolution_agent: upserted %d canonical embeddings to ChromaDB",
                    len(entity_ids),
                )
            except Exception as exc:
                log.error(
                    "resolution_agent: ChromaDB batch upsert failed: %s", exc
                )

    # ─────────────────────────────────────────────────────────────────────────
    # LLM arbitration for ambiguous pairs (Layer 3, 0.60–0.84)
    # ─────────────────────────────────────────────────────────────────────────

    async def _arbitrate_ambiguous(
        self,
        entity_a: dict[str, Any],
        entity_b: dict[str, Any],
        similarity: float,
        entity_type: str,
    ) -> dict[str, Any]:
        """
        Call qwen3:22b to provide a recommendation on an ambiguous entity pair.
        Returns LLM recommendation dict. On any failure, returns {}.
        The recommendation is advisory only — the human reviewer makes the final call.
        """
        prompt = ARBITRATION_PROMPT_TEMPLATE.format(
            entity_type=entity_type,
            entity_a=_entity_summary(entity_a),
            entity_b=_entity_summary(entity_b),
            similarity=similarity,
        )

        try:
            result, _meta = await self.llm_generate_json(
                task_type="entity_resolution_arbitration",
                prompt=prompt,
                system=ARBITRATION_SYSTEM_PROMPT,
            )
        except Exception as exc:
            log.warning(
                "resolution_agent: LLM arbitration failed for '%s' vs '%s': %s",
                _get_name(entity_a), _get_name(entity_b), exc,
            )
            return {}

        if not isinstance(result, dict):
            return {}

        # Validate expected fields
        return {
            "are_same":   bool(result.get("are_same", False)),
            "confidence": int(result.get("confidence", 0)),
            "reasoning":  str(result.get("reasoning", ""))[:500],
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Evidence backfill
    # ─────────────────────────────────────────────────────────────────────────

    async def _backfill_evidence(
        self,
        entity_id: str,
        run_id: str,
        pending_evidence: list[dict[str, Any]],
    ) -> None:
        """
        Write pending_evidence records to DB now that entity_id is known.
        Collection agents defer evidence writes to avoid writing orphan records
        (evidence must reference a valid entity_id).

        Failures are logged but non-fatal — evidence loss is recoverable
        in a future enrichment run.
        """
        if not pending_evidence:
            return

        records: list[dict[str, Any]] = []
        for ev in pending_evidence:
            record = dict(ev)
            record["entity_id"] = entity_id
            record["run_id"]    = run_id
            if "link_id" not in record:
                record["link_id"] = str(uuid.uuid4())
            records.append(record)

        try:
            await self.write_evidence_batch(records)
            log.debug(
                "resolution_agent: wrote %d evidence records for entity %s",
                len(records), entity_id,
            )
        except Exception as exc:
            log.error(
                "resolution_agent: evidence backfill failed for entity %s: %s",
                entity_id, exc,
            )

    # ─────────────────────────────────────────────────────────────────────────
    # Utility
    # ─────────────────────────────────────────────────────────────────────────

    def _empty_patch(self, state: dict[str, Any]) -> dict[str, Any]:
        return {
            "canonical_entities": [],
            "merge_decisions":    [],
            "ambiguous_merges":   [],
            "current_phase":      "ENRICHMENT",
            **self.agent_status_patch("success", state.get("agent_statuses", {})),
            **self.token_count_patch(
                state.get("total_tokens_in", 0),
                state.get("total_tokens_out", 0),
                state.get("agent_token_counts", {}),
            ),
            **self.entity_count_patch(state.get("agent_entity_counts", {})),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Module-level helpers (used inside and outside the class)
# ─────────────────────────────────────────────────────────────────────────────

def _get_name(entity: dict[str, Any]) -> str:
    """Return the best available name for an entity (for logs and prompts)."""
    return (
        entity.get("canonical_name")
        or entity.get("name")
        or entity.get("org_name")
        or entity.get("person_name")
        or entity.get("raw_entity_id", "unknown")
    )


def _slim_snapshot(entity: dict[str, Any]) -> dict[str, Any]:
    """
    Return a trimmed entity snapshot for storage in ambiguous_merges.
    Strips _pending_evidence (can be large) and keeps only key fields.
    """
    KEEP_KEYS = {
        "raw_entity_id", "entity_id", "canonical_name", "name", "entity_type",
        "entity_subtype", "primary_city", "city", "primary_state", "state",
        "overall_confidence", "sensitivity_tier", "external_ids", "source_agent",
        "source_urls", "website_url", "linkedin_url",
    }
    return {k: v for k, v in entity.items() if k in KEEP_KEYS}
