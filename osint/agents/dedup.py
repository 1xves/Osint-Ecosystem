"""
osint/agents/dedup.py

Phase 11.1 — Entity Deduplication Agent.

Position in pipeline:
    enrichment_agent → [dedup_agent] → relationship_agent

Problem this solves:
    Resolution feeds canonical entities that may still contain near-duplicates:
    entities where the same real-world person or company resolved to two or more
    dict records with slightly different canonical_name spellings (e.g. from
    different source documents: "Goldman Sachs", "Goldman, Sachs & Co.", or
    "John Smith" vs "John H. Smith").

    The relationship agent is expensive — it queries LittleSis, runs LLM
    inference passes, and does graph traversals. Feeding it duplicate entities
    multiplies work and produces redundant edges that corrupt the graph.

What this agent does:
    1. Clusters entities by entity_type bucket first (person types together,
       corporate types together — we never merge a person into a company).
    2. Within each bucket, uses SequenceMatcher ratio ≥ DEDUP_THRESHOLD (0.92)
       to identify likely duplicates.
    3. For each cluster of duplicates, elects a PRIMARY (highest overall_confidence,
       falling back to most enriched category_fields, then first in list order).
    4. Merges category_fields from all duplicates into the primary — union of
       keys; primary's value wins on conflict (never overwrite populated data
       with empty data from a duplicate).
    5. Logs every merge at INFO level: source entity names + IDs.
    6. Returns deduplicated entity list as enriched_entities in state.

Design constraints:
    - Pure in-memory — no DB writes, no LLM calls, no API calls.
    - O(n²) within each bucket — acceptable for typical runs of 50-200 entities.
    - Threshold 0.92 is intentionally high: false negatives (misses) are safer
      than false positives (incorrect merges). Two entities that sound similar
      but are distinct (e.g. "First National Bank" in two cities) should NOT merge.
    - Never merges across entity_type buckets (see DEDUP_BUCKETS).

State consumed:
    enriched_entities: list[dict]  — from enrichment_agent

State produced:
    enriched_entities: list[dict]  — deduplicated
    dedup_merges:      list[dict]  — audit log of merges performed
"""

from __future__ import annotations

import logging
from difflib import SequenceMatcher
from typing import Any

from osint.agents.base import BaseAgent

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

# Minimum name similarity to consider two entities duplicates.
# High threshold — prefer false negatives over false positives.
DEDUP_THRESHOLD = 0.92

# Entity types are grouped into buckets.  Only entities in the SAME bucket
# are compared against each other.  Cross-bucket merges are never permitted.
#
# Rationale: "Goldman Sachs Foundation" (philanthropic) and "Goldman Sachs"
# (corporate) may score 0.93 similarity but are genuinely distinct entities
# with different legal identities.  Bucket separation prevents these merges.
DEDUP_BUCKETS: dict[str, str] = {
    # Person bucket
    "person":           "person",
    "executive_hnw":    "person",
    "hnwi":             "person",
    "politician":       "person",
    # Corporate bucket
    "corporate":        "corporate",
    "investor":         "corporate",
    "real_estate":      "corporate",
    # Nonprofit bucket
    "nonprofit":        "nonprofit",
    "philanthropic":    "nonprofit",
    # Remaining types get their own singleton bucket (no cross-type merges)
    "illicit":          "illicit",
    "political":        "political",
    "government":       "government",
    "media":            "media",
    "other":            "other",
}

# Confidence tier rankings for primary election.
# Higher index = higher confidence.
_CONFIDENCE_RANK: dict[str, int] = {
    "low":    0,
    "medium": 1,
    "high":   2,
}


# ─────────────────────────────────────────────────────────────────────────────
# Agent class
# ─────────────────────────────────────────────────────────────────────────────

class DedupAgent(BaseAgent):
    """
    Entity deduplication pass between enrichment and relationship extraction.

    No LLM calls, no API calls — pure in-memory SequenceMatcher clustering.
    """

    AGENT_NAME    = "dedup_agent"
    AGENT_VERSION = "1.0"

    async def run(self, state: dict[str, Any]) -> dict[str, Any]:
        run_id = state["run_id"]
        entities: list[dict[str, Any]] = state.get("enriched_entities", [])

        log.info("dedup_agent: starting — %d entities to deduplicate", len(entities))

        if len(entities) < 2:
            log.info("dedup_agent: <2 entities — nothing to deduplicate")
            return self._build_patch(entities, [], state)

        deduped, merges = self._deduplicate(entities, run_id)

        removed = len(entities) - len(deduped)
        log.info(
            "dedup_agent: complete — %d/%d entities kept, %d duplicates merged",
            len(deduped), len(entities), removed,
        )

        self._entities_produced = len(deduped)
        return self._build_patch(deduped, merges, state)

    # ─────────────────────────────────────────────────────────────────────────
    # Core deduplication logic
    # ─────────────────────────────────────────────────────────────────────────

    def _deduplicate(
        self,
        entities: list[dict[str, Any]],
        run_id: str,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """
        Cluster entities and merge duplicates.

        Returns:
            (deduped_entities, merge_audit_log)
        """
        # Group by bucket
        buckets: dict[str, list[dict[str, Any]]] = {}
        for entity in entities:
            etype  = (entity.get("entity_type") or "other").lower()
            bucket = DEDUP_BUCKETS.get(etype, etype)
            buckets.setdefault(bucket, []).append(entity)

        kept: list[dict[str, Any]] = []
        merges: list[dict[str, Any]] = []

        for bucket_name, bucket_entities in buckets.items():
            bucket_kept, bucket_merges = self._cluster_bucket(
                bucket_entities, bucket_name, run_id
            )
            kept.extend(bucket_kept)
            merges.extend(bucket_merges)

        return kept, merges

    def _cluster_bucket(
        self,
        entities: list[dict[str, Any]],
        bucket_name: str,
        run_id: str,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """
        Within a single entity-type bucket, find and merge duplicate clusters.

        Algorithm: O(n²) greedy Union-Find by name similarity.
        For n ≤ 200 entities per run this is trivially fast (< 40K comparisons).
        """
        n = len(entities)
        # Union-Find parent array
        parent = list(range(n))

        def find(i: int) -> int:
            while parent[i] != i:
                parent[i] = parent[parent[i]]  # path compression
                i = parent[i]
            return i

        def union(i: int, j: int) -> None:
            ri, rj = find(i), find(j)
            if ri != rj:
                parent[rj] = ri

        # Precompute normalized names for faster comparisons
        names = [
            _normalize_name(
                e.get("canonical_name") or e.get("name") or ""
            )
            for e in entities
        ]

        # Compare all pairs — union if similarity ≥ threshold
        for i in range(n):
            if not names[i]:
                continue
            for j in range(i + 1, n):
                if not names[j]:
                    continue
                sim = SequenceMatcher(None, names[i], names[j]).ratio()
                if sim >= DEDUP_THRESHOLD:
                    union(i, j)

        # Group by cluster root
        clusters: dict[int, list[int]] = {}
        for i in range(n):
            root = find(i)
            clusters.setdefault(root, []).append(i)

        kept: list[dict[str, Any]] = []
        merges: list[dict[str, Any]] = []

        for root, indices in clusters.items():
            if len(indices) == 1:
                # No duplicate — keep as-is
                kept.append(entities[indices[0]])
                continue

            # Elect primary from the cluster
            primary_idx = self._elect_primary(entities, indices)
            primary = entities[primary_idx]
            duplicates = [entities[i] for i in indices if i != primary_idx]

            # Merge category_fields from all duplicates into primary
            merged_cat = self._merge_category_fields(primary, duplicates)
            if merged_cat is not None:
                primary = {**primary, "category_fields": merged_cat}

            kept.append(primary)

            # Build audit record
            merge_record = {
                "run_id":        run_id,
                "bucket":        bucket_name,
                "primary_id":    primary.get("entity_id", ""),
                "primary_name":  primary.get("canonical_name", primary.get("name", "")),
                "merged_ids":    [e.get("entity_id", "") for e in duplicates],
                "merged_names":  [
                    e.get("canonical_name", e.get("name", ""))
                    for e in duplicates
                ],
                "cluster_size":  len(indices),
            }
            merges.append(merge_record)

            log.info(
                "dedup_agent: merged %d duplicates into primary '%s' (bucket=%s). "
                "Removed: %s",
                len(duplicates),
                primary.get("canonical_name", primary.get("name", "")),
                bucket_name,
                [e.get("canonical_name", e.get("name", "")) for e in duplicates],
            )

        return kept, merges

    def _elect_primary(
        self,
        entities: list[dict[str, Any]],
        indices: list[int],
    ) -> int:
        """
        Choose the most authoritative entity from a duplicate cluster.

        Priority (descending):
        1. Overall confidence tier (high > medium > low)
        2. Number of populated category_fields keys (more enrichment = better)
        3. Position in the input list (earlier = stable ordering)
        """
        def score(idx: int) -> tuple[int, int]:
            e = entities[idx]
            conf_rank = _CONFIDENCE_RANK.get(
                (e.get("overall_confidence") or "low").lower(), 0
            )
            cat_size  = len(e.get("category_fields") or {})
            return (conf_rank, cat_size)

        return max(indices, key=score)

    def _merge_category_fields(
        self,
        primary: dict[str, Any],
        duplicates: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        """
        Merge category_fields from duplicates into the primary's category_fields.

        Rules:
        - Primary's value always wins on key conflict.
        - A populated value from a duplicate is only used if primary has no
          value for that key (missing or empty).
        - Returns None if no new keys were contributed (avoids unnecessary copies).
        """
        primary_cat = dict(primary.get("category_fields") or {})
        added_keys = 0

        for dup in duplicates:
            dup_cat = dup.get("category_fields") or {}
            for key, value in dup_cat.items():
                if key not in primary_cat or not primary_cat[key]:
                    # Primary lacks this key or it's falsy — inherit from dup
                    primary_cat[key] = value
                    added_keys += 1

        return primary_cat if added_keys > 0 else None

    # ─────────────────────────────────────────────────────────────────────────
    # State construction
    # ─────────────────────────────────────────────────────────────────────────

    def _build_patch(
        self,
        entities: list[dict[str, Any]],
        merges: list[dict[str, Any]],
        state: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "enriched_entities": entities,
            "dedup_merges":      merges,
            "current_phase":     "RELATIONSHIP",
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


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_name(name: str) -> str:
    """
    Normalize an entity name for comparison.

    Strips common corporate suffixes that inflate similarity between
    legitimately distinct entities sharing a root name.

    Examples:
        "Blackrock Inc."        → "blackrock"
        "Blackrock LLC"         → "blackrock"
        "John Smith Jr."        → "john smith jr"   (keep generational suffix)
    """
    if not name:
        return ""

    # Lowercase
    name = name.lower().strip()

    # Remove punctuation except spaces and hyphens
    import re
    name = re.sub(r"[.,']", "", name)

    # Strip trailing corporate type suffixes (these create false similarity)
    _CORP_SUFFIXES = (
        r"\s+inc$", r"\s+incorporated$",
        r"\s+llc$", r"\s+lp$", r"\s+llp$",
        r"\s+ltd$", r"\s+limited$",
        r"\s+corp$", r"\s+corporation$",
        r"\s+co$", r"\s+company$",
        r"\s+pllc$", r"\s+pc$",
        r"\s+plc$", r"\s+sa$", r"\s+ag$",
        r"\s+gmbh$", r"\s+bv$", r"\s+nv$",
        r"\s+holdings$", r"\s+holding$",
        r"\s+group$", r"\s+associates$",
        r"\s+partners$", r"\s+fund$",
        r"\s+ventures$",
    )
    for suffix_pat in _CORP_SUFFIXES:
        name = re.sub(suffix_pat, "", name).strip()

    return name
