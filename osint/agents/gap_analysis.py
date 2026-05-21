"""
osint/agents/gap_analysis.py

Gap Analysis Agent — first node in the analytical phase, runs after collection_gate.

Responsibility:
    1. Count raw_entities by entity_type across all 10 collection categories.
    2. Compare counts against MIN_ENTITIES_PER_CATEGORY thresholds.
    3. Compute a coverage_score per category (0.0–1.0).
    4. Identify thin categories: expected_min > 0 AND coverage_score < PASS2_TRIGGER_THRESHOLD.
    5. For each thin category, call LLM (qwen3:14b) to generate 3 targeted search queries
       that guide Pass 2 re-collection.  Skipped entirely if no thin categories exist.
    6. Persist an assessment record to DB summarising coverage.
    7. Return state patch with gap_analysis, pass2_targets, current_phase.

Coverage score formula:
    expected_min == 0  (illicit)  → coverage_score = 1.0  (absence is valid intelligence)
    otherwise                     → coverage_score = min(entities_found / expected_min, 1.0)

Pass 2 trigger:
    Any category where expected_min > 0 AND coverage_score < PASS2_TRIGGER_THRESHOLD (0.6).

LLM model:
    TaskType: "gap_analysis"  →  qwen3:14b (see config.MODEL_ROUTING)
    One call per thin category.  Called only after thin categories are identified.
    Fallback: if LLM call fails, pass2_targets entry is still created with empty
    suggested_queries — the collection agent will use its default Pass 2 queries.

State fields set:
    gap_analysis      dict[str, GapAnalysisEntry]
    pass2_targets     list[Pass2Target]
    current_phase     "COLLECTION_PASS2" | "RESOLUTION"

State fields NOT modified:
    raw_entities      (untouched — collection phase output)
    canonical_entities, etc. (downstream — not yet populated)
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from osint.agents.base import BaseAgent
from osint.core.config import MIN_ENTITIES_PER_CATEGORY, PASS2_TRIGGER_THRESHOLD

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

AGENT_NAME = "gap_analysis_agent"
AGENT_VERSION = "1.0"

# Maps entity_type → which collection agents produce that type.
# Used to populate agents_to_retry in pass2_targets so the dispatcher
# knows exactly which agents to re-run.
# pipeline_agent is deliberately excluded — it queries an internal service
# that doesn't benefit from Pass 2 targeted queries.
ENTITY_TYPE_TO_AGENTS: dict[str, list[str]] = {
    "investor":         ["investor_agent"],
    "philanthropic":    ["philanthropic_agent"],
    "corporate":        ["corporate_agent"],
    "political":        ["political_agent"],
    "nonprofit":        ["nonprofit_agent"],
    "executive_hnw":    ["executive_hnw_agent"],
    "community_leader": ["community_leader_agent"],
    "politician":       ["politician_agent"],
    "hnwi":             ["hnwi_agent"],
    "illicit":          ["illicit_agent"],
}

# Short human-readable label for each category used in LLM prompts
ENTITY_TYPE_LABELS: dict[str, str] = {
    "investor":         "venture capital and angel investors",
    "philanthropic":    "philanthropic foundations and major donors",
    "corporate":        "large corporations and corporate players",
    "political":        "political organizations and PACs",
    "nonprofit":        "nonprofit organizations and accelerators",
    "executive_hnw":    "executives and high-net-worth founders",
    "community_leader": "community leaders and civic influencers",
    "politician":       "elected officials and political figures",
    "hnwi":             "high-net-worth individuals and wealth holders",
    "illicit":          "illicit actors and sanctioned entities",
}

# LLM system prompt — focused on structured JSON output, no preamble
GAP_ANALYSIS_SYSTEM_PROMPT = (
    "You are an OSINT research assistant. Your job is to identify targeted search queries "
    "to find more entities of a specific type in a given city. "
    "Always respond with valid JSON only — no markdown, no explanation outside the JSON object."
)

# Per-category LLM prompt template
GAP_ANALYSIS_PROMPT_TEMPLATE = """\
We are building a startup ecosystem intelligence profile for {city_name}, {country}.

CATEGORY: {category_label}
ENTITIES FOUND SO FAR: {entities_found} (minimum expected: {expected_min})
ENTITIES ALREADY COLLECTED (do not repeat these):
{entity_names}

Generate 3 targeted web search queries that would find ADDITIONAL entities of this type
in {city_name} that are NOT already in the list above.

Respond with this exact JSON structure:
{{
  "analysis": "<one sentence explaining why coverage is thin and what to look for>",
  "suggested_queries": [
    "<query 1>",
    "<query 2>",
    "<query 3>"
  ]
}}"""


# ─────────────────────────────────────────────────────────────────────────────
# Agent
# ─────────────────────────────────────────────────────────────────────────────

class GapAnalysisAgent(BaseAgent):
    """
    Analyses collection coverage and generates Pass 2 targeting data.

    This is a pure analytical agent — it makes no external API calls,
    only reads from state and calls the local LLM for query generation.
    """

    AGENT_NAME = AGENT_NAME
    AGENT_VERSION = AGENT_VERSION

    async def run(self, state: dict[str, Any]) -> dict[str, Any]:
        city_name = state.get("city_name", "Unknown City")
        country = state.get("country_or_region", "United States")
        raw_entities: list[dict[str, Any]] = state.get("raw_entities", [])

        log.info(
            "gap_analysis_agent: analysing %d raw entities for %s",
            len(raw_entities), city_name,
        )

        # ── Step 1: Count entities by type ────────────────────────────────────
        entity_counts: dict[str, int] = defaultdict(int)
        entity_names_by_type: dict[str, list[str]] = defaultdict(list)

        for entity in raw_entities:
            etype = entity.get("entity_type")
            if not etype:
                continue
            entity_counts[etype] += 1
            name = entity.get("name") or entity.get("org_name") or entity.get("person_name")
            if name and len(entity_names_by_type[etype]) < 20:
                # Cap at 20 names per category — enough context for LLM, not overwhelming
                entity_names_by_type[etype].append(name)

        log.info("gap_analysis_agent: entity counts by type: %s", dict(entity_counts))

        # ── Step 2: Compute coverage per category ─────────────────────────────
        gap_analysis: dict[str, dict[str, Any]] = {}
        thin_categories: list[str] = []

        for category, expected_min in MIN_ENTITIES_PER_CATEGORY.items():
            found = entity_counts.get(category, 0)

            if expected_min == 0:
                # illicit — absence is valid intelligence, never thin
                coverage_score = 1.0
                is_thin = False
            else:
                coverage_score = min(found / expected_min, 1.0)
                is_thin = coverage_score < PASS2_TRIGGER_THRESHOLD

            gap_analysis[category] = {
                "entity_type":        category,
                "entities_found":     found,
                "expected_min":       expected_min,
                "coverage_score":     round(coverage_score, 4),
                "thin":               is_thin,
                "agents_to_retry":    ENTITY_TYPE_TO_AGENTS.get(category, []),
                "suggested_queries":  [],     # filled in step 3 if thin
                "analysis":           "",     # filled in step 3 if thin
            }

            if is_thin:
                thin_categories.append(category)
                log.info(
                    "gap_analysis_agent: THIN category '%s' — found=%d expected>=%d score=%.2f",
                    category, found, expected_min, coverage_score,
                )
            else:
                log.debug(
                    "gap_analysis_agent: OK category '%s' — found=%d score=%.2f",
                    category, found, coverage_score,
                )

        # ── Step 3: LLM query generation for thin categories ─────────────────
        # Only called when there are thin categories. One call per category.
        if thin_categories:
            log.info(
                "gap_analysis_agent: generating Pass 2 queries for %d thin categories: %s",
                len(thin_categories), thin_categories,
            )
            for category in thin_categories:
                await self._generate_queries_for_category(
                    category=category,
                    city_name=city_name,
                    country=country,
                    gap_entry=gap_analysis[category],
                    entity_names=entity_names_by_type.get(category, []),
                )
        else:
            log.info(
                "gap_analysis_agent: all categories at or above threshold — skipping LLM calls"
            )

        # ── Step 4: Build pass2_targets list ──────────────────────────────────
        pass2_targets: list[dict[str, Any]] = [
            {
                "entity_type":       category,
                "agents_to_retry":   gap_analysis[category]["agents_to_retry"],
                "entities_found":    gap_analysis[category]["entities_found"],
                "expected_min":      gap_analysis[category]["expected_min"],
                "coverage_score":    gap_analysis[category]["coverage_score"],
                "suggested_queries": gap_analysis[category]["suggested_queries"],
                "analysis":          gap_analysis[category]["analysis"],
            }
            for category in thin_categories
        ]

        # ── Step 5: Determine next phase ──────────────────────────────────────
        if thin_categories:
            next_phase = "COLLECTION_PASS2"
        else:
            next_phase = "RESOLUTION"

        log.info(
            "gap_analysis_agent: coverage complete — thin=%d next_phase=%s",
            len(thin_categories), next_phase,
        )

        # ── Step 6: Write assessment to DB ────────────────────────────────────
        await self._write_coverage_assessment(
            state=state,
            gap_analysis=gap_analysis,
            thin_categories=thin_categories,
            total_raw_entities=len(raw_entities),
        )

        # ── Step 7: Return state patch ────────────────────────────────────────
        return {
            "gap_analysis":   gap_analysis,
            "pass2_targets":  pass2_targets,
            "current_phase":  next_phase,
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
    # LLM query generation
    # ─────────────────────────────────────────────────────────────────────────

    async def _generate_queries_for_category(
        self,
        category: str,
        city_name: str,
        country: str,
        gap_entry: dict[str, Any],
        entity_names: list[str],
    ) -> None:
        """
        Call qwen3:14b to generate 3 targeted search queries for a thin category.
        Mutates gap_entry in-place (sets 'suggested_queries' and 'analysis').

        On any failure (network, parse error, malformed JSON), logs a warning
        and leaves suggested_queries as [] — the collection agent will fall back
        to its default Pass 2 queries.
        """
        names_block = (
            "\n".join(f"  - {n}" for n in entity_names)
            if entity_names
            else "  (none yet collected)"
        )

        prompt = GAP_ANALYSIS_PROMPT_TEMPLATE.format(
            city_name=city_name,
            country=country,
            category_label=ENTITY_TYPE_LABELS.get(category, category),
            entities_found=gap_entry["entities_found"],
            expected_min=gap_entry["expected_min"],
            entity_names=names_block,
        )

        try:
            result, _meta = await self.llm_generate_json(
                task_type="gap_analysis",
                prompt=prompt,
                system=GAP_ANALYSIS_SYSTEM_PROMPT,
            )
        except Exception as exc:
            log.warning(
                "gap_analysis_agent: LLM call failed for category '%s': %s",
                category, exc,
            )
            return

        # Validate and extract
        if not isinstance(result, dict):
            log.warning(
                "gap_analysis_agent: LLM returned non-dict for category '%s' (type=%s)",
                category, type(result).__name__,
            )
            return

        queries = result.get("suggested_queries")
        analysis = result.get("analysis", "")

        if not isinstance(queries, list):
            log.warning(
                "gap_analysis_agent: 'suggested_queries' not a list for category '%s' — got: %r",
                category, queries,
            )
            return

        # Filter to non-empty strings, cap at 3
        clean_queries = [q for q in queries if isinstance(q, str) and q.strip()][:3]

        if not clean_queries:
            log.warning(
                "gap_analysis_agent: no usable queries returned for category '%s'",
                category,
            )
            return

        gap_entry["suggested_queries"] = clean_queries
        gap_entry["analysis"] = str(analysis)[:500] if analysis else ""

        log.info(
            "gap_analysis_agent: generated %d queries for '%s': %s",
            len(clean_queries), category, clean_queries,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # DB assessment write
    # ─────────────────────────────────────────────────────────────────────────

    async def _write_coverage_assessment(
        self,
        state: dict[str, Any],
        gap_analysis: dict[str, dict[str, Any]],
        thin_categories: list[str],
        total_raw_entities: int,
    ) -> None:
        """
        Persist a coverage assessment record to DB.
        This is the audit trail for the gap analysis decision.

        NOTE: analytical_assessments requires claim_text, framework_name,
        framework_version, model_used, prompt_version.
        We build a structured claim from the coverage summary.
        """
        from osint.core.config import settings as _settings

        # Build a short claim_text summarising the result
        if thin_categories:
            claim_text = (
                f"Collection coverage below threshold for {len(thin_categories)} "
                f"categories: {', '.join(thin_categories)}. "
                f"Pass 2 triggered. Total raw entities: {total_raw_entities}."
            )
        else:
            claim_text = (
                f"All collection categories at or above threshold. "
                f"No Pass 2 required. Total raw entities: {total_raw_entities}."
            )

        # Build serialisable summary (omit suggested_queries — large, in state)
        coverage_summary = {
            cat: {
                "entities_found": entry["entities_found"],
                "expected_min":   entry["expected_min"],
                "coverage_score": entry["coverage_score"],
                "thin":           entry["thin"],
            }
            for cat, entry in gap_analysis.items()
        }

        assessment = {
            "run_id":            state["run_id"],
            "assessment_type":   "gap_analysis",
            "claim_text":        claim_text,
            "claim_json":        {
                "thin_categories":    thin_categories,
                "pass2_triggered":    len(thin_categories) > 0,
                "total_raw_entities": total_raw_entities,
                "coverage_summary":   coverage_summary,
            },
            "framework_name":    "gap_analysis",
            "framework_version": self.AGENT_VERSION,
            "model_used":        _settings.ollama_default_model,
            "prompt_version":    self.AGENT_VERSION,
            "confidence":        "high",  # Coverage counts are deterministic, not LLM
            "needs_review":      False,
            "created_at":        datetime.now(timezone.utc).isoformat(),
        }

        try:
            await self.write_assessment(assessment)
        except Exception as exc:
            log.warning(
                "gap_analysis_agent: failed to write coverage assessment: %s", exc
            )
            # Non-fatal — assessment failure does not block the pipeline
