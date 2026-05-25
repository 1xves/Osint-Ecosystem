"""
osint/agents/scoring.py

Analysis & Scoring Agent — runs after relationship mapping, before verification.

Assigns 9 numerical scores (0–100) to every canonical entity using qwen3:14b.
One LLM call per entity. Input context includes the entity profile, its
relationship graph (edges from relationships_draft), and the city/market context.

9 Dimensions:
    score_influence             Overall influence in the local ecosystem
    score_startup_relevance     Relevance to startup/venture ecosystem specifically
    score_partner_potential     Strategic partnership value
    score_supporter_potential   Likelihood to be a supporter/ally
    score_competitor_potential  Competitive threat to a startup entrant
    score_blocker_risk          Risk of creating regulatory/legal/political obstacles
    score_investment_potential  Value as investment or grant recipient
    score_support_target        Deserving of active ecosystem support
    score_recruiting_potential  Value as talent source or recruiting target

Rules:
    - Score rationale REQUIRED for any dimension score ≥ 70 (RATIONALE_REQUIRED_ABOVE)
    - Evidence citation REQUIRED for score_blocker_risk ≥ 60 (BLOCKER_EVIDENCE_REQUIRED_ABOVE)
    - Rationale written to analytical_assessments table
    - Scores written to entities table via update_entity_scores()
    - Classification flags updated based on CLASSIFICATION_THRESHOLDS

After scoring:
    - scored_entities: enriched_entities + scores merged in
    - ranked_lists: per-dimension sorted entity_id lists (descending)
    - agent_statuses: updated
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from osint.agents.base import BaseAgent
from osint.core.config import (
    CLASSIFICATION_THRESHOLDS,
    RATIONALE_REQUIRED_ABOVE,
    BLOCKER_EVIDENCE_REQUIRED_ABOVE,
    settings,
)

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

AGENT_NAME    = "scoring_agent"
AGENT_VERSION = "1.0"

# 9 score dimension names (must match entities table columns)
SCORE_DIMENSIONS = [
    "score_influence",
    "score_startup_relevance",
    "score_partner_potential",
    "score_supporter_potential",
    "score_competitor_potential",
    "score_blocker_risk",
    "score_investment_potential",
    "score_support_target",
    "score_recruiting_potential",
]

# Dimension display names for prompts
DIMENSION_LABELS = {
    "score_influence":            "Ecosystem Influence",
    "score_startup_relevance":    "Startup Ecosystem Relevance",
    "score_partner_potential":    "Partnership Potential",
    "score_supporter_potential":  "Supporter Potential",
    "score_competitor_potential": "Competitive Threat",
    "score_blocker_risk":         "Blocker / Obstacle Risk",
    "score_investment_potential": "Investment / Grant Potential",
    "score_support_target":       "Support Target Value",
    "score_recruiting_potential": "Recruiting Potential",
}

# Scoring rubric per dimension
DIMENSION_RUBRICS = {
    "score_influence": (
        "How influential is this entity in the local startup ecosystem? "
        "Consider: board seats, media presence, network size, capital deployed, "
        "event hosting, policy-making authority."
    ),
    "score_startup_relevance": (
        "How central is this entity to the startup and venture ecosystem specifically? "
        "High = active investors, accelerators, startup-focused foundations, corporate venture arms. "
        "Low = unrelated businesses, philanthropic causes outside tech."
    ),
    "score_partner_potential": (
        "How valuable would this entity be as a strategic partner? "
        "Consider alignment of interests, complementary resources, "
        "distribution access, capital availability."
    ),
    "score_supporter_potential": (
        "How likely is this entity to actively support startup growth in the ecosystem? "
        "Consider past support behavior, mission alignment, political/civic engagement."
    ),
    "score_competitor_potential": (
        "What is the competitive threat posed by this entity? "
        "Consider: competing for same deals, same market, blocking patents, "
        "exclusive channel relationships. Score 0 if not a competitive concern."
    ),
    "score_blocker_risk": (
        "What risk does this entity pose as an obstacle or blocker? "
        "Consider: regulatory authority, political power, NIMBY tendencies, "
        "litigation history, illicit activities, reputational risk. "
        "MANDATORY: any score ≥ 60 requires specific evidence citation."
    ),
    "score_investment_potential": (
        "How compelling is this entity as an investment or grant funding target? "
        "Consider: traction, team quality, market position, innovation, scalability. "
        "Score 0 for non-investable entity types (politicians, established corporations, etc.)."
    ),
    "score_support_target": (
        "Does this entity deserve active ecosystem support? "
        "Consider: mission alignment, underserved status, potential impact. "
        "High for worthy nonprofits, underrepresented founders, mission-driven orgs."
    ),
    "score_recruiting_potential": (
        "How valuable is this entity as a source of talent or as a potential hire/advisor? "
        "Consider: executives with relevant experience, founders with track records, "
        "large employer talent pools."
    ),
}

# LLM system prompt
SCORING_SYSTEM_PROMPT = (
    "You are an expert OSINT analyst specialising in startup ecosystem intelligence. "
    "You score entities on 9 dimensions using 0-100 integer scores. "
    "Be calibrated: a score of 50 means average for the ecosystem, "
    "90+ means exceptional, 10 or below means very low relevance. "
    "Respond with valid JSON only — no markdown, no explanation outside the JSON."
)

SCORING_PROMPT_TEMPLATE = """\
Score the following entity on 9 dimensions for a startup ecosystem intelligence report.
City / Market: {city_name}, {country}

ENTITY PROFILE:
{entity_profile}

KNOWN RELATIONSHIPS:
{relationships_summary}

SCORING RUBRICS:
{rubrics}

Respond with this JSON structure:
{{
  "score_influence": <int 0-100>,
  "score_startup_relevance": <int 0-100>,
  "score_partner_potential": <int 0-100>,
  "score_supporter_potential": <int 0-100>,
  "score_competitor_potential": <int 0-100>,
  "score_blocker_risk": <int 0-100>,
  "score_investment_potential": <int 0-100>,
  "score_support_target": <int 0-100>,
  "score_recruiting_potential": <int 0-100>,
  "rationale": {{
    "<dimension_key>": "<one sentence rationale — REQUIRED for any score ≥ 70>",
    ...
  }},
  "blocker_evidence": "<specific evidence for score_blocker_risk if ≥ 60, else null>"
}}"""


# ─────────────────────────────────────────────────────────────────────────────
# Agent
# ─────────────────────────────────────────────────────────────────────────────

class ScoringAgent(BaseAgent):
    """
    9-dimension scoring agent. One LLM call per entity.
    Writes scores to DB and rationale to analytical_assessments.
    """

    AGENT_NAME = AGENT_NAME
    AGENT_VERSION = AGENT_VERSION

    async def run(self, state: dict[str, Any]) -> dict[str, Any]:
        run_id      = state["run_id"]
        city_name   = state.get("city_name", "Unknown City")
        country     = state.get("country_or_region", "United States")
        entities    = state.get("enriched_entities") or state.get("canonical_entities", [])
        rel_draft   = state.get("relationships_draft", [])

        log.info(
            "scoring_agent: scoring %d entities, %d relationships available",
            len(entities), len(rel_draft),
        )

        if not entities:
            return self._empty_patch(state)

        # ── Build entity→relationship index ──────────────────────────────────
        entity_relationships: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for edge in rel_draft:
            src = edge.get("source_entity_id")
            tgt = edge.get("target_entity_id")
            if src:
                entity_relationships[src].append(edge)
            if tgt:
                entity_relationships[tgt].append(edge)

        # Build entity_id → entity name index for relationship summaries
        entity_names: dict[str, str] = {
            e.get("entity_id", ""): e.get("canonical_name", "?")
            for e in entities
            if e.get("entity_id")
        }

        # ── Score each entity ─────────────────────────────────────────────────
        scored_entities: list[dict[str, Any]] = []
        ranked_acc: dict[str, list[tuple[int, str]]] = {d: [] for d in SCORE_DIMENSIONS}

        # Score concurrently in batches of 10 (to keep Ollama from choking)
        BATCH_SIZE = 10
        for batch_start in range(0, len(entities), BATCH_SIZE):
            batch = entities[batch_start:batch_start + BATCH_SIZE]
            tasks = [
                self._score_entity(
                    entity=entity,
                    city_name=city_name,
                    country=country,
                    run_id=run_id,
                    entity_relationships=entity_relationships.get(entity.get("entity_id", ""), []),
                    entity_names=entity_names,
                )
                for entity in batch
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, result in enumerate(results):
                entity = batch[i]
                if isinstance(result, Exception):
                    log.warning(
                        "scoring_agent: scoring failed for '%s': %s",
                        entity.get("canonical_name"), result,
                    )
                    # Return entity with zero scores
                    scored = dict(entity)
                    for dim in SCORE_DIMENSIONS:
                        scored[dim] = 0
                    scored_entities.append(scored)
                else:
                    scored_entities.append(result)

        # ── Build ranked lists ────────────────────────────────────────────────
        for entity in scored_entities:
            eid = entity.get("entity_id", "")
            for dim in SCORE_DIMENSIONS:
                score = entity.get(dim, 0)
                if eid:
                    ranked_acc[dim].append((score, eid))

        ranked_lists: dict[str, list[str]] = {}
        for dim, entries in ranked_acc.items():
            entries.sort(key=lambda x: x[0], reverse=True)
            ranked_lists[dim] = [eid for _score, eid in entries]

        log.info(
            "scoring_agent: scored %d entities. Top influence: %s",
            len(scored_entities),
            ranked_lists.get("score_influence", [])[:3],
        )

        return {
            "scored_entities": scored_entities,
            "ranked_lists":    ranked_lists,
            "current_phase":   "VERIFICATION",
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
    # Single entity scoring
    # ─────────────────────────────────────────────────────────────────────────

    async def _score_entity(
        self,
        entity: dict[str, Any],
        city_name: str,
        country: str,
        run_id: str,
        entity_relationships: list[dict[str, Any]],
        entity_names: dict[str, str],
    ) -> dict[str, Any]:
        """
        Score a single entity on all 9 dimensions.
        Returns entity dict with scores and updated classification flags merged in.
        Writes rationale to analytical_assessments for scores ≥ 70.
        Writes updated scores to entities table.
        """
        entity_id   = entity.get("entity_id", "")
        entity_name = entity.get("canonical_name", entity.get("name", "Unknown"))
        entity_type = entity.get("entity_type", "unknown")

        # ── Build prompt context ──────────────────────────────────────────────
        profile_text = _build_entity_profile(entity)
        rel_summary  = _build_relationship_summary(entity_id, entity_relationships, entity_names)
        rubrics_text = "\n".join(
            f"- {DIMENSION_LABELS[dim]}: {DIMENSION_RUBRICS[dim]}"
            for dim in SCORE_DIMENSIONS
        )

        prompt = SCORING_PROMPT_TEMPLATE.format(
            city_name=city_name,
            country=country,
            entity_profile=profile_text,
            relationships_summary=rel_summary,
            rubrics=rubrics_text,
        )

        # ── LLM call ──────────────────────────────────────────────────────────
        try:
            raw_result, _meta = await self.llm_generate_json(
                task_type="entity_scoring",
                prompt=prompt,
                system=SCORING_SYSTEM_PROMPT,
            )
        except Exception as exc:
            log.warning(
                "scoring_agent: LLM call failed for '%s': %s", entity_name, exc
            )
            scored = dict(entity)
            for dim in SCORE_DIMENSIONS:
                scored[dim] = 0
            return scored

        if not isinstance(raw_result, dict):
            log.warning(
                "scoring_agent: non-dict LLM response for '%s' (type=%s)",
                entity_name, type(raw_result).__name__,
            )
            scored = dict(entity)
            for dim in SCORE_DIMENSIONS:
                scored[dim] = 0
            return scored

        # ── Extract and validate scores ───────────────────────────────────────
        scores: dict[str, int] = {}
        for dim in SCORE_DIMENSIONS:
            raw_val = raw_result.get(dim)
            try:
                val = int(raw_val)
                val = max(0, min(100, val))   # Clamp to 0–100
            except (TypeError, ValueError):
                val = 0
            scores[dim] = val

        rationale: dict[str, str] = {}
        raw_rationale = raw_result.get("rationale", {})
        if isinstance(raw_rationale, dict):
            for dim, text in raw_rationale.items():
                if dim in SCORE_DIMENSIONS and isinstance(text, str):
                    rationale[dim] = text[:500]

        blocker_evidence: str | None = raw_result.get("blocker_evidence")
        if blocker_evidence:
            blocker_evidence = str(blocker_evidence)[:500]

        # Validate rationale requirement (≥ 70 must have rationale)
        for dim, score in scores.items():
            if score >= RATIONALE_REQUIRED_ABOVE and dim not in rationale:
                log.warning(
                    "scoring_agent: MISSING rationale for '%s' %s=%d",
                    entity_name, dim, score,
                )
                rationale[dim] = f"Score {score} assigned (rationale not provided by LLM)"

        # Validate blocker evidence requirement
        if scores.get("score_blocker_risk", 0) >= BLOCKER_EVIDENCE_REQUIRED_ABOVE:
            if not blocker_evidence:
                log.warning(
                    "scoring_agent: MISSING blocker_evidence for '%s' (score=%d)",
                    entity_name, scores["score_blocker_risk"],
                )
                blocker_evidence = "High blocker risk score assigned (specific evidence not cited by LLM)"

        # ── Compute classification flags ──────────────────────────────────────
        classification_flags = _compute_classification_flags(entity, scores)

        # ── Build scored entity dict ──────────────────────────────────────────
        scored = dict(entity)
        scored.update(scores)
        scored.update(classification_flags)
        if blocker_evidence:
            cat = scored.setdefault("category_fields", {})
            cat["blocker_evidence"] = blocker_evidence

        # ── Write scores and enrichment data to DB ───────────────────────────
        if entity_id:
            try:
                await self._db.update_entity_scores(entity_id, scores, classification_flags)
                # Persist enriched category_fields — populated during the enrichment
                # phase (HUD properties, FinCEN CTR, CourtListener, EDGAR, etc.) but
                # never written to Supabase for non-OFAC entities until now.
                category_fields = scored.get("category_fields", {})
                if category_fields:
                    await self._db.update_entity_category_fields(entity_id, category_fields)
            except Exception as exc:
                log.warning(
                    "scoring_agent: DB update failed for '%s': %s", entity_name, exc
                )

        # ── Write rationale to analytical_assessments ─────────────────────────
        if rationale and entity_id:
            await self._write_score_rationale(
                entity_id=entity_id,
                entity_name=entity_name,
                entity_type=entity_type,
                scores=scores,
                rationale=rationale,
                blocker_evidence=blocker_evidence,
                run_id=run_id,
            )

        log.debug(
            "scoring_agent: scored '%s' — influence=%d startup_rel=%d blocker=%d",
            entity_name,
            scores.get("score_influence", 0),
            scores.get("score_startup_relevance", 0),
            scores.get("score_blocker_risk", 0),
        )

        return scored

    # ─────────────────────────────────────────────────────────────────────────
    # Rationale persistence
    # ─────────────────────────────────────────────────────────────────────────

    async def _write_score_rationale(
        self,
        entity_id: str,
        entity_name: str,
        entity_type: str,
        scores: dict[str, int],
        rationale: dict[str, str],
        blocker_evidence: str | None,
        run_id: str,
    ) -> None:
        """
        Write a scoring rationale assessment to analytical_assessments table.
        One record per entity covering all scored dimensions with rationale.
        """
        # Build claim_text: summary of top dimensions
        top_dims = sorted(
            [(v, k) for k, v in scores.items()],
            reverse=True
        )[:3]
        claim_text = (
            f"Scores for {entity_name} ({entity_type}): "
            + ", ".join(f"{DIMENSION_LABELS.get(k, k)}={v}" for v, k in top_dims)
        )

        claim_json = {
            "scores": scores,
            "rationale": rationale,
        }
        if blocker_evidence:
            claim_json["blocker_evidence"] = blocker_evidence

        assessment = {
            "entity_id":         entity_id,
            "run_id":            run_id,
            "assessment_type":   "score_rationale",
            "claim_text":        claim_text,
            "claim_json":        claim_json,
            "framework_name":    "entity_scoring",
            "framework_version": self.AGENT_VERSION,
            "model_used":        settings.ollama_default_model,
            "prompt_version":    self.AGENT_VERSION,
            "confidence":        "medium",   # LLM-generated, not verified
            "needs_review":      False,
            "derived_from":      [],
        }

        try:
            await self.write_assessment(assessment)
        except Exception as exc:
            log.warning(
                "scoring_agent: failed to write assessment for '%s': %s",
                entity_name, exc,
            )

    # ─────────────────────────────────────────────────────────────────────────
    # Utility
    # ─────────────────────────────────────────────────────────────────────────

    def _empty_patch(self, state: dict[str, Any]) -> dict[str, Any]:
        return {
            "scored_entities": [],
            "ranked_lists":    {},
            "current_phase":   "VERIFICATION",
            **self.agent_status_patch("success", state.get("agent_statuses", {})),
            **self.token_count_patch(
                state.get("total_tokens_in", 0),
                state.get("total_tokens_out", 0),
                state.get("agent_token_counts", {}),
            ),
            **self.entity_count_patch(state.get("agent_entity_counts", {})),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Module helpers
# ─────────────────────────────────────────────────────────────────────────────

def _build_entity_profile(entity: dict[str, Any]) -> str:
    """Build a concise entity profile string for the scoring prompt."""
    lines = [
        f"Name: {entity.get('canonical_name', entity.get('name', 'Unknown'))}",
        f"Type: {entity.get('entity_type', '?')} / {entity.get('entity_subtype', '?')}",
        f"City: {entity.get('primary_city', '?')}, {entity.get('primary_state', '')}",
        f"Confidence: {entity.get('overall_confidence', '?')}",
    ]

    # Description
    desc = entity.get("description", "")
    if desc:
        lines.append(f"Description: {desc[:300]}")

    # Key category fields (first 5 non-status, non-empty entries)
    cat = entity.get("category_fields", {})
    if isinstance(cat, dict):
        shown = 0
        for k, v in cat.items():
            if k.endswith("_status") or not v or shown >= 5:
                continue
            if isinstance(v, (list, dict)):
                lines.append(f"{k}: {json.dumps(v)[:200]}")
            else:
                lines.append(f"{k}: {str(v)[:200]}")
            shown += 1

    return "\n".join(lines)


def _build_relationship_summary(
    entity_id: str,
    edges: list[dict[str, Any]],
    entity_names: dict[str, str],
) -> str:
    """Summarize an entity's known relationships as a short text."""
    if not edges:
        return "(none discovered)"

    summaries = []
    for edge in edges[:10]:  # Cap at 10 to stay within context
        rel_type = edge.get("relationship_type", "?")
        src_id   = edge.get("source_entity_id", "")
        tgt_id   = edge.get("target_entity_id", "")

        if src_id == entity_id:
            other_name = entity_names.get(tgt_id, tgt_id[:8])
            summaries.append(f"→ {rel_type} → {other_name}")
        else:
            other_name = entity_names.get(src_id, src_id[:8])
            summaries.append(f"{other_name} → {rel_type} →")

    return "\n".join(summaries) if summaries else "(none)"


def _compute_classification_flags(
    entity: dict[str, Any],
    scores: dict[str, int],
) -> dict[str, bool]:
    """
    Compute updated classification flags from scores.
    OR-logic: flag is True if either the collection agent set it OR score ≥ threshold.
    """
    flags: dict[str, bool] = {}
    for flag_name, cfg in CLASSIFICATION_THRESHOLDS.items():
        dim     = cfg["dimension"]
        min_val = cfg["min"]
        score_true  = scores.get(dim, 0) >= min_val
        entity_true = bool(entity.get(flag_name, False))
        flags[flag_name] = score_true or entity_true
    return flags
