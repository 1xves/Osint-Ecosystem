"""
osint/agents/verification.py

Verification Agent — runs after scoring, before briefing.

Responsibility:
    1. For every scored entity, generate 2–4 verifiable claims drawn from the
       entity's own collected data and its relationship context.
    2. Call the LLM (qwen3:14b) once per entity to evaluate each claim against
       available evidence, producing a structured verdict.
    3. Apply the hard gate:
           verdict == "fail" AND confidence == "high"
               → entity is flagged, needs_review set to True in DB,
                 entity excluded from verified_entity_ids (briefing scope)
           verdict == "fail" AND confidence in ("medium", "low")
               → entity flagged in state, included in briefing with caveat
           verdict == "unverifiable"
               → always passes gate (absence of evidence ≠ disproof)
           verdict == "pass"
               → passes gate
    4. Persist one analytical_assessments record per entity summarising
       the verification outcome.
    5. Return state patch with verification_results, verification_summary,
       verified_entity_ids, flagged_entity_ids, current_phase.

Claim types generated per entity:
    - EXISTENCE  : "[Name] is a [entity_type] operating in [city_name]."
    - PROFESSIONAL: "[Name] serves as [title/role] at [employer]."
                    (generated when employer/title fields are populated)
    - NOTABLE    : Key distinguishing fact derived from the top populated field.
    - BLOCKER    : "[Name] poses a blocking risk: [blocker_evidence]."
                    (generated only when blocker_risk >= BLOCKER_EVIDENCE_REQUIRED_ABOVE)

LLM model:
    TaskType: "claim_verification"  →  qwen3:14b  (see config.MODEL_ROUTING)
    One call per entity, all claims in a single prompt.

Batching:
    Entities processed in concurrent batches of BATCH_SIZE (10).

DB side effects:
    - Calls flag_entities_for_review() for high-confidence fails (targeted UPDATE).
    - Writes one analytical_assessments record per entity.
    - Writes rejected_items records for high-confidence failed entities.

State fields set:
    verification_results    dict[entity_id → VerificationResult]
    verification_summary    dict with aggregate counts
    verified_entity_ids     list[str] — passes the hard gate
    flagged_entity_ids      list[str] — high-confidence fails, excluded from briefing
    current_phase           "BRIEFING"

State fields NOT modified:
    scored_entities, ranked_lists, relationships_draft (upstream — read-only)
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any

from osint.agents.base import BaseAgent
from osint.core.config import (
    BLOCKER_EVIDENCE_REQUIRED_ABOVE,
    settings,
)

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

AGENT_NAME    = "verification_agent"
AGENT_VERSION = "1.0"

BATCH_SIZE = 10

# Minimum relationship strength to generate a relationship claim for verification
RELATIONSHIP_CLAIM_MIN_STRENGTH = 0.70

# System prompt — verification-focused, JSON-only output
VERIFICATION_SYSTEM_PROMPT = (
    "You are an OSINT intelligence verification specialist. "
    "Evaluate specific claims about real entities against the available evidence. "
    "For each claim:\n"
    "  \"pass\"         — the claim is directly supported by the evidence provided\n"
    "  \"fail\"         — the claim is directly contradicted by evidence provided\n"
    "  \"unverifiable\" — evidence is insufficient to confirm or deny the claim\n"
    "Use 'fail' only when specific evidence directly contradicts the claim. "
    "Absence of evidence is NOT grounds for 'fail'. "
    "Always respond with valid JSON only — no markdown, no explanation outside the JSON object."
)

# Per-entity prompt template
VERIFICATION_PROMPT_TEMPLATE = """\
ENTITY PROFILE
  Name:        {name}
  Type:        {entity_type}
  City:        {city_name}
  Confidence:  {overall_confidence}
  Description: {description}

COLLECTED DATA FIELDS
{fields_block}

RELATIONSHIP CONTEXT (top relationships)
{relationship_block}

CLAIMS TO VERIFY ({claim_count} claims)
{claims_block}

Evaluate each claim against the data above.
Respond with this exact JSON structure:
{{
  "verifications": [
    {{
      "claim_index": 0,
      "verdict": "pass|fail|unverifiable",
      "confidence": "high|medium|low",
      "reasoning": "<one sentence citing specific evidence>"
    }}
  ]
}}"""

# Fields to extract as evidence — ordered by informativeness per entity type
EVIDENCE_FIELDS_ORDER = [
    "description", "headline", "bio",
    "current_employer", "primary_employer", "employer_name",
    "title", "role", "primary_role",
    "fund_name", "fund_type", "total_aum_usd",
    "portfolio_companies", "recent_investments",
    "org_name", "legal_name",
    "ein", "founded_year",
    "total_assets_usd", "annual_revenue_usd",
    "board_seats", "is_founder", "primary_company",
    "top_donors", "party_affiliation", "office_held",
    "court_cases", "regulatory_actions", "sanction_status",
    "linkedin_url", "source_urls",
]

# Claim type labels
CLAIM_EXISTENCE    = "EXISTENCE"
CLAIM_PROFESSIONAL = "PROFESSIONAL"
CLAIM_NOTABLE      = "NOTABLE"
CLAIM_BLOCKER      = "BLOCKER"
CLAIM_RELATIONSHIP = "RELATIONSHIP"


# ─────────────────────────────────────────────────────────────────────────────
# Agent
# ─────────────────────────────────────────────────────────────────────────────

class VerificationAgent(BaseAgent):
    """
    Verifies key claims for every scored entity before briefing.

    Hard gate: entities with any high-confidence "fail" verdict are
    excluded from verified_entity_ids and will not appear in the briefing.
    """

    AGENT_NAME    = AGENT_NAME
    AGENT_VERSION = AGENT_VERSION

    async def run(self, state: dict[str, Any]) -> dict[str, Any]:
        city_name   = state.get("city_name", "Unknown City")
        country     = state.get("country_or_region", "United States")
        scored_entities: list[dict[str, Any]] = state.get("scored_entities", [])
        relationships: list[dict[str, Any]]   = state.get("relationships_draft", [])

        log.info(
            "verification_agent: verifying %d scored entities for %s",
            len(scored_entities), city_name,
        )

        if not scored_entities:
            log.warning("verification_agent: no scored entities found — skipping verification")
            return {
                "verification_results":  {},
                "verification_summary":  {
                    "total_entities": 0,
                    "passed": 0, "failed": 0, "unverifiable": 0,
                    "flagged_entity_ids": [],
                },
                "verified_entity_ids":   [],
                "flagged_entity_ids":    [],
                "current_phase":         "BRIEFING",
                **self.agent_status_patch("success", state.get("agent_statuses", {})),
                **self.token_count_patch(
                    state.get("total_tokens_in", 0),
                    state.get("total_tokens_out", 0),
                    state.get("agent_token_counts", {}),
                ),
                **self.entity_count_patch(state.get("agent_entity_counts", {})),
            }

        # ── Build relationship index keyed by entity_id ───────────────────────
        # Allows O(1) lookup of an entity's relationships for claim generation
        rel_index: dict[str, list[dict[str, Any]]] = {}
        for edge in relationships:
            for eid in (edge.get("source_id"), edge.get("target_id")):
                if eid:
                    rel_index.setdefault(eid, []).append(edge)

        # ── Process in batches ────────────────────────────────────────────────
        all_results: dict[str, dict[str, Any]] = {}

        batches = [
            scored_entities[i: i + BATCH_SIZE]
            for i in range(0, len(scored_entities), BATCH_SIZE)
        ]

        log.info(
            "verification_agent: processing %d batches of up to %d entities",
            len(batches), BATCH_SIZE,
        )

        for batch_idx, batch in enumerate(batches):
            log.debug(
                "verification_agent: running batch %d/%d (%d entities)",
                batch_idx + 1, len(batches), len(batch),
            )
            results = await asyncio.gather(
                *[
                    self._verify_entity(entity, city_name, country, rel_index)
                    for entity in batch
                ],
                return_exceptions=True,
            )
            for entity, result in zip(batch, results):
                eid = entity.get("entity_id", "")
                if isinstance(result, Exception):
                    log.warning(
                        "verification_agent: entity '%s' raised exception: %s",
                        entity.get("canonical_name", eid), result,
                    )
                    # Non-fatal — treat as unverifiable, do not flag
                    all_results[eid] = _unverifiable_result(entity)
                else:
                    all_results[eid] = result

        # ── Apply hard gate ───────────────────────────────────────────────────
        verified_entity_ids: list[str] = []
        flagged_entity_ids:  list[str] = []

        for eid, result in all_results.items():
            if result.get("hard_fail"):
                flagged_entity_ids.append(eid)
            else:
                verified_entity_ids.append(eid)

        log.info(
            "verification_agent: gate results — verified=%d flagged=%d",
            len(verified_entity_ids), len(flagged_entity_ids),
        )

        # ── DB side effects ───────────────────────────────────────────────────
        # 1. Flag hard-fail entities for human review
        if flagged_entity_ids:
            await self._flag_entities(flagged_entity_ids)

        # 2. Write rejection records for hard-fail entities
        for eid in flagged_entity_ids:
            result = all_results[eid]
            await self._write_fail_rejection(eid, result, state)

        # 3. Write assessment per entity
        await self._write_assessments(all_results, state)

        # ── Build summary ─────────────────────────────────────────────────────
        passed_count       = sum(1 for r in all_results.values() if r.get("overall_verdict") == "pass")
        failed_count       = sum(1 for r in all_results.values() if r.get("overall_verdict") == "fail")
        unverifiable_count = sum(1 for r in all_results.values() if r.get("overall_verdict") == "unverifiable")

        verification_summary = {
            "total_entities":    len(all_results),
            "passed":            passed_count,
            "failed":            failed_count,
            "unverifiable":      unverifiable_count,
            "flagged_entity_ids": flagged_entity_ids,
        }

        log.info(
            "verification_agent: summary — total=%d passed=%d failed=%d unverifiable=%d flagged=%d",
            len(all_results), passed_count, failed_count, unverifiable_count, len(flagged_entity_ids),
        )

        return {
            "verification_results":  all_results,
            "verification_summary":  verification_summary,
            "verified_entity_ids":   verified_entity_ids,
            "flagged_entity_ids":    flagged_entity_ids,
            "current_phase":         "BRIEFING",
            **self.agent_status_patch("success", state.get("agent_statuses", {})),
            **self.token_count_patch(
                state.get("total_tokens_in", 0),
                state.get("total_tokens_out", 0),
                state.get("agent_token_counts", {}),
            ),
            **self.entity_count_patch(state.get("agent_entity_counts", {})),
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Per-entity verification
    # ─────────────────────────────────────────────────────────────────────────

    async def _verify_entity(
        self,
        entity: dict[str, Any],
        city_name: str,
        country: str,
        rel_index: dict[str, list[dict[str, Any]]],
    ) -> dict[str, Any]:
        """
        Generate claims for a single entity, call LLM, parse results.
        Returns a VerificationResult dict.
        """
        eid  = entity.get("entity_id", "")
        name = (
            entity.get("canonical_name")
            or entity.get("name")
            or entity.get("org_name")
            or "Unknown"
        )

        # ── Generate claims ───────────────────────────────────────────────────
        claims = self._generate_claims(entity, city_name, rel_index)

        if not claims:
            # Entity has no verifiable claims — treat as unverifiable
            log.debug("verification_agent: '%s' has no verifiable claims — skipping LLM", name)
            return _unverifiable_result(entity)

        # ── Build prompt ──────────────────────────────────────────────────────
        fields_block       = _build_fields_block(entity)
        relationship_block = _build_relationship_block(entity, rel_index)
        claims_block       = _build_claims_block(claims)

        prompt = VERIFICATION_PROMPT_TEMPLATE.format(
            name=name,
            entity_type=entity.get("entity_type", "unknown"),
            city_name=city_name,
            overall_confidence=entity.get("overall_confidence", "unknown"),
            description=(entity.get("description") or "")[:400],
            fields_block=fields_block,
            relationship_block=relationship_block,
            claim_count=len(claims),
            claims_block=claims_block,
        )

        # ── LLM call ──────────────────────────────────────────────────────────
        try:
            result_json, _meta = await self.llm_generate_json(
                task_type="claim_verification",
                prompt=prompt,
                system=VERIFICATION_SYSTEM_PROMPT,
            )
        except Exception as exc:
            log.warning(
                "verification_agent: LLM call failed for entity '%s': %s",
                name, exc,
            )
            return _unverifiable_result(entity)

        # ── Parse + validate verifications ────────────────────────────────────
        verifications_raw = result_json.get("verifications") if isinstance(result_json, dict) else None

        if not isinstance(verifications_raw, list):
            log.warning(
                "verification_agent: LLM returned malformed response for '%s' — type=%s",
                name, type(result_json).__name__,
            )
            return _unverifiable_result(entity)

        # Parse per-claim verifications, indexing back to the claims list
        claims_verified: list[dict[str, Any]] = []
        has_high_fail = False

        for v in verifications_raw:
            if not isinstance(v, dict):
                continue
            idx = v.get("claim_index")
            if not isinstance(idx, int) or idx < 0 or idx >= len(claims):
                continue

            verdict    = _safe_verdict(v.get("verdict", "unverifiable"))
            confidence = _safe_confidence(v.get("confidence", "low"))
            reasoning  = str(v.get("reasoning", ""))[:300]

            claim_record = {
                "claim_type":  claims[idx]["claim_type"],
                "claim_text":  claims[idx]["claim_text"],
                "verdict":     verdict,
                "confidence":  confidence,
                "reasoning":   reasoning,
            }
            claims_verified.append(claim_record)

            if verdict == "fail" and confidence == "high":
                has_high_fail = True

        # If LLM returned fewer verifications than claims, mark remaining unverifiable
        verified_indices = {
            v.get("claim_index")
            for v in verifications_raw
            if isinstance(v, dict) and isinstance(v.get("claim_index"), int)
        }
        for idx, claim in enumerate(claims):
            if idx not in verified_indices:
                claims_verified.append({
                    "claim_type": claim["claim_type"],
                    "claim_text": claim["claim_text"],
                    "verdict":    "unverifiable",
                    "confidence": "low",
                    "reasoning":  "LLM did not return a verdict for this claim.",
                })

        # ── Determine overall verdict ─────────────────────────────────────────
        all_verdicts = [c["verdict"] for c in claims_verified]
        if "fail" in all_verdicts:
            overall_verdict = "fail"
        elif "unverifiable" in all_verdicts and "pass" not in all_verdicts:
            overall_verdict = "unverifiable"
        else:
            overall_verdict = "pass"

        log.debug(
            "verification_agent: '%s' overall_verdict=%s hard_fail=%s",
            name, overall_verdict, has_high_fail,
        )

        return {
            "entity_id":      eid,
            "entity_name":    name,
            "entity_type":    entity.get("entity_type", "unknown"),
            "claims_verified": claims_verified,
            "overall_verdict": overall_verdict,
            "hard_fail":       has_high_fail,
            "flagged":         has_high_fail,
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Claim generation
    # ─────────────────────────────────────────────────────────────────────────

    def _generate_claims(
        self,
        entity: dict[str, Any],
        city_name: str,
        rel_index: dict[str, list[dict[str, Any]]],
    ) -> list[dict[str, str]]:
        """
        Generate 2–4 verifiable claims for an entity.
        Returns a list of {"claim_type": str, "claim_text": str} dicts.
        Claims are ordered: EXISTENCE, PROFESSIONAL, NOTABLE, BLOCKER/RELATIONSHIP.
        """
        claims: list[dict[str, str]] = []
        eid    = entity.get("entity_id", "")
        etype  = entity.get("entity_type", "unknown")
        name   = (
            entity.get("canonical_name")
            or entity.get("name")
            or entity.get("org_name")
            or "this entity"
        )

        # 1. EXISTENCE claim — always generated
        claims.append({
            "claim_type": CLAIM_EXISTENCE,
            "claim_text": (
                f"{name} is a {_humanize_type(etype)} with a documented presence "
                f"in {city_name}."
            ),
        })

        # 2. PROFESSIONAL claim — generated when employer + title available
        employer = (
            entity.get("current_employer")
            or entity.get("primary_employer")
            or entity.get("employer_name")
            or entity.get("primary_company")
        )
        title = (
            entity.get("title")
            or entity.get("role")
            or entity.get("primary_role")
        )
        if employer and title:
            claims.append({
                "claim_type": CLAIM_PROFESSIONAL,
                "claim_text": (
                    f"{name} currently serves as {title} at {employer}."
                ),
            })
        elif employer:
            claims.append({
                "claim_type": CLAIM_PROFESSIONAL,
                "claim_text": f"{name} is currently affiliated with {employer}.",
            })

        # 3. NOTABLE claim — pick the most informative non-identity fact
        notable = _pick_notable_claim(entity, name, etype)
        if notable:
            claims.append({"claim_type": CLAIM_NOTABLE, "claim_text": notable})

        # 4. BLOCKER claim — only when blocker_risk is high
        blocker_risk = entity.get("score_blocker_risk", 0) or 0
        if blocker_risk >= BLOCKER_EVIDENCE_REQUIRED_ABOVE:
            blocker_evidence = (
                entity.get("blocker_evidence")
                or entity.get("blocker_rationale")
                or ""
            )
            if blocker_evidence:
                claims.append({
                    "claim_type": CLAIM_BLOCKER,
                    "claim_text": (
                        f"{name} poses a meaningful blocking risk to market entrants: "
                        f"{str(blocker_evidence)[:200]}"
                    ),
                })
            else:
                # Fall back to score-based claim
                claims.append({
                    "claim_type": CLAIM_BLOCKER,
                    "claim_text": (
                        f"{name} has been assessed as a significant blocking risk "
                        f"(score={blocker_risk}/100) based on regulatory or political factors."
                    ),
                })

        # 5. RELATIONSHIP claim — one high-strength relationship if exists
        if len(claims) < 4:
            entity_rels = rel_index.get(eid, [])
            high_strength = [
                r for r in entity_rels
                if (r.get("relationship_strength") or 0) >= RELATIONSHIP_CLAIM_MIN_STRENGTH
            ]
            if high_strength:
                # Pick the strongest
                strongest = max(high_strength, key=lambda r: r.get("relationship_strength", 0))
                rel_type   = strongest.get("relationship_type", "is connected to")
                other_id   = (
                    strongest.get("target_id")
                    if strongest.get("source_id") == eid
                    else strongest.get("source_id")
                )
                other_name = strongest.get("target_name") or strongest.get("source_name") or other_id
                claims.append({
                    "claim_type": CLAIM_RELATIONSHIP,
                    "claim_text": (
                        f"{name} has a documented {_humanize_rel_type(rel_type)} "
                        f"relationship with {other_name}."
                    ),
                })

        return claims  # 2–5 claims per entity

    # ─────────────────────────────────────────────────────────────────────────
    # DB side effects
    # ─────────────────────────────────────────────────────────────────────────

    async def _flag_entities(self, entity_ids: list[str]) -> None:
        """
        Set needs_review = true for hard-fail entities via targeted SQL UPDATE.
        Delegates to supabase client method.
        """
        try:
            await self._db.flag_entities_for_review(entity_ids)
            log.info(
                "verification_agent: flagged %d entities for review", len(entity_ids)
            )
        except Exception as exc:
            log.warning(
                "verification_agent: failed to flag entities in DB: %s", exc
            )
            # Non-fatal — flags are best-effort; results are still in state

    async def _write_fail_rejection(
        self,
        entity_id: str,
        result: dict[str, Any],
        state: dict[str, Any],
    ) -> None:
        """Write a rejected_item record for a hard-fail entity."""
        # Find the highest-confidence fail
        fail_claims = [
            c for c in result.get("claims_verified", [])
            if c.get("verdict") == "fail" and c.get("confidence") == "high"
        ]
        lead_claim = fail_claims[0] if fail_claims else {}

        try:
            await self.write_rejected_item(
                stage="verification",
                item_type="entity",
                item_id=entity_id,
                item_snapshot={
                    "entity_id":   entity_id,
                    "entity_name": result.get("entity_name"),
                    "entity_type": result.get("entity_type"),
                    "overall_verdict": result.get("overall_verdict"),
                    "fail_claims": fail_claims[:3],  # cap to keep snapshot small
                },
                rejection_reason="verification_hard_fail",
                rejection_detail=(
                    f"High-confidence fail: {lead_claim.get('claim_text', '')} "
                    f"— {lead_claim.get('reasoning', '')}"
                )[:500],
            )
        except Exception as exc:
            log.warning(
                "verification_agent: failed to write rejection for entity '%s': %s",
                entity_id, exc,
            )

    async def _write_assessments(
        self,
        all_results: dict[str, dict[str, Any]],
        state: dict[str, Any],
    ) -> None:
        """
        Write one analytical_assessments record per entity.
        Best-effort — individual failures logged but do not abort.
        """
        for eid, result in all_results.items():
            claim_count       = len(result.get("claims_verified", []))
            overall_verdict   = result.get("overall_verdict", "unverifiable")
            entity_name       = result.get("entity_name", eid)

            claim_text = (
                f"Verification of {entity_name}: {claim_count} claim(s) evaluated. "
                f"Overall verdict: {overall_verdict}."
            )
            if result.get("hard_fail"):
                claim_text += " Entity flagged for human review (high-confidence fail)."

            assessment = {
                "run_id":            state["run_id"],
                "entity_id":         eid,
                "assessment_type":   "claim_verification",
                "claim_text":        claim_text,
                "claim_json":        {
                    "entity_id":        eid,
                    "entity_name":      entity_name,
                    "overall_verdict":  overall_verdict,
                    "hard_fail":        result.get("hard_fail", False),
                    "claims_verified":  result.get("claims_verified", []),
                },
                "framework_name":    "claim_verification",
                "framework_version": self.AGENT_VERSION,
                "model_used":        settings.ollama_default_model,
                "prompt_version":    self.AGENT_VERSION,
                "confidence":        overall_verdict,   # reuse verdict as confidence label
                "needs_review":      result.get("hard_fail", False),
                "created_at":        datetime.now(timezone.utc).isoformat(),
            }

            try:
                await self.write_assessment(assessment)
            except Exception as exc:
                log.warning(
                    "verification_agent: failed to write assessment for entity '%s': %s",
                    entity_name, exc,
                )


# ─────────────────────────────────────────────────────────────────────────────
# Prompt building helpers (module-level, no DB access needed)
# ─────────────────────────────────────────────────────────────────────────────

def _build_fields_block(entity: dict[str, Any]) -> str:
    """Build a readable key=value block from the entity's most informative fields."""
    lines: list[str] = []
    for field in EVIDENCE_FIELDS_ORDER:
        val = entity.get(field)
        if val is None:
            continue
        if isinstance(val, list):
            if val:
                lines.append(f"  {field}: {', '.join(str(v) for v in val[:5])}")
        elif isinstance(val, dict):
            pass  # skip nested dicts — too noisy
        else:
            text = str(val)
            if text and text.lower() not in ("none", "null", "false", ""):
                lines.append(f"  {field}: {text[:150]}")
        if len(lines) >= 12:
            break  # cap at 12 evidence fields — keep prompt bounded
    return "\n".join(lines) if lines else "  (no structured data fields available)"


def _build_relationship_block(
    entity: dict[str, Any],
    rel_index: dict[str, list[dict[str, Any]]],
) -> str:
    """Summarise up to 5 relationships for the entity."""
    eid = entity.get("entity_id", "")
    edges = rel_index.get(eid, [])
    if not edges:
        return "  (no relationships recorded)"

    # Sort by strength descending
    top_edges = sorted(edges, key=lambda e: e.get("relationship_strength", 0), reverse=True)[:5]
    lines: list[str] = []
    for edge in top_edges:
        rel_type   = edge.get("relationship_type", "CONNECTED_TO")
        strength   = edge.get("relationship_strength", 0.0)
        source_id  = edge.get("source_id", "")
        other_name = (
            edge.get("target_name") if source_id == eid else edge.get("source_name")
        ) or "unknown"
        direction = "→" if source_id == eid else "←"
        lines.append(
            f"  {direction} {_humanize_rel_type(rel_type)} {other_name} "
            f"(strength={strength:.2f})"
        )
    return "\n".join(lines)


def _build_claims_block(claims: list[dict[str, str]]) -> str:
    """Format numbered claims for the LLM prompt."""
    return "\n".join(
        f"  [{i}] ({c['claim_type']}) {c['claim_text']}"
        for i, c in enumerate(claims)
    )


def _pick_notable_claim(entity: dict[str, Any], name: str, etype: str) -> str | None:
    """
    Pick the single most informative notable fact about this entity.
    Returns a complete sentence or None if nothing notable found.
    """
    # Investor: AUM or notable portfolio
    if etype == "investor":
        aum = entity.get("total_aum_usd")
        if aum:
            return f"{name} manages approximately ${aum:,.0f} in assets under management."
        portfolio = entity.get("portfolio_companies")
        if portfolio and isinstance(portfolio, list) and portfolio:
            top = portfolio[:3]
            return f"{name} has invested in {', '.join(str(p) for p in top)}."

    # Corporate: revenue or founding year
    if etype == "corporate":
        revenue = entity.get("annual_revenue_usd")
        if revenue:
            return f"{name} reports annual revenue of approximately ${revenue:,.0f}."
        founded = entity.get("founded_year")
        if founded:
            return f"{name} was founded in {founded}."

    # Politician: office held
    if etype in ("politician", "political"):
        office = entity.get("office_held") or entity.get("party_affiliation")
        if office:
            return f"{name} holds or is affiliated with: {office}."

    # Philanthropic: total assets or grant programs
    if etype == "philanthropic":
        assets = entity.get("total_assets_usd")
        if assets:
            return f"{name} holds approximately ${assets:,.0f} in foundation assets."
        grantees = entity.get("grantee_names")
        if grantees and isinstance(grantees, list) and grantees:
            return f"{name} has funded organizations including {grantees[0]}."

    # Nonprofit: EIN or assets
    if etype == "nonprofit":
        ein = entity.get("ein")
        if ein:
            return f"{name} is a registered nonprofit (EIN: {ein})."
        assets = entity.get("total_assets_usd")
        if assets:
            return f"{name} reports {assets:,.0f} in total assets."

    # Executive/HNWI: board seats or founder status
    if etype in ("executive_hnw", "hnwi"):
        boards = entity.get("board_seats")
        if boards and isinstance(boards, list) and boards:
            return f"{name} sits on the board of {boards[0]}."
        if entity.get("is_founder"):
            company = entity.get("primary_company") or entity.get("primary_employer")
            if company:
                return f"{name} co-founded {company}."

    # Community leader: notable role
    if etype == "community_leader":
        role = entity.get("primary_role") or entity.get("role")
        org  = entity.get("primary_employer") or entity.get("employer_name")
        if role and org:
            return f"{name} serves as {role} at {org}."

    # Illicit: sanction status or court cases
    if etype == "illicit":
        sanction = entity.get("sanction_status")
        if sanction:
            return f"{name} carries a recorded sanction status: {str(sanction)[:100]}."
        cases = entity.get("court_cases")
        if cases and isinstance(cases, list) and cases:
            return f"{name} is party to {len(cases)} documented legal proceeding(s)."

    # Generic fallback: description snippet
    desc = entity.get("description") or entity.get("bio") or entity.get("headline")
    if desc and len(str(desc)) > 30:
        return f"{name}: {str(desc)[:180].rstrip()}."

    return None


def _humanize_type(etype: str) -> str:
    labels = {
        "investor":         "venture capital or angel investor",
        "philanthropic":    "philanthropic foundation",
        "corporate":        "corporate entity",
        "political":        "political organization",
        "nonprofit":        "nonprofit organization",
        "executive_hnw":    "high-net-worth executive",
        "community_leader": "community leader",
        "politician":       "elected official or political figure",
        "hnwi":             "high-net-worth individual",
        "illicit":          "entity flagged for potential illicit activity",
    }
    return labels.get(etype, etype.replace("_", " "))


def _humanize_rel_type(rel_type: str) -> str:
    labels = {
        "INVESTED_IN":              "investor-portfolio",
        "FOUNDED":                  "founder",
        "EMPLOYED_BY":              "employment",
        "SITS_ON_BOARD_OF":         "board membership",
        "DONATED_TO":               "political donation",
        "RECEIVED_GRANT_FROM":      "grant recipient",
        "LITIGATION_AGAINST":       "litigation",
        "SUBSIDIARY_OF":            "corporate subsidiary",
        "CO_INVESTOR_WITH":         "co-investment",
        "CO_FOUNDED_WITH":          "co-founder",
        "MENTIONED_WITH":           "co-mention",
        "POLITICALLY_CONNECTED_TO": "political connection",
        "REGULATORY_OVERSIGHT":     "regulatory oversight",
    }
    return labels.get(rel_type, rel_type.replace("_", " ").lower())


def _safe_verdict(raw: Any) -> str:
    if raw in ("pass", "fail", "unverifiable"):
        return raw
    return "unverifiable"


def _safe_confidence(raw: Any) -> str:
    if raw in ("high", "medium", "low"):
        return raw
    return "low"


def _unverifiable_result(entity: dict[str, Any]) -> dict[str, Any]:
    """Return a benign unverifiable result (no LLM data, no flags)."""
    eid  = entity.get("entity_id", "")
    name = (
        entity.get("canonical_name")
        or entity.get("name")
        or entity.get("org_name")
        or eid
    )
    return {
        "entity_id":       eid,
        "entity_name":     name,
        "entity_type":     entity.get("entity_type", "unknown"),
        "claims_verified": [],
        "overall_verdict": "unverifiable",
        "hard_fail":       False,
        "flagged":         False,
    }
