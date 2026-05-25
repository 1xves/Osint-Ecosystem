"""
osint/agents/orchestrator.py

Orchestrator Agent — pipeline entry point.

Responsibilities:
1. Accept run parameters (city_name, country_or_region)
2. Generate scope_parameters: market size estimate, known characteristics, priority categories
3. Generate 4 framings (mainstream / heterodox / adjacent_domain / practitioner)
4. Write run record to Postgres
5. Initialize rate limit state in Redis
6. Return state patch that kicks off Phase 1 collection

This agent does NOT collect data — it only scopes the run.
All LLM output goes to analytical_assessments (framing type).
No entities are produced here.

Framing purpose: the 4 framings give collection agents 4 different angles
to search from. Each framing names what to look for and why.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from osint.agents.base import BaseAgent
from osint.core.config import RATE_LIMITS, settings
from osint.llm.routing import TaskType

log = logging.getLogger(__name__)

SCOPE_SYSTEM_PROMPT = """You are a startup ecosystem intelligence analyst.
You specialize in understanding the power structures, capital flows, and key players
in city-level startup ecosystems in the United States.

When given a city and country, your job is to:
1. Estimate market characteristics (ecosystem maturity, known industries, notable characteristics)
2. Identify which entity categories are most important to investigate for this specific city
3. Generate distinct analytical framings — each offering a different lens on who matters and why

Be specific to the city. Do not give generic answers. If you know specific characteristics
of this city's ecosystem, include them. If you don't, say so — do not fabricate specifics.

Always respond with valid JSON."""

SCOPE_PROMPT_TEMPLATE = """Analyze the startup ecosystem for: {city_name}, {country_or_region}

Generate the following JSON object:

{{
  "market_size_estimate": "<small|medium|large|major> — estimated relative scale of this startup ecosystem",
  "known_characteristics": [
    "<list of known facts or characteristics about this city's startup ecosystem — be specific>"
  ],
  "priority_categories": {{
    "<entity_type>": "<reason this category is particularly important for this city>"
  }},
  "key_search_terms": [
    "<city-specific terms, industry names, neighborhood names, or organizations likely to appear in searches>"
  ],
  "data_availability_note": "<honest assessment of how much structured data will be available for this city vs. requires SerpAPI fallback>"
}}

entity_type options: investor, philanthropic, corporate, political, nonprofit, executive_hnw, community_leader, politician, hnwi, illicit"""

FRAMING_SYSTEM_PROMPT = """You are a strategic intelligence analyst generating analytical framings
for a startup ecosystem investigation. Each framing is a distinct perspective or lens
through which to understand power, capital, and influence in the ecosystem.

Framings are used to guide collection agents — each framing tells an agent what angle to
search from and what kinds of entities to prioritize.

Generate exactly 4 framings, one of each type. Be concrete and city-specific."""

FRAMING_PROMPT_TEMPLATE = """Generate 4 analytical framings for the startup ecosystem of {city_name}, {country_or_region}.

Context about this ecosystem:
{scope_summary}

Return a JSON object with a "framings" key containing exactly 4 framing objects, one per framing_type:

{{
  "framings": [
    {{
      "framing_type": "mainstream",
      "framing_label": "<short label>",
      "framing_description": "<what lens this applies — 2-3 sentences>",
      "entities_to_prioritize": ["<entity_type1>", "<entity_type2>"],
      "search_angle": "<what to specifically look for under this framing>"
    }},
    {{
      "framing_type": "heterodox",
      "framing_label": "<short label>",
      "framing_description": "<contrarian or non-obvious perspective>",
      "entities_to_prioritize": ["<entity_type1>"],
      "search_angle": "<what to look for>"
    }},
    {{
      "framing_type": "adjacent_domain",
      "framing_label": "<short label>",
      "framing_description": "<a domain adjacent to tech startups that shapes this ecosystem>",
      "entities_to_prioritize": ["<entity_type1>", "<entity_type2>"],
      "search_angle": "<what to look for>"
    }},
    {{
      "framing_type": "practitioner",
      "framing_label": "<short label>",
      "framing_description": "<the on-the-ground practitioner view — operators, connectors, builders>",
      "entities_to_prioritize": ["<entity_type1>", "<entity_type2>"],
      "search_angle": "<what to look for>"
    }}
  ]
}}"""


class OrchestratorAgent(BaseAgent):
    """
    Pipeline entry point. Scopes the run and generates analytical framings.
    """

    AGENT_NAME = "orchestrator"
    AGENT_VERSION = "1.0"

    async def run(self, state: dict[str, Any]) -> dict[str, Any]:
        city_name = state["city_name"]
        country_or_region = state["country_or_region"]
        run_id = state["run_id"]
        city_key = state["city_key"]

        log.info("Orchestrator: scoping run for %s, %s", city_name, country_or_region)

        # ── Step 1: Write/update the run record in Postgres ───────────────────
        await self._db.upsert_run({
            "run_id":               run_id,
            "city_name":            city_name,
            "country_or_region":    country_or_region,
            "city_key":             city_key,
            "run_status":           "running",
            "model_default":        settings.ollama_default_model,
            "model_escalation":     settings.ollama_escalation_model,
            "triggered_by":         state.get("operator_id"),
            "trigger_type":         "manual",
        })

        # ── Step 2: Initialize run state in Redis ─────────────────────────────
        await self._redis.set_run_status(run_id, "running")
        await self._redis.set_run_phase(run_id, "INIT")
        await self._redis.publish_run_event(run_id, {
            "event": "phase_change",
            "phase": "INIT",
            "timestamp": self.now_iso(),
        })

        # ── Step 3: Generate scope parameters via LLM ─────────────────────────
        scope_prompt = SCOPE_PROMPT_TEMPLATE.format(
            city_name=city_name,
            country_or_region=country_or_region,
        )
        scope_json, scope_meta = await self.llm_generate_json(
            task_type=TaskType.FRAMING_GENERATION,
            prompt=scope_prompt,
            system=SCOPE_SYSTEM_PROMPT,
        )
        log.info("Orchestrator: scope generated — %s market, %d priority categories",
                 scope_json.get("market_size_estimate"),
                 len(scope_json.get("priority_categories", {})))

        # Write scope as analytical assessment
        scope_assessment_id = await self.write_assessment({
            "run_id":           run_id,
            "assessment_type":  "framing",
            "claim_text":       f"Ecosystem scope for {city_name}: {scope_json.get('market_size_estimate')} market",
            "claim_json":       scope_json,
            "framework_name":   "ecosystem_scope_v1",
            "framework_version": self.AGENT_VERSION,
            "derived_from":     [],
            "model_used":       scope_meta.get("model", settings.ollama_default_model),
            "prompt_version":   self.AGENT_VERSION,
            "confidence":       "medium",  # Scope is always medium confidence — it's an initial estimate
            "needs_review":     False,
            "is_current":       True,
        })

        # ── Step 4: Generate 4 framings ───────────────────────────────────────
        scope_summary = json.dumps({
            "market_size": scope_json.get("market_size_estimate"),
            "characteristics": scope_json.get("known_characteristics", [])[:5],
            "priority_categories": list(scope_json.get("priority_categories", {}).keys()),
        }, indent=2)

        framing_prompt = FRAMING_PROMPT_TEMPLATE.format(
            city_name=city_name,
            country_or_region=country_or_region,
            scope_summary=scope_summary,
        )
        framings_raw, framing_meta = await self.llm_generate_json(
            task_type=TaskType.FRAMING_GENERATION,
            prompt=framing_prompt,
            system=FRAMING_SYSTEM_PROMPT,
        )

        # Normalize: LLM may return list or {"framings": [...]}
        if isinstance(framings_raw, list):
            framings_list = framings_raw
        elif isinstance(framings_raw, dict):
            framings_list = framings_raw.get("framings", list(framings_raw.values()))
        else:
            framings_list = []
            log.warning("Orchestrator: unexpected framing output type %s", type(framings_raw))

        # Validate and enrich framings
        valid_framing_types = {"mainstream", "heterodox", "adjacent_domain", "practitioner"}
        framings: list[dict[str, Any]] = []
        seen_types: set[str] = set()

        for raw_framing in framings_list:
            if not isinstance(raw_framing, dict):
                continue
            framing_type = raw_framing.get("framing_type")
            if framing_type not in valid_framing_types:
                log.warning("Orchestrator: invalid framing_type '%s' — skipping", framing_type)
                continue
            if framing_type in seen_types:
                log.warning("Orchestrator: duplicate framing_type '%s' — skipping", framing_type)
                continue
            seen_types.add(framing_type)

            framing = {
                "framing_id":           self.new_uuid(),
                "run_id":               run_id,
                "framing_type":         framing_type,
                "framing_label":        raw_framing.get("framing_label", framing_type),
                "framing_description":  raw_framing.get("framing_description", ""),
                "entities_to_prioritize": raw_framing.get("entities_to_prioritize", []),
                "search_angle":         raw_framing.get("search_angle", ""),
                "model_used":           framing_meta.get("model", settings.ollama_default_model),
                "prompt_version":       self.AGENT_VERSION,
                "generated_at":         self.now_iso(),
            }
            framings.append(framing)

            # Write each framing as an analytical assessment
            await self.write_assessment({
                "run_id":           run_id,
                "assessment_type":  "framing",
                "claim_text":       f"{framing_type}: {framing['framing_label']} — {framing['framing_description'][:200]}",
                "claim_json":       framing,
                "framework_name":   f"framing_{framing_type}_v1",
                "framework_version": self.AGENT_VERSION,
                "derived_from":     [],
                "model_used":       framing["model_used"],
                "prompt_version":   self.AGENT_VERSION,
                "confidence":       "medium",
                "needs_review":     False,
                "is_current":       True,
            })

        # Fill in any missing framing types with generic fallbacks
        # so the pipeline always has all 4 framings regardless of LLM output quality.
        missing_types = valid_framing_types - seen_types
        if missing_types:
            log.warning(
                "Orchestrator: LLM missed framing types %s — injecting generic fallbacks",
                missing_types,
            )
            _fallbacks: dict[str, dict[str, Any]] = {
                "mainstream": {
                    "framing_type": "mainstream",
                    "framing_label": "Established Ecosystem",
                    "framing_description": (
                        f"The dominant narrative around {city_name}'s startup ecosystem — "
                        "who the recognized players are, what sectors get attention, "
                        "and where institutional capital flows."
                    ),
                    "entities_to_prioritize": ["investor", "corporate", "executive_hnw"],
                    "search_angle": f"Top investors, accelerators, and tech companies in {city_name}",
                },
                "heterodox": {
                    "framing_type": "heterodox",
                    "framing_label": "Hidden Influence",
                    "framing_description": (
                        f"Non-obvious power brokers and contrarian forces shaping {city_name}'s "
                        "ecosystem — political connections, regulatory leverage, and actors "
                        "not typically covered in tech press."
                    ),
                    "entities_to_prioritize": ["political", "politician", "illicit"],
                    "search_angle": f"Political donors, lobbyists, and regulatory actors in {city_name}",
                },
                "adjacent_domain": {
                    "framing_type": "adjacent_domain",
                    "framing_label": "Adjacent Sectors",
                    "framing_description": (
                        f"Domains adjacent to tech that shape {city_name}'s ecosystem — "
                        "real estate, healthcare, education, manufacturing, and logistics "
                        "interests that intersect with startup activity."
                    ),
                    "entities_to_prioritize": ["corporate", "nonprofit", "philanthropic"],
                    "search_angle": f"Healthcare, real estate, and education institutions in {city_name}",
                },
                "practitioner": {
                    "framing_type": "practitioner",
                    "framing_label": "Operators & Builders",
                    "framing_description": (
                        f"The on-the-ground view of {city_name}'s ecosystem — founders, "
                        "operators, community organizers, and connectors who are building "
                        "rather than just funding."
                    ),
                    "entities_to_prioritize": ["executive_hnw", "community_leader", "hnwi"],
                    "search_angle": f"Founders, operators, and community leaders in {city_name}",
                },
            }
            for missing in missing_types:
                fallback = _fallbacks[missing].copy()
                fallback["city_name"]  = city_name
                fallback["run_id"]     = run_id
                fallback["generated_at"] = self.now_iso()
                framings.append(fallback)

        log.info("Orchestrator: %d framings generated", len(framings))

        # ── Step 5: Initialize rate limit state snapshot ──────────────────────
        rate_limit_state = await self._redis.get_all_rate_states(list(RATE_LIMITS.keys()))

        # ── Step 6: Build and return state patch ─────────────────────────────
        await self._redis.set_run_phase(run_id, "COLLECTION_PASS1")
        await self._redis.publish_run_event(run_id, {
            "event": "phase_change",
            "phase": "COLLECTION_PASS1",
            "timestamp": self.now_iso(),
        })

        patch: dict[str, Any] = {
            "run_status":       "running",
            "current_phase":    "COLLECTION_PASS1",
            "scope_parameters": scope_json,
            "framings":         framings,
            "gate_cleared":     False,
            "pass_number":      1,
            "pass2_targets":    [],
            "rate_limit_state": rate_limit_state,

            # Agent tracking
            **self.agent_status_patch("success", state.get("agent_statuses", {})),
            **self.token_count_patch(
                state.get("total_tokens_in", 0),
                state.get("total_tokens_out", 0),
                state.get("agent_token_counts", {}),
            ),
            **self.entity_count_patch(state.get("agent_entity_counts", {})),
        }

        return patch
