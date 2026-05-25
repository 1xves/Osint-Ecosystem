"""
osint/agents/briefing.py

Briefing Agent — final node in the OSINT pipeline.

Responsibility:
    1. Consume verified_entity_ids, scored_entities, ranked_lists,
       relationships_draft, gap_analysis, and verification_summary.
    2. Generate a structured 20-section intelligence brief using LLM
       (task_type="brief_drafting" → qwen3:14b), one call per section.
    3. All 20 sections are generated concurrently via asyncio.gather.
    4. Assemble briefing_json (structured dict) and briefing_markdown.
    5. Write the complete briefing to DB as an analytical_assessments record
       (assessment_type="final_briefing").
    6. Call complete_run() to finalize the run record.
    7. Return state patch with briefing_json, briefing_markdown, current_phase="DONE".

20 Sections:
    Part I — Ecosystem Overview
        1. Executive Summary
        2. Market Landscape

    Part II — Stakeholder Categories (one per entity type)
        3. Investor Ecosystem
        4. Corporate Players
        5. Nonprofit & Accelerator Ecosystem
        6. Political Organizations
        7. Philanthropic Foundations
        8. Political Figures
        9. Key Executives & Founders
        10. Community Leaders & Civic Influencers
        11. High-Net-Worth Individuals
        12. Illicit Risk Assessment

    Part III — Intelligence Analysis
        13. Network Topology & Key Connectors
        14. Top Ecosystem Influencers
        15. Strategic Partnership Targets
        16. Competitive Threat Assessment
        17. Regulatory & Blocking Risks
        18. Investment & Capital Opportunities

    Part IV — Quality & Confidence
        19. Coverage Gaps & Recommendations
        20. Intelligence Confidence Assessment

Each section LLM response:
    {
      "summary": "<2-5 sentence narrative>",
      "key_points": ["...", ...],          // 3-7 items
      "notable_entities": [               // 0-5 items
        {"entity_id": "...", "name": "...", "notes": "<one line>"}
      ],
      "confidence_note": "<optional caveat or null>"
    }

Fallback (LLM failure or parse error): structured section with empty narrative,
entity list derived directly from state — briefing never omits a section.

LLM model:
    TaskType: "brief_drafting"  →  qwen3:14b
    One concurrent call per section (20 total).

DB side effects:
    - Writes one analytical_assessments record (assessment_type="final_briefing").
    - Calls complete_run() to finalize the run record with aggregate statistics.

State fields set:
    briefing_json       dict[str, Any]
    briefing_markdown   str
    current_phase       "DONE"

State fields NOT modified:
    All upstream fields are read-only in this final node.
"""

from __future__ import annotations

import asyncio
import json
import logging
import textwrap
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from osint.agents.base import BaseAgent
from osint.core.config import settings

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

AGENT_NAME    = "briefing_agent"
AGENT_VERSION = "1.0"

# Maximum entities to include in each section's LLM context
MAX_ENTITIES_PER_SECTION     = 8
MAX_ENTITIES_RANKED_SECTION  = 10
MAX_RELATIONSHIPS_IN_CONTEXT = 15

# Maximum characters for entity description snippets in prompts
DESCRIPTION_MAX_CHARS = 200

# ─────────────────────────────────────────────────────────────────────────────
# Section Definitions
# ─────────────────────────────────────────────────────────────────────────────

# Each entry: (section_id, part, title, section_type, entity_type_or_dimension)
# section_type:
#   "overview"   → uses aggregate stats across all entity types
#   "market"     → uses scope_parameters + framings
#   "category"   → filtered by entity_type, sorted by score_influence
#   "ranked"     → uses a specific ranked_lists dimension, top N
#   "network"    → uses relationships_draft
#   "gaps"       → uses gap_analysis + pass2_targets
#   "confidence" → uses verification_summary + agent_statuses

SECTIONS: list[dict[str, str]] = [
    # Part I: Ecosystem Overview
    {
        "section_id":  "executive_summary",
        "part":        "I",
        "title":       "Executive Summary",
        "type":        "overview",
        "aux":         "",
        "description": "High-level synthesis of the startup ecosystem's most important actors, dynamics, and intelligence findings. Cover breadth of entities found, standout relationships, and top intelligence conclusions.",
    },
    {
        "section_id":  "market_landscape",
        "part":        "I",
        "title":       "Market Landscape",
        "type":        "market",
        "aux":         "",
        "description": "Overview of the city's market context, startup ecosystem size, known characteristics, and opportunity landscape based on collected scope parameters and analyst framings.",
    },
    # Part II: Stakeholder Categories
    {
        "section_id":  "investor_ecosystem",
        "part":        "II",
        "title":       "Investor Ecosystem",
        "type":        "category",
        "aux":         "investor",
        "description": "Venture capital firms, angel investors, family offices, and institutional investors active in or relevant to the local startup ecosystem. Note fund sizes, portfolio focus, and investment activity.",
    },
    {
        "section_id":  "corporate_players",
        "part":        "II",
        "title":       "Corporate Players",
        "type":        "category",
        "aux":         "corporate",
        "description": "Large corporations with local footprint, corporate venture arms, or strategic relevance to startups. Highlight acquisition appetite, partnership history, and market influence.",
    },
    {
        "section_id":  "nonprofit_accelerators",
        "part":        "II",
        "title":       "Nonprofit & Accelerator Ecosystem",
        "type":        "category",
        "aux":         "nonprofit",
        "description": "Nonprofits, accelerators, incubators, and ecosystem support organizations. Highlight programming, mentorship capacity, and startup support infrastructure.",
    },
    {
        "section_id":  "political_organizations",
        "part":        "II",
        "title":       "Political Organizations",
        "type":        "category",
        "aux":         "political",
        "description": "Political action committees, party organizations, and advocacy groups with local influence. Highlight relevant policy positions, funding patterns, and ecosystem impact.",
    },
    {
        "section_id":  "philanthropic_foundations",
        "part":        "II",
        "title":       "Philanthropic Foundations",
        "type":        "category",
        "aux":         "philanthropic",
        "description": "Foundations and major philanthropic donors supporting the ecosystem. Note grant focus areas, asset size, and alignment with innovation or entrepreneurship.",
    },
    {
        "section_id":  "political_figures",
        "part":        "II",
        "title":       "Political Figures",
        "type":        "category",
        "aux":         "politician",
        "description": "Elected officials and public figures with policy influence over the local business environment. Highlight regulatory positions, committee assignments, and startup-relevant legislation.",
    },
    {
        "section_id":  "executives_founders",
        "part":        "II",
        "title":       "Key Executives & Founders",
        "type":        "category",
        "aux":         "executive_hnw",
        "description": "High-profile executives, serial founders, and high-net-worth operators driving the ecosystem. Highlight founding history, board roles, and influence networks.",
    },
    {
        "section_id":  "community_leaders",
        "part":        "II",
        "title":       "Community Leaders & Civic Influencers",
        "type":        "category",
        "aux":         "community_leader",
        "description": "Civic leaders, media personalities, and community organizers with outsized local influence. Highlight platform size, relationship networks, and ecosystem engagement.",
    },
    {
        "section_id":  "hnwi",
        "part":        "II",
        "title":       "High-Net-Worth Individuals",
        "type":        "category",
        "aux":         "hnwi",
        "description": "Documented high-net-worth individuals and family offices. Highlight wealth sources, investment interests, and community influence.",
    },
    {
        "section_id":  "illicit_risk",
        "part":        "II",
        "title":       "Illicit Risk Assessment",
        "type":        "category",
        "aux":         "illicit",
        "description": "Entities flagged for potential illicit activity, sanctions exposure, or regulatory violations. Treat absence of illicit actors as positive intelligence. Note any OFAC findings or court involvement.",
    },
    # Part III: Intelligence Analysis
    {
        "section_id":  "network_topology",
        "part":        "III",
        "title":       "Network Topology & Key Connectors",
        "type":        "network",
        "aux":         "",
        "description": "High-density relationship clusters, bridge entities that connect different ecosystem segments, and isolated actors with weak network ties. Identify the most central connectors.",
    },
    {
        "section_id":  "top_influencers",
        "part":        "III",
        "title":       "Top Ecosystem Influencers",
        "type":        "ranked",
        "aux":         "score_influence",
        "description": "Entities with the highest overall ecosystem influence scores. These are the individuals and organizations most capable of opening doors, shaping narratives, and driving systemic change.",
    },
    {
        "section_id":  "partnership_targets",
        "part":        "III",
        "title":       "Strategic Partnership Targets",
        "type":        "ranked",
        "aux":         "score_partner_potential",
        "description": "Entities with the highest strategic partnership value for a startup market entrant. Highlight mutual interests, relationship pathways, and engagement recommendations.",
    },
    {
        "section_id":  "competitive_landscape",
        "part":        "III",
        "title":       "Competitive Threat Assessment",
        "type":        "ranked",
        "aux":         "score_competitor_potential",
        "description": "Entities identified as potential competitors. Highlight their market position, startup engagement history, and threat vectors.",
    },
    {
        "section_id":  "blocking_risks",
        "part":        "III",
        "title":       "Regulatory & Blocking Risks",
        "type":        "ranked",
        "aux":         "score_blocker_risk",
        "description": "Entities with the highest risk of creating regulatory, legal, or political obstacles for a market entrant. Include specific risk factors where available.",
    },
    {
        "section_id":  "investment_opportunities",
        "part":        "III",
        "title":       "Investment & Capital Opportunities",
        "type":        "ranked",
        "aux":         "score_investment_potential",
        "description": "Entities with the highest value as investment or grant recipients, or as sources of capital. Include portfolio fit and engagement pathway notes.",
    },
    # Phase 11.4 additions — Part III continued
    {
        "section_id":  "offshore_sanctions_risk",
        "part":        "III",
        "title":       "Offshore & Sanctions Risk",
        "type":        "category",
        "aux":         "",
        "description": (
            "Entities flagged in ICIJ Offshore Leaks (Panama Papers, Paradise Papers, Pandora Papers) "
            "or on OFAC / UN / EU / BIS sanctions lists. For each flagged entity, state the dataset, "
            "offshore jurisdiction or sanctions program, and known officer or beneficial owner linkages. "
            "All findings are from leaked documents — presence does NOT constitute proof of wrongdoing. "
            "Treat as indicators requiring further due diligence. If no entities are flagged, state that explicitly."
        ),
    },
    {
        "section_id":  "litigation_history",
        "part":        "III",
        "title":       "Litigation History",
        "type":        "category",
        "aux":         "",
        "description": (
            "Active and historical legal proceedings involving ecosystem entities, drawn from CourtListener / PACER data. "
            "Include case names, courts, filing dates, case types (civil, criminal, bankruptcy), and current status where known. "
            "Focus on cases that represent material risk to partners, investors, or market entrants. "
            "If no litigation records exist in the dataset, state that explicitly."
        ),
    },
    {
        "section_id":  "capital_network",
        "part":        "III",
        "title":       "Capital Network",
        "type":        "network",
        "aux":         "",
        "description": (
            "Map the flow of capital through the ecosystem: who funds whom, who receives grants, "
            "who holds real estate portfolios (HUD data), and which investors are co-invested across portfolio companies. "
            "Identify the most connected capital nodes — entities that sit at the intersection of multiple funding flows. "
            "Draw on HUD property portfolios, EDGAR ownership filings, Form D SEC disclosures, "
            "and investor co-investment relationships where available."
        ),
    },
    # Part IV: Quality & Confidence
    {
        "section_id":  "coverage_gaps",
        "part":        "IV",
        "title":       "Coverage Gaps & Recommendations",
        "type":        "gaps",
        "aux":         "",
        "description": "Entity categories with thin coverage relative to targets. Identify what was searched, what was found, and recommendations for further investigation.",
    },
    {
        "section_id":  "confidence_assessment",
        "part":        "IV",
        "title":       "Intelligence Confidence Assessment",
        "type":        "confidence",
        "aux":         "",
        "description": "Overall reliability of this intelligence report. Covers verification outcomes, data source quality, entity confidence distribution, and caveats the reader should be aware of.",
    },
]

assert len(SECTIONS) == 23, f"Expected 23 sections, got {len(SECTIONS)}"

# ─────────────────────────────────────────────────────────────────────────────
# Prompts
# ─────────────────────────────────────────────────────────────────────────────

BRIEFING_SYSTEM_PROMPT = (
    "You are a senior intelligence analyst producing a structured ecosystem intelligence brief. "
    "Write clearly and precisely — no filler, no preamble, no repetition. "
    "Each insight should be grounded in the entity data provided. "
    "When data is thin, say so briefly rather than speculating. "
    "Always respond with valid JSON only — no markdown, no explanation outside the JSON object."
)

SECTION_PROMPT_TEMPLATE = """\
INTELLIGENCE BRIEF: {city_name}, {country}
SECTION: {section_title} (Part {part})
TASK: {description}

CITY CONTEXT
{city_context}

{section_context}

Write this section of the intelligence brief. Respond with:
{{
  "summary": "<2–5 sentences of analytical narrative. Cite specific entity names where relevant>",
  "key_points": [
    "<specific insight with entity name>",
    "<specific insight with entity name>"
  ],
  "notable_entities": [
    {{"entity_id": "<uuid>", "name": "<name>", "notes": "<one-line observation>"}}
  ],
  "confidence_note": "<optional: data quality caveat, or null if data is solid>"
}}

Rules:
- key_points: 3–7 items. Each must be specific and actionable.
- notable_entities: 0–5 items. Only entities directly referenced in your summary.
- confidence_note: Include only if there is a meaningful caveat. Use null otherwise.
- No speculation beyond the data provided. If data is sparse, say so.
"""


# ─────────────────────────────────────────────────────────────────────────────
# Agent
# ─────────────────────────────────────────────────────────────────────────────

class BriefingAgent(BaseAgent):
    """
    Generates the final 20-section intelligence brief.

    All 20 sections are drafted concurrently. Failures in individual sections
    produce fallback content — the briefing is never incomplete.
    """

    AGENT_NAME    = AGENT_NAME
    AGENT_VERSION = AGENT_VERSION

    async def run(self, state: dict[str, Any]) -> dict[str, Any]:
        city_name   = state.get("city_name", "Unknown City")
        country     = state.get("country_or_region", "United States")
        run_id      = state["run_id"]

        scored_entities: list[dict[str, Any]]     = state.get("scored_entities", [])
        ranked_lists: dict[str, list[str]]         = state.get("ranked_lists", {})
        relationships: list[dict[str, Any]]        = state.get("relationships_draft", [])
        gap_analysis: dict[str, dict[str, Any]]    = state.get("gap_analysis", {})
        verification_summary: dict[str, Any]       = state.get("verification_summary", {})
        verified_entity_ids: list[str]             = state.get("verified_entity_ids", [])
        agent_statuses: dict[str, str]             = state.get("agent_statuses", {})
        scope_parameters: dict[str, Any]           = state.get("scope_parameters", {})
        framings: list[dict[str, Any]]             = state.get("framings", [])

        log.info(
            "briefing_agent: drafting %d-section brief for %s — %d verified entities",
            len(SECTIONS), city_name, len(verified_entity_ids),
        )

        # ── Pre-build indices ─────────────────────────────────────────────────
        # Filter scored_entities to verified scope
        verified_set = set(verified_entity_ids) if verified_entity_ids else {
            e.get("entity_id") for e in scored_entities
        }
        verified_entities = [
            e for e in scored_entities
            if e.get("entity_id") in verified_set
        ]

        # Index by entity_id for fast lookup
        entity_index: dict[str, dict[str, Any]] = {
            e["entity_id"]: e
            for e in verified_entities
            if e.get("entity_id")
        }

        # Group by entity_type
        by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for entity in verified_entities:
            etype = entity.get("entity_type")
            if etype:
                by_type[etype].append(entity)

        # Sort each type group by score_influence descending
        for etype in by_type:
            by_type[etype].sort(
                key=lambda e: e.get("score_influence", 0) or 0,
                reverse=True,
            )

        # Relationship connectivity index (entity_id → edge count)
        connectivity: dict[str, int] = defaultdict(int)
        for edge in relationships:
            for eid in (edge.get("source_id"), edge.get("target_id")):
                if eid and eid in verified_set:
                    connectivity[eid] += 1

        # City context block (shared across all section prompts)
        city_context = _build_city_context(city_name, country, scope_parameters, framings, verified_entities)

        # ── Generate all sections concurrently ────────────────────────────────
        section_tasks = [
            self._draft_section(
                section_def=sec,
                city_name=city_name,
                country=country,
                city_context=city_context,
                entity_index=entity_index,
                by_type=by_type,
                ranked_lists=ranked_lists,
                relationships=relationships,
                connectivity=connectivity,
                gap_analysis=gap_analysis,
                verification_summary=verification_summary,
                agent_statuses=agent_statuses,
                verified_entities=verified_entities,
                scope_parameters=scope_parameters,
                framings=framings,
            )
            for sec in SECTIONS
        ]

        section_results = await asyncio.gather(*section_tasks, return_exceptions=True)

        # ── Assemble briefing_json ────────────────────────────────────────────
        sections_json: list[dict[str, Any]] = []
        for sec_def, result in zip(SECTIONS, section_results):
            if isinstance(result, Exception):
                log.warning(
                    "briefing_agent: section '%s' raised exception: %s",
                    sec_def["section_id"], result,
                )
                sections_json.append(_fallback_section(sec_def, by_type, entity_index))
            elif result is None:
                sections_json.append(_fallback_section(sec_def, by_type, entity_index))
            else:
                sections_json.append(result)

        # Statistics block
        statistics = _build_statistics(verified_entities, relationships, verification_summary, gap_analysis)

        # Phase 11.4 — Source attribution table
        source_attribution = _build_source_attribution(verified_entities, relationships)

        briefing_json: dict[str, Any] = {
            "run_id":          run_id,
            "city_name":       city_name,
            "country":         country,
            "generated_at":    datetime.now(timezone.utc).isoformat(),
            "entity_count":    len(verified_entities),
            "relationship_count": len(relationships),
            "statistics":      statistics,
            "sections":        sections_json,
            "source_attribution": source_attribution,
        }

        # ── Generate markdown ─────────────────────────────────────────────────
        briefing_markdown = _render_markdown(briefing_json)

        log.info(
            "briefing_agent: brief assembled — %d sections, %d chars markdown",
            len(sections_json), len(briefing_markdown),
        )

        # ── DB side effects ───────────────────────────────────────────────────
        # 1. Write summary assessment (section summaries only, bounded size)
        await self._write_briefing_assessment(run_id, briefing_json, verification_summary)

        # 2. Write FULL briefing as a separate assessment so the API can retrieve it.
        #    assessment_type="final_briefing_full" contains the complete sections JSON.
        #    GET /runs/{run_id}/briefing reads this record.
        await self._write_full_briefing(run_id, briefing_json, briefing_markdown)

        # 3. Finalize run record
        await self._finalize_run(state, verified_entities, relationships, verification_summary, gap_analysis)

        # ── Return state patch ────────────────────────────────────────────────
        return {
            "briefing_json":      briefing_json,
            "briefing_markdown":  briefing_markdown,
            "current_phase":      "DONE",
            **self.agent_status_patch("success", agent_statuses),
            **self.token_count_patch(
                state.get("total_tokens_in", 0),
                state.get("total_tokens_out", 0),
                state.get("agent_token_counts", {}),
            ),
            **self.entity_count_patch(state.get("agent_entity_counts", {})),
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Section drafting
    # ─────────────────────────────────────────────────────────────────────────

    async def _draft_section(
        self,
        section_def: dict[str, str],
        city_name: str,
        country: str,
        city_context: str,
        entity_index: dict[str, dict[str, Any]],
        by_type: dict[str, list[dict[str, Any]]],
        ranked_lists: dict[str, list[str]],
        relationships: list[dict[str, Any]],
        connectivity: dict[str, int],
        gap_analysis: dict[str, dict[str, Any]],
        verification_summary: dict[str, Any],
        agent_statuses: dict[str, str],
        verified_entities: list[dict[str, Any]],
        scope_parameters: dict[str, Any] | None = None,
        framings: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Draft a single section via LLM. Returns section dict."""
        sec_id   = section_def["section_id"]
        sec_type = section_def["type"]
        sec_aux  = section_def.get("aux", "")

        # ── Build section-specific context block ──────────────────────────────
        if sec_type == "overview":
            section_context = _build_overview_context(by_type, entity_index)

        elif sec_type == "market":
            section_context = _build_market_context(
                scope_parameters=scope_parameters or {},
                framings=framings or [],
            )

        elif sec_type == "category":
            section_context = _build_category_context(
                by_type.get(sec_aux, []),
                sec_aux,
                MAX_ENTITIES_PER_SECTION,
            )

        elif sec_type == "ranked":
            eid_list = ranked_lists.get(sec_aux, [])
            top_entities = [
                entity_index[eid]
                for eid in eid_list[:MAX_ENTITIES_RANKED_SECTION]
                if eid in entity_index
            ]
            section_context = _build_ranked_context(top_entities, sec_aux)

        elif sec_type == "network":
            section_context = _build_network_context(
                relationships, entity_index, connectivity, MAX_RELATIONSHIPS_IN_CONTEXT
            )

        elif sec_type == "gaps":
            section_context = _build_gaps_context(gap_analysis)

        elif sec_type == "confidence":
            section_context = _build_confidence_context(
                verification_summary, agent_statuses, gap_analysis, verified_entities
            )

        else:
            section_context = "(no context available for this section type)"

        prompt = SECTION_PROMPT_TEMPLATE.format(
            city_name=city_name,
            country=country,
            section_title=section_def["title"],
            part=section_def["part"],
            description=section_def["description"],
            city_context=city_context,
            section_context=section_context,
        )

        try:
            result_json, _meta = await self.llm_generate_json(
                task_type="brief_drafting",
                prompt=prompt,
                system=BRIEFING_SYSTEM_PROMPT,
            )
        except Exception as exc:
            log.warning(
                "briefing_agent: LLM failed for section '%s': %s", sec_id, exc
            )
            return _fallback_section(section_def, by_type, entity_index)

        # Parse and validate
        if not isinstance(result_json, dict):
            log.warning(
                "briefing_agent: section '%s' returned non-dict (type=%s)",
                sec_id, type(result_json).__name__,
            )
            return _fallback_section(section_def, by_type, entity_index)

        summary    = str(result_json.get("summary", ""))[:2000]
        key_points = [str(p) for p in (result_json.get("key_points") or []) if p][:7]
        notable    = _validate_notable_entities(result_json.get("notable_entities"), entity_index)
        conf_note  = result_json.get("confidence_note") or None

        return {
            "section_id":       sec_id,
            "part":             section_def["part"],
            "title":            section_def["title"],
            "summary":          summary,
            "key_points":       key_points,
            "notable_entities": notable,
            "confidence_note":  str(conf_note)[:300] if conf_note else None,
            "llm_generated":    True,
        }

    # ─────────────────────────────────────────────────────────────────────────
    # DB side effects
    # ─────────────────────────────────────────────────────────────────────────

    async def _write_briefing_assessment(
        self,
        run_id: str,
        briefing_json: dict[str, Any],
        verification_summary: dict[str, Any],
    ) -> None:
        """Persist the briefing as an analytical_assessments record."""
        entity_count = briefing_json.get("entity_count", 0)
        section_count = len(briefing_json.get("sections", []))

        assessment = {
            "run_id":            run_id,
            "assessment_type":   "final_briefing",
            "claim_text":        (
                f"Final intelligence brief generated: {entity_count} verified entities, "
                f"{section_count} sections. "
                f"Verification: {verification_summary.get('passed', 0)} passed, "
                f"{verification_summary.get('failed', 0)} failed."
            ),
            "claim_json": {
                "entity_count":          entity_count,
                "relationship_count":    briefing_json.get("relationship_count", 0),
                "section_count":         section_count,
                "verification_summary":  verification_summary,
                "statistics":            briefing_json.get("statistics", {}),
                # Store only section titles + summaries (not full content) to keep record bounded
                "section_summaries": [
                    {
                        "section_id": s["section_id"],
                        "title":      s["title"],
                        "summary":    s.get("summary", "")[:500],
                    }
                    for s in briefing_json.get("sections", [])
                ],
            },
            "framework_name":    "final_briefing",
            "framework_version": self.AGENT_VERSION,
            "model_used":        settings.ollama_default_model,
            "prompt_version":    self.AGENT_VERSION,
            "confidence":        "high",
            "needs_review":      False,
            "created_at":        datetime.now(timezone.utc).isoformat(),
        }
        try:
            await self.write_assessment(assessment)
        except Exception as exc:
            log.warning("briefing_agent: failed to write briefing assessment: %s", exc)

    async def _write_full_briefing(
        self,
        run_id: str,
        briefing_json: dict[str, Any],
        briefing_markdown: str,
    ) -> None:
        """
        Persist the COMPLETE briefing JSON to analytical_assessments as
        assessment_type="final_briefing_full".

        This is what the API's GET /runs/{run_id}/briefing endpoint reads.
        Stored separately from the summary assessment so the API can return
        the full 20-section content without re-running the pipeline.

        The claim_json field contains the complete briefing_json (all sections,
        all key_points, all notable_entities).
        The claim_text field contains the full markdown for the format=markdown endpoint.
        """
        assessment = {
            "run_id":            run_id,
            "assessment_type":   "final_briefing_full",
            "claim_text":        briefing_markdown[:10000],  # truncated for claim_text; full in claim_json
            "claim_json":        briefing_json,              # COMPLETE — all 20 sections
            "framework_name":    "final_briefing",
            "framework_version": self.AGENT_VERSION,
            "model_used":        settings.ollama_default_model,
            "prompt_version":    self.AGENT_VERSION,
            "confidence":        "high",
            "needs_review":      False,
            "created_at":        datetime.now(timezone.utc).isoformat(),
        }
        try:
            await self.write_assessment(assessment)
            log.info(
                "briefing_agent: full briefing written to DB "
                "(%d sections, %d chars markdown)",
                len(briefing_json.get("sections", [])), len(briefing_markdown),
            )
        except Exception as exc:
            log.warning("briefing_agent: failed to write full briefing: %s", exc)
            # Non-fatal — briefing is in state and summary assessment is already written

    async def _finalize_run(
        self,
        state: dict[str, Any],
        verified_entities: list[dict[str, Any]],
        relationships: list[dict[str, Any]],
        verification_summary: dict[str, Any],
        gap_analysis: dict[str, dict[str, Any]],
    ) -> None:
        """Call complete_run() to finalize the run record."""
        thin_categories = [
            cat for cat, data in gap_analysis.items()
            if data.get("thin", False)
        ]
        summary = {
            "entities_total":      len(verified_entities),
            "relationships_total": len(relationships),
            "claims_verified":     verification_summary.get("passed", 0),
            "claims_failed":       verification_summary.get("failed", 0),
            "items_rejected":      len(state.get("ambiguous_merges", [])) + len(state.get("relationships_rejected", [])),
            "overall_confidence":  _compute_overall_confidence(verified_entities),
            "pass_count":          state.get("pass_number", 1),
            "gap_fill_triggered":  bool(thin_categories),
            "categories_thin":     thin_categories,
        }
        try:
            await self._db.complete_run(
                run_id=state["run_id"],
                status="complete",
                summary=summary,
            )
        except Exception as exc:
            log.warning("briefing_agent: failed to finalize run record: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# Context builders (module-level, pure functions)
# ─────────────────────────────────────────────────────────────────────────────

def _build_city_context(
    city_name: str,
    country: str,
    scope_parameters: dict[str, Any],
    framings: list[dict[str, Any]],
    verified_entities: list[dict[str, Any]],
) -> str:
    """Build the city context block shared across all section prompts."""
    entity_counts = defaultdict(int)
    for e in verified_entities:
        etype = e.get("entity_type")
        if etype:
            entity_counts[etype] += 1

    count_lines = ", ".join(f"{v} {k}" for k, v in sorted(entity_counts.items()))
    market_size = scope_parameters.get("market_size_estimate", "unknown")
    characteristics = scope_parameters.get("known_characteristics", [])
    chars_text = "; ".join(str(c) for c in characteristics[:3]) if characteristics else "not specified"

    framing_text = ""
    if framings:
        framing_text = f"\nPerspective framing: {framings[0].get('perspective', '')} — {framings[0].get('framing_text', '')[:200]}"

    return (
        f"City: {city_name}, {country}\n"
        f"Market size estimate: {market_size}\n"
        f"Key characteristics: {chars_text}\n"
        f"Verified entities in scope: {count_lines or 'none'}"
        f"{framing_text}"
    )


def _build_overview_context(
    by_type: dict[str, list[dict[str, Any]]],
    entity_index: dict[str, dict[str, Any]],
) -> str:
    """Context for the Executive Summary section."""
    lines = ["ENTITY SUMMARY BY TYPE:"]
    for etype, entities in sorted(by_type.items()):
        top_names = [
            e.get("canonical_name") or e.get("name") or e.get("org_name", "")
            for e in entities[:3]
        ]
        lines.append(f"  {etype} ({len(entities)} total): {', '.join(top_names)}")
    return "\n".join(lines)


def _build_market_context(
    scope_parameters: dict[str, Any],
    framings: list[dict[str, Any]],
) -> str:
    """Context for the Market Landscape section."""
    lines = ["SCOPE PARAMETERS AND MARKET FRAMINGS:"]
    for k, v in scope_parameters.items():
        if v:
            lines.append(f"  {k}: {v}")
    if framings:
        lines.append("\nANALYST FRAMINGS:")
        for framing in framings[:4]:
            perspective = framing.get("perspective", "")
            text        = framing.get("framing_text", "")[:250]
            lines.append(f"  [{perspective}] {text}")
    return "\n".join(lines)


def _build_category_context(
    entities: list[dict[str, Any]],
    entity_type: str,
    max_entities: int,
) -> str:
    """Context for a stakeholder category section."""
    if not entities:
        return f"ENTITIES OF TYPE '{entity_type.upper()}':\n  (none found in this city)"

    top = entities[:max_entities]
    lines = [f"ENTITIES OF TYPE '{entity_type.upper()}' (top {len(top)} by influence):"]
    for e in top:
        name  = e.get("canonical_name") or e.get("name") or e.get("org_name", "unknown")
        score = e.get("score_influence", 0) or 0
        conf  = e.get("overall_confidence", "unknown")
        desc  = (e.get("description") or e.get("bio") or e.get("headline") or "")[:DESCRIPTION_MAX_CHARS]
        lines.append(f"\n  [{name}] (influence={score}, confidence={conf})")
        if desc:
            lines.append(f"    {desc}")
        # Add notable fields per type
        employer = e.get("current_employer") or e.get("primary_employer")
        title    = e.get("title") or e.get("role")
        if title and employer:
            lines.append(f"    Role: {title} at {employer}")
        elif employer:
            lines.append(f"    Affiliated: {employer}")
        aum = e.get("total_aum_usd")
        if aum:
            lines.append(f"    AUM: ${aum:,.0f}")
        blocker = e.get("score_blocker_risk", 0) or 0
        if blocker >= 60:
            lines.append(f"    ⚠ Blocker risk: {blocker}/100")
    return "\n".join(lines)


def _build_ranked_context(
    top_entities: list[dict[str, Any]],
    dimension: str,
) -> str:
    """Context for ranked analysis sections."""
    dim_label = dimension.replace("score_", "").replace("_", " ").title()
    if not top_entities:
        return f"RANKED ENTITIES ({dim_label}):\n  (no entities ranked on this dimension)"

    lines = [f"TOP ENTITIES BY {dim_label.upper()}:"]
    for rank, e in enumerate(top_entities, start=1):
        name  = e.get("canonical_name") or e.get("name") or e.get("org_name", "unknown")
        score = e.get(dimension, 0) or 0
        etype = e.get("entity_type", "")
        desc  = (e.get("description") or "")[:150]
        lines.append(f"\n  #{rank}. [{name}] ({etype}) — {dim_label}: {score}/100")
        if desc:
            lines.append(f"     {desc}")
    return "\n".join(lines)


def _build_network_context(
    relationships: list[dict[str, Any]],
    entity_index: dict[str, dict[str, Any]],
    connectivity: dict[str, int],
    max_rels: int,
) -> str:
    """Context for the Network Topology section."""
    lines = []

    # Top connectors by edge count
    top_connectors = sorted(connectivity.items(), key=lambda x: x[1], reverse=True)[:8]
    if top_connectors:
        lines.append("TOP CONNECTORS (by relationship count):")
        for eid, count in top_connectors:
            e = entity_index.get(eid, {})
            name = e.get("canonical_name") or e.get("name") or eid
            lines.append(f"  {name}: {count} connections")

    # Sample high-strength edges
    strong_edges = sorted(
        relationships,
        key=lambda e: e.get("relationship_strength", 0) or 0,
        reverse=True,
    )[:max_rels]

    if strong_edges:
        lines.append("\nHIGH-STRENGTH RELATIONSHIPS (top edges):")
        for edge in strong_edges:
            src = entity_index.get(edge.get("source_id", ""), {})
            tgt = entity_index.get(edge.get("target_id", ""), {})
            src_name = src.get("canonical_name") or src.get("name") or edge.get("source_id", "?")
            tgt_name = tgt.get("canonical_name") or tgt.get("name") or edge.get("target_id", "?")
            rel_type = edge.get("relationship_type", "CONNECTED_TO")
            strength = edge.get("relationship_strength", 0.0)
            lines.append(f"  {src_name} →[{rel_type}]→ {tgt_name} (strength={strength:.2f})")

    return "\n".join(lines) if lines else "(no relationship data available)"


def _build_gaps_context(gap_analysis: dict[str, dict[str, Any]]) -> str:
    """Context for the Coverage Gaps section."""
    if not gap_analysis:
        return "GAP ANALYSIS:\n  (no gap analysis data available)"

    lines = ["GAP ANALYSIS BY CATEGORY:"]
    for cat, data in sorted(gap_analysis.items()):
        found    = data.get("entities_found", 0)
        expected = data.get("expected_min", 0)
        score    = data.get("coverage_score", 1.0)
        thin     = data.get("thin", False)
        marker   = "⚠ THIN" if thin else "OK"
        lines.append(
            f"  {cat}: found={found} expected≥{expected} "
            f"coverage={score:.0%} [{marker}]"
        )
        analysis = data.get("analysis", "")
        if analysis and thin:
            lines.append(f"    Analysis: {analysis[:200]}")
    return "\n".join(lines)


def _build_confidence_context(
    verification_summary: dict[str, Any],
    agent_statuses: dict[str, str],
    gap_analysis: dict[str, dict[str, Any]],
    verified_entities: list[dict[str, Any]],
) -> str:
    """Context for the Confidence Assessment section."""
    lines = ["VERIFICATION OUTCOMES:"]
    lines.append(f"  Total entities verified: {verification_summary.get('total_entities', 0)}")
    lines.append(f"  Passed:        {verification_summary.get('passed', 0)}")
    lines.append(f"  Failed:        {verification_summary.get('failed', 0)}")
    lines.append(f"  Unverifiable:  {verification_summary.get('unverifiable', 0)}")
    lines.append(f"  Flagged:       {len(verification_summary.get('flagged_entity_ids', []))}")

    lines.append("\nAGENT STATUS SUMMARY:")
    status_counts: dict[str, int] = defaultdict(int)
    for agent, status in agent_statuses.items():
        status_counts[status] += 1
    for status, count in sorted(status_counts.items()):
        lines.append(f"  {status}: {count} agents")

    # Confidence distribution
    conf_dist: dict[str, int] = defaultdict(int)
    for e in verified_entities:
        conf_dist[e.get("overall_confidence", "unknown")] += 1
    lines.append("\nENTITY CONFIDENCE DISTRIBUTION:")
    for conf, count in sorted(conf_dist.items()):
        lines.append(f"  {conf}: {count} entities")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Assembly helpers
# ─────────────────────────────────────────────────────────────────────────────

def _validate_notable_entities(
    raw: Any,
    entity_index: dict[str, dict[str, Any]],
) -> list[dict[str, str]]:
    """Validate and clean notable_entities list from LLM response."""
    if not isinstance(raw, list):
        return []
    result = []
    for item in raw[:5]:
        if not isinstance(item, dict):
            continue
        eid   = str(item.get("entity_id", "")).strip()
        name  = str(item.get("name", "")).strip()
        notes = str(item.get("notes", "")).strip()[:200]
        if not name:
            continue
        # Validate entity_id if provided
        if eid and eid not in entity_index:
            eid = ""  # clear invalid ID, keep the name
        result.append({"entity_id": eid, "name": name, "notes": notes})
    return result


def _fallback_section(
    section_def: dict[str, str],
    by_type: dict[str, list[dict[str, Any]]],
    entity_index: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Fallback section when LLM call fails — entity list only, no narrative."""
    sec_type = section_def.get("type", "")
    sec_aux  = section_def.get("aux", "")

    notable: list[dict[str, str]] = []
    if sec_type == "category" and sec_aux:
        for e in by_type.get(sec_aux, [])[:5]:
            eid  = e.get("entity_id", "")
            name = e.get("canonical_name") or e.get("name") or e.get("org_name", "")
            if name:
                notable.append({"entity_id": eid, "name": name, "notes": ""})

    return {
        "section_id":       section_def["section_id"],
        "part":             section_def["part"],
        "title":            section_def["title"],
        "summary":          "(Section generation failed — entity list shown only.)",
        "key_points":       [],
        "notable_entities": notable,
        "confidence_note":  "LLM section generation failed; this section requires manual review.",
        "llm_generated":    False,
    }


def _build_statistics(
    verified_entities: list[dict[str, Any]],
    relationships: list[dict[str, Any]],
    verification_summary: dict[str, Any],
    gap_analysis: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Build the statistics block for briefing_json."""
    by_type: dict[str, int] = defaultdict(int)
    conf_dist: dict[str, int] = defaultdict(int)
    flagged_count = 0
    for e in verified_entities:
        etype = e.get("entity_type", "unknown")
        by_type[etype] += 1
        conf = e.get("overall_confidence", "unknown")
        conf_dist[conf] += 1
        if e.get("needs_review"):
            flagged_count += 1

    thin_categories = [
        cat for cat, data in gap_analysis.items()
        if data.get("thin", False)
    ]

    return {
        "total_entities":            len(verified_entities),
        "total_relationships":       len(relationships),
        "entities_by_type":          dict(by_type),
        "entities_by_confidence":    dict(conf_dist),
        "entities_flagged":          flagged_count,
        "verification_passed":       verification_summary.get("passed", 0),
        "verification_failed":       verification_summary.get("failed", 0),
        "thin_categories":           thin_categories,
        "coverage_scores": {
            cat: round(data.get("coverage_score", 0), 3)
            for cat, data in gap_analysis.items()
        },
    }


def _compute_overall_confidence(entities: list[dict[str, Any]]) -> str:
    """Compute overall run confidence from entity confidence distribution."""
    if not entities:
        return "low"
    counts: dict[str, int] = defaultdict(int)
    for e in entities:
        conf = e.get("overall_confidence", "low")
        counts[conf] += 1
    total = len(entities)
    high_pct  = counts.get("high", 0) / total
    med_pct   = counts.get("medium", 0) / total
    if high_pct >= 0.5:
        return "high"
    elif high_pct + med_pct >= 0.5:
        return "medium"
    return "low"


# ─────────────────────────────────────────────────────────────────────────────
# Phase 11.4 — Source attribution
# ─────────────────────────────────────────────────────────────────────────────

# Which category_fields keys signal that a particular data source contributed data.
# Maps category_fields key → (domain, human label, quality tier)
_SOURCE_SIGNALS: list[tuple[str, str, str, str]] = [
    # (category_fields_key_prefix, domain, label, quality)
    ("ofac_matches",            "ofac",            "OFAC SDN List",                  "primary"),
    ("icij_nodes",              "icij_leaks",       "ICIJ Offshore Leaks",            "secondary"),
    ("icij_shell_chain",        "icij_leaks",       "ICIJ Offshore Leaks",            "secondary"),
    ("un_sanctions",            "un_sanctions",     "UN Security Council Sanctions",  "primary"),
    ("eu_sanctions",            "eu_sanctions",     "EU Consolidated Sanctions",      "primary"),
    ("bis_denied",              "bis_denied",       "BIS Denied Persons List",        "primary"),
    ("linkedin_url",            "proxycurl",        "Proxycurl / LinkedIn",           "primary"),
    ("littlesis_id",            "littlesis",        "LittleSis",                      "secondary"),
    ("litigation_cases",        "courtlistener",    "CourtListener / PACER",          "secondary"),
    ("edgar_proxy_data",        "edgar",            "SEC EDGAR",                      "primary"),
    ("edgar_10k_data",          "edgar",            "SEC EDGAR",                      "primary"),
    ("hud_properties",          "hud",              "HUD Multifamily Housing",        "primary"),
    ("fincen_ctr_summary",      "fincen",           "FinCEN Currency Transaction Reports", "primary"),
    ("opencorporates_data",     "opencorporates",   "OpenCorporates",                 "secondary"),
    ("bizapedia_officers",      "bizapedia",        "Bizapedia",                      "secondary"),
    ("patent_count",            "patentview",       "PatentView / USPTO",             "secondary"),
    ("ftm_contributions",       "followthemoney",   "Follow the Money",               "secondary"),
    ("wayback_pages",           "wayback",          "Wayback Machine",                "tertiary"),
    ("gdelt_articles",          "gdelt",            "GDELT Project",                  "tertiary"),
]


def _build_source_attribution(
    entities: list[dict[str, Any]],
    relationships: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """
    Build a source attribution map from entity category_fields and relationship edges.

    Returns:
        dict keyed by domain: {label, quality, records_used}
    """
    # domain → {label, quality, records_used (count of entities/edges that used this source)}
    attr: dict[str, dict[str, Any]] = {}

    def _register(domain: str, label: str, quality: str, count: int = 1) -> None:
        if domain not in attr:
            attr[domain] = {"label": label, "quality": quality, "records_used": 0}
        attr[domain]["records_used"] += count

    for entity in entities:
        cat = entity.get("category_fields") or {}
        for key_prefix, domain, label, quality in _SOURCE_SIGNALS:
            val = cat.get(key_prefix)
            if val:  # any truthy value — list, dict, string, int
                # For list-type values, count individual records; otherwise count as 1
                count = len(val) if isinstance(val, list) else 1
                _register(domain, label, quality, count)

    # Relationship sources (deduplicate by relationship_id, count per domain)
    for edge in relationships:
        src = edge.get("_source_quality") or edge.get("source_quality", "")
        if not src:
            continue
        # Map source_quality tier back to a generic "pipeline_inference" if no domain known
        # (LLM-inferred edges don't have a domain; WORKS_UNDER is internal)
        rel_type = edge.get("relationship_type", "")
        if edge.get("is_inferred") or rel_type == "WORKS_UNDER":
            _register("pipeline_inference", "Pipeline Inference (2-hop)", "tertiary")

    return attr


# ─────────────────────────────────────────────────────────────────────────────
# Markdown renderer
# ─────────────────────────────────────────────────────────────────────────────

def _render_markdown(briefing: dict[str, Any]) -> str:
    """Render briefing_json into a human-readable markdown document."""
    city   = briefing.get("city_name", "Unknown City")
    ts     = briefing.get("generated_at", "")[:10]
    stats  = briefing.get("statistics", {})

    lines: list[str] = [
        f"# Startup Ecosystem Intelligence Brief: {city}",
        f"*Generated: {ts}*",
        "",
        "---",
        "",
        "## Brief Statistics",
        "",
        f"- **Total Verified Entities**: {stats.get('total_entities', 0)}",
        f"- **Total Relationships**: {stats.get('total_relationships', 0)}",
        f"- **Claims Verified (Passed)**: {stats.get('verification_passed', 0)}",
        f"- **Entities Flagged for Review**: {stats.get('entities_flagged', 0)}",
        "",
    ]

    current_part: str = ""
    for section in briefing.get("sections", []):
        part  = section.get("part", "")
        title = section.get("title", "")

        # Part header
        if part != current_part:
            current_part = part
            part_labels = {"I": "Ecosystem Overview", "II": "Stakeholder Categories",
                           "III": "Intelligence Analysis", "IV": "Quality & Confidence"}
            lines.extend(["", "---", "", f"# Part {part}: {part_labels.get(part, '')}", ""])

        # Section header
        lines.append(f"## {title}")
        lines.append("")

        # Summary
        summary = section.get("summary", "")
        if summary and not summary.startswith("(Section generation failed"):
            lines.append(summary)
            lines.append("")

        # Key points
        key_points = section.get("key_points", [])
        if key_points:
            lines.append("**Key Points:**")
            lines.append("")
            for point in key_points:
                lines.append(f"- {point}")
            lines.append("")

        # Notable entities
        notables = section.get("notable_entities", [])
        if notables:
            lines.append("**Notable Entities:**")
            lines.append("")
            for ne in notables:
                name  = ne.get("name", "")
                notes = ne.get("notes", "")
                if notes:
                    lines.append(f"- **{name}**: {notes}")
                else:
                    lines.append(f"- **{name}**")
            lines.append("")

        # Confidence note
        conf_note = section.get("confidence_note")
        if conf_note:
            lines.append(f"> ⚠️ *{conf_note}*")
            lines.append("")

        # Separator between sections
        lines.append("")

    lines.append("---")
    lines.append("")

    # ── Phase 11.4: Source Attribution Table ──────────────────────────────────
    source_map = briefing.get("source_attribution", {})
    if source_map:
        lines.append("## Data Sources")
        lines.append("")
        lines.append("| Source | Domain | Quality | Records Used |")
        lines.append("|--------|--------|---------|--------------|")
        for domain, attrs in sorted(source_map.items()):
            label   = attrs.get("label", domain)
            quality = attrs.get("quality", "tertiary")
            count   = attrs.get("records_used", 0)
            lines.append(f"| {label} | `{domain}` | {quality} | {count} |")
        lines.append("")

    lines.append("---")
    lines.append("")
    lines.append(
        f"*This brief was generated automatically by the OSINT Intelligence Pipeline. "
        f"All findings should be verified against primary sources before acting on them. "
        f"ICIJ Offshore Leaks data reflects leaked documents only — inclusion does not "
        f"constitute proof of wrongdoing.*"
    )

    return "\n".join(lines)
