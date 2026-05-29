"""
osint/agents/pipeline.py

Pipeline Intelligence Agent — Phase 1 collection agent.

Queries the research pipeline's Supabase database DIRECTLY (no Flask API
dependency) to extract named entities from synthesis records and high-relevance
extractions. This bypasses the old /api/v1/companies|deals|founders endpoints
which do not exist in the actual pipeline.

Architecture
------------
The research pipeline (Supabase project: wuojatgaxkeqpubsvrrg) runs independent
of the OSINT system (Supabase project: gdiuwayqjrejwosuxmel). Both share the same
password but are separate projects. This agent connects to the pipeline DB using
PIPELINE_DATABASE_URL from settings.

Data sources queried:
  synthesis table   — structured narrative outputs per agent run
                      (main_themes, strongest_findings, overall_summary)
  extractions table — per-source key findings with relevance scores (0–10)
                      Only relevance >= 8 rows are saved in the pipeline DB.

City filtering strategy:
  sessions.topic ILIKE '%{city_name}%'
  This is how the pipeline tags sessions — the topic is the research question
  which always contains the city name.

Entity extraction:
  LLM (qwen3:7b — fast extraction model) reads synthesis prose and returns
  named entities with entity_type, subtype, role, and evidence snippet.
  Extractions (key_finding rows) are batched and extracted similarly.
  Both paths use the same structured JSON output format.

Output:
  Appends to state["raw_entities"] — raw pre-resolution entity dicts.
  entity_types produced: corporate, investor, executive_hnw, nonprofit,
                         philanthropic, politician, community_leader.

Failure modes:
  - PIPELINE_DATABASE_URL not set → skips with warning (non-blocking)
  - Pipeline DB unreachable → skips with warning (non-blocking)
  - LLM returns no valid entities → empty list (non-blocking)
  - All failures are logged; zero entities returned, run continues.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from osint.agents.base import BaseAgent
from osint.core.config import settings

log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

# Max synthesis records to fetch per run (city-filtered)
MAX_SYNTHESIS_RECORDS = 100

# Max extractions to fetch per run (city-filtered, highest relevance first)
MAX_EXTRACTION_RECORDS = 300

# Min relevance score for extractions to be considered (pipeline already filters
# at MIN_SAVE_SCORE=8, but we set a floor here too for safety)
MIN_EXTRACTION_RELEVANCE = 8

# Max entities the LLM is allowed to return per synthesis record
MAX_ENTITIES_PER_RECORD = 8

# Valid entity types the LLM may produce — anything else is dropped
VALID_ENTITY_TYPES = frozenset({
    "corporate", "investor", "executive_hnw", "nonprofit",
    "philanthropic", "politician", "community_leader", "hnwi",
})

# ── Extraction prompt ─────────────────────────────────────────────────────────

ENTITY_EXTRACTION_PROMPT = """\
You are an OSINT analyst extracting named entities from research synthesis text.

City: {city_name}

Research synthesis text:
{text}

Extract all NAMED individuals, companies, organizations, and funds mentioned.
Return ONLY a JSON array. Each object must have:
  "name":        Exact proper noun (full name — no generic terms like "startup" or "fund")
  "entity_type": One of: corporate | investor | executive_hnw | nonprofit | philanthropic | politician | community_leader | hnwi
  "subtype":     Optional — e.g. "venture_capital", "startup", "mayor", "foundation"
  "role":        Brief role description (12 words max)
  "evidence":    Exact quote or paraphrase from text supporting this entity (30 words max)

Rules:
- Only include entities explicitly named in the text.
- Do NOT invent names or infer from context.
- Drop any name that is a generic descriptor (e.g. "a local VC", "the mayor").
- If no named entities exist, return an empty array: []

Return ONLY valid JSON. No markdown, no explanation.
"""


class PipelineAgent(BaseAgent):
    """
    Phase 1 collection agent that ingests named entities from the research
    pipeline's Supabase database. Falls back gracefully if unavailable.
    """

    AGENT_NAME = "pipeline_agent"
    AGENT_VERSION = "2.0"

    async def run(self, state: dict[str, Any]) -> dict[str, Any]:
        city_name = state["city_name"]
        run_id = state["run_id"]

        log.info("PipelineAgent: collecting from pipeline DB for %s", city_name)

        if not settings.pipeline_database_url:
            log.warning(
                "PipelineAgent: PIPELINE_DATABASE_URL not set — skipping. "
                "Add it to .env to enable pipeline entity ingestion."
            )
            await self.write_search_record(
                source_searched="pipeline_db",
                query_used=f"pipeline synthesis for {city_name}",
                result_found=False,
                entity_type="corporate",
                failure_reason="PIPELINE_DATABASE_URL not configured",
            )
            return self._empty_patch()

        # ── Fetch data from pipeline DB ───────────────────────────────────────
        synthesis_records, extraction_records = await self._fetch_pipeline_data(city_name)
        total_source_records = len(synthesis_records) + len(extraction_records)

        if total_source_records == 0:
            log.warning(
                "PipelineAgent: no synthesis or extraction records found for '%s' in pipeline DB",
                city_name,
            )
            await self.write_search_record(
                source_searched="pipeline_db",
                query_used=f"synthesis + extractions for {city_name}",
                result_found=False,
                entity_type="corporate",
                failure_reason=f"No pipeline records found for city: {city_name}",
            )
            return self._empty_patch()

        log.info(
            "PipelineAgent: fetched %d synthesis + %d extraction records for %s",
            len(synthesis_records), len(extraction_records), city_name,
        )

        # ── Extract entities via LLM ──────────────────────────────────────────
        new_raw_entities: list[dict[str, Any]] = []

        # Pass 1: synthesis records (richest signal — multi-source narrative)
        synthesis_entities = await self._extract_from_synthesis(
            synthesis_records, city_name, run_id
        )
        new_raw_entities.extend(synthesis_entities)
        log.info("PipelineAgent: synthesis pass → %d entities", len(synthesis_entities))

        # Pass 2: top extractions (individual key findings)
        if extraction_records:
            extraction_entities = await self._extract_from_extractions(
                extraction_records, city_name, run_id
            )
            new_raw_entities.extend(extraction_entities)
            log.info("PipelineAgent: extraction pass → %d entities", len(extraction_entities))

        await self.write_search_record(
            source_searched="pipeline_db",
            query_used=f"synthesis + extractions for {city_name}",
            result_found=bool(new_raw_entities),
            entity_type="corporate",
            result_count=len(new_raw_entities),
        )

        log.info("PipelineAgent: %d total raw entities from pipeline DB", len(new_raw_entities))

        return {
            "raw_entities": new_raw_entities,
            **self.agent_status_patch("success"),
            **self.token_count_patch(),
            **self.entity_count_patch(),
        }

    # ── Database layer ────────────────────────────────────────────────────────

    async def _fetch_pipeline_data(
        self, city_name: str
    ) -> tuple[list[dict], list[dict]]:
        """
        Connect to the pipeline Supabase, fetch synthesis records and
        high-relevance extractions for the given city. Returns two lists.
        Both are empty on any connection/query failure.
        """
        try:
            import asyncpg
        except ImportError:
            log.error(
                "PipelineAgent: asyncpg not installed — cannot query pipeline DB. "
                "Run: pip install asyncpg --break-system-packages"
            )
            return [], []

        conn = None
        try:
            conn = await asyncpg.connect(
                settings.pipeline_database_url,
                timeout=15,
                command_timeout=30,
            )

            synthesis_rows = await self._query_synthesis(conn, city_name)
            extraction_rows = await self._query_extractions(conn, city_name)

            return synthesis_rows, extraction_rows

        except Exception as exc:
            log.warning("PipelineAgent: pipeline DB connection/query failed: %s", exc)
            return [], []
        finally:
            if conn is not None:
                try:
                    await conn.close()
                except Exception:
                    pass

    async def _query_synthesis(
        self, conn: Any, city_name: str
    ) -> list[dict]:
        """
        Fetch synthesis records for runs whose session topic mentions the city.
        Returns list of dicts with keys: main_themes, strongest_findings,
        overall_summary, confidence, run_topic, agent_name, session_topic.
        """
        sql = """
            SELECT
                sy.main_themes,
                sy.strongest_findings,
                sy.overall_summary,
                sy.confidence,
                r.topic       AS run_topic,
                r.agent       AS agent_name,
                sess.topic    AS session_topic
            FROM synthesis sy
            JOIN runs r     ON sy.run_id    = r.id
            JOIN sessions sess ON r.session_id = sess.id
            WHERE sess.topic ILIKE $1
              AND r.status = 'done'
              AND sy.confidence IN ('high', 'medium')
            ORDER BY sess.created_at DESC
            LIMIT $2
        """
        rows = await conn.fetch(sql, f"%{city_name}%", MAX_SYNTHESIS_RECORDS)
        return [dict(r) for r in rows]

    async def _query_extractions(
        self, conn: Any, city_name: str
    ) -> list[dict]:
        """
        Fetch high-relevance extractions for city-related sessions.
        Only rows where relevance >= MIN_EXTRACTION_RELEVANCE are returned.
        """
        sql = """
            SELECT
                e.key_finding,
                e.title,
                e.year,
                e.source_db,
                e.relevance,
                r.topic    AS run_topic,
                r.agent    AS agent_name
            FROM extractions e
            JOIN runs r     ON e.run_id    = r.id
            JOIN sessions sess ON r.session_id = sess.id
            WHERE sess.topic ILIKE $1
              AND r.status   = 'done'
              AND e.relevance >= $2
              AND e.key_finding IS NOT NULL
              AND e.key_finding != ''
            ORDER BY e.relevance DESC
            LIMIT $3
        """
        rows = await conn.fetch(
            sql, f"%{city_name}%", MIN_EXTRACTION_RELEVANCE, MAX_EXTRACTION_RECORDS
        )
        return [dict(r) for r in rows]

    # ── LLM extraction layer ──────────────────────────────────────────────────

    async def _extract_from_synthesis(
        self,
        records: list[dict],
        city_name: str,
        run_id: str,
    ) -> list[dict[str, Any]]:
        """
        Run LLM entity extraction over synthesis records.
        Each synthesis record is processed individually (they're already
        summarised — no need to batch). Entities are deduplicated by name.
        """
        all_entities: list[dict[str, Any]] = []
        seen_names: set[str] = set()

        for record in records:
            # Build text block from the most entity-rich synthesis fields
            text_parts = []
            if record.get("strongest_findings"):
                text_parts.append(record["strongest_findings"])
            if record.get("main_themes"):
                text_parts.append(record["main_themes"])
            if record.get("overall_summary"):
                text_parts.append(record["overall_summary"])

            text = "\n\n".join(p for p in text_parts if p and p.strip())
            if not text or len(text) < 50:
                continue

            raw_entities = await self._llm_extract_entities(text, city_name)

            for raw in raw_entities:
                name = (raw.get("name") or "").strip()
                if not name or self.is_garbage_entity_name(name):
                    continue
                key = name.lower()
                if key in seen_names:
                    continue
                seen_names.add(key)

                entity_dict = self._build_entity_dict(
                    raw, city_name, run_id,
                    source_text=f"[synthesis:{record.get('agent_name','?')}] {text[:200]}",
                )
                if entity_dict:
                    all_entities.append(entity_dict)

        return all_entities

    async def _extract_from_extractions(
        self,
        records: list[dict],
        city_name: str,
        run_id: str,
    ) -> list[dict[str, Any]]:
        """
        Run LLM entity extraction over batched key_finding rows.
        Records are concatenated into blocks of 10 for efficiency.
        """
        BATCH_SIZE = 10
        all_entities: list[dict[str, Any]] = []
        seen_names: set[str] = set()

        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i: i + BATCH_SIZE]

            parts = []
            for rec in batch:
                finding = (rec.get("key_finding") or "").strip()
                title   = (rec.get("title")       or "").strip()
                if finding:
                    parts.append(f"- {title + ': ' if title else ''}{finding}")

            text = "\n".join(parts)
            if not text or len(text) < 30:
                continue

            raw_entities = await self._llm_extract_entities(text, city_name)

            for raw in raw_entities:
                name = (raw.get("name") or "").strip()
                if not name or self.is_garbage_entity_name(name):
                    continue
                key = name.lower()
                if key in seen_names:
                    continue
                seen_names.add(key)

                entity_dict = self._build_entity_dict(
                    raw, city_name, run_id,
                    source_text=f"[extraction] {text[:200]}",
                )
                if entity_dict:
                    all_entities.append(entity_dict)

        return all_entities

    async def _llm_extract_entities(
        self, text: str, city_name: str
    ) -> list[dict]:
        """
        Call the LLM with the entity extraction prompt.
        Uses qwen3:7b (extraction model — fast, structured output).
        Returns parsed list of dicts, or [] on any failure.
        """
        prompt = ENTITY_EXTRACTION_PROMPT.format(
            city_name=city_name,
            text=text[:4000],   # stay well within context window
        )

        try:
            raw_response = await self.llm.generate(
                prompt=prompt,
                task_type="structured_extraction_text",
                expect_json=True,
            )
        except Exception as exc:
            log.debug("PipelineAgent: LLM call failed: %s", exc)
            return []

        # Parse JSON response
        try:
            result = json.loads(raw_response) if isinstance(raw_response, str) else raw_response
            if isinstance(result, list):
                return result[:MAX_ENTITIES_PER_RECORD]
            if isinstance(result, dict) and "entities" in result:
                return result["entities"][:MAX_ENTITIES_PER_RECORD]
        except (json.JSONDecodeError, TypeError):
            log.debug("PipelineAgent: failed to parse LLM response as JSON")

        return []

    # ── Entity builder ────────────────────────────────────────────────────────

    def _build_entity_dict(
        self,
        raw: dict,
        city_name: str,
        run_id: str,
        source_text: str = "",
    ) -> dict[str, Any] | None:
        """
        Convert an LLM-extracted entity dict into a full OSINT entity dict.
        Returns None if entity_type is invalid.
        """
        name        = (raw.get("name")        or "").strip()
        entity_type = (raw.get("entity_type") or "corporate").strip().lower()
        subtype     = (raw.get("subtype")     or "").strip() or None
        role        = (raw.get("role")        or "").strip()
        evidence    = (raw.get("evidence")    or source_text).strip()

        if entity_type not in VALID_ENTITY_TYPES:
            log.debug("PipelineAgent: invalid entity_type %r for %r — dropping", entity_type, name)
            return None

        now_iso = datetime.now(timezone.utc).isoformat()

        # Minimal category_fields seeded with what the LLM returned
        category_fields: dict[str, Any] = {}
        if entity_type == "corporate":
            category_fields["corporate_subtype"] = subtype or "company"
            category_fields["corporate_subtype_status"] = "REPORTED"
        elif entity_type == "investor":
            category_fields["investor_type"] = subtype or "unknown"
            category_fields["investor_type_status"] = "REPORTED"
        elif entity_type == "executive_hnw":
            category_fields["current_role"] = role or None
            category_fields["current_role_status"] = "REPORTED" if role else "NOT_COLLECTED"
        elif entity_type in ("nonprofit", "philanthropic"):
            category_fields["mission_focus"] = role or None
            category_fields["mission_focus_status"] = "REPORTED" if role else "NOT_COLLECTED"
        elif entity_type == "politician":
            category_fields["current_office"] = role or None
            category_fields["current_office_status"] = "REPORTED" if role else "NOT_COLLECTED"

        return {
            "entity_id": None,
            "canonical_name": name,
            "entity_type": entity_type,
            "entity_subtype": subtype,
            "aliases": [],
            "valid_from": now_iso,
            "valid_to": None,
            "superseded_by": None,

            "primary_city": city_name,
            "primary_city_status": "NOT_COLLECTED",
            "primary_state": None,
            "primary_state_status": "NOT_COLLECTED",
            "primary_country": "United States",
            "primary_country_status": "NOT_COLLECTED",

            "website_url": None,
            "website_url_status": "NOT_COLLECTED",
            "linkedin_url": None,
            "linkedin_url_status": "NOT_COLLECTED",
            "twitter_handle": None,
            "twitter_handle_status": "NOT_COLLECTED",

            "description": role or None,
            "description_status": "REPORTED" if role else "NOT_COLLECTED",
            "description_source_url": "",

            "external_ids": {},
            "source_agent": self.AGENT_NAME,
            "source_run_ids": [run_id],
            "merge_provenance": [],
            "source_urls": [],
            "last_seen": now_iso,
            "last_verified": None,

            "overall_confidence": "medium",
            "source_count": 1,
            "corroboration_count": 0,

            "partner_candidate": False,
            "competitor_candidate": False,
            "blocker_candidate": False,
            "investment_candidate": entity_type == "corporate",
            "support_candidate": False,
            "recruiter_candidate": False,
            "top_influencer": False,

            "score_influence": 0,
            "score_startup_relevance": 0,
            "score_partner_potential": 0,
            "score_supporter_potential": 0,
            "score_competitor_potential": 0,
            "score_blocker_risk": 0,
            "score_investment_potential": 0,
            "score_support_target": 0,
            "score_recruiting_potential": 0,

            "needs_review": False,
            "sensitivity_tier": "standard",

            "category_fields": category_fields,

            "_raw_entity_id": str(uuid.uuid4()),
            "_source": "pipeline_db",
            "_pending_evidence": [
                {
                    "entity_id": None,
                    "run_id": run_id,
                    "supported_field": "canonical_name",
                    "supported_value": name,
                    "source_url": "",
                    "source_type": "internal_database",
                    "source_api": "pipeline_db",
                    "retrieved_at": now_iso,
                    "evidence_snippet": evidence[:500] if evidence else f"Mentioned in pipeline synthesis for {city_name}",
                    "claim_type": "direct_statement",
                    "confidence": "medium",
                    "agent_name": self.AGENT_NAME,
                    "prompt_version": self.AGENT_VERSION,
                }
            ],
        }

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _empty_patch(self) -> dict[str, Any]:
        return {
            "raw_entities": [],
            **self.agent_status_patch("success"),
            **self.token_count_patch(),
            **self.entity_count_patch(),
        }
