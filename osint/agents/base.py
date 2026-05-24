"""
osint/agents/base.py

Abstract base class for all OSINT pipeline agents.

Every agent (collection, analytical, synthesis) inherits from BaseAgent.
BaseAgent provides:
- Structured logging with run_id context
- DB client references (supabase, neo4j, chromadb, redis)
- LLM router reference
- Rate limiter + API clients reference
- Automatic agent_output record creation and completion
- Helper methods: write_search_record, write_entity, write_evidence, write_rejected_item
- Timing and token tracking
- Consistent error handling pattern

Every agent is an async callable:
    result = await agent(state)

The callable returns a dict that is a PATCH to OSINTRunState
(LangGraph merges this with the existing state).
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from osint.core.config import settings
from osint.core.rate_limiter import RateLimiter
from osint.db.supabase import SupabaseClient
from osint.db.neo4j import Neo4jClient
from osint.db.chromadb import ChromaDBClient
from osint.db.redis import RedisClient
from osint.llm.routing import LLMRouter
from osint.schemas.records import OsintSearchRecord

log = logging.getLogger(__name__)


class AgentError(Exception):
    """Non-fatal agent error that should be caught and recorded."""
    pass


class AgentFatalError(Exception):
    """Fatal agent error that stops the pipeline."""
    pass


class BaseAgent(ABC):
    """
    Abstract base class for all OSINT pipeline agents.

    Concrete agents implement the `run` method.
    The `__call__` method handles timing, error recording, and DB writes.
    """

    # Subclasses must define these
    AGENT_NAME: str = "base_agent"
    AGENT_VERSION: str = "1.0"

    def __init__(
        self,
        db: SupabaseClient,
        neo4j: Neo4jClient,
        chroma: ChromaDBClient,
        redis: RedisClient,
        llm: LLMRouter,
        rate_limiter: RateLimiter,
    ) -> None:
        self._db = db
        self._neo4j = neo4j
        self._chroma = chroma
        self._redis = redis
        self._llm = llm
        self._rl = rate_limiter

        # Per-call tracking (reset on each __call__)
        self._run_id: str = ""
        self._output_id: str = ""
        self._started_at: float = 0.0
        self._tokens_in: int = 0
        self._tokens_out: int = 0
        self._llm_calls: int = 0
        self._api_calls: int = 0
        self._api_cached: int = 0
        self._entities_produced: int = 0
        self._relationships_produced: int = 0
        self._items_rejected: int = 0

    # ─────────────────────────────────────────────────────────────────────────
    # Public interface (LangGraph calls this)
    # ─────────────────────────────────────────────────────────────────────────

    async def __call__(self, state: dict[str, Any]) -> dict[str, Any]:
        """
        LangGraph node callable.
        Wraps agent.run() with timing, error handling, and DB writes.
        Returns state patch dict.
        """
        self._run_id = state["run_id"]
        self._started_at = time.monotonic()
        self._reset_counters()

        agent_log = logging.getLogger(f"osint.agents.{self.AGENT_NAME}")
        agent_log.info("STARTED (run=%s)", self._run_id)

        # Register agent as running in Redis for live status
        await self._redis.set_agent_status(self._run_id, self.AGENT_NAME, "running")

        # Reserve output_id — the DB row is written on completion (success or error).
        # agent_outputs only accepts terminal statuses; 'running' state lives in Redis only.
        self._output_id = str(uuid.uuid4())
        self._started_at_iso = datetime.now(timezone.utc).isoformat()

        try:
            patch = await self.run(state)

        except AgentFatalError as e:
            agent_log.error("FATAL: %s", e)
            await self._on_error(str(e))
            raise

        except Exception as e:
            agent_log.error("ERROR: %s", e, exc_info=True)
            await self._on_error(str(e))
            # Non-fatal — return partial state patch with error recorded
            return self._error_patch(state, str(e))

        else:
            elapsed_ms = int((time.monotonic() - self._started_at) * 1000)
            await self._on_success(elapsed_ms)
            agent_log.info(
                "DONE (run=%s, entities=%d, tokens_in=%d, tokens_out=%d, elapsed=%dms)",
                self._run_id, self._entities_produced,
                self._tokens_in, self._tokens_out, elapsed_ms,
            )
            return patch

    # ─────────────────────────────────────────────────────────────────────────
    # Abstract interface — implement in each agent
    # ─────────────────────────────────────────────────────────────────────────

    @abstractmethod
    async def run(self, state: dict[str, Any]) -> dict[str, Any]:
        """
        Execute agent logic. Returns a state patch dict.

        Args:
            state: Current OSINTRunState (read-only — never mutate).

        Returns:
            Dict of fields to merge into OSINTRunState.
            Must include agent_statuses update: {"agent_statuses": {self.AGENT_NAME: "success"}}
        """
        ...

    # ─────────────────────────────────────────────────────────────────────────
    # LLM helper methods
    # ─────────────────────────────────────────────────────────────────────────

    async def llm_generate_json(
        self,
        task_type: str,
        prompt: str,
        system: str | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """
        Call LLM via router, track token usage, return (parsed_json, metadata).
        """
        result_json, meta = await self._llm.call_json(task_type, prompt, system)
        self._tokens_in += meta.get("tokens_in", 0)
        self._tokens_out += meta.get("tokens_out", 0)
        self._llm_calls += 1
        return result_json, meta

    async def llm_generate(
        self,
        task_type: str,
        prompt: str,
        system: str | None = None,
    ) -> dict[str, Any]:
        """Call LLM via router, track token usage, return generation dict."""
        result = await self._llm.call(task_type, prompt, system)
        self._tokens_in += result.get("tokens_in", 0)
        self._tokens_out += result.get("tokens_out", 0)
        self._llm_calls += 1
        return result

    async def embed(self, text: str) -> list[float]:
        """Generate embedding via LLM router."""
        return await self._llm.embed(text)

    # ─────────────────────────────────────────────────────────────────────────
    # DB write helpers
    # ─────────────────────────────────────────────────────────────────────────

    async def write_entity(self, entity: dict[str, Any]) -> str:
        """Write entity to DB, increment counter, return entity_id."""
        entity_id = await self._db.write_entity(entity)
        self._entities_produced += 1
        await self._redis.increment_entity_count(self._run_id, entity["entity_type"])
        return entity_id

    async def write_evidence(self, evidence: dict[str, Any]) -> str:
        """Write evidence record to DB, return link_id."""
        return await self._db.write_evidence(evidence)

    async def write_evidence_batch(self, records: list[dict[str, Any]]) -> list[str]:
        return await self._db.write_evidence_batch(records)

    async def write_relationship(self, edge: dict[str, Any]) -> str:
        """Write relationship edge to DB, increment counter, return relationship_id."""
        relationship_id = await self._db.write_relationship(edge)
        self._relationships_produced += 1
        return relationship_id

    async def write_search_record(
        self,
        source_searched: str,
        query_used: str,
        result_found: bool,
        entity_type: str | None = None,
        entity_id: str | None = None,
        raw_entity_id: str | None = None,
        result_count: int | None = None,
        failure_reason: str | None = None,
        http_status_code: int | None = None,
        response_time_ms: int | None = None,
        served_from_cache: bool = False,
        cache_key: str | None = None,
        search_framing: str | None = None,
    ) -> str:
        """
        Write an OsintSearchRecord — called on EVERY search attempt, success or failure.
        This is the proof-of-search audit trail.
        """
        record: dict[str, Any] = {
            "run_id":          self._run_id,
            "agent_name":      self.AGENT_NAME,
            "entity_type":     entity_type,
            "entity_id":       entity_id,
            "raw_entity_id":   raw_entity_id,
            "source_searched": source_searched,
            "query_used":      query_used,
            "search_framing":  search_framing,
            "result_found":    result_found,
            "result_count":    result_count,
            "failure_reason":  failure_reason,
            "http_status_code": http_status_code,
            "response_time_ms": response_time_ms,
            "served_from_cache": served_from_cache,
            "cache_key":       cache_key,
            "timestamp":       datetime.now(timezone.utc).isoformat(),
        }
        if served_from_cache:
            self._api_cached += 1
        else:
            self._api_calls += 1

        return await self._db.write_search_record(record)

    async def write_rejected_item(
        self,
        stage: str,
        item_type: str,
        item_snapshot: dict[str, Any],
        rejection_reason: str,
        rejection_detail: str | None = None,
        item_id: str | None = None,
    ) -> str:
        """Record a rejected item. Increments rejection counter."""
        self._items_rejected += 1
        return await self._db.write_rejected_item({
            "run_id":           self._run_id,
            "agent_name":       self.AGENT_NAME,
            "stage":            stage,
            "item_type":        item_type,
            "item_id":          item_id,
            "item_snapshot":    item_snapshot,
            "rejection_reason": rejection_reason,
            "rejection_detail": rejection_detail,
            "timestamp":        datetime.now(timezone.utc).isoformat(),
        })

    async def write_assessment(self, assessment: dict[str, Any]) -> str:
        """Write an analytical assessment to DB."""
        return await self._db.write_assessment(assessment)

    # ─────────────────────────────────────────────────────────────────────────
    # State patch builders
    # ─────────────────────────────────────────────────────────────────────────

    def agent_status_patch(
        self,
        status: str,
        existing_statuses: dict[str, str] | None = None,  # kept for call-site compat; ignored
        errors: list[str] | None = None,
        existing_errors: dict[str, str] | None = None,    # kept for call-site compat; ignored
    ) -> dict[str, Any]:
        """
        Build a DELTA agent status patch.

        Returns only this agent's own key so the LangGraph _merge_dicts reducer
        can merge contributions from all parallel collection agents without
        INVALID_CONCURRENT_GRAPH_UPDATE.

        existing_statuses / existing_errors are accepted but ignored — merging
        is handled by the reducer, not by the caller.
        """
        patch: dict[str, Any] = {
            "agent_statuses": {self.AGENT_NAME: status},
        }
        if errors:
            patch["agent_errors"] = {self.AGENT_NAME: "; ".join(errors)}
        return patch

    def token_count_patch(
        self,
        existing_tokens_in: int = 0,             # kept for call-site compat; ignored
        existing_tokens_out: int = 0,            # kept for call-site compat; ignored
        existing_agent_token_counts: dict[str, int] | None = None,  # ignored
    ) -> dict[str, Any]:
        """
        Build a DELTA token count patch.

        Returns only this agent's contribution; _add_int / _merge_dicts reducers
        accumulate across all parallel agents.
        """
        return {
            "total_tokens_in": self._tokens_in,
            "total_tokens_out": self._tokens_out,
            "agent_token_counts": {
                self.AGENT_NAME: self._tokens_in + self._tokens_out,
            },
        }

    def entity_count_patch(
        self,
        existing_counts: dict[str, int] | None = None,  # kept for call-site compat; ignored
    ) -> dict[str, Any]:
        """Build a DELTA agent entity count patch."""
        return {
            "agent_entity_counts": {self.AGENT_NAME: self._entities_produced}
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Internal lifecycle helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _reset_counters(self) -> None:
        self._tokens_in = 0
        self._tokens_out = 0
        self._llm_calls = 0
        self._api_calls = 0
        self._api_cached = 0
        self._entities_produced = 0
        self._relationships_produced = 0
        self._items_rejected = 0

    async def _on_success(self, elapsed_ms: int) -> None:
        await self._redis.set_agent_status(self._run_id, self.AGENT_NAME, "success")
        await self._db.write_agent_output({
            "output_id":              self._output_id,
            "run_id":                 self._run_id,
            "agent_name":             self.AGENT_NAME,
            "agent_status":           "success",
            "model_used":             settings.ollama_default_model,
            "prompt_version":         self.AGENT_VERSION,
            "started_at":             self._started_at_iso,
            "tokens_in":              self._tokens_in,
            "tokens_out":             self._tokens_out,
            "llm_call_count":         self._llm_calls,
            "latency_ms":             elapsed_ms,
            "api_calls_made":         self._api_calls,
            "api_calls_cached":       self._api_cached,
            "entities_produced":      self._entities_produced,
            "relationships_produced": self._relationships_produced,
            "items_rejected":         self._items_rejected,
            "completed_at":           datetime.now(timezone.utc).isoformat(),
        })

    async def _on_error(self, error_message: str) -> None:
        await self._redis.set_agent_status(self._run_id, self.AGENT_NAME, "error")
        elapsed_ms = int((time.monotonic() - self._started_at) * 1000)
        await self._db.write_agent_output({
            "output_id":      self._output_id,
            "run_id":         self._run_id,
            "agent_name":     self.AGENT_NAME,
            "agent_status":   "error",
            "model_used":     settings.ollama_default_model,
            "prompt_version": self.AGENT_VERSION,
            "started_at":     self._started_at_iso,
            "error_message":  error_message[:1000],
            "tokens_in":      self._tokens_in,
            "tokens_out":     self._tokens_out,
            "latency_ms":     elapsed_ms,
            "completed_at":   datetime.now(timezone.utc).isoformat(),
        })

    def _error_patch(self, state: dict[str, Any], error_message: str) -> dict[str, Any]:
        """
        Returns a DELTA error patch — only this agent's own keys.
        _merge_dicts reducer handles merging with existing state.
        """
        return {
            "agent_statuses": {self.AGENT_NAME: "error"},
            "agent_errors":   {self.AGENT_NAME: error_message},
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Utility
    # ─────────────────────────────────────────────────────────────────────────

    def now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def new_uuid(self) -> str:
        return str(uuid.uuid4())
