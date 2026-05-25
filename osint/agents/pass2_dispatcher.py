"""
osint/agents/pass2_dispatcher.py

Pass 2 Dispatcher — targeted re-collection of thin categories.

Responsibility:
    1. Read pass2_targets from state (produced by gap_analysis_agent).
    2. For each thin category, call the corresponding collection agent(s)
       with pass_number=2 set in state so agents use LLM-generated suggested_queries.
    3. Run all targeted agents concurrently (same pattern as Pass 1 fan-out).
    4. Merge new raw_entities from all agents into existing raw_entities.
    5. Persist an assessment recording what was dispatched and how many new
       entities each agent produced.
    6. Return state patch ready for resolution_agent.

Design decisions:
    Collection agent instances are injected at construction time — the same
    instances used in Pass 1.  This means:
    - Same shared infrastructure clients (DB, Redis, rate limiter)
    - Rate limiter state carries over; budgets already partially consumed
    - Each agent call produces its own agent_outputs DB record (new UUID)

    pipeline_agent is intentionally excluded: it calls an internal service
    that has no mechanism to accept targeted queries and doesn't benefit
    from gap-fill logic.

    Concurrent merge: all agents run from the same state snapshot. Each
    agent returns "raw_entities = baseline + agent_new". To merge N agents
    correctly, the dispatcher computes per-agent deltas (new entities only)
    and appends them all to the baseline. Token tracking uses the same
    delta approach.

State fields read:
    pass2_targets     list[dict] — [{entity_type, agents_to_retry, suggested_queries, ...}]
    raw_entities      list[dict] — baseline for delta extraction
    agent_statuses    dict       — extended with Pass 2 agent results
    total_tokens_in   int        — accumulated; dispatcher adds Pass 2 tokens
    total_tokens_out  int        — same

State fields written:
    raw_entities      list[dict] — baseline + all Pass 2 new entities
    pass_number       int        — set to 2
    current_phase     str        — "RESOLUTION"
    agent_statuses    dict       — updated with Pass 2 run results
    agent_token_counts dict      — updated with Pass 2 token usage
    agent_entity_counts dict     — updated with Pass 2 entity counts
    total_tokens_in   int        — updated with Pass 2 LLM usage
    total_tokens_out  int        — same
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from osint.agents.base import BaseAgent

log = logging.getLogger(__name__)

AGENT_NAME    = "pass2_dispatcher"
AGENT_VERSION = "1.0"


class Pass2Dispatcher(BaseAgent):
    """
    Coordinator for Pass 2 targeted gap-fill collection.

    Unlike collection agents, this agent does not call external APIs directly.
    It orchestrates other agents by running them with pass_number=2 state.
    """

    AGENT_NAME    = AGENT_NAME
    AGENT_VERSION = AGENT_VERSION

    def __init__(
        self,
        *deps: Any,
        collection_agents: dict[str, BaseAgent],
    ) -> None:
        """
        Args:
            *deps:             Positional deps forwarded to BaseAgent
                               (db, neo4j, chroma, redis, llm, rate_limiter).
            collection_agents: Mapping of agent_name → agent instance.
                               Should include all 9 targeted collection agents
                               (investor, philanthropic, corporate, political,
                               nonprofit, executive_hnw, community_leader,
                               politician, hnwi).
                               pipeline_agent is excluded by design.
        """
        super().__init__(*deps)
        self._collection_agents = collection_agents

    # ─────────────────────────────────────────────────────────────────────────
    # Core run method
    # ─────────────────────────────────────────────────────────────────────────

    async def run(self, state: dict[str, Any]) -> dict[str, Any]:
        pass2_targets: list[dict[str, Any]] = state.get("pass2_targets", [])
        city_name = state.get("city_name", "Unknown")

        log.info(
            "pass2_dispatcher: starting Pass 2 for %s — %d thin categories: %s",
            city_name,
            len(pass2_targets),
            [t.get("entity_type") for t in pass2_targets],
        )

        # ── Resolve agents to run ─────────────────────────────────────────────
        # Each pass2_target specifies which agent(s) to retry. Deduplicate
        # in case two targets resolve to the same agent (shouldn't happen
        # given current 1:1 entity_type → agent mapping, but be defensive).
        agents_to_run: list[tuple[str, BaseAgent]] = []
        seen_agents: set[str] = set()

        for target in pass2_targets:
            entity_type = target.get("entity_type", "unknown")
            for agent_name in target.get("agents_to_retry", []):
                if agent_name in seen_agents:
                    log.debug(
                        "pass2_dispatcher: skipping duplicate agent '%s' for type '%s'",
                        agent_name, entity_type,
                    )
                    continue
                agent = self._collection_agents.get(agent_name)
                if agent is None:
                    log.warning(
                        "pass2_dispatcher: no agent registered for '%s' "
                        "(requested by entity_type='%s') — skipping",
                        agent_name, entity_type,
                    )
                    continue
                agents_to_run.append((agent_name, agent))
                seen_agents.add(agent_name)
                log.info(
                    "pass2_dispatcher: will run '%s' for entity_type='%s' "
                    "with %d suggested queries",
                    agent_name,
                    entity_type,
                    len(target.get("suggested_queries", [])),
                )

        if not agents_to_run:
            log.warning(
                "pass2_dispatcher: no resolvable agents in pass2_targets — "
                "skipping Pass 2 and routing directly to resolution"
            )
            return self._passthrough_patch(state)

        # ── Build Pass 2 state snapshot ───────────────────────────────────────
        # Set pass_number=2 so every collection agent activates its Pass 2
        # branch and reads suggested_queries from pass2_targets.
        # All agents run from the SAME snapshot (same baseline raw_entities).
        pass2_state: dict[str, Any] = {**state, "pass_number": 2}
        baseline_entity_count = len(state.get("raw_entities", []))

        log.info(
            "pass2_dispatcher: running %d agents concurrently "
            "(baseline raw_entities=%d)",
            len(agents_to_run),
            baseline_entity_count,
        )

        # ── Run agents concurrently ───────────────────────────────────────────
        # return_exceptions=True so one agent failure doesn't kill others.
        raw_results: list[Any] = await asyncio.gather(
            *[agent(pass2_state) for _, agent in agents_to_run],
            return_exceptions=True,
        )

        # ── Merge results ─────────────────────────────────────────────────────
        # After base.py was changed to return DELTAS:
        #   - result["total_tokens_in"] is the agent's own token count (delta), NOT cumulative
        #   - result["agent_statuses"] is {agent_name: status} (single-key dict, delta)
        #   - result["agent_token_counts"] is {agent_name: tokens} (single-key dict, delta)
        # We aggregate manually here since these agents run outside LangGraph's
        # reducer machinery (they're called directly via asyncio.gather).
        tokens_in_delta  = 0
        tokens_out_delta = 0

        # Collect pass2 agent deltas into combined dicts
        merged_statuses      = {}
        merged_token_counts  = {}
        merged_entity_counts = {}

        # New entities from each agent — delta-based (all started from same baseline)
        all_new_entities: list[dict[str, Any]] = []
        agent_new_entity_counts: dict[str, int] = {}
        agent_errors: dict[str, str] = {}

        for (agent_name, _), result in zip(agents_to_run, raw_results):
            if isinstance(result, Exception):
                log.error(
                    "pass2_dispatcher: agent '%s' raised an exception: %s",
                    agent_name, result,
                    exc_info=False,
                )
                agent_errors[agent_name] = str(result)[:500]
                merged_statuses[agent_name] = "error"
                continue

            # Agents now return DELTA raw_entities (just their new finds).
            new_entities = result.get("raw_entities", [])
            all_new_entities.extend(new_entities)
            agent_new_entity_counts[agent_name] = len(new_entities)

            # Token counts are already deltas (agent's own usage only, not cumulative)
            tokens_in_delta  += result.get("total_tokens_in",  0)
            tokens_out_delta += result.get("total_tokens_out", 0)

            # Merge per-agent tracking (each result is a single-key delta dict)
            if "agent_statuses" in result:
                merged_statuses.update(result["agent_statuses"])
            if "agent_token_counts" in result:
                merged_token_counts.update(result["agent_token_counts"])
            if "agent_entity_counts" in result:
                merged_entity_counts.update(result["agent_entity_counts"])

            agent_tokens_in  = result.get("total_tokens_in",  0)
            agent_tokens_out = result.get("total_tokens_out", 0)
            log.info(
                "pass2_dispatcher: agent '%s' produced %d new entities "
                "(tokens_in+=%d tokens_out+=%d)",
                agent_name, len(new_entities), agent_tokens_in, agent_tokens_out,
            )

        # ── Summary ───────────────────────────────────────────────────────────
        total_new = len(all_new_entities)
        log.info(
            "pass2_dispatcher: Pass 2 COMPLETE — "
            "%d new entities across %d agents (errors=%d)",
            total_new,
            len(agents_to_run),
            len(agent_errors),
        )
        if total_new == 0:
            log.warning(
                "pass2_dispatcher: Pass 2 collected ZERO new entities — "
                "gap analysis targeted queries may need tuning, or sources "
                "genuinely have no additional data"
            )

        # ── Persist assessment ────────────────────────────────────────────────
        await self._write_dispatch_assessment(
            state=state,
            agents_run=[n for n, _ in agents_to_run],
            agent_new_entity_counts=agent_new_entity_counts,
            agent_errors=agent_errors,
            total_new=total_new,
        )

        # ── Build state patch (DELTA only — reducers handle accumulation) ───────
        # raw_entities: return only new pass-2 entities (operator.add reducer appends)
        # agent_statuses: combined dict of all pass2 agents + dispatcher own entry
        # token counts: deltas only (reducers accumulate)
        # Do NOT spread **state — that would feed existing list/int fields back
        # through their reducers and double-count everything.
        merged_statuses[self.AGENT_NAME] = "success"

        return {
            "raw_entities":        all_new_entities,  # delta: new entities from pass 2 only
            "pass_number":         2,
            "current_phase":       "RESOLUTION",
            "agent_statuses":      merged_statuses,   # delta: all pass2 agents + dispatcher
            "agent_token_counts":  merged_token_counts,
            "agent_entity_counts": merged_entity_counts,
            "total_tokens_in":     tokens_in_delta,   # delta: pass2 tokens only
            "total_tokens_out":    tokens_out_delta,
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _passthrough_patch(self, state: dict[str, Any]) -> dict[str, Any]:
        """
        Return a no-op patch when there are no agents to run.
        Routes directly to resolution with current state unchanged.
        """
        return {
            **self.agent_status_patch("success"),
            "pass_number":   2,
            "current_phase": "RESOLUTION",
        }

    async def _write_dispatch_assessment(
        self,
        state: dict[str, Any],
        agents_run: list[str],
        agent_new_entity_counts: dict[str, int],
        agent_errors: dict[str, str],
        total_new: int,
    ) -> None:
        """
        Write an analytical assessment recording the Pass 2 dispatch outcome.
        This is the audit trail for the gap-fill operation.
        """
        pass2_targets = state.get("pass2_targets", [])
        categories_targeted = [t.get("entity_type") for t in pass2_targets]

        if agent_errors:
            claim_text = (
                f"Pass 2 gap-fill dispatched {len(agents_run)} agents for "
                f"categories: {', '.join(categories_targeted)}. "
                f"Collected {total_new} new entities. "
                f"{len(agent_errors)} agent(s) failed: {list(agent_errors.keys())}."
            )
        else:
            claim_text = (
                f"Pass 2 gap-fill dispatched {len(agents_run)} agents for "
                f"categories: {', '.join(categories_targeted)}. "
                f"Collected {total_new} new entities across all targeted agents."
            )

        assessment = {
            "run_id":           state["run_id"],
            "assessment_type":  "pass2_dispatch",
            "claim_text":       claim_text,
            "claim_json": {
                "categories_targeted":      categories_targeted,
                "agents_dispatched":        agents_run,
                "agent_new_entity_counts":  agent_new_entity_counts,
                "agent_errors":             agent_errors,
                "total_new_entities":       total_new,
                "pass2_targets_detail": [
                    {
                        "entity_type":       t.get("entity_type"),
                        "coverage_score":    t.get("coverage_score"),
                        "entities_found_p1": t.get("entities_found"),
                        "expected_min":      t.get("expected_min"),
                        "query_count":       len(t.get("suggested_queries", [])),
                    }
                    for t in pass2_targets
                ],
            },
            "framework_name":    "pass2_dispatcher",
            "framework_version": self.AGENT_VERSION,
            "model_used":        "none",   # dispatcher makes no LLM calls
            "prompt_version":    self.AGENT_VERSION,
            "confidence":        "high",
            "needs_review":      len(agent_errors) > 0,
            "created_at":        datetime.now(timezone.utc).isoformat(),
        }

        try:
            await self.write_assessment(assessment)
        except Exception as exc:
            log.warning(
                "pass2_dispatcher: failed to write dispatch assessment: %s", exc
            )
            # Non-fatal — assessment failure does not block the pipeline
