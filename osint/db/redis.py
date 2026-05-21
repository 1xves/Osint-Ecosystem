"""
osint/db/redis.py

Redis client wrapper for the OSINT system.

Responsibilities:
1. API response caching (delegated to RateLimiter — this client handles direct access)
2. Rate limit state retrieval (read-only from what RateLimiter writes)
3. Run-level state that needs to survive agent restarts within a run
4. Pub/sub for real-time run status updates (used by API → frontend)

Redis key namespace:
    cache:{sha256}                  — API response cache (written by RateLimiter)
    ratelimit:{domain}:*            — Rate limit counters (written by RateLimiter)
    spend:{domain}:{run_id}         — Budget tracking (written by RateLimiter)
    run:{run_id}:status             — Live run status string
    run:{run_id}:phase              — Current pipeline phase
    run:{run_id}:entity_counts      — JSON: entity_type → count
    run:{run_id}:agent_statuses     — JSON: agent_name → status

Usage:
    redis = RedisClient()
    await redis.connect()
    await redis.set_run_status(run_id, "running")
    status = await redis.get_run_status(run_id)
"""

from __future__ import annotations

import json
import logging
from typing import Any

import redis.asyncio as aioredis

from osint.core.config import settings

log = logging.getLogger(__name__)

# TTL for run state keys — runs don't outlive 24h
RUN_STATE_TTL = 86400  # 24 hours


class RedisClientError(Exception):
    pass


class RedisClient:
    """
    Async Redis client using redis-py with asyncio support.
    Shared across agents within a worker process.
    """

    def __init__(self) -> None:
        self._redis: aioredis.Redis | None = None

    async def connect(self) -> None:
        """
        Initialize the Redis connection pool.
        Call once at worker startup.
        """
        self._redis = aioredis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True,
            max_connections=20,
        )
        # Verify connection
        await self._redis.ping()
        log.info("RedisClient: connected to %s", settings.redis_url)

    async def disconnect(self) -> None:
        """Close the Redis connection. Call at worker shutdown."""
        if self._redis:
            await self._redis.aclose()
            self._redis = None

    def _redis_required(self) -> aioredis.Redis:
        if self._redis is None:
            raise RedisClientError(
                "RedisClient not connected. Call await redis.connect() first."
            )
        return self._redis

    def get_raw_client(self) -> aioredis.Redis:
        """
        Return the raw Redis client for use by RateLimiter.
        RateLimiter manages its own key namespace.
        """
        return self._redis_required()

    # ─────────────────────────────────────────────────────────────────────────
    # Run state management
    # Used by agents to broadcast live progress without DB writes on every step.
    # ─────────────────────────────────────────────────────────────────────────

    async def set_run_status(self, run_id: str, status: str) -> None:
        r = self._redis_required()
        await r.set(f"run:{run_id}:status", status, ex=RUN_STATE_TTL)

    async def get_run_status(self, run_id: str) -> str | None:
        r = self._redis_required()
        return await r.get(f"run:{run_id}:status")

    async def set_run_phase(self, run_id: str, phase: str) -> None:
        r = self._redis_required()
        await r.set(f"run:{run_id}:phase", phase, ex=RUN_STATE_TTL)

    async def get_run_phase(self, run_id: str) -> str | None:
        r = self._redis_required()
        return await r.get(f"run:{run_id}:phase")

    async def set_agent_status(self, run_id: str, agent_name: str, status: str) -> None:
        """Update a single agent's status within the run's agent_statuses map."""
        r = self._redis_required()
        key = f"run:{run_id}:agent_statuses"
        # Use a Redis hash so we can update individual agents without overwriting the whole map
        await r.hset(key, agent_name, status)
        await r.expire(key, RUN_STATE_TTL)

    async def get_agent_statuses(self, run_id: str) -> dict[str, str]:
        """Return all agent statuses for a run. Empty dict if none set."""
        r = self._redis_required()
        result = await r.hgetall(f"run:{run_id}:agent_statuses")
        return result or {}

    async def increment_entity_count(
        self, run_id: str, entity_type: str, amount: int = 1
    ) -> int:
        """
        Atomically increment the entity count for a type within a run.
        Returns the new count.
        """
        r = self._redis_required()
        key = f"run:{run_id}:entity_counts:{entity_type}"
        new_count = await r.incrby(key, amount)
        await r.expire(key, RUN_STATE_TTL)
        return int(new_count)

    async def get_entity_counts(self, run_id: str) -> dict[str, int]:
        """Return all entity counts for a run by type."""
        r = self._redis_required()
        # Scan for all entity count keys for this run
        pattern = f"run:{run_id}:entity_counts:*"
        counts = {}
        async for key in r.scan_iter(pattern):
            entity_type = key.split(":")[-1]
            val = await r.get(key)
            counts[entity_type] = int(val or 0)
        return counts

    async def set_run_state_snapshot(self, run_id: str, snapshot: dict[str, Any]) -> None:
        """
        Store a full run state snapshot in Redis.
        Used for run recovery if a worker crashes mid-pipeline.
        """
        r = self._redis_required()
        await r.set(
            f"run:{run_id}:state_snapshot",
            json.dumps(snapshot),
            ex=RUN_STATE_TTL,
        )

    async def get_run_state_snapshot(self, run_id: str) -> dict[str, Any] | None:
        r = self._redis_required()
        raw = await r.get(f"run:{run_id}:state_snapshot")
        if raw:
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                log.warning("get_run_state_snapshot: failed to decode for run_id=%s", run_id)
                return None
        return None

    async def cleanup_run_state(self, run_id: str) -> None:
        """
        Delete all Redis keys for a completed run.
        Call after persisting final state to Postgres.
        """
        r = self._redis_required()
        pattern = f"run:{run_id}:*"
        keys_to_delete = []
        async for key in r.scan_iter(pattern):
            keys_to_delete.append(key)
        if keys_to_delete:
            await r.delete(*keys_to_delete)
            log.info("cleanup_run_state: removed %d keys for run_id=%s", len(keys_to_delete), run_id)

    # ─────────────────────────────────────────────────────────────────────────
    # Rate limit state (read-only — RateLimiter writes these)
    # ─────────────────────────────────────────────────────────────────────────

    async def get_rate_state(self, domain: str) -> dict[str, Any]:
        """
        Read current rate limit state for a domain.
        Returns dict compatible with OSINTRunState.rate_limit_state[domain].
        This is read-only — RateLimiter owns writing these keys.
        """
        r = self._redis_required()
        from osint.core.config import RATE_LIMITS

        config = RATE_LIMITS.get(domain, {})
        requests_key = f"ratelimit:{domain}:requests"
        reset_key = f"ratelimit:{domain}:reset_at"

        requests_made = int(await r.get(requests_key) or 0)
        reset_at_raw = await r.get(reset_key)

        daily_budget = config.get("requests_per_day") or config.get("requests_per_month")

        return {
            "domain": domain,
            "requests_made": requests_made,
            "daily_budget": daily_budget,
            "remaining": max(0, (daily_budget or 999999) - requests_made),
            "reset_at": reset_at_raw,
        }

    async def get_all_rate_states(self, domains: list[str]) -> dict[str, dict[str, Any]]:
        """
        Get rate limit state for all specified domains.
        Used to populate state.rate_limit_state before a run starts.
        """
        result = {}
        for domain in domains:
            result[domain] = await self.get_rate_state(domain)
        return result

    async def get_proxycurl_spend(self, run_id: str) -> float:
        """Return current Proxycurl spend for a run."""
        r = self._redis_required()
        raw = await r.get(f"spend:proxycurl:{run_id}")
        return float(raw or 0)

    # ─────────────────────────────────────────────────────────────────────────
    # Cache stats (for monitoring, not for agent logic)
    # ─────────────────────────────────────────────────────────────────────────

    async def get_cache_hit_count(self, run_id: str) -> int:
        r = self._redis_required()
        raw = await r.get(f"run:{run_id}:cache_hits")
        return int(raw or 0)

    async def increment_cache_hit(self, run_id: str) -> None:
        r = self._redis_required()
        await r.incr(f"run:{run_id}:cache_hits")
        await r.expire(f"run:{run_id}:cache_hits", RUN_STATE_TTL)

    # ─────────────────────────────────────────────────────────────────────────
    # Pub/Sub (for live run status streaming to frontend via API)
    # ─────────────────────────────────────────────────────────────────────────

    async def publish_run_event(self, run_id: str, event: dict[str, Any]) -> None:
        """
        Publish a run state change event to the run's pub/sub channel.
        Frontend subscribes to this via SSE through the FastAPI /runs/{run_id}/stream endpoint.
        """
        r = self._redis_required()
        channel = f"run_events:{run_id}"
        await r.publish(channel, json.dumps(event))

    async def subscribe_to_run(self, run_id: str) -> aioredis.client.PubSub:
        """
        Return a subscribed PubSub object for a run's event channel.
        Caller is responsible for listening and closing.
        """
        r = self._redis_required()
        pubsub = r.pubsub()
        await pubsub.subscribe(f"run_events:{run_id}")
        return pubsub
