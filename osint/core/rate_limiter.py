"""
osint/core/rate_limiter.py

Domain-aware rate limiter for all external API calls.
All agent HTTP calls go through this — never call an external API directly.

Features:
- Per-domain request rate enforcement
- Exponential backoff on 429/503 responses
- Redis-backed state (survives worker restarts, shared across workers)
- Budget enforcement for paid-per-call APIs (Proxycurl)
- Automatic cache-before-fetch (Redis cache checked before any HTTP call)

Usage:
    limiter = RateLimiter(redis_client)
    response = await limiter.get("crunchbase", url, params=params)
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
import logging
from typing import Any
from datetime import datetime, timezone

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from osint.core.config import RATE_LIMITS, settings

log = logging.getLogger(__name__)


class RateLimitExceeded(Exception):
    """Raised when daily/monthly quota is exhausted."""
    pass


class BudgetExceeded(Exception):
    """Raised when per-run spend cap is hit (e.g., Proxycurl)."""
    pass


class RateLimiter:
    """
    Async rate limiter backed by Redis for shared state across workers.
    One instance per worker process; Redis coordinates across workers.
    """

    def __init__(self, redis_client):
        """
        Args:
            redis_client: Connected async Redis client instance.
        """
        self._redis = redis_client

    # ─────────────────────────────────────────────────────────────────────────
    # Public interface
    # ─────────────────────────────────────────────────────────────────────────

    async def get(
        self,
        domain: str,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 30.0,
        skip_cache: bool = False,
    ) -> dict[str, Any]:
        """
        Make a rate-limited GET request with Redis caching.

        1. Check Redis cache — return cached response if valid.
        2. Enforce rate limit — wait if needed.
        3. Make HTTP request with retry on transient errors.
        4. Cache successful response.
        5. Return response body as dict.

        Args:
            domain: Key into RATE_LIMITS config (e.g., "crunchbase").
            url: Full URL to request.
            params: Query parameters.
            headers: Additional headers.
            timeout: Request timeout in seconds.
            skip_cache: Force a fresh request even if cached.

        Returns:
            Parsed JSON response body.

        Raises:
            RateLimitExceeded: Daily/monthly quota exhausted.
            httpx.HTTPStatusError: Non-retryable HTTP error.
        """
        cache_key = self._cache_key(domain, url, params)
        config = RATE_LIMITS.get(domain, {})

        # ── 1. Cache check ─────────────────────────────────────────────────
        if not skip_cache:
            cached = await self._get_cache(cache_key)
            if cached is not None:
                log.debug("Cache hit: %s %s", domain, url)
                return cached

        # ── 2. Rate limit enforcement ──────────────────────────────────────
        await self._enforce_rate_limit(domain, config)

        # ── 3. HTTP request with retry ─────────────────────────────────────
        response_data = await self._fetch_with_retry(domain, url, params, headers, timeout, config)

        # ── 4. Cache the response ─────────────────────────────────────────
        ttl = config.get("cache_ttl_seconds", 3600)
        await self._set_cache(cache_key, response_data, ttl)

        return response_data

    async def post(
        self,
        domain: str,
        url: str,
        json_body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 30.0,
    ) -> dict[str, Any]:
        """
        Make a rate-limited POST request. POST responses are not cached.
        """
        config = RATE_LIMITS.get(domain, {})
        await self._enforce_rate_limit(domain, config)
        return await self._fetch_with_retry(
            domain, url, None, headers, timeout, config, method="POST", json_body=json_body
        )

    async def check_budget(self, domain: str, run_id: str) -> None:
        """
        Check if per-run budget is exceeded for paid-per-call APIs.
        Call before each Proxycurl request.

        Raises:
            BudgetExceeded: If spend has exceeded the configured cap.
        """
        config = RATE_LIMITS.get(domain, {})
        budget_cap = config.get("budget_per_run_usd")
        if budget_cap is None:
            return  # No budget cap for this domain

        spend_key = f"spend:{domain}:{run_id}"
        current_spend_raw = await self._redis.get(spend_key)
        current_spend = float(current_spend_raw or 0)

        if current_spend >= budget_cap:
            raise BudgetExceeded(
                f"{domain} budget cap ${budget_cap:.2f} reached for run {run_id}. "
                f"Current spend: ${current_spend:.2f}"
            )

    async def record_spend(self, domain: str, run_id: str, amount_usd: float) -> float:
        """
        Record a spend against the per-run budget. Returns new total spend.
        """
        spend_key = f"spend:{domain}:{run_id}"
        new_total = await self._redis.incrbyfloat(spend_key, amount_usd)
        # Expire after 24h — runs don't last longer than this
        await self._redis.expire(spend_key, 86400)
        return float(new_total)

    async def get_rate_state(self, domain: str) -> dict[str, Any]:
        """
        Returns current rate limit state for a domain.
        Used to populate state.rate_limit_state.
        """
        config = RATE_LIMITS.get(domain, {})
        requests_key = f"ratelimit:{domain}:requests"
        reset_key = f"ratelimit:{domain}:reset_at"

        requests_made = int(await self._redis.get(requests_key) or 0)
        reset_at = await self._redis.get(reset_key)

        daily_budget = config.get("requests_per_day") or config.get("requests_per_month")

        return {
            "domain": domain,
            "requests_made": requests_made,
            "daily_budget": daily_budget,
            "remaining": max(0, (daily_budget or 999999) - requests_made),
            "reset_at": reset_at.decode() if reset_at else None,
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Internal helpers
    # ─────────────────────────────────────────────────────────────────────────

    async def _enforce_rate_limit(self, domain: str, config: dict) -> None:
        """
        Enforce per-minute and per-second rate limits using Redis counters.
        Sleeps if needed. Raises RateLimitExceeded if daily quota is gone.
        """
        requests_per_minute = config.get("requests_per_minute")
        requests_per_second = config.get("requests_per_second")
        requests_per_day = config.get("requests_per_day")

        if requests_per_day:
            day_key = f"ratelimit:{domain}:day:{datetime.now(timezone.utc).date()}"
            day_count = int(await self._redis.get(day_key) or 0)
            if day_count >= requests_per_day:
                raise RateLimitExceeded(
                    f"{domain} daily limit of {requests_per_day} requests reached."
                )
            await self._redis.incr(day_key)
            await self._redis.expire(day_key, 86400)  # expire tomorrow

        if requests_per_minute:
            minute_key = f"ratelimit:{domain}:minute:{int(time.time() // 60)}"
            minute_count = int(await self._redis.get(minute_key) or 0)
            if minute_count >= requests_per_minute:
                sleep_for = 60 - (time.time() % 60)
                log.info("Rate limit: sleeping %.1fs for %s", sleep_for, domain)
                await asyncio.sleep(sleep_for)
            await self._redis.incr(minute_key)
            await self._redis.expire(minute_key, 120)  # 2 minute window

        if requests_per_second:
            second_key = f"ratelimit:{domain}:second:{int(time.time())}"
            second_count = int(await self._redis.get(second_key) or 0)
            if second_count >= requests_per_second:
                await asyncio.sleep(1.0)
            await self._redis.incr(second_key)
            await self._redis.expire(second_key, 5)

    async def _fetch_with_retry(
        self,
        domain: str,
        url: str,
        params: dict | None,
        headers: dict | None,
        timeout: float,
        config: dict,
        method: str = "GET",
        json_body: dict | None = None,
    ) -> dict[str, Any]:
        """
        Makes the actual HTTP request with exponential backoff on transient errors.
        """
        backoff_seconds = config.get("retry_backoff_seconds", [2, 5, 15, 30])
        max_attempts = len(backoff_seconds) + 1

        last_exception = None

        async with httpx.AsyncClient(timeout=timeout) as client:
            for attempt, wait_time in enumerate([0] + backoff_seconds):
                if wait_time > 0:
                    log.warning(
                        "Retry %d/%d for %s after %.0fs",
                        attempt, max_attempts, domain, wait_time
                    )
                    await asyncio.sleep(wait_time)

                try:
                    if method == "GET":
                        resp = await client.get(url, params=params, headers=headers)
                    else:
                        resp = await client.post(url, json=json_body, headers=headers)

                    if resp.status_code in (429, 503, 502):
                        # Transient — retry
                        log.warning("%s returned %d, will retry", domain, resp.status_code)
                        last_exception = httpx.HTTPStatusError(
                            f"HTTP {resp.status_code}", request=resp.request, response=resp
                        )
                        continue

                    resp.raise_for_status()
                    return resp.json()

                except (httpx.ConnectError, httpx.TimeoutException) as e:
                    log.warning("Network error on %s: %s", domain, e)
                    last_exception = e
                    continue

        raise last_exception or RuntimeError(f"All retries exhausted for {domain} {url}")

    def _cache_key(self, domain: str, url: str, params: dict | None) -> str:
        """Deterministic cache key from domain + URL + sorted params."""
        raw = f"{domain}:{url}:{json.dumps(params or {}, sort_keys=True)}"
        return f"cache:{hashlib.sha256(raw.encode()).hexdigest()}"

    async def _get_cache(self, key: str) -> dict | None:
        """Retrieve from Redis cache. Returns None on miss."""
        raw = await self._redis.get(key)
        if raw:
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                return None
        return None

    async def _set_cache(self, key: str, data: dict, ttl: int) -> None:
        """Write to Redis cache with TTL."""
        await self._redis.setex(key, ttl, json.dumps(data))
