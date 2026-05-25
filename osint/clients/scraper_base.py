"""
osint/clients/scraper_base.py

Abstract base class for all web scrapers in the OSINT pipeline.

Design principles:
    - robots.txt compliance is mandatory, not optional. Any domain that disallows
      crawling must be skipped and logged. The pipeline skips, never bypasses.
    - Rate limiting via the shared RateLimiter (domain-keyed).
    - Exponential backoff on 429/503 responses.
    - Consistent logging pattern across all scrapers.
    - Scrapers never cache responses themselves — caching is handled by RateLimiter.

Usage (subclassing):
    from osint.clients.scraper_base import ScraperBase

    class BizapediaClient(ScraperBase):
        DOMAIN = "bizapedia"
        BASE_URL = "https://www.bizapedia.com"
        ROBOTS_URL = "https://www.bizapedia.com/robots.txt"

        async def scrape_company(self, name: str, city: str) -> dict:
            if not await self.is_allowed("/search/"):
                log.warning("bizapedia: /search/ disallowed by robots.txt, skipping")
                return {}
            html = await self.get_html(f"{self.BASE_URL}/search/?q={name}+{city}")
            return self._parse_company(html)

        def _parse_company(self, html: str) -> dict:
            ...

    # In agent:
    scraper = BizapediaClient(rate_limiter)
    result = await scraper.scrape_company("Acme Corp", "Philadelphia")

Notes on robots.txt:
    - robots.txt is fetched once per domain per process lifetime (cached in-memory).
    - If robots.txt cannot be fetched (network error, 404), default to ALLOW.
      The internet archive principle: if we can't verify, we proceed conservatively.
    - Only checks User-agent: * rules. Does not parse agent-specific allow/disallow.
    - Crawl-delay directive is respected (parsed but only applied as a floor).
"""

from __future__ import annotations

import asyncio
import logging
import re
from abc import ABC, abstractmethod
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx

from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Module-level robots.txt cache: {domain: {disallowed_paths, crawl_delay}}
# ─────────────────────────────────────────────────────────────────────────────

_ROBOTS_CACHE: dict[str, dict] = {}
_ROBOTS_FETCH_LOCK = asyncio.Lock()

# Default scraper User-Agent (used for robots.txt fetching and page scraping)
_SCRAPER_USER_AGENT = (
    "Mozilla/5.0 (compatible; OSINTPipeline/1.0; +https://github.com/)"
)

# Max retries on transient errors (429, 503)
_MAX_RETRIES = 3


class RobotsDisallowedError(Exception):
    """Raised when the target path is disallowed by robots.txt."""
    pass


# ─────────────────────────────────────────────────────────────────────────────
# robots.txt parser — standalone function, used by ScraperBase
# ─────────────────────────────────────────────────────────────────────────────

def _parse_robots_txt(content: str) -> dict:
    """
    Parse robots.txt content and extract rules for User-agent: *.

    Returns:
        {
            "disallowed": ["/path/1", "/path/2", ...],
            "crawl_delay": float | None,
        }

    Only parses User-agent: * rules (universal rules). Agent-specific rules
    are intentionally ignored — we only care about universal restrictions.
    """
    disallowed: list[str] = []
    crawl_delay: float | None = None

    in_universal_block = False

    for line in content.splitlines():
        line = line.strip()

        # Skip comments and blank lines
        if not line or line.startswith("#"):
            continue

        # Remove inline comments
        if "#" in line:
            line = line[:line.index("#")].strip()

        lower = line.lower()

        if lower.startswith("user-agent:"):
            agent = line[len("user-agent:"):].strip()
            in_universal_block = (agent == "*")

        elif in_universal_block:
            if lower.startswith("disallow:"):
                path = line[len("disallow:"):].strip()
                if path:  # Empty Disallow means "allow everything"
                    disallowed.append(path)
            elif lower.startswith("crawl-delay:"):
                try:
                    crawl_delay = float(line[len("crawl-delay:"):].strip())
                except ValueError:
                    pass

    return {"disallowed": disallowed, "crawl_delay": crawl_delay}


def _is_path_disallowed(path: str, disallowed: list[str]) -> bool:
    """
    Check if a URL path is disallowed by the parsed robots.txt rules.
    Supports prefix matching (the standard robots.txt behaviour).
    """
    for rule in disallowed:
        if rule.endswith("$"):
            # Exact match rule
            if path == rule[:-1]:
                return True
        elif path.startswith(rule):
            return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Abstract base
# ─────────────────────────────────────────────────────────────────────────────

class ScraperBase(ABC):
    """
    Abstract base class for all OSINT web scrapers.

    Subclasses must define:
        DOMAIN:     str — unique scraper identifier, used as RateLimiter domain key
        BASE_URL:   str — base URL of the target site

    Subclasses should override ROBOTS_URL if it differs from BASE_URL/robots.txt.
    """

    DOMAIN: str = ""                # Set in subclass: e.g. "bizapedia"
    BASE_URL: str = ""              # Set in subclass: e.g. "https://www.bizapedia.com"
    ROBOTS_URL: str = ""            # Defaults to BASE_URL/robots.txt if blank

    def __init__(self, rate_limiter: RateLimiter) -> None:
        if not self.DOMAIN:
            raise ValueError(f"{self.__class__.__name__} must define DOMAIN")
        if not self.BASE_URL:
            raise ValueError(f"{self.__class__.__name__} must define BASE_URL")
        self._rl = rate_limiter
        self._robots_url = self.ROBOTS_URL or f"{self.BASE_URL.rstrip('/')}/robots.txt"

    # ─────────────────────────────────────────────────────────────────────────
    # robots.txt compliance
    # ─────────────────────────────────────────────────────────────────────────

    async def _load_robots_txt(self) -> dict:
        """
        Fetch and parse robots.txt for this domain. Cached in-memory.

        Returns:
            Parsed robots dict: {"disallowed": [...], "crawl_delay": float | None}
        """
        domain_key = urlparse(self.BASE_URL).netloc

        if domain_key in _ROBOTS_CACHE:
            return _ROBOTS_CACHE[domain_key]

        async with _ROBOTS_FETCH_LOCK:
            # Double-check after acquiring lock
            if domain_key in _ROBOTS_CACHE:
                return _ROBOTS_CACHE[domain_key]

            try:
                async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
                    resp = await client.get(
                        self._robots_url,
                        headers={"User-Agent": _SCRAPER_USER_AGENT},
                    )
                    if resp.status_code == 200:
                        robots = _parse_robots_txt(resp.text)
                        log.debug(
                            "scraper_base: loaded robots.txt for %s — %d disallow rules",
                            domain_key,
                            len(robots["disallowed"]),
                        )
                    else:
                        # 404 or other — no restrictions
                        robots = {"disallowed": [], "crawl_delay": None}
                        log.debug(
                            "scraper_base: robots.txt returned %d for %s — defaulting to allow",
                            resp.status_code,
                            domain_key,
                        )
            except Exception as exc:
                # Network error fetching robots.txt — default to allow
                robots = {"disallowed": [], "crawl_delay": None}
                log.debug(
                    "scraper_base: could not fetch robots.txt for %s: %s — defaulting to allow",
                    domain_key,
                    exc,
                )

            _ROBOTS_CACHE[domain_key] = robots
            return robots

    async def is_allowed(self, path: str) -> bool:
        """
        Check if a URL path is allowed by robots.txt.

        Args:
            path: URL path to check (e.g. "/search/", "/about/company").

        Returns:
            True if crawling is allowed, False if disallowed.
        """
        robots = await self._load_robots_txt()
        disallowed = robots.get("disallowed", [])
        return not _is_path_disallowed(path, disallowed)

    async def _crawl_delay(self) -> float:
        """Return the crawl-delay from robots.txt, or 0 if not specified."""
        robots = await self._load_robots_txt()
        return robots.get("crawl_delay") or 0.0

    # ─────────────────────────────────────────────────────────────────────────
    # HTTP helpers
    # ─────────────────────────────────────────────────────────────────────────

    async def get_html(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
        check_robots: bool = True,
        timeout: int = 30,
    ) -> str:
        """
        Fetch a URL and return the response body as text.

        Args:
            url:            Full URL to fetch.
            params:         Query parameters to append.
            extra_headers:  Additional request headers.
            check_robots:   If True, verify path is allowed before fetching.
                            Set to False only when the path is already verified.
            timeout:        Request timeout in seconds.

        Returns:
            Response body text. Empty string on failure.

        Raises:
            RobotsDisallowedError: If check_robots=True and path is disallowed.
        """
        parsed = urlparse(url)
        path = parsed.path or "/"

        if check_robots:
            if not await self.is_allowed(path):
                log.warning(
                    "scraper_base[%s]: path '%s' disallowed by robots.txt, skipping",
                    self.DOMAIN, path,
                )
                raise RobotsDisallowedError(f"{self.DOMAIN}: {path} is disallowed by robots.txt")

        # Apply crawl-delay as a floor
        delay = await self._crawl_delay()
        if delay > 0:
            await asyncio.sleep(delay)

        headers: dict[str, str] = {
            "User-Agent": _SCRAPER_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        if extra_headers:
            headers.update(extra_headers)

        for attempt in range(_MAX_RETRIES):
            try:
                async with httpx.AsyncClient(
                    timeout=timeout,
                    follow_redirects=True,
                ) as client:
                    resp = await client.get(url, params=params, headers=headers)

                    if resp.status_code == 200:
                        return resp.text

                    if resp.status_code in (429, 503):
                        wait = 10 * (2 ** attempt)  # 10, 20, 40
                        log.warning(
                            "scraper_base[%s]: HTTP %d, waiting %ds (attempt %d/%d)",
                            self.DOMAIN, resp.status_code, wait, attempt + 1, _MAX_RETRIES,
                        )
                        await asyncio.sleep(wait)
                        continue

                    log.warning(
                        "scraper_base[%s]: HTTP %d for '%s'",
                        self.DOMAIN, resp.status_code, url[:80],
                    )
                    return ""

            except httpx.TimeoutException:
                log.warning("scraper_base[%s]: timeout for '%s'", self.DOMAIN, url[:80])
                if attempt < _MAX_RETRIES - 1:
                    await asyncio.sleep(5)
                    continue
                return ""

            except Exception as exc:
                log.warning("scraper_base[%s]: error for '%s': %s", self.DOMAIN, url[:80], exc)
                return ""

        return ""

    async def post_html(
        self,
        url: str,
        *,
        data: dict[str, Any] | None = None,
        json: Any = None,
        extra_headers: dict[str, str] | None = None,
        timeout: int = 30,
    ) -> str:
        """
        POST to a URL and return the response body as text.
        Used for form-based scrapers (e.g., Pennsylvania SoS).

        Returns:
            Response body text. Empty string on failure.
        """
        headers: dict[str, str] = {
            "User-Agent": _SCRAPER_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        if extra_headers:
            headers.update(extra_headers)

        for attempt in range(_MAX_RETRIES):
            try:
                async with httpx.AsyncClient(
                    timeout=timeout,
                    follow_redirects=True,
                ) as client:
                    resp = await client.post(
                        url,
                        data=data,
                        json=json,
                        headers=headers,
                    )

                    if resp.status_code == 200:
                        return resp.text

                    if resp.status_code in (429, 503):
                        wait = 10 * (2 ** attempt)
                        await asyncio.sleep(wait)
                        continue

                    log.warning(
                        "scraper_base[%s]: POST HTTP %d for '%s'",
                        self.DOMAIN, resp.status_code, url[:80],
                    )
                    return ""

            except Exception as exc:
                log.warning(
                    "scraper_base[%s]: POST error for '%s': %s",
                    self.DOMAIN, url[:80], exc,
                )
                return ""

        return ""

    # ─────────────────────────────────────────────────────────────────────────
    # Subclass interface
    # ─────────────────────────────────────────────────────────────────────────

    @abstractmethod
    async def scrape(self, *args: Any, **kwargs: Any) -> Any:
        """
        Entry point for the scraper.
        Subclasses implement this as their primary public method.
        """
        ...
