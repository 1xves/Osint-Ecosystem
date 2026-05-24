"""
osint/clients/wayback.py

Internet Archive Wayback Machine CDX API client.

The Wayback Machine is used by the verification agent for dead link recovery:
when an evidence URL returns 404 / 410 / 301, we attempt to find an archived
snapshot of the page. If found, the evidence URL is replaced with the archive
URL so the claim is still verifiable.

API: https://web.archive.org/cdx/search/cdx
Documentation: https://github.com/internetarchive/wayback/tree/master/wayback-cdx-server
Authentication: None required. Public API.

CDX API overview:
    - Returns an index of archived pages matching a URL.
    - Supports exact URL and prefix matching.
    - Returns: timestamp, original URL, digest, MIME type, HTTP status code.
    - Timestamp format: YYYYMMDDHHmmss (e.g. 20230615142300)

Key parameters used:
    url:        The URL to check.
    output:     'json' for structured output.
    limit:      Max results (we use limit=5 — just need the most recent snapshot).
    fl:         Fields to return (timestamp, original, statuscode, mimetype).
    filter:     Filter to successful captures only (statuscode:200).
    from:       Start date (YYYYMMDD) — we ignore very old snapshots.
    collapse:   Deduplicate by digest (returns unique page versions only).

Rate limits:
    No published limit but the IA requests politeness. We use 10 req/min.
    Snapshots are heavily cached; re-requests are essentially free.

Important:
    Wayback Machine is NOT a substitute for the original source. Evidence
    records that use Wayback URLs must be flagged with:
        source_type = "archived_web_page"
        source_name = "Internet Archive Wayback Machine"
    The original URL should be preserved in the evidence snippet.
"""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urlencode

from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

DOMAIN = "wayback"
CDX_API_URL = "https://web.archive.org/cdx/search/cdx"
PLAYBACK_BASE_URL = "https://web.archive.org/web"

# Minimum year to accept for archived snapshots (avoid very stale pages)
MIN_ARCHIVE_YEAR = "2015"

# HTTP status codes that indicate the Wayback snapshot is good
GOOD_STATUS_CODES = {"200", "301", "302"}


class WaybackClient:
    """
    Async client for the Internet Archive Wayback Machine CDX API.

    Primary use: dead link recovery for the verification agent.
    """

    def __init__(self, rate_limiter: RateLimiter) -> None:
        self._rl = rate_limiter

    # ─────────────────────────────────────────────────────────────────────────
    # Availability check: does the URL have an archived snapshot?
    # ─────────────────────────────────────────────────────────────────────────

    async def find_snapshot(
        self,
        url: str,
        min_year: str = MIN_ARCHIVE_YEAR,
        require_200: bool = True,
    ) -> dict[str, Any] | None:
        """
        Find the most recent successful archived snapshot of a URL.

        Args:
            url:            The original URL to look up.
            min_year:       Only accept snapshots from this year or later (YYYY).
            require_200:    If True, only return snapshots with HTTP 200 status.

        Returns:
            A dict with snapshot info, or None if no snapshot found.
            Dict keys: timestamp, original_url, archive_url, status_code, mimetype.
        """
        params: dict[str, Any] = {
            "url": url,
            "output": "json",
            "fl": "timestamp,original,statuscode,mimetype,digest",
            "limit": 5,
            "from": f"{min_year}0101",
            "collapse": "digest",     # Return unique page versions only
        }
        if require_200:
            params["filter"] = "statuscode:200"

        # Sort newest first
        params["fastLatest"] = True

        try:
            response = await self._rl.get(
                DOMAIN,
                CDX_API_URL,
                params=params,
            )
        except Exception as exc:
            log.warning("wayback: CDX lookup failed for '%s': %s", url[:80], exc)
            return None

        return self._parse_cdx_response(response, url)

    # ─────────────────────────────────────────────────────────────────────────
    # Batch check: check multiple URLs at once
    # ─────────────────────────────────────────────────────────────────────────

    async def find_snapshots_batch(
        self,
        urls: list[str],
        min_year: str = MIN_ARCHIVE_YEAR,
    ) -> dict[str, dict[str, Any] | None]:
        """
        Check multiple URLs for Wayback snapshots.
        Runs sequentially to respect rate limits.

        Args:
            urls:       List of URLs to check.
            min_year:   Only accept snapshots from this year or later.

        Returns:
            Dict mapping original_url → snapshot_dict (or None if not found).
        """
        results: dict[str, dict[str, Any] | None] = {}
        for url in urls:
            results[url] = await self.find_snapshot(url, min_year=min_year)
        return results

    # ─────────────────────────────────────────────────────────────────────────
    # Availability API: quick check (no CDX overhead)
    # ─────────────────────────────────────────────────────────────────────────

    async def is_available(self, url: str) -> dict[str, Any]:
        """
        Use the Wayback Availability API for a fast check.
        Returns the most recent snapshot URL if it exists.

        This is cheaper than CDX for simple "does this exist?" checks.
        Returns: {available: true/false, url: str | None, timestamp: str | None}
        """
        try:
            response = await self._rl.get(
                DOMAIN,
                "https://archive.org/wayback/available",
                params={"url": url},
            )
            archived = response.get("archived_snapshots", {}).get("closest", {})
            return {
                "available": archived.get("available", False),
                "archive_url": archived.get("url"),
                "timestamp": archived.get("timestamp"),
                "status_code": archived.get("status"),
            }
        except Exception as exc:
            log.debug("wayback: availability check failed for '%s': %s", url[:80], exc)
            return {"available": False, "archive_url": None, "timestamp": None, "status_code": None}

    # ─────────────────────────────────────────────────────────────────────────
    # Static helpers
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def build_playback_url(timestamp: str, original_url: str) -> str:
        """
        Build a direct Wayback Machine playback URL from a CDX timestamp
        and original URL.

        Args:
            timestamp:      14-digit timestamp from CDX (e.g. 20230615142300).
            original_url:   The original archived URL.

        Returns:
            Full Wayback playback URL (e.g. https://web.archive.org/web/20230615142300/https://...)
        """
        return f"{PLAYBACK_BASE_URL}/{timestamp}/{original_url}"

    @staticmethod
    def _parse_cdx_response(
        response: Any,
        original_url: str,
    ) -> dict[str, Any] | None:
        """
        Parse CDX API response (JSON array of arrays) into a snapshot dict.

        CDX JSON output format:
            [["urlkey","timestamp","original","mimetype","statuscode","digest","length"], ...]
        First row is header. Subsequent rows are captures.
        """
        if not response:
            return None

        # CDX with output=json returns a list of lists
        if not isinstance(response, list) or len(response) < 2:
            return None

        # First row is header
        headers_row = response[0]
        if not isinstance(headers_row, list):
            return None

        # Build column index
        try:
            col = {h: i for i, h in enumerate(headers_row)}
        except Exception:
            return None

        # Find the first data row with a good status code
        for row in response[1:]:
            if not isinstance(row, list):
                continue
            try:
                timestamp = row[col.get("timestamp", 0)]
                status    = row[col.get("statuscode", 4)]
                mimetype  = row[col.get("mimetype", 3)]

                if status not in GOOD_STATUS_CODES:
                    continue

                archive_url = WaybackClient.build_playback_url(timestamp, original_url)
                return {
                    "timestamp":    timestamp,
                    "original_url": original_url,
                    "archive_url":  archive_url,
                    "status_code":  status,
                    "mimetype":     mimetype,
                }
            except (IndexError, TypeError):
                continue

        return None
