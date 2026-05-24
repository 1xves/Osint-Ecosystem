"""
osint/clients/bis_entity_list.py

US Bureau of Industry and Security (BIS) Entity List client.

The BIS Entity List identifies foreign individuals, companies, and other
organizations subject to specific export license requirements because they
have been determined to act contrary to US national security or foreign
policy interests.

This is distinct from OFAC sanctions — BIS is export control, not financial
sanctions. An entity can be on the BIS list without being on OFAC's SDN list.
Coverage is particularly strong for:
    - Technology transfer and dual-use export violations
    - Companies supporting proliferation programs (DPRK, Iran, Russia/China)
    - Research institutions with ties to prohibited end-users

Source:
    BIS publishes the Entity List as a structured CSV/JSON file.
    The most reliable bulk download endpoint:
    https://www.bis.doc.gov/index.php/policy-guidance/lists-of-parties-of-concern/entity-list
    (CSV download link on that page)

    We use the consolidated CSV from the BIS API:
    https://efts.bis.doc.gov/completeSearchResult?sType=html&searchText=*
    — or the direct CSV file (more stable):
    https://www.bis.doc.gov/entities/0EBF8B49E77F8F168BB6D3BF1E9C3D21A28F69B1.csv

    Note: BIS does not publish a machine-readable JSON API. This client
    fetches the CSV and parses it.

Auth: None — fully public.
Cache: 24 hours (Redis), same lazy-init pattern as UNSanctionsClient.
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
from typing import Any

import httpx

from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

DOMAIN = "bis_entity_list"

# BIS publishes two complementary lists. We use the main Entity List.
ENTITY_LIST_CSV_URL = (
    "https://www.bis.doc.gov/index.php/policy-guidance/lists-of-parties-of-concern"
    "/entity-list/export/csv"
)
# Fallback: static CSV published via data.bis.doc.gov
ENTITY_LIST_FALLBACK_URL = (
    "https://efts.bis.doc.gov/results/export/csv"
    "?query=*&wizard=true&type=entity"
)

REDIS_CACHE_KEY = "bis_entity_list:entries"
REDIS_TTL_SECONDS = 86400  # 24 hours

MIN_MATCH_SCORE = 70


class BISEntityListClient:
    """
    Async client for the BIS Entity List.

    Same caching pattern as UNSanctionsClient: load once to Redis,
    search in-memory with no per-entity HTTP calls.
    """

    def __init__(self, rate_limiter: RateLimiter) -> None:
        self._rl = rate_limiter
        self._entries: list[dict[str, Any]] = []
        self._loaded = False
        self._load_lock = asyncio.Lock()

    async def ensure_loaded(self) -> None:
        if self._loaded:
            return
        async with self._load_lock:
            if self._loaded:
                return
            await self._load()

    async def _load(self) -> None:
        try:
            cached_json = await self._rl.redis_get(REDIS_CACHE_KEY)
            if cached_json:
                self._entries = json.loads(cached_json)
                self._loaded = True
                log.info("bis_entity_list: loaded %d entries from Redis cache", len(self._entries))
                return
        except Exception as e:
            log.debug("bis_entity_list: Redis cache read failed: %s", e)

        await self.refresh_cache()

    async def refresh_cache(self) -> int:
        """Fetch BIS Entity List CSV, parse, cache in Redis. Returns entry count."""
        log.info("bis_entity_list: fetching entity list CSV")
        csv_text: str | None = None

        for url in [ENTITY_LIST_CSV_URL, ENTITY_LIST_FALLBACK_URL]:
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    resp = await client.get(url, follow_redirects=True)
                    if resp.status_code == 200:
                        csv_text = resp.text
                        break
                    log.debug("bis_entity_list: %s returned %d", url, resp.status_code)
            except Exception as e:
                log.debug("bis_entity_list: fetch from %s failed: %s", url, e)

        if not csv_text:
            log.error("bis_entity_list: all fetch attempts failed")
            return 0

        entries = self._parse_csv(csv_text)
        self._entries = entries
        self._loaded = True

        try:
            await self._rl.redis_set(
                REDIS_CACHE_KEY,
                json.dumps(entries),
                ttl_seconds=REDIS_TTL_SECONDS,
            )
        except Exception as e:
            log.debug("bis_entity_list: Redis cache write failed: %s", e)

        log.info("bis_entity_list: loaded and cached %d entries", len(entries))
        return len(entries)

    def _parse_csv(self, csv_text: str) -> list[dict[str, Any]]:
        """
        Parse BIS Entity List CSV.

        BIS CSV columns (approximate — column names vary slightly between
        published versions):
            Name, Address, Federal Register Citation, License Requirement,
            License Review Policy, Fed Reg, Effective Date, FR Citation,
            Country, Related Entity

        We normalize to a consistent schema.
        """
        entries: list[dict[str, Any]] = []
        try:
            reader = csv.DictReader(io.StringIO(csv_text))
            for row in reader:
                # Normalize column names — BIS has changed these over time
                name = (
                    row.get("Name") or row.get("Entity Name") or
                    row.get("name") or ""
                ).strip()
                if not name:
                    continue

                country = (
                    row.get("Country") or row.get("country") or ""
                ).strip()
                address = (
                    row.get("Address") or row.get("address") or ""
                ).strip()
                fed_reg = (
                    row.get("Federal Register Citation") or
                    row.get("FR Citation") or
                    row.get("Fed Reg") or ""
                ).strip()
                license_req = (
                    row.get("License Requirement") or
                    row.get("License Requirement (See CFR part 744)") or ""
                ).strip()

                entries.append({
                    "name": name,
                    "_name_lower": name.lower(),
                    "country": country,
                    "address": address,
                    "federal_register_citation": fed_reg,
                    "license_requirement": license_req,
                    "list": "BIS Entity List",
                })
        except Exception as e:
            log.error("bis_entity_list: CSV parse error: %s", e)

        return entries

    async def search(
        self,
        name: str,
        country: str | None = None,
        min_score: int = MIN_MATCH_SCORE,
    ) -> dict[str, Any]:
        """
        Fuzzy name search against the cached BIS Entity List.

        Args:
            name:      Entity or individual name to search.
            country:   Optional ISO 3166 country filter (e.g. "CN", "IR").
            min_score: Minimum match score 0-100.

        Returns:
            {"matches": [{"name": ..., "country": ..., "address": ...,
                          "federal_register_citation": ...,
                          "license_requirement": ...,
                          "score": int}],
             "total_searched": int,
             "query": str}
        """
        await self.ensure_loaded()

        name_tokens = set(name.lower().split())
        if not name_tokens:
            return {"matches": [], "total_searched": 0, "query": name}

        matches: list[dict[str, Any]] = []

        for entry in self._entries:
            if country and entry.get("country", "").upper() != country.upper():
                continue

            candidate_tokens = set(entry["_name_lower"].split())
            if not candidate_tokens:
                continue

            intersection = name_tokens & candidate_tokens
            score = int(len(intersection) / len(name_tokens | candidate_tokens) * 100)

            if name_tokens.issubset(candidate_tokens):
                score = max(score, 85)

            score = min(score, 100)

            if score >= min_score:
                result = {k: v for k, v in entry.items() if not k.startswith("_")}
                result["score"] = score
                matches.append(result)

        matches.sort(key=lambda x: x["score"], reverse=True)

        return {
            "matches": matches[:10],
            "total_searched": len(self._entries),
            "query": name,
        }

    def is_available(self) -> bool:
        return self._loaded and len(self._entries) > 0
