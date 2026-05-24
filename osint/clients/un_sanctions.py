"""
osint/clients/un_sanctions.py

UN Security Council Consolidated Sanctions List client.

Source: https://scsanctions.un.org/resources/xml/en/consolidated.xml
        Published by the UN Security Council 1267/1988/2253 Committee and
        related committees (Libya, Sudan, DPRK, etc.)
Auth:   None — fully public.
Update: List is updated irregularly; daily cache refresh is sufficient.

Caching strategy:
    This client fetches the entire consolidated XML list (typically 1-3 MB)
    and caches the parsed entries in Redis under key "un_sanctions:entries"
    with a 24-hour TTL. Individual search() calls operate against this
    in-memory cache — no per-entity HTTP request is made.

    The cache is populated on first use (lazy init) or by calling
    refresh_cache() explicitly from a scheduled job.

Entity types covered:
    - Individuals: terrorists, proliferators, money launderers designated
      by the Security Council
    - Entities: organizations, companies, and vessels under UN sanctions

Important note on ICIJ vs UN sanctions:
    UN sanctions are distinct from ICIJ offshore leaks data. A UN sanctions
    match is a formal multilateral designation — higher evidentiary weight
    than an ICIJ appearance, but covers a narrower population (Security
    Council designated entities only).

Mandatory controls: All matches must set needs_review=True,
sensitivity_tier="restricted", confidence_required="high".
"""

from __future__ import annotations

import asyncio
import json
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any

import httpx

from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

DOMAIN = "un_sanctions"
LIST_URL = "https://scsanctions.un.org/resources/xml/en/consolidated.xml"
REDIS_CACHE_KEY = "un_sanctions:entries"
REDIS_TTL_SECONDS = 86400  # 24 hours

# Minimum fuzzy name match score to consider a hit (0-100)
MIN_MATCH_SCORE = 70


class UNSanctionsClient:
    """
    Async client for the UN Consolidated Sanctions List.

    Maintains a parsed, in-memory (and Redis-cached) copy of the list.
    Individual search() calls are local — no HTTP call per entity.
    """

    def __init__(self, rate_limiter: RateLimiter) -> None:
        self._rl = rate_limiter
        self._entries: list[dict[str, Any]] = []   # In-memory after first load
        self._loaded = False
        self._load_lock = asyncio.Lock()

    # ─────────────────────────────────────────────────────────────────────────
    # Cache management
    # ─────────────────────────────────────────────────────────────────────────

    async def ensure_loaded(self) -> None:
        """Ensure the sanctions list is loaded (from Redis cache or HTTP)."""
        if self._loaded:
            return
        async with self._load_lock:
            if self._loaded:
                return
            await self._load()

    async def _load(self) -> None:
        """Try Redis cache first, then HTTP fetch."""
        # Attempt Redis cache read via rate_limiter's Redis connection
        try:
            cached_json = await self._rl.redis_get(REDIS_CACHE_KEY)
            if cached_json:
                self._entries = json.loads(cached_json)
                self._loaded = True
                log.info("un_sanctions: loaded %d entries from Redis cache", len(self._entries))
                return
        except Exception as e:
            log.debug("un_sanctions: Redis cache read failed: %s — will fetch from URL", e)

        await self.refresh_cache()

    async def refresh_cache(self) -> int:
        """
        Fetch the UN consolidated XML list, parse it, cache in Redis.
        Returns count of entries loaded.
        """
        log.info("un_sanctions: fetching consolidated list from %s", LIST_URL)
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(LIST_URL)
                resp.raise_for_status()
                xml_text = resp.text
        except Exception as e:
            log.error("un_sanctions: failed to fetch list: %s", e)
            return 0

        entries = self._parse_xml(xml_text)
        self._entries = entries
        self._loaded = True

        # Write to Redis cache
        try:
            await self._rl.redis_set(
                REDIS_CACHE_KEY,
                json.dumps(entries),
                ttl_seconds=REDIS_TTL_SECONDS,
            )
        except Exception as e:
            log.debug("un_sanctions: Redis cache write failed: %s", e)

        log.info("un_sanctions: loaded and cached %d entries", len(entries))
        return len(entries)

    def _parse_xml(self, xml_text: str) -> list[dict[str, Any]]:
        """
        Parse UN consolidated XML into a flat list of entry dicts.

        XML structure:
            <CONSOLIDATED_LIST>
              <INDIVIDUALS>
                <INDIVIDUAL>
                  <DATAID>...</DATAID>
                  <FIRST_NAME>...</FIRST_NAME>
                  <SECOND_NAME>...</SECOND_NAME>
                  <UN_LIST_TYPE>...</UN_LIST_TYPE>
                  <REFERENCE_NUMBER>...</REFERENCE_NUMBER>
                  <LISTED_ON>...</LISTED_ON>
                  <NATIONALITY>...</NATIONALITY>
                  <INDIVIDUAL_ALIAS>...</INDIVIDUAL_ALIAS>
                  <INDIVIDUAL_ADDRESS>...</INDIVIDUAL_ADDRESS>
                  <DESIGNATION>...</DESIGNATION>
                  <ADDITIONAL_INFORMATION>...</ADDITIONAL_INFORMATION>
                </INDIVIDUAL>
              </INDIVIDUALS>
              <ENTITIES>
                <ENTITY>
                  <DATAID>...</DATAID>
                  <FIRST_NAME>...</FIRST_NAME>   (entity name)
                  <UN_LIST_TYPE>...</UN_LIST_TYPE>
                  ...
                </ENTITY>
              </ENTITIES>
            </CONSOLIDATED_LIST>
        """
        entries: list[dict[str, Any]] = []
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            log.error("un_sanctions: XML parse error: %s", e)
            return entries

        def _text(elem: ET.Element | None) -> str:
            return (elem.text or "").strip() if elem is not None else ""

        for entry_type in ["INDIVIDUAL", "ENTITY"]:
            for elem in root.iter(entry_type):
                first = _text(elem.find("FIRST_NAME"))
                second = _text(elem.find("SECOND_NAME"))
                third = _text(elem.find("THIRD_NAME"))
                fourth = _text(elem.find("FOURTH_NAME"))

                name_parts = [p for p in [first, second, third, fourth] if p]
                canonical_name = " ".join(name_parts)
                if not canonical_name:
                    continue

                # Collect all aliases
                aliases: list[str] = []
                for alias_elem in elem.findall("INDIVIDUAL_ALIAS"):
                    alias_name = _text(alias_elem.find("ALIAS_NAME"))
                    if alias_name:
                        aliases.append(alias_name)
                for alias_elem in elem.findall("ENTITY_ALIAS"):
                    alias_name = _text(alias_elem.find("ALIAS_NAME"))
                    if alias_name:
                        aliases.append(alias_name)

                # Build all searchable names (canonical + aliases)
                searchable_names = [canonical_name] + aliases
                searchable_lower = [n.lower() for n in searchable_names]

                entries.append({
                    "dataid": _text(elem.find("DATAID")),
                    "type": entry_type.lower(),   # "individual" or "entity"
                    "canonical_name": canonical_name,
                    "aliases": aliases,
                    "_searchable_lower": searchable_lower,
                    "list_type": _text(elem.find("UN_LIST_TYPE")),
                    "reference_number": _text(elem.find("REFERENCE_NUMBER")),
                    "listed_on": _text(elem.find("LISTED_ON")),
                    "nationality": _text(elem.find("NATIONALITY")),
                    "designation": _text(elem.find("DESIGNATION")),
                    "additional_info": _text(elem.find("ADDITIONAL_INFORMATION")),
                })

        return entries

    # ─────────────────────────────────────────────────────────────────────────
    # Search interface
    # ─────────────────────────────────────────────────────────────────────────

    async def search(
        self,
        name: str,
        entity_type: str | None = None,
        min_score: int = MIN_MATCH_SCORE,
    ) -> dict[str, Any]:
        """
        Fuzzy name search against the cached UN consolidated list.

        Uses token-based overlap scoring — not edit distance. This handles
        name reorderings (common in Arabic and East Asian names) better
        than edit distance approaches.

        Args:
            name:        Name to search for (individual or entity).
            entity_type: Optional "individual" or "entity" filter.
            min_score:   Minimum overlap score 0-100. Default: 70.

        Returns:
            {"matches": [{"dataid": ..., "canonical_name": ...,
                          "aliases": [...], "list_type": ...,
                          "reference_number": ..., "listed_on": ...,
                          "score": int,
                          "matched_name": str}],
             "total_searched": int,
             "query": str}
        """
        await self.ensure_loaded()

        name_tokens = set(name.lower().split())
        if not name_tokens:
            return {"matches": [], "total_searched": 0, "query": name}

        matches: list[dict[str, Any]] = []

        for entry in self._entries:
            if entity_type and entry["type"] != entity_type.lower():
                continue

            best_score = 0
            best_matched_name = ""

            for candidate_lower, candidate_original in zip(
                entry["_searchable_lower"],
                [entry["canonical_name"]] + entry["aliases"],
            ):
                candidate_tokens = set(candidate_lower.split())
                if not candidate_tokens:
                    continue

                # Token overlap: intersection / union (Jaccard-like, but weighted toward query)
                intersection = name_tokens & candidate_tokens
                score = int(len(intersection) / len(name_tokens | candidate_tokens) * 100)

                # Bonus: if all query tokens appear in candidate
                if name_tokens.issubset(candidate_tokens):
                    score = max(score, 85)

                # Bonus: if first token matches exactly (name prefix match)
                name_first = list(name_tokens)[0] if name_tokens else ""
                candidate_first = list(candidate_tokens)[0] if candidate_tokens else ""
                if name_first and candidate_first and name_first == candidate_first:
                    score = max(score, score + 10)

                score = min(score, 100)

                if score > best_score:
                    best_score = score
                    best_matched_name = candidate_original

            if best_score >= min_score:
                result = {k: v for k, v in entry.items() if not k.startswith("_")}
                result["score"] = best_score
                result["matched_name"] = best_matched_name
                matches.append(result)

        # Sort by score descending
        matches.sort(key=lambda x: x["score"], reverse=True)

        return {
            "matches": matches[:10],  # Cap at 10 results
            "total_searched": len(self._entries),
            "query": name,
        }

    def is_available(self) -> bool:
        """True if the list has been successfully loaded."""
        return self._loaded and len(self._entries) > 0
