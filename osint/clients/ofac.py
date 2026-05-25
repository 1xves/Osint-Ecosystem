"""
osint/clients/ofac.py

OFAC (Office of Foreign Assets Control) sanctions list client.

OFAC does not provide a REST search API for automated systems. The correct
approach is to download the public SDN (Specially Designated Nationals) CSV
file, cache it, and run local fuzzy matching.

SDN CSV:  https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.CSV
CSV columns (pipe-delimited): ent_num | SDN_Name | SDN_Type | Program | Title |
    Call_Sign | Vess_type | Tonnage | GRT | Vess_flag | Vess_owner | Remarks

Local lookup uses difflib SequenceMatcher for fuzzy name scoring, identical
semantics to what the former API promised (0–100 integer score, min_score
threshold at call site).

Cache: SDN data is stored in Redis as a JSON blob under key "ofac:sdn_list".
TTL = 86400s (1 day), matching the RATE_LIMITS["ofac"]["cache_ttl_seconds"].
If Redis is unavailable, the CSV is re-downloaded every call (still correct,
just slower).
"""

from __future__ import annotations

import csv
import io
import json
import logging
from difflib import SequenceMatcher
from typing import Any

import httpx

from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

SDN_CSV_URL = "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.CSV"
CACHE_KEY   = "ofac:sdn_list"
CACHE_TTL   = 86400   # 1 day

# OFAC SDN CSV is pipe-delimited, not comma-delimited
CSV_DELIMITER = ","

# Column indices in the OFAC SDN CSV (fixed schema)
# Row format: ent_num,SDN_Name,SDN_Type,Program,Title,Call_Sign,Vess_type,Tonnage,GRT,Vess_flag,Vess_owner,Remarks
COL_ENT_NUM  = 0
COL_SDN_NAME = 1
COL_SDN_TYPE = 2
COL_PROGRAM  = 3


def _fuzzy_score(a: str, b: str) -> int:
    """Return 0-100 similarity score between two strings (case-insensitive)."""
    ratio = SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()
    return int(ratio * 100)


class OFACClient:
    def __init__(self, rate_limiter: RateLimiter) -> None:
        self._rl    = rate_limiter
        self._redis = rate_limiter._redis if hasattr(rate_limiter, "_redis") else None
        self._sdn_cache: list[dict[str, Any]] | None = None  # in-process cache

    # ─────────────────────────────────────────────────────────────────────────
    # Public API (same interface as before — enrichment_agent uses these)
    # ─────────────────────────────────────────────────────────────────────────

    async def search(
        self,
        name: str,
        min_score: int = 80,
        source: str = "sdn",     # kept for API compatibility — only "sdn" supported
    ) -> dict[str, Any]:
        """
        Search the OFAC SDN list by name using local fuzzy matching.

        Returns a dict matching the former REST API response shape:
            {"results": [{name, score, uid, programs, type, ...}, ...]}

        Scores are 0–100. Only results >= min_score are returned.
        """
        sdn_entries = await self._get_sdn_list()
        if not sdn_entries:
            log.warning("ofac: SDN list unavailable — returning empty result for '%s'", name)
            return {"results": []}

        hits: list[dict[str, Any]] = []
        for entry in sdn_entries:
            score = _fuzzy_score(name, entry["name"])
            if score >= min_score:
                hits.append({
                    "name":     entry["name"],
                    "score":    score,
                    "uid":      entry["ent_num"],
                    "programs": entry["programs"],
                    "type":     entry["sdn_type"],
                    "aliases":  [],
                    "addresses": [],
                })

        # Sort descending by score
        hits.sort(key=lambda x: x["score"], reverse=True)
        return {"results": hits}

    async def check_entity(
        self,
        name: str,
        min_score: int = 85,
    ) -> dict[str, Any]:
        """
        Convenience: run a full sanctions check. Returns {sdn: ..., name_searched: ...}
        """
        sdn_results = await self.search(name, min_score=min_score, source="sdn")
        return {
            "name_searched": name,
            "min_score":     min_score,
            "sdn":           sdn_results,
        }

    @staticmethod
    def has_hits(search_result: dict[str, Any]) -> bool:
        """Return True if an OFAC search returned any results above threshold."""
        return len(search_result.get("results", [])) > 0

    @staticmethod
    def extract_matches(search_result: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Extract structured matches from OFAC search result.
        Returns list of {name, score, uid, programs, type}.
        """
        return list(search_result.get("results", []))

    # ─────────────────────────────────────────────────────────────────────────
    # SDN list loader — Redis-cached, falls back to live download
    # ─────────────────────────────────────────────────────────────────────────

    async def _get_sdn_list(self) -> list[dict[str, Any]]:
        """
        Load the SDN list, preferring (in order):
        1. In-process memory cache (same process, same run)
        2. Redis cache (cross-process, TTL = 1 day)
        3. Live HTTP download from OFAC
        """
        if self._sdn_cache is not None:
            return self._sdn_cache

        # Try Redis cache
        if self._redis is not None:
            try:
                cached = await self._redis.get(CACHE_KEY)
                if cached:
                    entries = json.loads(cached)
                    self._sdn_cache = entries
                    log.debug("ofac: loaded %d SDN entries from Redis cache", len(entries))
                    return entries
            except Exception as exc:
                log.warning("ofac: Redis SDN cache read failed: %s", exc)

        # Live download
        entries = await self._download_sdn_list()
        if entries:
            self._sdn_cache = entries
            # Write to Redis for other workers / future calls
            if self._redis is not None:
                try:
                    await self._redis.set(CACHE_KEY, json.dumps(entries), ex=CACHE_TTL)
                    log.info("ofac: cached %d SDN entries in Redis (TTL=%ds)", len(entries), CACHE_TTL)
                except Exception as exc:
                    log.warning("ofac: failed to write SDN cache to Redis: %s", exc)

        return entries

    async def _download_sdn_list(self) -> list[dict[str, Any]]:
        """
        Download and parse the OFAC SDN CSV.
        Returns list of {ent_num, name, sdn_type, programs}.
        """
        log.info("ofac: downloading SDN list from %s", SDN_CSV_URL)
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(
                    SDN_CSV_URL,
                    headers={"User-Agent": "OSINT-Research-Pipeline/1.0 (internal)"},
                    follow_redirects=True,
                )
                resp.raise_for_status()
                raw_text = resp.text
        except Exception as exc:
            log.error("ofac: SDN download failed: %s", exc)
            return []

        entries: list[dict[str, Any]] = []
        try:
            reader = csv.reader(io.StringIO(raw_text), delimiter=CSV_DELIMITER)
            for row in reader:
                # Skip short/malformed rows and header
                if len(row) < 4:
                    continue
                ent_num  = row[COL_ENT_NUM].strip().strip('"')
                sdn_name = row[COL_SDN_NAME].strip().strip('"')
                sdn_type = row[COL_SDN_TYPE].strip().strip('"')
                programs = row[COL_PROGRAM].strip().strip('"')

                # Skip non-name rows (vessel, aircraft) and blank names
                if not sdn_name or sdn_name.lower() in ("sdn_name", "name", ""):
                    continue

                entries.append({
                    "ent_num":  ent_num,
                    "name":     sdn_name,
                    "sdn_type": sdn_type,
                    "programs": [p.strip() for p in programs.split(";") if p.strip()],
                })
        except Exception as exc:
            log.error("ofac: SDN CSV parse failed: %s", exc)
            return []

        log.info("ofac: parsed %d SDN entries from CSV", len(entries))
        return entries
