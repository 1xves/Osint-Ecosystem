"""
osint/clients/eu_sanctions.py

EU Consolidated Financial Sanctions List client.

The European Commission publishes a consolidated list of all persons, groups,
and entities subject to EU financial sanctions. This covers sanctions regimes
distinct from OFAC — particularly:
    - Russia (post-2022 comprehensive sanctions — oligarchs, banks, officials)
    - Belarus
    - Myanmar
    - Iran (nuclear + human rights)
    - Syria
    - Venezuela
    - ISIL/Al-Qaeda network

EU sanctions on Russian oligarchs diverge significantly from OFAC. Many
high-profile Russian businesspeople are on the EU list but not OFAC.

Source:
    European Commission Financial Sanctions Files Service
    XML endpoint (updated daily):
    https://webgate.ec.europa.eu/fsd/fsf/public/files/xmlFullSanctionsList_1_1/content

Auth: None — fully public.
Format: XML (same general structure as UN list)
Cache: 24 hours (Redis), lazy-init pattern.
"""

from __future__ import annotations

import asyncio
import json
import logging
import xml.etree.ElementTree as ET
from typing import Any

import httpx

from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

DOMAIN = "eu_sanctions"
LIST_URL = (
    "https://webgate.ec.europa.eu/fsd/fsf/public/files"
    "/xmlFullSanctionsList_1_1/content"
)
REDIS_CACHE_KEY = "eu_sanctions:entries"
REDIS_TTL_SECONDS = 86400  # 24 hours

MIN_MATCH_SCORE = 70

# EU sanction regime codes of highest relevance
PRIORITY_REGIMES = {
    "RUSSIA": "Russia",
    "UKRAINE-CRISIS": "Ukraine/Russia",
    "BELARUS": "Belarus",
    "IRAN": "Iran",
    "SYRIA": "Syria",
    "MYANMAR": "Myanmar",
    "VENEZUELA": "Venezuela",
    "ISIL": "ISIL/Al-Qaeda",
    "LIBYA": "Libya",
    "NORTH-KOREA": "North Korea",
    "SUDAN": "Sudan",
    "SOMALIA": "Somalia",
    "DRCONGO": "DR Congo",
    "TALIBAN": "Taliban",
}


class EUSanctionsClient:
    """
    Async client for the EU Consolidated Financial Sanctions List.

    Same lazy-init, Redis-cached pattern as UNSanctionsClient and
    BISEntityListClient. All search() calls are in-memory.
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
                log.info("eu_sanctions: loaded %d entries from Redis cache", len(self._entries))
                return
        except Exception as e:
            log.debug("eu_sanctions: Redis cache read failed: %s", e)

        await self.refresh_cache()

    async def refresh_cache(self) -> int:
        """Fetch EU XML list, parse, cache in Redis. Returns entry count."""
        log.info("eu_sanctions: fetching consolidated list from %s", LIST_URL)
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                resp = await client.get(LIST_URL)
                resp.raise_for_status()
                xml_text = resp.text
        except Exception as e:
            log.error("eu_sanctions: failed to fetch list: %s", e)
            return 0

        entries = self._parse_xml(xml_text)
        self._entries = entries
        self._loaded = True

        try:
            await self._rl.redis_set(
                REDIS_CACHE_KEY,
                json.dumps(entries),
                ttl_seconds=REDIS_TTL_SECONDS,
            )
        except Exception as e:
            log.debug("eu_sanctions: Redis cache write failed: %s", e)

        log.info("eu_sanctions: loaded and cached %d entries", len(entries))
        return len(entries)

    def _parse_xml(self, xml_text: str) -> list[dict[str, Any]]:
        """
        Parse EU consolidated XML into flat entry dicts.

        EU XML structure uses namespace-qualified elements. The root namespace
        is typically:
            xmlns="urn:eu:esf:dataFile"
        or similar. We use namespace-agnostic iteration to handle version changes.

        Key elements per SubjectEntity:
            logicalId, remark, regulation, nameAlias (multiple), birthdate,
            citizenship, address, identification
        """
        entries: list[dict[str, Any]] = []
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            log.error("eu_sanctions: XML parse error: %s", e)
            return entries

        def _local(tag: str) -> str:
            """Strip namespace from tag: '{ns}localname' → 'localname'."""
            return tag.split("}", 1)[1] if "}" in tag else tag

        def _find_local(elem: ET.Element, local_name: str) -> ET.Element | None:
            for child in elem:
                if _local(child.tag) == local_name:
                    return child
            return None

        def _text(elem: ET.Element | None, attr: str | None = None) -> str:
            if elem is None:
                return ""
            if attr:
                return (elem.get(attr) or "").strip()
            return (elem.text or "").strip()

        # The EU list has <SubjectEntity> as the top-level sanctioned entity
        for subj in root.iter():
            if _local(subj.tag) not in ("SubjectEntity", "Entity", "Person"):
                continue

            logical_id = subj.get("logicalId") or subj.get("id") or ""

            # Collect all name aliases
            names: list[str] = []
            for alias in subj.iter():
                if _local(alias.tag) == "nameAlias":
                    whole = alias.get("wholeName") or ""
                    first = alias.get("firstName") or ""
                    middle = alias.get("middleName") or ""
                    last = alias.get("lastName") or ""
                    name_str = whole or " ".join(p for p in [first, middle, last] if p)
                    if name_str.strip():
                        names.append(name_str.strip())

            if not names:
                continue

            canonical_name = names[0]
            aliases = names[1:]

            # Regulation / regime
            regulation_elem = _find_local(subj, "regulation")
            regime = ""
            if regulation_elem is not None:
                programme = regulation_elem.get("programme") or ""
                regime = PRIORITY_REGIMES.get(programme.upper(), programme)

            # Subject type
            subj_type = _local(subj.tag).lower()  # "person" or "entity"

            # Nationality / citizenship
            nationality = ""
            for cit in subj.iter():
                if _local(cit.tag) == "citizenship":
                    nationality = cit.get("countryIso2Code") or ""
                    break

            all_names_lower = [n.lower() for n in ([canonical_name] + aliases)]

            entries.append({
                "logical_id": logical_id,
                "type": subj_type,
                "canonical_name": canonical_name,
                "aliases": aliases,
                "_searchable_lower": all_names_lower,
                "regime": regime,
                "nationality": nationality,
                "list": "EU Consolidated Financial Sanctions",
            })

        return entries

    async def search(
        self,
        name: str,
        entity_type: str | None = None,
        regime: str | None = None,
        min_score: int = MIN_MATCH_SCORE,
    ) -> dict[str, Any]:
        """
        Fuzzy name search against the cached EU sanctions list.

        Args:
            name:        Name to search.
            entity_type: Optional "person" or "entity" filter.
            regime:      Optional regime name filter (partial match, e.g. "Russia").
            min_score:   Minimum match score 0-100.

        Returns:
            {"matches": [{"logical_id": ..., "canonical_name": ...,
                          "aliases": [...], "regime": ..., "nationality": ...,
                          "score": int, "matched_name": str}],
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
            if regime and regime.lower() not in entry.get("regime", "").lower():
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

                intersection = name_tokens & candidate_tokens
                score = int(len(intersection) / len(name_tokens | candidate_tokens) * 100)

                if name_tokens.issubset(candidate_tokens):
                    score = max(score, 85)

                score = min(score, 100)

                if score > best_score:
                    best_score = score
                    best_matched_name = candidate_original

            if best_score >= min_score:
                result = {k: v for k, v in entry.items() if not k.startswith("_")}
                result["score"] = best_score
                result["matched_name"] = best_matched_name
                matches.append(result)

        matches.sort(key=lambda x: x["score"], reverse=True)

        return {
            "matches": matches[:10],
            "total_searched": len(self._entries),
            "query": name,
        }

    def is_available(self) -> bool:
        return self._loaded and len(self._entries) > 0
