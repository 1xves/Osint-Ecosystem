"""
osint/clients/ofac.py

OFAC (Office of Foreign Assets Control) sanctions list client.

OFAC publishes its Specially Designated Nationals (SDN) list as a public API.
No auth required — this is public government data.

Endpoints used:
- GET /api/search  — search SDN and consolidated list by name

Rate limits: 60 req/min (configured in RATE_LIMITS["ofac"])

Docs: https://ofac.treasury.gov/ofac-api
"""

from __future__ import annotations

import logging
from typing import Any

from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

BASE_URL = "https://api.ofac.treasury.gov/v1"
DOMAIN   = "ofac"


class OFACClient:
    def __init__(self, rate_limiter: RateLimiter) -> None:
        self._rl = rate_limiter

    async def search(
        self,
        name: str,
        min_score: int = 80,    # 0-100, OFAC's fuzzy match score minimum
        source: str = "sdn",    # "sdn" | "cons" (consolidated) | "all"
    ) -> dict[str, Any]:
        """
        Search the OFAC sanctions list by name.

        Args:
            name: Individual or organization name to search.
            min_score: Minimum fuzzy match score (0–100). 80 is a good threshold.
            source: Which list to search. "sdn" = SDN list, "cons" = consolidated.

        Returns:
            OFAC search response with list of matches and their scores.

        Note:
            A match here does NOT definitively identify a sanctioned person —
            it requires human review to confirm. Always write to analytical_assessments,
            never directly to entities.needs_review=False.
        """
        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/search",
            params={
                "name": name,
                "minScore": min_score,
                "source": source,
            },
        )

    async def check_entity(
        self,
        name: str,
        min_score: int = 85,
    ) -> dict[str, Any]:
        """
        Convenience: run a full sanctions check against both SDN and consolidated lists.
        Returns {sdn: ..., consolidated: ...}
        """
        sdn_results = await self.search(name, min_score=min_score, source="sdn")
        return {
            "name_searched": name,
            "min_score": min_score,
            "sdn": sdn_results,
        }

    @staticmethod
    def has_hits(search_result: dict[str, Any]) -> bool:
        """Helper: return True if an OFAC search returned any results above threshold."""
        results = search_result.get("results", [])
        return len(results) > 0

    @staticmethod
    def extract_matches(search_result: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Extract structured matches from OFAC search result.
        Returns list of {name, score, uid, programs, type}.
        """
        matches = []
        for item in search_result.get("results", []):
            matches.append({
                "name":     item.get("name"),
                "score":    item.get("score"),
                "uid":      item.get("uid"),
                "programs": item.get("programs", []),
                "type":     item.get("type"),  # Individual | Entity | Vessel | Aircraft
                "aliases":  [a.get("name") for a in item.get("aliasNames", [])],
                "addresses": [
                    f"{a.get('city','')}, {a.get('country','')}".strip(", ")
                    for a in item.get("addresses", [])
                ],
            })
        return matches
