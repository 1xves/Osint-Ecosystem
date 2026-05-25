"""
osint/clients/icij.py

ICIJ Offshore Leaks runtime client — Neo4j backed.

Queries the ICIJ subgraph loaded by osint/etl/global/icij_leaks.py into Neo4j.
All ICIJ nodes carry the base label :ICIJNode; specific type labels are:
    :ICIJEntity       — offshore shell companies
    :ICIJOfficer      — officers / beneficial owners
    :ICIJAddress      — registered addresses
    :ICIJIntermediary — registered agents / law firms

Why Neo4j directly (not the ICIJ public API):
    The ICIJ public API only returns direct matches. Variable-depth shell company
    chains (entity → intermediary → offshore entity, spanning multiple hops) require
    Cypher traversal — not possible via the API. Neo4j gives us full graph access
    for shell company chain detection, which is the highest-value ICIJ signal.

Warning:
    ICIJ data is from leaked documents. The presence of an entity in this database
    does NOT constitute proof of wrongdoing. All matches must be flagged with
    needs_review=True and sensitivity_tier='restricted'. The enrichment agent
    handles sensitivity classification — this client is intentionally neutral.

Design principles:
    - Connects to the same Neo4j instance as the pipeline (settings.neo4j_*)
    - Read-only — never writes to the ICIJ subgraph
    - Returns empty results gracefully if Neo4j is unreachable or ETL not run
    - Fuzzy name matching: CONTAINS query → Python-side SequenceMatcher filter
    - Shell chain traversal uses variable-depth Cypher [*1..N] on :ICIJ_REL edges

Usage:
    client = ICIJClient()
    matches = await client.find_entity_matches("Acme Holdings", country="PA")
    if matches:
        chain = await client.get_shell_chain(matches[0]["icij_id"], max_depth=4)
    officers = await client.get_officers(matches[0]["icij_id"])
    await client.close()

ETL prerequisite:
    Run the ICIJ ETL before using this client:
        ICIJ_DATA_DIR=/path/to/icij/csvs python -m osint.etl.runner --sources icij_leaks

Dependencies:
    neo4j — pip install neo4j
"""

from __future__ import annotations

import logging
from difflib import SequenceMatcher
from typing import Any

from osint.core.config import settings

log = logging.getLogger(__name__)

DOMAIN = "icij_leaks"

# Minimum SequenceMatcher score to accept a name match.
# ICIJ names are often transliterated from non-Latin scripts — use a slightly
# lower threshold than domestic sources to account for romanization variance.
# Calibrated to match the ~0.88 Jaro-Winkler threshold from the build plan.
_FUZZY_MIN = 0.75

# Maximum candidates fetched from Neo4j before Python-side filtering.
_CANDIDATE_LIMIT = 80

# Maximum nodes returned from shell chain traversal per query.
_CHAIN_NODE_LIMIT = 150


class ICIJClient:
    """
    Async runtime client for querying the ICIJ Offshore Leaks Neo4j subgraph.

    Instantiates lazily — no connection is made until the first query.
    If Neo4j is unavailable (e.g., ETL not run), all methods return empty results
    and log a single warning rather than raising.
    """

    def __init__(self) -> None:
        self._driver = None
        self._connected = False
        self._unavailable = False  # set True after first failed connect; suppresses repeat warnings

    async def _ensure_connected(self) -> bool:
        """
        Lazily initialize the Neo4j driver.
        Returns True if connected, False if Neo4j is unavailable.
        """
        if self._connected:
            return True
        if self._unavailable:
            return False

        if not settings.neo4j_uri:
            log.debug("ICIJClient: NEO4J_URI not configured — skipping ICIJ enrichment")
            self._unavailable = True
            return False

        try:
            from neo4j import AsyncGraphDatabase
            self._driver = AsyncGraphDatabase.driver(
                settings.neo4j_uri,
                auth=(settings.neo4j_user, settings.neo4j_password),
            )
            await self._driver.verify_connectivity()

            # Confirm ICIJ data has been loaded
            async with self._driver.session() as session:
                result = await session.run(
                    "MATCH (n:ICIJNode) RETURN COUNT(n) AS cnt LIMIT 1"
                )
                record = await result.single()
                if not record or record["cnt"] == 0:
                    log.warning(
                        "ICIJClient: Neo4j reachable but no ICIJNode data found. "
                        "Run: ICIJ_DATA_DIR=/path/to/csvs "
                        "python -m osint.etl.runner --sources icij_leaks"
                    )
                    await self._driver.close()
                    self._unavailable = True
                    return False

            self._connected = True
            log.info("ICIJClient: connected to %s (ICIJ data present)", settings.neo4j_uri)
            return True

        except ImportError:
            log.warning("ICIJClient: neo4j package not installed — ICIJ enrichment disabled")
            self._unavailable = True
            return False
        except Exception as exc:
            log.warning(
                "ICIJClient: could not connect to Neo4j (%s) — ICIJ enrichment skipped",
                exc,
            )
            self._unavailable = True
            return False

    async def close(self) -> None:
        """Close the Neo4j driver."""
        if self._driver and self._connected:
            await self._driver.close()
            self._driver = None
            self._connected = False

    # ─────────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────────

    async def find_entity_matches(
        self,
        name: str,
        country: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Search for ICIJ nodes matching a pipeline entity name.

        Searches across ICIJEntity, ICIJOfficer, and ICIJIntermediary labels.
        Uses CONTAINS on name_lower for candidate retrieval, then applies
        Python-side SequenceMatcher filtering at threshold ≥ _FUZZY_MIN.

        Args:
            name:    Entity canonical name to search for.
            country: Optional ISO country code to narrow results
                     (matched against country_codes property).

        Returns:
            List of match dicts, ordered by similarity score descending.
            Each dict: {icij_id, name, icij_type, source_dataset,
                        countries, country_codes, similarity}
        """
        if not await self._ensure_connected():
            return []

        name = name.strip()
        if len(name) < 3:
            return []

        # Use first significant word for the CONTAINS index scan
        search_term = name.lower().split()[0] if name.lower().split() else name.lower()

        country_clause = (
            "AND (n.country_codes CONTAINS $country OR n.countries CONTAINS $country)"
            if country else ""
        )

        cypher = f"""
        MATCH (n:ICIJNode)
        WHERE n.name_lower CONTAINS $term
          {country_clause}
        RETURN
            n.icij_id        AS icij_id,
            n.name           AS name,
            n.icij_type      AS icij_type,
            n.source_dataset AS source_dataset,
            n.countries      AS countries,
            n.country_codes  AS country_codes,
            n.jurisdiction   AS jurisdiction,
            n.status         AS status
        LIMIT {_CANDIDATE_LIMIT}
        """

        try:
            async with self._driver.session() as session:
                result = await session.run(
                    cypher,
                    term=search_term,
                    country=country or "",
                )
                records = await result.data()
        except Exception as exc:
            log.warning("ICIJClient.find_entity_matches: query failed: %s", exc)
            return []

        # Python-side similarity filter
        matches = []
        for rec in records:
            candidate_name = (rec.get("name") or "").strip()
            if not candidate_name:
                continue
            sim = SequenceMatcher(None, name.lower(), candidate_name.lower()).ratio()
            if sim >= _FUZZY_MIN:
                matches.append({**rec, "similarity": round(sim, 3)})

        matches.sort(key=lambda x: x["similarity"], reverse=True)
        return matches

    async def get_shell_chain(
        self,
        icij_node_id: str,
        max_depth: int = 5,
    ) -> list[dict[str, Any]]:
        """
        Traverse the ICIJ graph from a given node up to max_depth hops.

        Returns all connected ICIJ nodes, showing the relationship types on
        the path from the start node. Useful for detecting multi-layer shell
        company structures (entity → intermediary → offshore entity chain).

        Args:
            icij_node_id: The ICIJ node_id (icij_id property) to start from.
            max_depth:    Maximum traversal depth (default 5). Hard cap at 6.

        Returns:
            List of connected node dicts. Each dict:
            {icij_id, name, icij_type, source_dataset, countries,
             country_codes, jurisdiction, rel_types (path rel_type list)}
        """
        if not await self._ensure_connected():
            return []

        depth = min(max_depth, 6)  # hard cap — prevent runaway traversals

        cypher = f"""
        MATCH path = (start:ICIJNode {{icij_id: $start_id}})-[r:ICIJ_REL*1..{depth}]-(connected:ICIJNode)
        WHERE connected.icij_id <> $start_id
        WITH connected,
             [rel IN relationships(path) | rel.rel_type] AS rel_types
        RETURN DISTINCT
            connected.icij_id        AS icij_id,
            connected.name           AS name,
            connected.icij_type      AS icij_type,
            connected.source_dataset AS source_dataset,
            connected.countries      AS countries,
            connected.country_codes  AS country_codes,
            connected.jurisdiction   AS jurisdiction,
            rel_types
        LIMIT {_CHAIN_NODE_LIMIT}
        """

        try:
            async with self._driver.session() as session:
                result = await session.run(cypher, start_id=icij_node_id)
                records = await result.data()
            return [dict(r) for r in records]
        except Exception as exc:
            log.warning(
                "ICIJClient.get_shell_chain: query failed for %s: %s",
                icij_node_id, exc,
            )
            return []

    async def get_officers(
        self,
        icij_entity_id: str,
    ) -> list[dict[str, Any]]:
        """
        Retrieve officers linked to an ICIJEntity via the 'officer_of' rel_type.

        Args:
            icij_entity_id: The ICIJ node_id of the entity.

        Returns:
            List of officer dicts: {icij_id, name, countries, source_dataset}
        """
        if not await self._ensure_connected():
            return []

        cypher = """
        MATCH (o:ICIJOfficer)-[r:ICIJ_REL {rel_type: 'officer_of'}]->(e:ICIJEntity {icij_id: $entity_id})
        RETURN
            o.icij_id        AS icij_id,
            o.name           AS name,
            o.countries      AS countries,
            o.country_codes  AS country_codes,
            o.source_dataset AS source_dataset
        LIMIT 50
        """

        try:
            async with self._driver.session() as session:
                result = await session.run(cypher, entity_id=icij_entity_id)
                records = await result.data()
            return [dict(r) for r in records]
        except Exception as exc:
            log.warning(
                "ICIJClient.get_officers: query failed for %s: %s",
                icij_entity_id, exc,
            )
            return []

    # ─────────────────────────────────────────────────────────────────────────
    # Static helpers
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def source_label(source_id: str) -> str:
        """Convert an ICIJ sourceID to a human-readable label."""
        label_map = {
            "panama_papers":   "Panama Papers (2016)",
            "paradise_papers": "Paradise Papers (2017)",
            "pandora_papers":  "Pandora Papers (2021)",
            "offshore_leaks":  "ICIJ Offshore Leaks",
        }
        return label_map.get((source_id or "").lower(), source_id or "ICIJ")
