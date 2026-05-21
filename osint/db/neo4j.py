"""
osint/db/neo4j.py

Neo4j client wrapper for the OSINT system.

Neo4j is a DERIVED view — Postgres is canonical.
The sync flow is: agents write to Postgres first, then call neo4j.sync_relationships()
to materialize the graph. Never write to Neo4j directly in an agent.

Node labels: Entity (with entity_type as sub-label, e.g., :Entity:Investor)
Edge types: One per RelationshipType enum value (e.g., INVESTED_IN, SITS_ON_BOARD_OF)

Usage:
    neo4j = Neo4jClient()
    await neo4j.connect()
    await neo4j.upsert_node(entity_dict)
    await neo4j.upsert_edge(relationship_edge_dict)
    await neo4j.sync_from_postgres(db_client, run_id)
"""

from __future__ import annotations

import logging
from typing import Any

from neo4j import AsyncGraphDatabase, AsyncDriver

from osint.core.config import settings

log = logging.getLogger(__name__)


class Neo4jClientError(Exception):
    pass


class Neo4jClient:
    """
    Async Neo4j client using the official neo4j-python-driver.
    """

    def __init__(self) -> None:
        self._driver: AsyncDriver | None = None

    async def connect(self) -> None:
        """
        Initialize the Neo4j driver. Call once at worker startup.
        Raises if neo4j credentials are not set.
        """
        if not settings.neo4j_password or settings.neo4j_password == "changeme":
            log.warning(
                "Neo4jClient: using default password 'changeme'. "
                "Set NEO4J_PASSWORD in .env before production use."
            )
        self._driver = AsyncGraphDatabase.driver(
            settings.neo4j_uri,
            auth=(settings.neo4j_user, settings.neo4j_password),
        )
        await self._driver.verify_connectivity()
        log.info("Neo4jClient: connected to %s", settings.neo4j_uri)

    async def disconnect(self) -> None:
        """Close the driver. Call at worker shutdown."""
        if self._driver:
            await self._driver.close()
            self._driver = None

    def _driver_required(self) -> AsyncDriver:
        if self._driver is None:
            raise Neo4jClientError(
                "Neo4jClient not connected. Call await neo4j.connect() first."
            )
        return self._driver

    # ─────────────────────────────────────────────────────────────────────────
    # Schema initialization
    # ─────────────────────────────────────────────────────────────────────────

    async def create_indexes(self) -> None:
        """
        Create uniqueness constraints and indexes on first run.
        Idempotent — safe to call on every startup.
        """
        driver = self._driver_required()
        constraints = [
            "CREATE CONSTRAINT entity_id_unique IF NOT EXISTS FOR (e:Entity) REQUIRE e.entity_id IS UNIQUE",
            "CREATE INDEX entity_type_idx IF NOT EXISTS FOR (e:Entity) ON (e.entity_type)",
            "CREATE INDEX entity_name_idx IF NOT EXISTS FOR (e:Entity) ON (e.canonical_name)",
            "CREATE INDEX entity_city_idx IF NOT EXISTS FOR (e:Entity) ON (e.primary_city)",
        ]
        async with driver.session() as session:
            for stmt in constraints:
                await session.run(stmt)
        log.info("Neo4jClient: indexes/constraints verified")

    # ─────────────────────────────────────────────────────────────────────────
    # Node operations
    # ─────────────────────────────────────────────────────────────────────────

    async def upsert_node(self, entity: dict[str, Any]) -> None:
        """
        MERGE a node from a canonical entity dict.
        Uses entity_id as the unique key — safe to call multiple times.

        Node labels: :Entity and :Investor / :Philanthropic / etc. (title-cased entity_type)
        """
        driver = self._driver_required()
        # Convert entity_type to PascalCase Neo4j label
        # e.g., "executive_hnw" → "ExecutiveHnw", "investor" → "Investor"
        entity_type_label = "".join(
            word.capitalize() for word in entity["entity_type"].split("_")
        )

        node_props = {
            "entity_id":        entity["entity_id"],
            "canonical_name":   entity["canonical_name"],
            "entity_type":      entity["entity_type"],
            "entity_subtype":   entity.get("entity_subtype"),
            "primary_city":     entity.get("primary_city"),
            "primary_state":    entity.get("primary_state"),
            "primary_country":  entity.get("primary_country", "United States"),
            "website_url":      entity.get("website_url"),
            "linkedin_url":     entity.get("linkedin_url"),
            "overall_confidence": entity.get("overall_confidence"),
            "score_influence":  entity.get("score_influence", 0),
            "score_partner_potential": entity.get("score_partner_potential", 0),
            "score_blocker_risk": entity.get("score_blocker_risk", 0),
            "partner_candidate": entity.get("partner_candidate", False),
            "blocker_candidate": entity.get("blocker_candidate", False),
            "needs_review":     entity.get("needs_review", False),
            "sensitivity_tier": entity.get("sensitivity_tier", "standard"),
        }

        # Use parameterized Cypher for safety — no string interpolation of user data
        cypher = f"""
        MERGE (e:Entity {{entity_id: $entity_id}})
        SET e += $props
        SET e:{entity_type_label}
        """
        async with driver.session() as session:
            await session.run(cypher, entity_id=entity["entity_id"], props=node_props)

        log.debug("upsert_node: %s (%s)", entity["entity_id"], entity["canonical_name"])

    async def upsert_nodes_batch(self, entities: list[dict[str, Any]]) -> None:
        """
        Batch upsert multiple entity nodes.
        Uses UNWIND for efficiency — single transaction.
        """
        driver = self._driver_required()
        if not entities:
            return

        # Build list of node param dicts (same shape for all entity types)
        nodes = [
            {
                "entity_id":      e["entity_id"],
                "canonical_name": e["canonical_name"],
                "entity_type":    e["entity_type"],
                "entity_subtype": e.get("entity_subtype"),
                "primary_city":   e.get("primary_city"),
                "primary_state":  e.get("primary_state"),
                "primary_country": e.get("primary_country", "United States"),
                "overall_confidence": e.get("overall_confidence"),
                "score_influence": e.get("score_influence", 0),
                "score_partner_potential": e.get("score_partner_potential", 0),
                "score_blocker_risk": e.get("score_blocker_risk", 0),
                "partner_candidate": e.get("partner_candidate", False),
                "blocker_candidate": e.get("blocker_candidate", False),
                "needs_review": e.get("needs_review", False),
            }
            for e in entities
        ]

        cypher = """
        UNWIND $nodes AS props
        MERGE (e:Entity {entity_id: props.entity_id})
        SET e += props
        """
        async with driver.session() as session:
            await session.run(cypher, nodes=nodes)

        log.info("upsert_nodes_batch: %d nodes", len(nodes))

    async def delete_node(self, entity_id: str) -> None:
        """
        Remove a node and all its edges. Use only for test cleanup.
        Production never deletes — entities are expired via temporal versioning.
        """
        driver = self._driver_required()
        async with driver.session() as session:
            await session.run(
                "MATCH (e:Entity {entity_id: $eid}) DETACH DELETE e",
                eid=entity_id,
            )

    # ─────────────────────────────────────────────────────────────────────────
    # Edge operations
    # ─────────────────────────────────────────────────────────────────────────

    async def upsert_edge(self, edge: dict[str, Any]) -> None:
        """
        MERGE a relationship edge between two Entity nodes.

        relationship_type must be one of the 18 RelationshipType enum values.
        Both source and target must exist as nodes (call upsert_node first).

        Edge properties stored: relationship_id, run_id, confidence,
        relationship_strength, valid_from, valid_to, verified.
        """
        driver = self._driver_required()
        rel_type = edge["relationship_type"]  # e.g., "INVESTED_IN"

        # Cypher with parameterized type requires backtick syntax since we can't
        # use $params for relationship types in MERGE.
        # rel_type comes from a validated enum — safe to interpolate.
        cypher = f"""
        MATCH (src:Entity {{entity_id: $src_id}})
        MATCH (tgt:Entity {{entity_id: $tgt_id}})
        MERGE (src)-[r:{rel_type} {{relationship_id: $rel_id}}]->(tgt)
        SET r += $props
        """
        edge_props = {
            "run_id":              edge["run_id"],
            "confidence":          edge.get("confidence", "medium"),
            "relationship_strength": edge.get("relationship_strength"),
            "valid_from":          edge.get("valid_from"),
            "valid_to":            edge.get("valid_to"),
            "verified":            edge.get("verified", False),
            "sensitive_claim":     edge.get("sensitive_claim", False),
        }

        async with driver.session() as session:
            result = await session.run(
                cypher,
                src_id=edge["source_entity_id"],
                tgt_id=edge["target_entity_id"],
                rel_id=edge["relationship_id"],
                props=edge_props,
            )
            summary = await result.consume()
            if summary.counters.relationships_created == 0 and summary.counters.properties_set == 0:
                log.warning(
                    "upsert_edge: no nodes matched for %s → %s (%s). "
                    "Did you upsert_node first?",
                    edge["source_entity_id"], edge["target_entity_id"], rel_type,
                )

    async def upsert_edges_batch(self, edges: list[dict[str, Any]]) -> None:
        """
        Batch upsert relationship edges.
        Groups by relationship_type — one query per type.
        This avoids dynamic Cypher with UNWIND for edge types.
        """
        driver = self._driver_required()
        if not edges:
            return

        # Group by relationship_type
        by_type: dict[str, list[dict]] = {}
        for edge in edges:
            rel_type = edge["relationship_type"]
            by_type.setdefault(rel_type, []).append(edge)

        async with driver.session() as session:
            for rel_type, type_edges in by_type.items():
                edge_params = [
                    {
                        "src_id": e["source_entity_id"],
                        "tgt_id": e["target_entity_id"],
                        "rel_id": e["relationship_id"],
                        "run_id": e["run_id"],
                        "confidence": e.get("confidence", "medium"),
                        "relationship_strength": e.get("relationship_strength"),
                        "verified": e.get("verified", False),
                    }
                    for e in type_edges
                ]
                cypher = f"""
                UNWIND $edges AS e
                MATCH (src:Entity {{entity_id: e.src_id}})
                MATCH (tgt:Entity {{entity_id: e.tgt_id}})
                MERGE (src)-[r:{rel_type} {{relationship_id: e.rel_id}}]->(tgt)
                SET r += e
                """
                await session.run(cypher, edges=edge_params)

        log.info("upsert_edges_batch: %d edges across %d types", len(edges), len(by_type))

    # ─────────────────────────────────────────────────────────────────────────
    # Sync from Postgres
    # ─────────────────────────────────────────────────────────────────────────

    async def sync_from_postgres(
        self,
        db_client: Any,  # SupabaseClient — avoid circular import
        run_id: str,
    ) -> dict[str, int]:
        """
        Materialize the Neo4j graph from Postgres for a completed run.

        1. Fetch all active entities produced by this run
        2. Upsert nodes
        3. Fetch all verified relationships for this run
        4. Upsert edges
        5. Mark relationships as neo4j_synced in Postgres

        Returns summary dict: {nodes_synced, edges_synced}
        """
        log.info("Neo4j sync: starting for run_id=%s", run_id)

        # 1 + 2: nodes
        entities = await db_client.get_entities_by_run(run_id)
        if entities:
            await self.upsert_nodes_batch(entities)
        log.info("Neo4j sync: %d nodes upserted", len(entities))

        # 3 + 4 + 5: edges
        edges = await db_client.get_relationships_by_run(run_id)
        if edges:
            await self.upsert_edges_batch(edges)
            await db_client.mark_neo4j_synced([e["relationship_id"] for e in edges])
        log.info("Neo4j sync: %d edges upserted", len(edges))

        return {"nodes_synced": len(entities), "edges_synced": len(edges)}

    # ─────────────────────────────────────────────────────────────────────────
    # Read operations (used by API layer)
    # ─────────────────────────────────────────────────────────────────────────

    async def get_neighbors(
        self,
        entity_id: str,
        relationship_types: list[str] | None = None,
        depth: int = 1,
    ) -> list[dict[str, Any]]:
        """
        Fetch all entities connected to entity_id up to `depth` hops.
        Optionally filtered by relationship_type list.
        """
        driver = self._driver_required()
        if relationship_types:
            rel_filter = "|".join(relationship_types)
            rel_clause = f"[:{rel_filter}*1..{depth}]"
        else:
            rel_clause = f"[*1..{depth}]"

        cypher = f"""
        MATCH (start:Entity {{entity_id: $entity_id}})-{rel_clause}-(neighbor:Entity)
        RETURN DISTINCT neighbor
        LIMIT 200
        """
        async with driver.session() as session:
            result = await session.run(cypher, entity_id=entity_id)
            rows = await result.data()
            return [dict(r["neighbor"]) for r in rows]

    async def get_shortest_path(
        self, source_id: str, target_id: str
    ) -> list[dict[str, Any]]:
        """Return the shortest path between two entities in the graph."""
        driver = self._driver_required()
        cypher = """
        MATCH p = shortestPath(
            (src:Entity {entity_id: $src_id})-[*]-(tgt:Entity {entity_id: $tgt_id})
        )
        RETURN p
        """
        async with driver.session() as session:
            result = await session.run(cypher, src_id=source_id, tgt_id=target_id)
            record = await result.single()
            if not record:
                return []
            path = record["p"]
            return [dict(node) for node in path.nodes]
