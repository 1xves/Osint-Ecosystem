"""
osint/db/chromadb.py

ChromaDB client wrapper for entity resolution via embedding similarity.

Used exclusively by the Entity Resolution Agent (Layer 3) to find semantically
similar entities when exact ID match (Layer 1) and fuzzy string match (Layer 2)
both fail.

Collection: "entity_embeddings"
  - documents: canonical_name + key descriptors (not full entity JSON)
  - embeddings: nomic-embed-text vectors
  - metadatas: entity_id, entity_type, city_key, run_id

Decision thresholds (from RESOLUTION_THRESHOLDS in config):
  - distance < 0.15  (similarity ≥ 0.85): auto-merge
  - distance < 0.40  (similarity ≥ 0.60): human review queue
  - distance ≥ 0.40: reject — not the same entity

Usage:
    chroma = ChromaDBClient()
    await chroma.connect()
    await chroma.upsert_entity_embedding(entity_id, embed_text, metadata)
    results = await chroma.query_similar(embed_text, city_key, top_k=5)
"""

from __future__ import annotations

import asyncio
import logging
from functools import partial
from typing import Any

import chromadb
from chromadb import AsyncHttpClient

from osint.core.config import settings

log = logging.getLogger(__name__)

# ChromaDB collection name — single collection for all entity types.
# Entity type filtering is done via metadata, not separate collections.
COLLECTION_NAME = "entity_embeddings"


class ChromaDBClientError(Exception):
    pass


class ChromaDBClient:
    """
    Async ChromaDB client wrapping chromadb.AsyncHttpClient.

    ChromaDB does not have a native async API; the AsyncHttpClient
    is async at the network level but many operations are still blocking.
    We use asyncio.get_event_loop().run_in_executor for blocking calls.
    """

    def __init__(self) -> None:
        self._client: chromadb.AsyncHttpClient | None = None
        self._collection: Any | None = None

    async def connect(self) -> None:
        """
        Connect to the ChromaDB HTTP server.
        Call once at worker startup. ChromaDB must be running (see docker-compose.yml).
        """
        self._client = await AsyncHttpClient(
            host=settings.chromadb_host,
            port=settings.chromadb_port,
        )
        self._collection = await self._client.get_or_create_collection(
            name=COLLECTION_NAME,
            metadata={
                "hnsw:space": "cosine",     # Cosine similarity for text embeddings
                "description": "Entity embeddings for resolution Layer 3",
            },
        )
        count = await self._collection.count()
        log.info(
            "ChromaDBClient: connected to %s:%d — collection '%s' has %d documents",
            settings.chromadb_host, settings.chromadb_port, COLLECTION_NAME, count,
        )

    def _collection_required(self) -> Any:
        if self._collection is None:
            raise ChromaDBClientError(
                "ChromaDBClient not connected. Call await chroma.connect() first."
            )
        return self._collection

    # ─────────────────────────────────────────────────────────────────────────
    # Write operations
    # ─────────────────────────────────────────────────────────────────────────

    async def upsert_entity_embedding(
        self,
        entity_id: str,
        embed_text: str,
        embedding: list[float],
        metadata: dict[str, Any],
    ) -> None:
        """
        Upsert a single entity embedding into the collection.

        Args:
            entity_id: UUID string — used as the ChromaDB document ID.
            embed_text: The text that was embedded (for inspection/debugging).
            embedding: Float vector from nomic-embed-text.
            metadata: Dict must include: entity_type, city_key, run_id, canonical_name.
                      All values must be str, int, float, or bool (ChromaDB restriction).

        Raises:
            ChromaDBClientError if required metadata keys are missing.
        """
        collection = self._collection_required()
        required_meta = ["entity_type", "city_key", "run_id", "canonical_name"]
        missing = [k for k in required_meta if k not in metadata]
        if missing:
            raise ChromaDBClientError(
                f"upsert_entity_embedding: metadata missing required keys: {missing}"
            )

        await collection.upsert(
            ids=[entity_id],
            embeddings=[embedding],
            documents=[embed_text],
            metadatas=[metadata],
        )
        log.debug("upsert_entity_embedding: %s (%s)", entity_id, metadata.get("canonical_name"))

    async def upsert_entities_batch(
        self,
        entity_ids: list[str],
        embed_texts: list[str],
        embeddings: list[list[float]],
        metadatas: list[dict[str, Any]],
    ) -> None:
        """
        Batch upsert multiple entity embeddings.
        All lists must have the same length.
        """
        collection = self._collection_required()
        if not (len(entity_ids) == len(embed_texts) == len(embeddings) == len(metadatas)):
            raise ChromaDBClientError(
                "upsert_entities_batch: all input lists must have the same length"
            )
        if not entity_ids:
            return

        await collection.upsert(
            ids=entity_ids,
            embeddings=embeddings,
            documents=embed_texts,
            metadatas=metadatas,
        )
        log.info("upsert_entities_batch: %d embeddings stored", len(entity_ids))

    async def delete_entity_embedding(self, entity_id: str) -> None:
        """Remove a single entity's embedding. Used when an entity is superseded."""
        collection = self._collection_required()
        await collection.delete(ids=[entity_id])

    async def delete_run_embeddings(self, run_id: str) -> None:
        """
        Remove all embeddings associated with a run.
        Used for test cleanup and failed run rollback.
        """
        collection = self._collection_required()
        await collection.delete(where={"run_id": run_id})
        log.info("delete_run_embeddings: removed embeddings for run_id=%s", run_id)

    # ─────────────────────────────────────────────────────────────────────────
    # Query operations
    # ─────────────────────────────────────────────────────────────────────────

    async def query_similar(
        self,
        embedding: list[float],
        city_key: str,
        entity_type: str | None = None,
        top_k: int = 5,
    ) -> list[dict[str, Any]]:
        """
        Find the top-k most similar entities to a given embedding vector.

        Filters by city_key to avoid cross-city false positives.
        Optionally filters by entity_type for precision.

        Returns list of dicts:
          {
            "entity_id": str,
            "canonical_name": str,
            "distance": float,      # cosine distance (0.0 = identical, 2.0 = opposite)
            "similarity": float,    # 1.0 - (distance / 2.0) — normalized to 0–1
            "entity_type": str,
            "metadata": dict,
          }
        """
        collection = self._collection_required()

        where_filter: dict[str, Any] = {"city_key": city_key}
        if entity_type:
            where_filter["entity_type"] = entity_type

        results = await collection.query(
            query_embeddings=[embedding],
            n_results=min(top_k, await collection.count()),
            where=where_filter if where_filter else None,
            include=["documents", "metadatas", "distances"],
        )

        if not results["ids"] or not results["ids"][0]:
            return []

        output = []
        for i, doc_id in enumerate(results["ids"][0]):
            distance = results["distances"][0][i]
            metadata = results["metadatas"][0][i] if results["metadatas"] else {}
            output.append({
                "entity_id":    doc_id,
                "canonical_name": metadata.get("canonical_name", ""),
                "distance":     distance,
                "similarity":   max(0.0, 1.0 - distance),  # cosine distance in chromadb is already normalized 0–1 for cosine space
                "entity_type":  metadata.get("entity_type"),
                "metadata":     metadata,
            })

        return output

    async def query_by_text(
        self,
        text: str,
        city_key: str,
        embed_fn: Any,              # Async callable: (text) -> list[float]
        entity_type: str | None = None,
        top_k: int = 5,
    ) -> list[dict[str, Any]]:
        """
        Convenience method: embed a text string, then query_similar.
        embed_fn should be the OllamaClient.embed method.
        """
        embedding = await embed_fn(text)
        return await self.query_similar(
            embedding=embedding,
            city_key=city_key,
            entity_type=entity_type,
            top_k=top_k,
        )

    async def get_entity_embedding(self, entity_id: str) -> dict[str, Any] | None:
        """Retrieve a single entity's stored embedding and metadata."""
        collection = self._collection_required()
        results = await collection.get(
            ids=[entity_id],
            include=["embeddings", "documents", "metadatas"],
        )
        if not results["ids"]:
            return None
        return {
            "entity_id": results["ids"][0],
            "embedding": results["embeddings"][0] if results["embeddings"] else None,
            "document":  results["documents"][0] if results["documents"] else None,
            "metadata":  results["metadatas"][0] if results["metadatas"] else None,
        }

    async def collection_count(self) -> int:
        """Return total number of documents in the entity_embeddings collection."""
        collection = self._collection_required()
        return await collection.count()

    async def disconnect(self) -> None:
        """
        Release resources. ChromaDB's AsyncHttpClient holds no persistent connection
        (each request creates its own HTTP session), so this is a no-op. Present so
        the worker's _disconnect_all() helper can call client.disconnect() uniformly
        across all DB clients without special-casing ChromaDB.
        """
        self._client = None
        self._collection = None

    # ─────────────────────────────────────────────────────────────────────────
    # Entity resolution helpers
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def build_embed_text(entity: dict[str, Any]) -> str:
        """
        Build the text to embed for entity resolution.
        Uses name + type + city + key descriptors.
        Keep it short — nomic-embed-text has 8192 token limit but shorter is more precise.

        Design: the embedding should capture what makes this entity unique,
        not its full data profile. City is included to reduce cross-city false positives.
        """
        parts = [
            entity.get("canonical_name", ""),
            entity.get("entity_type", ""),
            entity.get("entity_subtype", ""),
            entity.get("primary_city", ""),
            entity.get("primary_state", ""),
        ]
        # Add a few category-specific discriminators
        cat = entity.get("category_fields", {})
        if isinstance(cat, dict):
            # Investor: thesis
            if entity.get("entity_type") == "investor" and cat.get("investment_thesis"):
                parts.append(cat["investment_thesis"][:200])
            # Individual: role
            if entity.get("entity_type") in ("executive_hnw", "hnwi") and cat.get("primary_role"):
                parts.append(cat["primary_role"])
            # Nonprofit/Philanthropic: mission
            if cat.get("mission_statement"):
                parts.append(cat["mission_statement"][:200])
        return " | ".join(p for p in parts if p)
