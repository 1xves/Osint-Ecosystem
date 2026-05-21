"""
tests/integration/test_connectivity.py

Live connectivity tests for all four infrastructure services.

Requires:
- Docker containers running (docker compose up -d)
- .env with valid credentials for all services
- Supabase migration already applied (001_initial_schema.sql)

Run with:
    cd /path/to/project
    pytest tests/integration/test_connectivity.py -v

These tests perform real network calls and will fail if services are down.
They are intentionally excluded from the unit test run — use `-m integration`
or run this file directly.
"""

import pytest
import asyncpg
import httpx

from osint.core.config import settings


# ─────────────────────────────────────────────────────────────────────────────
# Supabase / PostgreSQL
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_supabase_connects():
    """asyncpg can open a connection pool using DATABASE_URL from .env."""
    pool = await asyncpg.create_pool(
        settings.database_url,
        min_size=1,
        max_size=2,
        command_timeout=15,
    )
    assert pool is not None
    await pool.close()


@pytest.mark.asyncio
async def test_supabase_entities_table_exists():
    """entities table was created by the migration."""
    pool = await asyncpg.create_pool(
        settings.database_url,
        min_size=1,
        max_size=2,
        command_timeout=15,
    )
    row = await pool.fetchrow(
        "SELECT COUNT(*) AS n FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_name = 'entities'"
    )
    await pool.close()
    assert row["n"] == 1, "entities table not found — was the migration run?"


@pytest.mark.asyncio
async def test_supabase_all_nine_tables_exist():
    """All 9 OSINT tables must be present after migration."""
    required_tables = {
        "entities", "entity_evidence", "analytical_assessments",
        "osint_search_records", "relationships", "file_media_store",
        "agent_runs", "agent_outputs", "rejected_items",
    }
    pool = await asyncpg.create_pool(
        settings.database_url,
        min_size=1,
        max_size=2,
        command_timeout=15,
    )
    rows = await pool.fetch(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'public'"
    )
    await pool.close()

    found_tables = {row["table_name"] for row in rows}
    missing = required_tables - found_tables
    assert not missing, f"Missing tables after migration: {missing}"


@pytest.mark.asyncio
async def test_supabase_entities_starts_empty():
    """Fresh project — entities table should have zero rows."""
    pool = await asyncpg.create_pool(
        settings.database_url,
        min_size=1,
        max_size=2,
        command_timeout=15,
    )
    row = await pool.fetchrow("SELECT COUNT(*) AS n FROM entities")
    await pool.close()
    assert row["n"] == 0, f"Expected empty entities table, found {row['n']} rows"


# ─────────────────────────────────────────────────────────────────────────────
# Neo4j
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_neo4j_connectivity():
    """neo4j driver can verify connectivity to the Bolt endpoint."""
    from neo4j import AsyncGraphDatabase

    driver = AsyncGraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )
    await driver.verify_connectivity()
    await driver.close()


@pytest.mark.asyncio
async def test_neo4j_starts_empty():
    """Fresh Neo4j instance should have no OSINT nodes."""
    from neo4j import AsyncGraphDatabase

    driver = AsyncGraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )
    async with driver.session() as session:
        result = await session.run("MATCH (n) RETURN COUNT(n) AS count")
        record = await result.single()
        node_count = record["count"]
    await driver.close()

    assert node_count == 0, (
        f"Expected empty Neo4j, found {node_count} nodes. "
        "If this is not a fresh instance, this failure is expected."
    )


# ─────────────────────────────────────────────────────────────────────────────
# ChromaDB
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_chromadb_heartbeat():
    """ChromaDB HTTP API returns a heartbeat response."""
    url = f"http://{settings.chromadb_host}:{settings.chromadb_port}/api/v1/heartbeat"
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.get(url)
    assert response.status_code == 200
    body = response.json()
    assert "nanosecond heartbeat" in body


@pytest.mark.asyncio
async def test_chromadb_list_collections():
    """ChromaDB can list collections (starts with zero)."""
    url = f"http://{settings.chromadb_host}:{settings.chromadb_port}/api/v1/collections"
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.get(url)
    assert response.status_code == 200
    collections = response.json()
    assert isinstance(collections, list)


# ─────────────────────────────────────────────────────────────────────────────
# Redis
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_redis_ping():
    """Redis responds to PING with PONG."""
    import redis.asyncio as aioredis

    client = aioredis.from_url(settings.redis_url)
    response = await client.ping()
    await client.aclose()
    assert response is True


@pytest.mark.asyncio
async def test_redis_set_get_delete():
    """Redis can SET, GET, and DELETE a key — confirms read/write access."""
    import redis.asyncio as aioredis

    client = aioredis.from_url(settings.redis_url)
    key = "osint:test:connectivity"
    try:
        await client.set(key, "ok", ex=60)
        value = await client.get(key)
        assert value == b"ok"
    finally:
        await client.delete(key)
        await client.aclose()
