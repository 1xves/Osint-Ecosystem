#!/usr/bin/env python3
"""
migrate.py — Apply schema migrations to the OSINT Supabase project.

Usage:
    cd /Users/ves/Documents/Claude/Projects/OSINT/project
    .venv/bin/python migrate.py

Applies migrations in order, skipping any already recorded in schema_migrations:
    001_initial_schema.sql
    002_additions.sql

Idempotent: safe to re-run at any time.
"""

import asyncio
import os
import sys
from pathlib import Path

import asyncpg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]
MIGRATIONS = [
    Path("migrations/001_initial_schema.sql"),
    Path("migrations/002_additions.sql"),
]

CREATE_TRACKING_TABLE = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    migration_name TEXT PRIMARY KEY,
    applied_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


async def main() -> None:
    print("Connecting to database...")
    try:
        conn = await asyncio.wait_for(
            asyncpg.connect(DATABASE_URL, ssl=False, statement_cache_size=0, command_timeout=60),
            timeout=15,
        )
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        sys.exit(1)

    print("✓ Connected")

    # Ensure tracking table exists
    await conn.execute(CREATE_TRACKING_TABLE)

    # Bootstrap: if schema_migrations is empty but 001's sentinel table already
    # exists, the migration was applied before the tracker was introduced.
    # Record it so we don't try to re-run it.
    sentinel_exists = await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
        "WHERE table_schema='public' AND table_name='agent_runs')"
    )
    if sentinel_exists:
        await conn.execute(
            "INSERT INTO schema_migrations (migration_name) VALUES ($1) ON CONFLICT DO NOTHING",
            "001_initial_schema.sql",
        )

    # Fetch already-applied migrations
    applied = {
        row["migration_name"]
        for row in await conn.fetch("SELECT migration_name FROM schema_migrations")
    }

    ran = 0
    for migration_path in MIGRATIONS:
        name = migration_path.name
        if name in applied:
            print(f"\n— {name} already applied, skipping")
            continue

        sql = migration_path.read_text()
        print(f"\nApplying {name} ({len(sql.splitlines())} lines)...")
        try:
            await conn.execute(sql)
            await conn.execute(
                "INSERT INTO schema_migrations (migration_name) VALUES ($1)", name
            )
            print(f"✓ {name} applied")
            ran += 1
        except Exception as e:
            print(f"✗ {name} failed: {e}")
            await conn.close()
            sys.exit(1)

    if ran == 0:
        print("\n✓ All migrations already applied — nothing to do.")

    # Verify key tables exist
    tables = await conn.fetch(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
    )
    print(f"\n✓ Tables in public schema ({len(tables)}):")
    for row in tables:
        print(f"  - {row['tablename']}")

    await conn.close()
    print("\n✓ Done.")


if __name__ == "__main__":
    asyncio.run(main())
