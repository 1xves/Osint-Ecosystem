#!/usr/bin/env python3
"""
run_migration_009.py — apply migration 009 directly via asyncpg.

Fixes:
  1. city_key: philadelphia_un → philadelphia_us (and any other _un keys)
  2. Mark orphaned "running" runs (>3h) as failed
  3. Mark orphaned "pending" runs (>24h) as failed

Run: .venv/bin/python3 run_migration_009.py
"""
import asyncio
import asyncpg
from pathlib import Path


def _load_env_var(key: str) -> str:
    env_file = Path(__file__).parent / ".env"
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        if k.strip().upper() == key.upper():
            return v.strip().strip('"').strip("'")
    raise KeyError(f"{key} not found in .env")


MIGRATION = """
-- 1. Fix city_key: *_un → *_us
UPDATE agent_runs
SET city_key = REPLACE(city_key, '_un', '_us')
WHERE city_key LIKE '%_un'
  AND city_key NOT LIKE '%_un_%';

-- 2. Orphaned running runs → failed
UPDATE agent_runs
SET
    run_status     = 'failed',
    failure_reason = 'Orphaned — run was killed mid-execution (status never updated)',
    completed_at   = NOW()
WHERE run_status = 'running'
  AND started_at < NOW() - INTERVAL '3 hours';

-- 3. Orphaned pending runs → failed
UPDATE agent_runs
SET
    run_status     = 'failed',
    failure_reason = 'Orphaned — job was never picked up by a worker',
    completed_at   = NOW()
WHERE run_status = 'pending'
  AND started_at < NOW() - INTERVAL '24 hours';
"""


async def main() -> None:
    db_url = _load_env_var("DATABASE_URL")
    conn = await asyncpg.connect(db_url, statement_cache_size=0)
    try:
        # Run statements individually so we get per-statement row counts.
        # Strip leading comment lines before filtering, otherwise blocks like
        # "-- comment\nUPDATE ..." are incorrectly skipped.
        def _strip_comments(s: str) -> str:
            lines = [l for l in s.splitlines() if not l.strip().startswith("--")]
            return "\n".join(lines).strip()

        stmts = [_strip_comments(s) for s in MIGRATION.split(";")]
        stmts = [s for s in stmts if s]

        labels = [
            "Fix city_key _un → _us",
            "Mark orphaned running runs as failed",
            "Mark orphaned pending runs as failed",
        ]

        for label, stmt in zip(labels, stmts):
            result = await conn.execute(stmt)
            # result is e.g. "UPDATE 11"
            print(f"  {label}: {result}")

        # Verify
        print("\nVerification:")
        rows = await conn.fetch(
            "SELECT run_status, count(*) AS n FROM agent_runs GROUP BY run_status ORDER BY 1"
        )
        for r in rows:
            print(f"  {r['run_status']:<12} {r['n']} runs")

        keys = await conn.fetch(
            "SELECT city_key, count(*) AS n FROM agent_runs GROUP BY city_key ORDER BY 1"
        )
        print()
        for r in keys:
            print(f"  city_key={r['city_key']!r}  ({r['n']} runs)")

    finally:
        await conn.close()


asyncio.run(main())
