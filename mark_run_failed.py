#!/usr/bin/env python3
"""
mark_run_failed.py

Marks a stuck/orphaned run as 'failed' in Supabase so the next run
can proceed cleanly.

Usage:
    cd project
    python3 mark_run_failed.py c85d1586-XXXX-XXXX-XXXX-XXXXXXXXXXXX

Or with a partial run_id prefix (will match if unique).
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timezone

import asyncpg


async def main(run_id_prefix: str) -> None:
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        # Fall back to .env
        env_file = os.path.join(os.path.dirname(__file__), ".env")
        if os.path.exists(env_file):
            for line in open(env_file):
                line = line.strip()
                if line.startswith("DATABASE_URL="):
                    db_url = line.split("=", 1)[1].strip().strip('"')
                    break

    if not db_url:
        print("ERROR: DATABASE_URL not set and not found in .env", file=sys.stderr)
        sys.exit(1)

    conn = await asyncpg.connect(db_url)
    try:
        # Find the run — accept partial prefix for convenience
        rows = await conn.fetch(
            "SELECT run_id, city_name, status, created_at FROM runs "
            "WHERE run_id::text LIKE $1 ORDER BY created_at DESC LIMIT 5",
            f"{run_id_prefix}%",
        )

        if not rows:
            print(f"No runs found matching prefix: {run_id_prefix}")
            sys.exit(1)

        if len(rows) > 1:
            print("Multiple matches found:")
            for r in rows:
                print(f"  {r['run_id']} — {r['city_name']} — {r['status']} — {r['created_at']}")
            print("Provide a longer prefix to disambiguate.")
            sys.exit(1)

        row = rows[0]
        run_id = row["run_id"]
        print(f"Run: {run_id}")
        print(f"City: {row['city_name']}")
        print(f"Current status: {row['status']}")

        if row["status"] in ("failed", "complete"):
            print(f"Status is already '{row['status']}' — nothing to do.")
            return

        now = datetime.now(timezone.utc)
        await conn.execute(
            """
            UPDATE runs
            SET status = 'failed',
                failure_reason = 'Orphaned — run was killed mid-execution',
                completed_at   = $1
            WHERE run_id = $2
            """,
            now,
            run_id,
        )
        print(f"✓ Marked run {run_id} as failed.")

    finally:
        await conn.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: python3 {sys.argv[0]} <run_id_or_prefix>")
        sys.exit(1)
    asyncio.run(main(sys.argv[1]))
