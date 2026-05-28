#!/usr/bin/env python3
"""
run_migration_010.py — apply migration 010 (continuous pipeline support).

Adds to agent_runs:
  - run_mode  TEXT  CHECK (run_mode IN ('full','enrichment_refresh','discovery_pass'))
  - run_type  TEXT
  - scheduled BOOLEAN

Creates continuous_schedule table with a default Philadelphia row.
Creates indexes on agent_runs(run_mode, run_status) and agent_runs(scheduled).

Run: .venv/bin/python3 run_migration_010.py
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


MIGRATION = Path(__file__).parent / "migrations" / "010_continuous_pipeline.sql"


def _strip_comments(s: str) -> str:
    """Remove leading SQL comment lines so empty-check works correctly."""
    lines = [l for l in s.splitlines() if not l.strip().startswith("--")]
    return "\n".join(lines).strip()


async def main() -> None:
    db_url = _load_env_var("DATABASE_URL")
    sql_text = MIGRATION.read_text()

    print(f"Connecting to DB...")
    conn = await asyncpg.connect(db_url)
    print("Connected.")

    stmts = [_strip_comments(s) for s in sql_text.split(";")]
    stmts = [s for s in stmts if s.strip()]

    print(f"Running {len(stmts)} SQL statement(s)...\n")

    for i, stmt in enumerate(stmts, 1):
        preview = stmt.strip().splitlines()[0][:80]
        print(f"  [{i}/{len(stmts)}] {preview}")
        try:
            await conn.execute(stmt)
            print(f"         ✓")
        except Exception as e:
            err = str(e)
            # "already exists" errors are safe to skip — migration is idempotent
            if "already exists" in err or "duplicate column" in err.lower():
                print(f"         → skipped (already applied: {err[:80]})")
            else:
                print(f"         ✗ ERROR: {err}")
                await conn.close()
                raise

    await conn.close()
    print("\nMigration 010 complete.")


if __name__ == "__main__":
    asyncio.run(main())
