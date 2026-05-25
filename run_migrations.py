#!/usr/bin/env python3
"""
run_migrations.py

Apply pending Supabase migrations in order.

Usage:
    python run_migrations.py            # run all pending migrations
    python run_migrations.py 005 006    # run specific migration numbers

Reads DATABASE_URL from the .env file in the same directory.
"""

from __future__ import annotations

import asyncio
import os
import re
import sys
from pathlib import Path


# ── Determine which migrations to run ────────────────────────────────────────

MIGRATIONS_DIR = Path(__file__).parent / "migrations"

def _load_env() -> str:
    env_file = Path(__file__).parent / ".env"
    if not env_file.exists():
        sys.exit(f"ERROR: .env not found at {env_file}")
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line.startswith("DATABASE_URL="):
            return line.split("=", 1)[1].strip()
    sys.exit("ERROR: DATABASE_URL not found in .env")


def _collect_migrations(filters: list[str]) -> list[Path]:
    """Return SQL files in numerical order, optionally filtered by number prefix."""
    all_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not filters:
        return all_files
    result = []
    for f in all_files:
        for prefix in filters:
            if re.match(rf"^0*{re.escape(prefix)}_", f.name) or f.name.startswith(prefix):
                result.append(f)
                break
    return result


# ── Runner ────────────────────────────────────────────────────────────────────

async def run_migrations(db_url: str, files: list[Path]) -> None:
    try:
        import asyncpg
    except ImportError:
        sys.exit("ERROR: asyncpg not installed. Run: pip install asyncpg")

    conn = await asyncpg.connect(db_url)
    try:
        for path in files:
            sql = path.read_text()
            print(f"\n── Applying {path.name} ──")
            try:
                await conn.execute(sql)
                print(f"   ✓ {path.name} applied successfully")
            except Exception as exc:
                print(f"   ✗ {path.name} FAILED: {exc}", file=sys.stderr)
                raise
    finally:
        await conn.close()


def main() -> None:
    db_url = _load_env()
    filters = sys.argv[1:]
    files = _collect_migrations(filters)

    if not files:
        print("No matching migration files found.")
        sys.exit(0)

    print(f"Migrations to run ({len(files)}):")
    for f in files:
        print(f"  {f.name}")

    asyncio.run(run_migrations(db_url, files))
    print("\nAll migrations complete.")


if __name__ == "__main__":
    main()
