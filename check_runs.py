"""
check_runs.py — quick Supabase run status viewer.

Reads DATABASE_URL directly from .env (no project imports needed).
Run: .venv/bin/python3 check_runs.py
"""
import asyncio
import asyncpg
import os
import re
from pathlib import Path


def _load_env_var(key: str) -> str:
    """Read a single variable from .env without importing pydantic or dotenv."""
    env_file = Path(__file__).parent / ".env"
    if not env_file.exists():
        raise FileNotFoundError(f".env not found at {env_file}")
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        if k.strip().upper() == key.upper():
            # Strip optional surrounding quotes
            return v.strip().strip('"').strip("'")
    raise KeyError(f"{key} not found in .env")


async def run() -> None:
    db_url = _load_env_var("DATABASE_URL")
    conn = await asyncpg.connect(db_url, statement_cache_size=0)
    try:
        rows = await conn.fetch(
            """
            SELECT
                run_id,
                city_name,
                run_status,
                failure_reason,
                total_entities_found,
                total_relationships_found,
                started_at,
                completed_at
            FROM agent_runs
            ORDER BY started_at DESC
            LIMIT 10
            """
        )
        if not rows:
            print("No runs found in agent_runs.")
            return

        for r in rows:
            d = dict(r)
            run_id_short = str(d["run_id"])[:8]
            status = d["run_status"]
            city = d.get("city_name") or "?"
            entities = d.get("total_entities_found") or 0
            rels = d.get("total_relationships_found") or 0
            started = d.get("started_at")
            failure = d.get("failure_reason")

            print(f"{run_id_short}  {status:<10}  {city:<20}  "
                  f"entities={entities}  rels={rels}  started={started}")
            if failure:
                print(f"          └─ FAIL: {failure.splitlines()[0]}")
    finally:
        await conn.close()


asyncio.run(run())
