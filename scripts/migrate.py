#!/usr/bin/env python3
"""
scripts/migrate.py — Apply a numbered migration file to the Supabase DB.

Usage (from project root with venv active):
    python scripts/migrate.py 004
    python scripts/migrate.py 004_fix_enum_constraints   # also accepted

The DATABASE_URL is read from .env automatically.
"""

import os
import re
import sys
from pathlib import Path

# ── Load .env ─────────────────────────────────────────────────────────────────
def load_env(env_path: Path) -> dict[str, str]:
    env = {}
    with env_path.open() as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, _, val = line.partition("=")
            env[key.strip()] = val.strip().strip('"').strip("'")
    return env


PROJECT_ROOT = Path(__file__).resolve().parent.parent
env_file = PROJECT_ROOT / ".env"
if not env_file.exists():
    print(f"✗ .env not found at {env_file}")
    sys.exit(1)

env = load_env(env_file)
DATABASE_URL = env.get("DATABASE_URL")
if not DATABASE_URL:
    print("✗ DATABASE_URL not set in .env")
    sys.exit(1)


# ── Find migration file ────────────────────────────────────────────────────────
if len(sys.argv) < 2:
    print("Usage: python scripts/migrate.py <migration_number_or_name>")
    print("  e.g. python scripts/migrate.py 004")
    sys.exit(1)

arg = sys.argv[1]
migrations_dir = PROJECT_ROOT / "migrations"

# Accept "004", "004_fix_enum_constraints", or "004_fix_enum_constraints.sql"
if arg.endswith(".sql"):
    candidate = migrations_dir / arg
else:
    # Find any file whose name starts with the given prefix
    matches = sorted(migrations_dir.glob(f"{arg}*.sql"))
    if not matches:
        # Try zero-padded prefix
        padded = arg.zfill(3)
        matches = sorted(migrations_dir.glob(f"{padded}*.sql"))
    if not matches:
        print(f"✗ No migration file matching '{arg}' in {migrations_dir}")
        sys.exit(1)
    candidate = matches[0]

if not candidate.exists():
    print(f"✗ Migration file not found: {candidate}")
    sys.exit(1)

sql = candidate.read_text()
print(f"→ Migration file: {candidate.name}")
print(f"→ DB:             {DATABASE_URL.split('@')[1] if '@' in DATABASE_URL else DATABASE_URL}")


# ── Run migration ──────────────────────────────────────────────────────────────
try:
    import psycopg2
except ImportError:
    print("✗ psycopg2 not available — activate the venv first:")
    print("  source .venv/bin/activate")
    print("  python scripts/migrate.py " + arg)
    sys.exit(1)

try:
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    cur = conn.cursor()

    print("→ Applying migration...\n")
    cur.execute(sql)

    print(f"\n✓ Migration {candidate.name} applied successfully.")
    cur.close()
    conn.close()

except psycopg2.Error as e:
    print(f"\n✗ Migration failed:\n  {e}")
    sys.exit(1)
