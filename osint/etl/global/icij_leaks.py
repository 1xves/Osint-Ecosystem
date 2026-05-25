"""
osint/etl/global/icij_leaks.py

ICIJ Offshore Leaks bulk data ETL module.

Source:
    The International Consortium of Investigative Journalists (ICIJ) publishes
    the Offshore Leaks database, containing entities from:
        - Panama Papers (2016)
        - Paradise Papers (2017)
        - Pandora Papers (2021)
        - Offshore Leaks (historical)

    Where to get the data (manual download required):
        1. Navigate to: https://offshoreleaks.icij.org/pages/database
        2. Download the full database ZIP (~500MB)
        3. Extract the ZIP — it contains these CSV files:
               nodes-entities.csv
               nodes-officers.csv
               nodes-addresses.csv
               nodes-intermediaries.csv
               relationships.csv
        4. Set ICIJ_DATA_DIR=/path/to/extracted/directory and re-run the ETL:
               python -m osint.etl.runner --sources icij_leaks

    The download URL changes with each data update. Always use the download
    page rather than a hardcoded URL.

Design:
    ICIJ data is loaded into the existing Neo4j instance as a SEPARATE subgraph.
    Pipeline entities (label :Entity) are never merged with ICIJ nodes at load
    time — matching happens at query time in the runtime client.

Neo4j node labels created:
    :ICIJNode           — base label on all ICIJ nodes (enables cross-type queries)
    :ICIJEntity         — offshore shell company / entity
    :ICIJOfficer        — officer / beneficial owner
    :ICIJAddress        — registered address
    :ICIJIntermediary   — registered agent / intermediary law firm

Neo4j relationship type created:
    :ICIJ_REL           — all ICIJ relationships; rel_type property carries the
                          original ICIJ relationship type (officer_of,
                          registered_address, intermediary_of, similar, etc.)

Neo4j indexes created:
    icij_entity_name       ON (n:ICIJEntity) (n.name_lower)
    icij_officer_name      ON (n:ICIJOfficer) (n.name_lower)
    icij_intermediary_name ON (n:ICIJIntermediary) (n.name_lower)
    icij_node_id_entity    UNIQUE ON (n:ICIJEntity) (n.icij_id)
    icij_node_id_officer   UNIQUE ON (n:ICIJOfficer) (n.icij_id)

Dependencies:
    neo4j     — pip install neo4j
    pandas    — pip install pandas

Note: pandas is used only during ETL. The runtime client queries Neo4j directly
with Cypher — no pandas dependency at query time.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

log = logging.getLogger(__name__)

DOMAIN = "icij_leaks"

# CSV filenames expected inside ICIJ_DATA_DIR
_CSV_FILES = {
    "entities":      "nodes-entities.csv",
    "officers":      "nodes-officers.csv",
    "addresses":     "nodes-addresses.csv",
    "intermediaries": "nodes-intermediaries.csv",
    "relationships": "relationships.csv",
}

# Rows per UNWIND batch — keeps Neo4j transaction memory bounded
_BATCH_SIZE = 500


# ─────────────────────────────────────────────────────────────────────────────
# ETL entry point
# ─────────────────────────────────────────────────────────────────────────────

async def run(
    db_path: str,
    *,
    force_refresh: bool = False,
) -> "ETLResult":  # noqa: F821
    """
    Load ICIJ Offshore Leaks bulk data into Neo4j.

    Data source:
        Directory path from ICIJ_DATA_DIR environment variable containing the
        CSV files extracted from the ICIJ database download ZIP.

    Args:
        db_path:       Unused for Neo4j sources — Neo4j URI comes from settings.
                       Stored in ETLResult.db_path as the Neo4j URI for tracing.
        force_refresh: If True, delete all existing ICIJNode nodes before reload.

    Returns:
        ETLResult with records_loaded = total nodes + relationships loaded.
    """
    from osint.etl.runner import ETLResult
    from osint.core.config import settings

    neo4j_uri = settings.neo4j_uri

    # ── Resolve data directory ──────────────────────────────────────────────
    data_dir_str = os.environ.get("ICIJ_DATA_DIR", "")
    if not data_dir_str:
        return ETLResult(
            source=DOMAIN,
            db_path=neo4j_uri,
            error=(
                "ICIJ data requires manual download.\n\n"
                "Steps:\n"
                "  1. Visit https://offshoreleaks.icij.org/pages/database\n"
                "  2. Download the full database ZIP\n"
                "  3. Extract the ZIP to a local directory\n"
                "  4. Re-run with:\n"
                "       ICIJ_DATA_DIR=/path/to/extracted python -m osint.etl.runner --sources icij_leaks"
            ),
        )

    data_dir = Path(data_dir_str)
    if not data_dir.is_dir():
        return ETLResult(
            source=DOMAIN,
            db_path=neo4j_uri,
            error=f"ICIJ_DATA_DIR does not exist or is not a directory: {data_dir}",
        )

    # Verify required CSVs exist
    missing = [
        name for name, fname in _CSV_FILES.items()
        if not (data_dir / fname).exists()
    ]
    if missing:
        return ETLResult(
            source=DOMAIN,
            db_path=neo4j_uri,
            error=(
                f"Missing ICIJ CSV files in {data_dir}: {missing}. "
                "Expected: " + ", ".join(_CSV_FILES.values())
            ),
        )

    # ── Connect to Neo4j ────────────────────────────────────────────────────
    try:
        from neo4j import AsyncGraphDatabase
    except ImportError:
        return ETLResult(
            source=DOMAIN,
            db_path=neo4j_uri,
            error="Missing dependency: neo4j. Install with: pip install neo4j",
        )

    try:
        driver = AsyncGraphDatabase.driver(
            neo4j_uri,
            auth=(settings.neo4j_user, settings.neo4j_password),
        )
        await driver.verify_connectivity()
    except Exception as exc:
        return ETLResult(
            source=DOMAIN,
            db_path=neo4j_uri,
            error=f"Neo4j connection failed: {exc}",
        )

    try:
        total_loaded = await _load_all(
            driver=driver,
            data_dir=data_dir,
            force_refresh=force_refresh,
        )
    except Exception as exc:
        log.exception("icij_leaks: unhandled error during load")
        return ETLResult(
            source=DOMAIN,
            db_path=neo4j_uri,
            error=str(exc),
        )
    finally:
        await driver.close()

    log.info("icij_leaks: ETL complete — %d nodes/relationships loaded", total_loaded)
    return ETLResult(source=DOMAIN, db_path=neo4j_uri, records_loaded=total_loaded)


# ─────────────────────────────────────────────────────────────────────────────
# Core loader
# ─────────────────────────────────────────────────────────────────────────────

async def _load_all(driver, data_dir: Path, force_refresh: bool) -> int:
    """Orchestrate the full load sequence. Returns total records written."""
    import pandas as pd

    total = 0

    async with driver.session() as session:
        # ── Schema: indexes and constraints ─────────────────────────────────
        await _create_schema(session)

        # ── Optional: wipe existing ICIJ data ───────────────────────────────
        if force_refresh:
            log.info("icij_leaks: force_refresh — deleting existing ICIJNode nodes")
            await session.run("MATCH (n:ICIJNode) DETACH DELETE n")

        # ── Check if already loaded ──────────────────────────────────────────
        if not force_refresh:
            result = await session.run("MATCH (n:ICIJNode) RETURN COUNT(n) AS cnt")
            record = await result.single()
            if record and record["cnt"] > 0:
                log.info(
                    "icij_leaks: %d ICIJNode nodes already exist. "
                    "Use --force-refresh to reload.",
                    record["cnt"],
                )
                return record["cnt"]

    # ── Load nodes ───────────────────────────────────────────────────────────
    node_configs = [
        ("entities",       "ICIJEntity"),
        ("officers",       "ICIJOfficer"),
        ("addresses",      "ICIJAddress"),
        ("intermediaries", "ICIJIntermediary"),
    ]

    for csv_key, label in node_configs:
        csv_path = data_dir / _CSV_FILES[csv_key]
        count = await _load_nodes(driver, csv_path, label)
        log.info("icij_leaks: loaded %d %s nodes", count, label)
        total += count

    # ── Load relationships ───────────────────────────────────────────────────
    rel_count = await _load_relationships(driver, data_dir / _CSV_FILES["relationships"])
    log.info("icij_leaks: loaded %d ICIJ_REL relationships", rel_count)
    total += rel_count

    return total


async def _create_schema(session) -> None:
    """Create Neo4j indexes and uniqueness constraints for ICIJ nodes."""
    statements = [
        # Uniqueness constraints (also create indexes)
        "CREATE CONSTRAINT icij_entity_id IF NOT EXISTS FOR (n:ICIJEntity) REQUIRE n.icij_id IS UNIQUE",
        "CREATE CONSTRAINT icij_officer_id IF NOT EXISTS FOR (n:ICIJOfficer) REQUIRE n.icij_id IS UNIQUE",
        "CREATE CONSTRAINT icij_address_id IF NOT EXISTS FOR (n:ICIJAddress) REQUIRE n.icij_id IS UNIQUE",
        "CREATE CONSTRAINT icij_intermediary_id IF NOT EXISTS FOR (n:ICIJIntermediary) REQUIRE n.icij_id IS UNIQUE",
        # Name search indexes (lowercase for case-insensitive search)
        "CREATE INDEX icij_entity_name IF NOT EXISTS FOR (n:ICIJEntity) ON (n.name_lower)",
        "CREATE INDEX icij_officer_name IF NOT EXISTS FOR (n:ICIJOfficer) ON (n.name_lower)",
        "CREATE INDEX icij_intermediary_name IF NOT EXISTS FOR (n:ICIJIntermediary) ON (n.name_lower)",
    ]
    for stmt in statements:
        await session.run(stmt)
    log.debug("icij_leaks: schema verified")


async def _load_nodes(driver, csv_path: Path, label: str) -> int:
    """Load one node CSV into Neo4j. Returns count of rows processed."""
    import pandas as pd

    log.info("icij_leaks: reading %s → :%s", csv_path.name, label)
    try:
        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, low_memory=False)
    except Exception as exc:
        log.error("icij_leaks: failed to read %s: %s", csv_path, exc)
        return 0

    # Normalize columns — ICIJ schema varies slightly across dataset versions
    df.columns = [c.strip().lower() for c in df.columns]

    # Ensure node_id column exists (primary key across all ICIJ node CSVs)
    if "node_id" not in df.columns:
        log.warning("icij_leaks: %s has no node_id column — skipping", csv_path.name)
        return 0

    total = 0
    rows = df.to_dict("records")

    async with driver.session() as session:
        for i in range(0, len(rows), _BATCH_SIZE):
            batch = rows[i : i + _BATCH_SIZE]
            params = [_node_row_to_props(r, label) for r in batch]
            # UNWIND MERGE — idempotent, safe to re-run
            cypher = f"""
            UNWIND $params AS p
            MERGE (n:ICIJNode {{icij_id: p.icij_id}})
            SET n:{label}
            SET n += p
            """
            await session.run(cypher, params=params)
            total += len(batch)

    return total


def _node_row_to_props(row: dict, label: str) -> dict:
    """Convert a raw CSV row dict to the Neo4j property dict we want to store."""
    name = (row.get("name") or "").strip()
    return {
        "icij_id":          row.get("node_id", ""),
        "name":             name,
        "name_lower":       name.lower(),
        # Entity-specific fields (empty string for non-entity nodes)
        "original_name":    (row.get("original_name") or "").strip(),
        "former_name":      (row.get("former_name") or "").strip(),
        "jurisdiction":     (row.get("jurisdiction") or "").strip(),
        "company_type":     (row.get("company_type") or "").strip(),
        "status":           (row.get("status") or "").strip(),
        "incorporation_date": (row.get("incorporation_date") or "").strip(),
        "inactivation_date":  (row.get("inactivation_date") or "").strip(),
        # Common fields
        "countries":        (row.get("countries") or "").strip(),
        "country_codes":    (row.get("country_codes") or "").strip(),
        "source_dataset":   (row.get("sourceid") or row.get("sourceID") or "").strip(),
        "valid_until":      (row.get("valid_until") or "").strip(),
        "note":             (row.get("note") or "").strip(),
        "icij_type":        label,   # convenience property for cross-type queries
    }


async def _load_relationships(driver, csv_path: Path) -> int:
    """Load ICIJ relationships.csv into Neo4j as :ICIJ_REL edges."""
    import pandas as pd

    log.info("icij_leaks: reading %s → :ICIJ_REL", csv_path.name)
    try:
        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, low_memory=False)
    except Exception as exc:
        log.error("icij_leaks: failed to read %s: %s", csv_path, exc)
        return 0

    df.columns = [c.strip().lower() for c in df.columns]

    # Normalize column names across ICIJ schema versions
    if "node_id_start" not in df.columns:
        # Older schema used "START_ID"/"END_ID"
        if "start_id" in df.columns:
            df = df.rename(columns={"start_id": "node_id_start", "end_id": "node_id_end"})
        else:
            log.warning("icij_leaks: relationships.csv has unexpected columns: %s", list(df.columns))
            return 0

    if "rel_type" not in df.columns and "type" in df.columns:
        df = df.rename(columns={"type": "rel_type"})

    # Drop rows with missing endpoints
    df = df.dropna(subset=["node_id_start", "node_id_end"])
    df = df[df["node_id_start"].str.strip() != ""]
    df = df[df["node_id_end"].str.strip() != ""]

    rows = df.to_dict("records")
    total = 0

    async with driver.session() as session:
        for i in range(0, len(rows), _BATCH_SIZE):
            batch = rows[i : i + _BATCH_SIZE]
            params = [
                {
                    "start_id":   r.get("node_id_start", "").strip(),
                    "end_id":     r.get("node_id_end", "").strip(),
                    "rel_type":   (r.get("rel_type") or r.get("link") or "related").strip(),
                    "start_date": (r.get("start_date") or "").strip(),
                    "end_date":   (r.get("end_date") or "").strip(),
                    "source_dataset": (r.get("sourceid") or r.get("sourceID") or "").strip(),
                }
                for r in batch
            ]
            # rel_type as a property — we can't parameterize Neo4j relationship types,
            # and ICIJ has ~20 distinct rel_type values. Storing as property allows
            # flexible runtime filtering without requiring one label per type.
            cypher = """
            UNWIND $params AS p
            MATCH (a:ICIJNode {icij_id: p.start_id})
            MATCH (b:ICIJNode {icij_id: p.end_id})
            MERGE (a)-[r:ICIJ_REL {start_id: p.start_id, end_id: p.end_id, rel_type: p.rel_type}]->(b)
            SET r.source_dataset = p.source_dataset,
                r.start_date     = p.start_date,
                r.end_date       = p.end_date
            """
            await session.run(cypher, params=params)
            total += len(batch)

    return total
