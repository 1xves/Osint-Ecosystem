"""
osint/etl/us/hud.py

HUD Multifamily Insured Properties ETL module.

Source:
    HUD Office of Housing publishes the FHA-insured multifamily loan portfolio,
    including owner entity names, loan amounts, and loan status.

    Where to get the data (manual download almost always required):
        The HUD disclosure download page uses a JavaScript-rendered form —
        direct HTTP GET to the CSV/XLSX files is unreliable. URLs change.

        Reliable manual steps:
          1. Navigate to: https://www.hud.gov/program_offices/housing/mfh/exp/mfhdiscl
          2. Select "Download all active loans" or equivalent option
          3. Save the resulting Excel or CSV file
          4. Set HUD_FILE=/path/to/file.xlsx (or .csv) and re-run the ETL

        Alternative: HUD Open Data / ArcGIS Hub
          https://hudgis-hud.opendata.arcgis.com/
          Search for "Multifamily Properties" — provides a direct CSV download
          that is more stable than the main HUD disclosure page.

          Direct ArcGIS CSV (may change):
          https://opendata.arcgis.com/datasets/8d45b8e52a664e15bfb06f8dc29310cd_0.csv

    Manual file override:
        Set environment variable HUD_FILE=/path/to/file.xlsx
        or pass manual_file_path=... to run() directly.

Data schema (DuckDB table: hud_properties):
    property_name  TEXT      — Property name
    owner_name     TEXT      — Owner entity name (normalized)
    city           TEXT      — City
    state          TEXT      — Two-letter state abbreviation
    zip_code       TEXT      — ZIP code
    loan_amount    REAL      — FHA loan amount (USD)
    loan_status    TEXT      — Loan status (e.g., "Current", "Defaulted", "Paid Off")
    program_type   TEXT      — HUD program type (e.g., "223(f)", "221(d)(4)")
    units          INTEGER   — Number of residential units
    maturity_date  TEXT      — Loan maturity date (ISO format string)

    PRIMARY KEY (property_name, owner_name, zip_code)

Runtime client: osint/clients/hud.py → HUDClient

Dependencies:
    duckdb    — pip install duckdb
    pandas    — pip install pandas
    httpx     — pip install httpx (for URL download attempts)
"""

from __future__ import annotations

import logging
import os
import re
from io import BytesIO, StringIO
from pathlib import Path

log = logging.getLogger(__name__)

DOMAIN     = "hud"
TABLE_NAME = "hud_properties"

# Candidate URLs — in rough order of reliability.
# All are fallible; manual download is the recommended path.
_CANDIDATE_URLS = [
    # ArcGIS Open Data — most stable of the options
    "https://opendata.arcgis.com/datasets/8d45b8e52a664e15bfb06f8dc29310cd_0.csv",
    # HUD dfiles direct — frequently changes or requires cookie/session
    "https://www.hud.gov/sites/dfiles/Housing/documents/MF_MortgageLoanPortfolio.csv",
    "https://www.hud.gov/sites/dfiles/Housing/documents/MFH_Discl_Detail.xlsx",
]

# Column synonyms — HUD changes column names between years and sources
_COL_MAP = {
    "property_name": re.compile(r"property.?name|project.?name", re.IGNORECASE),
    "owner_name":    re.compile(r"owner.?name|mortgagor|borrower.?name|owner.?entity", re.IGNORECASE),
    "city":          re.compile(r"^city$|property.?city|proj.?city", re.IGNORECASE),
    "state":         re.compile(r"^state$|property.?state|proj.?state", re.IGNORECASE),
    "zip_code":      re.compile(r"zip|postal.?code|zip.?code", re.IGNORECASE),
    "loan_amount":   re.compile(r"original.?mortgage|loan.?amount|mortgage.?amount|orig.?mortgage", re.IGNORECASE),
    "loan_status":   re.compile(r"^status$|loan.?status|current.?status|property.?status", re.IGNORECASE),
    "program_type":  re.compile(r"^program$|program.?type|section|prog.?cat", re.IGNORECASE),
    "units":         re.compile(r"^units$|total.?units|number.?of.?units|assisted.?units|total.?assisted", re.IGNORECASE),
    "maturity_date": re.compile(r"maturity|maturity.?date|loan.?maturity|term.?date", re.IGNORECASE),
}


async def run(
    db_path: str,
    *,
    force_refresh: bool = False,
    manual_file_path: str | None = None,
) -> "ETLResult":  # noqa: F821
    """
    Load HUD multifamily loan portfolio data into DuckDB.

    Data source priority:
        1. manual_file_path argument (explicit override)
        2. HUD_FILE environment variable
        3. URL download attempts (likely to fail — see module docstring)

    If all sources fail, returns ETLResult with a clear error explaining
    how to obtain the file manually.

    Args:
        db_path:           Path to the DuckDB file to create/update.
        force_refresh:     Re-load even if table already populated.
        manual_file_path:  Path to a locally downloaded HUD Excel or CSV file.

    Returns:
        ETLResult with records_loaded count.
    """
    from osint.etl.runner import ETLResult

    try:
        import duckdb
        import pandas as pd
    except ImportError as exc:
        return ETLResult(
            source=DOMAIN,
            db_path=db_path,
            error=f"Missing dependency: {exc}. Install with: pip install duckdb pandas openpyxl",
        )

    con = duckdb.connect(db_path)

    if not force_refresh:
        try:
            count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
            if count > 0:
                log.info(
                    "hud: table already populated (%d rows). "
                    "Use --force-refresh to reload.", count,
                )
                con.close()
                return ETLResult(source=DOMAIN, db_path=db_path, records_loaded=count)
        except Exception:
            pass

    # ── Resolve file source ──────────────────────────────────────────────────
    file_path = manual_file_path or os.environ.get("HUD_FILE", "")

    raw_bytes: bytes | None = None
    used_url = ""

    if file_path and Path(file_path).exists():
        log.info("hud: loading from local file: %s", file_path)
        try:
            raw_bytes = Path(file_path).read_bytes()
            used_url  = file_path
        except Exception as exc:
            con.close()
            return ETLResult(source=DOMAIN, db_path=db_path, error=f"File read failed: {exc}")
    else:
        try:
            import httpx
            async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
                for url in _CANDIDATE_URLS:
                    log.info("hud: attempting download from %s", url)
                    try:
                        resp = await client.get(
                            url,
                            headers={"User-Agent": "OSINT-Research/1.0 research@osint-system.local"},
                        )
                        content_type = resp.headers.get("content-type", "")
                        # Reject HTML responses — HUD returns 200 HTML for missing files
                        if resp.status_code == 200 and "html" not in content_type.lower():
                            raw_bytes = resp.content
                            used_url  = url
                            log.info(
                                "hud: downloaded %d bytes from %s",
                                len(raw_bytes), url,
                            )
                            break
                        else:
                            log.warning(
                                "hud: URL returned %s / content-type=%s, skipping: %s",
                                resp.status_code, content_type, url,
                            )
                    except Exception as exc:
                        log.warning("hud: download attempt failed for %s: %s", url, exc)
        except ImportError:
            pass  # httpx not available — fall through to manual instruction

    if not raw_bytes:
        con.close()
        return ETLResult(
            source=DOMAIN,
            db_path=db_path,
            error=(
                "HUD multifamily data requires manual download.\n\n"
                "Steps:\n"
                "  Option A — HUD Disclosure page:\n"
                "    1. Visit https://www.hud.gov/program_offices/housing/mfh/exp/mfhdiscl\n"
                "    2. Download the full loan portfolio as Excel or CSV\n"
                "    3. Re-run with: HUD_FILE=/path/to/file.xlsx "
                "python -m osint.etl.runner --sources hud\n\n"
                "  Option B — HUD Open Data (ArcGIS):\n"
                "    1. Visit https://hudgis-hud.opendata.arcgis.com/\n"
                "    2. Search 'Multifamily Properties' → Download as CSV\n"
                "    3. Re-run with: HUD_FILE=/path/to/file.csv "
                "python -m osint.etl.runner --sources hud"
            ),
        )

    # ── Parse ────────────────────────────────────────────────────────────────
    log.info("hud: parsing %d bytes from %s", len(raw_bytes), used_url)
    try:
        if used_url.endswith(".csv") or (used_url and "csv" in used_url.lower()):
            df = _parse_csv(raw_bytes)
        else:
            df = _parse_xlsx(raw_bytes)
            if df is None:
                # Fallback: maybe it's actually a CSV with a .xlsx extension
                df = _parse_csv(raw_bytes)
    except Exception as exc:
        con.close()
        return ETLResult(source=DOMAIN, db_path=db_path, error=f"Parse failed: {exc}")

    if df is None or df.empty:
        con.close()
        return ETLResult(
            source=DOMAIN,
            db_path=db_path,
            error=(
                "Parsed DataFrame is empty. The file may not match expected column "
                "patterns. Expected columns: property name, owner name, city, state, "
                "zip code, loan amount, loan status. Check the downloaded file format."
            ),
        )

    log.info("hud: parsed %d rows", len(df))

    try:
        records_loaded = _load_to_duckdb(con, df)
    except Exception as exc:
        con.close()
        return ETLResult(source=DOMAIN, db_path=db_path, error=f"DuckDB load failed: {exc}")

    con.close()
    log.info("hud: loaded %d records into %s", records_loaded, db_path)
    return ETLResult(source=DOMAIN, db_path=db_path, records_loaded=records_loaded)


def _parse_csv(raw_bytes: bytes) -> "pd.DataFrame | None":
    """Parse HUD CSV export into a normalized DataFrame."""
    import pandas as pd

    for encoding in ("utf-8", "latin-1", "cp1252"):
        try:
            df = pd.read_csv(
                StringIO(raw_bytes.decode(encoding, errors="replace")),
                low_memory=False,
                dtype=str,
            )
            return _normalize_df(df)
        except Exception:
            continue
    return None


def _parse_xlsx(raw_bytes: bytes) -> "pd.DataFrame | None":
    """Parse HUD Excel export into a normalized DataFrame."""
    import pandas as pd

    xl = pd.ExcelFile(BytesIO(raw_bytes))
    for sheet in xl.sheet_names:
        try:
            df = xl.parse(sheet, dtype=str)
            normalized = _normalize_df(df)
            if normalized is not None and not normalized.empty:
                return normalized
        except Exception:
            continue
    return None


def _normalize_df(df: "pd.DataFrame") -> "pd.DataFrame | None":
    """
    Map actual column names to our schema and return cleaned DataFrame.
    Returns None if required columns (property_name, owner_name) are missing.
    """
    import pandas as pd

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    col_map: dict[str, str] = {}

    for normalized_name, pattern in _COL_MAP.items():
        for actual_col in df.columns:
            if normalized_name not in col_map.values() and pattern.search(actual_col):
                col_map[actual_col] = normalized_name
                break

    if "property_name" not in col_map.values() or "owner_name" not in col_map.values():
        log.debug(
            "hud: column mapping failed — found: %s, mapped: %s",
            list(df.columns[:10]),
            list(col_map.values()),
        )
        return None

    result = df[list(col_map.keys())].rename(columns=col_map)

    # Fill missing optional columns
    for col in ["city", "state", "zip_code", "loan_status", "program_type", "maturity_date"]:
        if col not in result.columns:
            result[col] = ""

    for col in ["loan_amount", "units"]:
        if col not in result.columns:
            result[col] = None

    # Clean required string columns
    result["property_name"] = result["property_name"].astype(str).str.strip()
    result["owner_name"]    = result["owner_name"].astype(str).str.strip()
    result["state"]         = result["state"].astype(str).str.upper().str.strip()
    result["zip_code"]      = result["zip_code"].astype(str).str.strip().str[:5]

    # Drop meaningless rows
    result = result[result["owner_name"].str.len() > 0]
    result = result[~result["owner_name"].isin(["nan", "None", ""])]
    result = result[result["property_name"].str.len() > 0]

    # Numeric coercions
    result["loan_amount"] = pd.to_numeric(result["loan_amount"], errors="coerce")
    result["units"]       = pd.to_numeric(result["units"],       errors="coerce").astype("Int64")

    return result[["property_name", "owner_name", "city", "state", "zip_code",
                   "loan_amount", "loan_status", "program_type", "units", "maturity_date"]]


def _load_to_duckdb(con: "duckdb.DuckDBPyConnection", df: "pd.DataFrame") -> int:
    """Create/replace the hud_properties table and load the DataFrame."""
    con.execute(f"""
        CREATE OR REPLACE TABLE {TABLE_NAME} (
            property_name  TEXT    NOT NULL,
            owner_name     TEXT    NOT NULL,
            city           TEXT    NOT NULL DEFAULT '',
            state          TEXT    NOT NULL DEFAULT '',
            zip_code       TEXT    NOT NULL DEFAULT '',
            loan_amount    REAL,
            loan_status    TEXT    NOT NULL DEFAULT '',
            program_type   TEXT    NOT NULL DEFAULT '',
            units          INTEGER,
            maturity_date  TEXT    NOT NULL DEFAULT '',
            PRIMARY KEY (property_name, owner_name, zip_code)
        )
    """)

    con.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_hud_owner
        ON {TABLE_NAME} (LOWER(owner_name))
    """)
    con.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_hud_state
        ON {TABLE_NAME} (state)
    """)

    con.register("_hud_staging", df)
    con.execute(f"""
        INSERT OR REPLACE INTO {TABLE_NAME}
        SELECT
            TRIM(property_name)                           AS property_name,
            TRIM(owner_name)                              AS owner_name,
            COALESCE(TRIM(city), '')                      AS city,
            UPPER(COALESCE(TRIM(state), ''))              AS state,
            COALESCE(TRIM(zip_code), '')                  AS zip_code,
            CAST(loan_amount AS REAL)                     AS loan_amount,
            COALESCE(TRIM(loan_status), '')               AS loan_status,
            COALESCE(TRIM(program_type), '')              AS program_type,
            CAST(units AS INTEGER)                        AS units,
            COALESCE(TRIM(maturity_date), '')             AS maturity_date
        FROM _hud_staging
        WHERE property_name IS NOT NULL
          AND owner_name IS NOT NULL
          AND TRIM(property_name) != ''
          AND TRIM(owner_name) != ''
    """)
    con.unregister("_hud_staging")

    count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    return count
