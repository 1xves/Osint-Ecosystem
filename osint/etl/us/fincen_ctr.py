"""
osint/etl/us/fincen_ctr.py

FinCEN BSA (Bank Secrecy Act) CTR aggregate data ETL module.

Source:
    FinCEN publishes state-level aggregate Currency Transaction Report (CTR)
    counts by financial institution each year.

    Where to get the data (manual download required in most cases):
        1. Navigate to: https://www.fincen.gov/reports/bsa-filing-statistics
        2. Look for "CTRs by State" or "BSA Filing Statistics" Excel/CSV links.
           The URL changes year-to-year — there is no stable direct-download URL.
        3. Download the Excel file and pass the local path via FINCEN_CTR_FILE
           environment variable, or via the manual_file_path argument to run().

    Alternate source (more reliable URL, reformatted data):
        FDIC BankFind Suite exports contain similar institution-level data.
        The FinCEN CTR aggregates are also published via:
            https://bsaefiling.fincen.treas.gov/PublicAccessFiles/
        (requires HTTPS GET, no auth, but URL structure changes annually)

    This is AGGREGATE data only — no individual transactions, no PII.
    High CTR count for an institution is a signal of high-volume cash handling,
    which is relevant for AML / SAR correlation in the enrichment pipeline.

Data schema (DuckDB table: fincen_ctr):
    institution TEXT     — Financial institution name (normalized)
    state       TEXT     — Two-letter state abbreviation
    year        INTEGER  — Filing year
    ctr_count   INTEGER  — Number of CTRs filed

    PRIMARY KEY (institution, state, year)

Runtime client: osint/clients/fincen.py → FinCENClient

Manual file override:
    Set environment variable FINCEN_CTR_FILE=/path/to/file.xlsx
    or pass manual_file_path=... to run() directly.

Dependencies:
    duckdb     — pip install duckdb
    openpyxl   — pip install openpyxl (for .xlsx parsing via pandas)
    pandas     — pip install pandas
    httpx      — pip install httpx (for URL download attempts)

Note: pandas is used only during ETL (one-time run). The runtime client uses
DuckDB directly — no pandas dependency at query time.
"""

from __future__ import annotations

import logging
import os
import re
from io import BytesIO
from pathlib import Path

log = logging.getLogger(__name__)

DOMAIN     = "fincen_ctr"
TABLE_NAME = "fincen_ctr"

# Candidate download URLs — tried in order, all are fallible.
# FinCEN does not guarantee stable direct-download URLs year-over-year.
_CANDIDATE_URLS = [
    "https://www.fincen.gov/sites/default/files/shared/CTRs_by_State.xlsx",
    "https://bsaefiling.fincen.treas.gov/PublicAccessFiles/CTR_By_State.xlsx",
]

# Column name patterns we look for in the Excel sheet (case-insensitive, partial match)
_COL_INSTITUTION = re.compile(r"institution|financial.?institution|bank|filer", re.IGNORECASE)
_COL_STATE       = re.compile(r"^state$", re.IGNORECASE)
_COL_YEAR        = re.compile(r"^year$|filing.?year|calendar.?year", re.IGNORECASE)
_COL_COUNT       = re.compile(r"ctr.?count|count|number.?of.?ctr|reports", re.IGNORECASE)


async def run(
    db_path: str,
    *,
    force_refresh: bool = False,
    manual_file_path: str | None = None,
) -> "ETLResult":  # noqa: F821
    """
    Load FinCEN CTR aggregate data into DuckDB.

    Data source priority:
        1. manual_file_path argument (explicit override)
        2. FINCEN_CTR_FILE environment variable
        3. URL download attempt (likely to fail — see module docstring)

    If all three fail, returns ETLResult with a clear error message explaining
    where to manually obtain the file.

    Args:
        db_path:           Path to the DuckDB file to create/update.
        force_refresh:     Re-load even if table already populated.
        manual_file_path:  Path to a locally downloaded CTR Excel/CSV file.

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
                    "fincen_ctr: table already populated (%d rows). "
                    "Use --force-refresh to reload.", count,
                )
                con.close()
                return ETLResult(source=DOMAIN, db_path=db_path, records_loaded=count)
        except Exception:
            pass

    # ── Resolve file source ──────────────────────────────────────────────────
    # Priority: explicit arg > env var > URL download
    file_path = manual_file_path or os.environ.get("FINCEN_CTR_FILE", "")

    raw_bytes: bytes | None = None

    if file_path and Path(file_path).exists():
        log.info("fincen_ctr: loading from local file: %s", file_path)
        try:
            raw_bytes = Path(file_path).read_bytes()
        except Exception as exc:
            con.close()
            return ETLResult(source=DOMAIN, db_path=db_path, error=f"File read failed: {exc}")
    else:
        # Attempt URL downloads
        try:
            import httpx
            async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
                for url in _CANDIDATE_URLS:
                    log.info("fincen_ctr: attempting download from %s", url)
                    try:
                        resp = await client.get(
                            url,
                            headers={"User-Agent": "OSINT-Research/1.0 research@osint-system.local"},
                        )
                        # Reject HTML responses — FinCEN returns a 200 HTML error page
                        # instead of 404 for missing files
                        content_type = resp.headers.get("content-type", "")
                        if resp.status_code == 200 and "html" not in content_type.lower():
                            raw_bytes = resp.content
                            log.info(
                                "fincen_ctr: downloaded %d bytes from %s",
                                len(raw_bytes), url,
                            )
                            break
                        else:
                            log.warning(
                                "fincen_ctr: URL returned %s / content-type=%s, skipping: %s",
                                resp.status_code, content_type, url,
                            )
                    except Exception as exc:
                        log.warning("fincen_ctr: download attempt failed for %s: %s", url, exc)
        except ImportError:
            pass  # httpx not available — fall through to manual instruction

    if not raw_bytes:
        con.close()
        return ETLResult(
            source=DOMAIN,
            db_path=db_path,
            error=(
                "FinCEN CTR data requires manual download.\n\n"
                "Steps:\n"
                "  1. Visit https://www.fincen.gov/reports/bsa-filing-statistics\n"
                "  2. Download the 'CTRs by State' Excel file\n"
                "  3. Re-run with the file path:\n"
                "       FINCEN_CTR_FILE=/path/to/file.xlsx "
                "python -m osint.etl.runner --sources fincen_ctr\n"
                "     or via the manual_file_path argument."
            ),
        )

    # ── Parse ────────────────────────────────────────────────────────────────
    log.info("fincen_ctr: parsing %d bytes", len(raw_bytes))
    try:
        df = _parse_xlsx(raw_bytes)
    except Exception as exc:
        # Try CSV fallback
        try:
            df = _parse_csv(raw_bytes)
        except Exception:
            con.close()
            return ETLResult(source=DOMAIN, db_path=db_path, error=f"Parse failed: {exc}")

    if df is None or df.empty:
        con.close()
        return ETLResult(
            source=DOMAIN,
            db_path=db_path,
            error=(
                "Parsed DataFrame is empty. The file format may not match "
                "expected column patterns. Check that the file contains columns "
                "for institution name, state, year, and CTR count."
            ),
        )

    log.info("fincen_ctr: parsed %d rows", len(df))

    try:
        records_loaded = _load_to_duckdb(con, df)
    except Exception as exc:
        con.close()
        return ETLResult(source=DOMAIN, db_path=db_path, error=f"DuckDB load failed: {exc}")

    con.close()
    log.info("fincen_ctr: loaded %d records into %s", records_loaded, db_path)
    return ETLResult(source=DOMAIN, db_path=db_path, records_loaded=records_loaded)


def _parse_csv(raw_bytes: bytes) -> "pd.DataFrame | None":
    """Attempt to parse raw bytes as CSV."""
    import pandas as pd
    from io import StringIO

    for encoding in ("utf-8", "latin-1", "cp1252"):
        try:
            df = pd.read_csv(
                StringIO(raw_bytes.decode(encoding, errors="replace")),
                low_memory=False,
                dtype=str,
            )
            col_map = _map_columns(list(df.columns))
            if col_map:
                subset = df[list(col_map.keys())].rename(columns=col_map)
                return _clean_df(subset)
        except Exception:
            continue
    return None


def _parse_xlsx(raw_bytes: bytes) -> "pd.DataFrame | None":
    """
    Parse the FinCEN CTR Excel file into a normalized DataFrame.

    The Excel layout varies year-to-year. We use column-name pattern matching
    rather than positional indexing to be resilient to minor format changes.

    Returns DataFrame with columns: institution, state, year, ctr_count
    """
    import pandas as pd

    xl = pd.ExcelFile(BytesIO(raw_bytes))
    all_frames = []

    for sheet_name in xl.sheet_names:
        try:
            raw = xl.parse(sheet_name, header=None)
        except Exception:
            continue

        header_row = _find_header_row(raw)
        if header_row is None:
            log.debug("fincen_ctr: sheet '%s' — no header row found, skipping", sheet_name)
            continue

        df = xl.parse(sheet_name, header=header_row)
        df.columns = [str(c).strip() for c in df.columns]

        col_map = _map_columns(df.columns)
        if not col_map:
            log.debug(
                "fincen_ctr: sheet '%s' — column mapping failed: %s",
                sheet_name, list(df.columns),
            )
            continue

        subset = df[list(col_map.keys())].rename(columns=col_map)
        subset = _clean_df(subset)

        if "year" not in subset.columns or subset["year"].eq(0).all():
            year_match = re.search(r"20\d{2}", str(sheet_name))
            subset["year"] = int(year_match.group()) if year_match else 0

        all_frames.append(subset[["institution", "state", "year", "ctr_count"]])

    if not all_frames:
        return None

    import pandas as pd
    combined = pd.concat(all_frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["institution", "state", "year"])
    return combined


def _clean_df(df: "pd.DataFrame") -> "pd.DataFrame":
    """Coerce types and filter noise rows."""
    import pandas as pd

    df = df.copy()
    df["institution"] = df["institution"].astype(str).str.strip()
    df = df[df["institution"].str.len() > 0]
    df = df[~df["institution"].isin(["nan", "None", ""])]

    if "state" not in df.columns:
        df["state"] = ""
    df["state"] = df["state"].astype(str).str.upper().str.strip()

    if "year" not in df.columns:
        df["year"] = 0
    df["year"]      = pd.to_numeric(df["year"],      errors="coerce").fillna(0).astype(int)
    df["ctr_count"] = pd.to_numeric(df["ctr_count"], errors="coerce").fillna(0).astype(int)

    # Filter totals/subtotals rows
    df = df[df["institution"].str.lower().str.contains("total") == False]  # noqa: E712
    df = df[df["ctr_count"] > 0]
    return df


def _find_header_row(raw: "pd.DataFrame") -> int | None:
    """Find the row index that looks like a column header."""
    import pandas as pd

    for i, row in raw.iterrows():
        row_str = " ".join(str(v).lower() for v in row if pd.notna(v))
        if "state" in row_str and (
            "institution" in row_str or "bank" in row_str or "filer" in row_str
        ):
            return i
    return None


def _map_columns(columns: list[str]) -> dict[str, str]:
    """
    Map actual column names to normalized names.
    Returns dict: {actual_col_name → normalized_name} or {} if required cols missing.
    """
    mapping: dict[str, str] = {}

    for col in columns:
        if "institution" not in mapping.values() and _COL_INSTITUTION.search(col):
            mapping[col] = "institution"
        elif "state" not in mapping.values() and _COL_STATE.search(col):
            mapping[col] = "state"
        elif "year" not in mapping.values() and _COL_YEAR.search(col):
            mapping[col] = "year"
        elif "ctr_count" not in mapping.values() and _COL_COUNT.search(col):
            mapping[col] = "ctr_count"

    if "institution" not in mapping.values() or "ctr_count" not in mapping.values():
        return {}

    return mapping


def _load_to_duckdb(con: "duckdb.DuckDBPyConnection", df: "pd.DataFrame") -> int:
    """Create (or replace) the fincen_ctr table and load the DataFrame."""
    con.execute(f"""
        CREATE OR REPLACE TABLE {TABLE_NAME} (
            institution  TEXT    NOT NULL,
            state        TEXT    NOT NULL DEFAULT '',
            year         INTEGER NOT NULL DEFAULT 0,
            ctr_count    INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (institution, state, year)
        )
    """)

    con.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_fincen_institution
        ON {TABLE_NAME} (LOWER(institution))
    """)

    con.register("_ctr_staging", df)
    con.execute(f"""
        INSERT OR REPLACE INTO {TABLE_NAME}
        SELECT
            TRIM(institution)             AS institution,
            UPPER(TRIM(state))            AS state,
            CAST(year AS INTEGER)         AS year,
            CAST(ctr_count AS INTEGER)    AS ctr_count
        FROM _ctr_staging
        WHERE institution IS NOT NULL
          AND TRIM(institution) != ''
    """)
    con.unregister("_ctr_staging")

    count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    return count
