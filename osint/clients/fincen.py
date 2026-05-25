"""
osint/clients/fincen.py

FinCEN CTR (Currency Transaction Report) runtime client.

Queries the DuckDB index built by osint/etl/us/fincen_ctr.py.

This client performs NO network calls. It queries a local DuckDB file
that must be populated by running:
    python -m osint.etl.runner --sources fincen_ctr

Use case:
    Given a financial institution name, return its year-by-year CTR filing
    history. High CTR counts indicate high-volume cash handling — a signal
    used in illicit finance enrichment to flag AML risk.

Phase 8 role:
    Enrichment agent calls get_institution_ctr_history() for entities
    with entity_type in {"corporate"} that match known financial institution
    patterns (bank, credit union, thrift, etc.).

    Output stored in category_fields["fincen_ctr"]:
        {
            "institution":   str,
            "state":         str,
            "history":       [{year: int, ctr_count: int}],   # sorted by year
            "total_ctrs":    int,    # sum across all years in DB
            "peak_year":     int,    # year with highest CTR count
            "peak_count":    int,
        }

    No relationship edges are derived from CTR data — it enriches the entity
    profile only. CTR counts appear in the intelligence briefing as a risk
    indicator, not as a structural connection.

DuckDB file location:
    Default: ~/.osint/etl/fincen_ctr.duckdb
    Override: ETL_DB_DIR environment variable
"""

from __future__ import annotations

import logging
import os
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

_TABLE = "fincen_ctr"

# Minimum fuzzy match score to consider a name match valid
_FUZZY_MIN = 0.72

# Number of top fuzzy candidates to consider when exact match fails
_FUZZY_CANDIDATES = 5


def _default_db_path() -> str:
    """Return default DuckDB path, respecting ETL_DB_DIR env var."""
    base = Path(os.environ.get("ETL_DB_DIR", Path.home() / ".osint" / "etl"))
    return str(base / "fincen_ctr.duckdb")


class FinCENClient:
    """
    Runtime query client for the FinCEN CTR DuckDB index.

    Instantiated without a rate limiter — all queries are local DuckDB.
    """

    def __init__(self, db_path: str | None = None) -> None:
        self._db_path = db_path or _default_db_path()
        self._con: Any = None  # lazy-opened DuckDB connection

    def _connect(self) -> "duckdb.DuckDBPyConnection | None":  # type: ignore[name-defined]
        """Open (or return cached) read-only DuckDB connection."""
        if self._con is not None:
            return self._con
        try:
            import duckdb
            if not Path(self._db_path).exists():
                log.debug("fincen: DuckDB file not found at %s — ETL may not have run", self._db_path)
                return None
            self._con = duckdb.connect(self._db_path, read_only=True)
            return self._con
        except Exception as exc:
            log.debug("fincen: failed to open DuckDB: %s", exc)
            return None

    def close(self) -> None:
        """Close DuckDB connection."""
        if self._con:
            try:
                self._con.close()
            except Exception:
                pass
            self._con = None

    # ─────────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────────

    def get_institution_ctr_history(
        self,
        institution_name: str,
        state: str | None = None,
    ) -> dict[str, Any]:
        """
        Look up CTR filing history for a financial institution.

        Tries exact match first; falls back to fuzzy name matching if no
        exact match is found.

        Args:
            institution_name: Bank or financial institution name.
            state:            Optional two-letter state filter.

        Returns:
            Dict with keys: institution, state, history, total_ctrs,
                            peak_year, peak_count.
            Empty dict if no match or DuckDB not available.
        """
        con = self._connect()
        if con is None:
            return {}

        try:
            # Try exact match (case-insensitive)
            rows = self._query_exact(con, institution_name, state)
            if not rows:
                rows = self._query_fuzzy(con, institution_name, state)
            if not rows:
                return {}

            return self._build_result(rows)

        except Exception as exc:
            log.debug("fincen: query failed for '%s': %s", institution_name, exc)
            return {}

    def get_institutions_by_state(
        self,
        state: str,
        min_ctr_count: int = 100,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """
        Return top CTR-filing institutions in a given state.

        Useful for building a picture of which institutions in a target
        geography have the highest cash-transaction volumes.

        Args:
            state:         Two-letter state abbreviation.
            min_ctr_count: Minimum annual CTR count to include.
            limit:         Max institutions to return.

        Returns:
            List of dicts: [{institution, state, total_ctrs, peak_year, peak_count}]
        """
        con = self._connect()
        if con is None:
            return []

        try:
            rows = con.execute(f"""
                SELECT
                    institution,
                    state,
                    SUM(ctr_count)                             AS total_ctrs,
                    MAX(year)                                  AS peak_year,
                    MAX(ctr_count)                             AS peak_count
                FROM {_TABLE}
                WHERE UPPER(state) = UPPER(?)
                  AND ctr_count >= ?
                GROUP BY institution, state
                HAVING SUM(ctr_count) >= ?
                ORDER BY total_ctrs DESC
                LIMIT ?
            """, [state, min_ctr_count, min_ctr_count, limit]).fetchall()

            return [
                {
                    "institution": r[0],
                    "state":       r[1],
                    "total_ctrs":  int(r[2] or 0),
                    "peak_year":   int(r[3] or 0),
                    "peak_count":  int(r[4] or 0),
                }
                for r in rows
            ]
        except Exception as exc:
            log.debug("fincen: get_institutions_by_state failed: %s", exc)
            return []

    # ─────────────────────────────────────────────────────────────────────────
    # Private helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _query_exact(
        self,
        con: Any,
        name: str,
        state: str | None,
    ) -> list[tuple]:
        """Run case-insensitive exact match query."""
        if state:
            return con.execute(f"""
                SELECT institution, state, year, ctr_count
                FROM {_TABLE}
                WHERE LOWER(institution) = LOWER(?)
                  AND UPPER(state) = UPPER(?)
                ORDER BY year ASC
            """, [name, state]).fetchall()
        else:
            return con.execute(f"""
                SELECT institution, state, year, ctr_count
                FROM {_TABLE}
                WHERE LOWER(institution) = LOWER(?)
                ORDER BY year ASC
            """, [name]).fetchall()

    def _query_fuzzy(
        self,
        con: Any,
        name: str,
        state: str | None,
    ) -> list[tuple]:
        """
        Fuzzy name match using Python SequenceMatcher.

        Strategy: fetch all distinct institution names (optionally filtered by
        state) and find the best match above the minimum threshold.
        """
        name_lower = name.lower()

        if state:
            candidates = con.execute(f"""
                SELECT DISTINCT institution, state
                FROM {_TABLE}
                WHERE UPPER(state) = UPPER(?)
            """, [state]).fetchall()
        else:
            candidates = con.execute(f"""
                SELECT DISTINCT institution, state
                FROM {_TABLE}
            """).fetchall()

        best_score = 0.0
        best_institution: str | None = None
        best_state: str | None = None

        for (inst, st) in candidates:
            score = SequenceMatcher(None, name_lower, inst.lower()).ratio()
            if score > best_score:
                best_score = score
                best_institution = inst
                best_state = st

        if best_score < _FUZZY_MIN or best_institution is None:
            return []

        log.debug(
            "fincen: fuzzy match '%s' → '%s' (score=%.2f)",
            name, best_institution, best_score,
        )

        # Now fetch the history for the matched institution
        return con.execute(f"""
            SELECT institution, state, year, ctr_count
            FROM {_TABLE}
            WHERE LOWER(institution) = LOWER(?)
              AND UPPER(state) = UPPER(?)
            ORDER BY year ASC
        """, [best_institution, best_state]).fetchall()

    @staticmethod
    def _build_result(rows: list[tuple]) -> dict[str, Any]:
        """Build the output dict from CTR history rows."""
        if not rows:
            return {}

        history = [
            {"year": int(r[2]), "ctr_count": int(r[3])}
            for r in rows
            if r[3] and int(r[3]) > 0
        ]
        if not history:
            return {}

        total = sum(h["ctr_count"] for h in history)
        peak  = max(history, key=lambda h: h["ctr_count"])

        return {
            "institution": rows[0][0],
            "state":       rows[0][1],
            "history":     history,
            "total_ctrs":  total,
            "peak_year":   peak["year"],
            "peak_count":  peak["ctr_count"],
        }
