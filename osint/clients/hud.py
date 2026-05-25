"""
osint/clients/hud.py

HUD Multifamily Insured Properties runtime client.

Queries the DuckDB index built by osint/etl/us/hud.py.
No network calls — queries a local DuckDB file populated by ETL.

Requires ETL to have run:
    python -m osint.etl.runner --sources hud

Phase 8 role:
    Enrichment agent calls get_properties_by_owner() for entities with
    entity_type in {"corporate", "real_estate", "investor"}.

    Output stored in category_fields["hud_properties"]:
        [
            {
                "property_name": str,
                "owner_name":    str,
                "city":          str,
                "state":         str,
                "zip_code":      str,
                "loan_amount":   float | None,
                "loan_status":   str,
                "program_type":  str,
                "units":         int | None,
                "maturity_date": str,
            }
        ]

    And category_fields["hud_portfolio_value"]: float (sum of loan_amounts)

    Relationship agent reads hud_properties → OWNS edges (entity → property nodes).

DuckDB file location:
    Default: ~/.osint/etl/hud.duckdb
    Override: ETL_DB_DIR environment variable
"""

from __future__ import annotations

import logging
import os
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

_TABLE    = "hud_properties"
_FUZZY_MIN = 0.72


def _default_db_path() -> str:
    base = Path(os.environ.get("ETL_DB_DIR", Path.home() / ".osint" / "etl"))
    return str(base / "hud.duckdb")


class HUDClient:
    """
    Runtime query client for the HUD multifamily DuckDB index.

    Instantiated without a rate limiter — all queries are local DuckDB.
    """

    def __init__(self, db_path: str | None = None) -> None:
        self._db_path = db_path or _default_db_path()
        self._con: Any = None

    def _connect(self) -> Any:
        if self._con is not None:
            return self._con
        try:
            import duckdb
            if not Path(self._db_path).exists():
                log.debug("hud: DuckDB file not found at %s — run ETL first", self._db_path)
                return None
            self._con = duckdb.connect(self._db_path, read_only=True)
            return self._con
        except Exception as exc:
            log.debug("hud: failed to open DuckDB: %s", exc)
            return None

    def close(self) -> None:
        if self._con:
            try:
                self._con.close()
            except Exception:
                pass
            self._con = None

    # ─────────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────────

    def get_properties_by_owner(
        self,
        owner_name: str,
        city: str | None = None,
        state: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """
        Find HUD-insured multifamily properties for a given owner entity.

        Tries exact (case-insensitive) match first, then fuzzy match.

        Args:
            owner_name:  Entity name to search (company name or individual).
            city:        Optional city filter (narrows result set).
            state:       Optional state filter (two-letter abbrev).
            limit:       Max properties to return.

        Returns:
            List of property dicts. Empty list if no match or ETL not run.
        """
        con = self._connect()
        if con is None:
            return []

        try:
            rows = self._query_exact(con, owner_name, city, state, limit)
            if not rows:
                rows = self._query_fuzzy(con, owner_name, city, state, limit)
            return [self._row_to_dict(r) for r in rows]
        except Exception as exc:
            log.debug("hud: query failed for '%s': %s", owner_name, exc)
            return []

    def get_portfolio_summary(
        self,
        owner_name: str,
        state: str | None = None,
    ) -> dict[str, Any]:
        """
        Return portfolio summary for an owner: total properties, total loan
        value, total residential units, states represented.

        Args:
            owner_name:  Owner entity name.
            state:       Optional state filter.

        Returns:
            {
                "owner_name":        str,
                "property_count":    int,
                "total_loan_value":  float | None,
                "total_units":       int | None,
                "states":            [str],
                "active_loans":      int,
            }
        """
        props = self.get_properties_by_owner(owner_name, state=state, limit=500)
        if not props:
            return {}

        total_loan = sum(p["loan_amount"] for p in props if p.get("loan_amount"))
        total_units = sum(p["units"] for p in props if p.get("units"))
        states = sorted(set(p["state"] for p in props if p.get("state")))
        active = sum(
            1 for p in props
            if p.get("loan_status", "").lower() in ("current", "active", "performing")
        )

        return {
            "owner_name":       owner_name,
            "property_count":   len(props),
            "total_loan_value": total_loan or None,
            "total_units":      total_units or None,
            "states":           states,
            "active_loans":     active,
        }

    def get_properties_by_city(
        self,
        city: str,
        state: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """
        Return HUD-insured properties in a given city.
        Useful for real estate market coverage analysis.
        """
        con = self._connect()
        if con is None:
            return []

        try:
            if state:
                rows = con.execute(f"""
                    SELECT *
                    FROM {_TABLE}
                    WHERE LOWER(city) = LOWER(?)
                      AND UPPER(state) = UPPER(?)
                    ORDER BY loan_amount DESC NULLS LAST
                    LIMIT ?
                """, [city, state, limit]).fetchall()
            else:
                rows = con.execute(f"""
                    SELECT *
                    FROM {_TABLE}
                    WHERE LOWER(city) = LOWER(?)
                    ORDER BY loan_amount DESC NULLS LAST
                    LIMIT ?
                """, [city, limit]).fetchall()

            cols = self._column_names(con)
            return [dict(zip(cols, r)) for r in rows]
        except Exception as exc:
            log.debug("hud: get_properties_by_city failed: %s", exc)
            return []

    # ─────────────────────────────────────────────────────────────────────────
    # Private helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _query_exact(
        self,
        con: Any,
        owner_name: str,
        city: str | None,
        state: str | None,
        limit: int,
    ) -> list[tuple]:
        conditions = ["LOWER(owner_name) = LOWER(?)"]
        params: list[Any] = [owner_name]

        if city:
            conditions.append("LOWER(city) = LOWER(?)")
            params.append(city)
        if state:
            conditions.append("UPPER(state) = UPPER(?)")
            params.append(state)

        where = " AND ".join(conditions)
        params.append(limit)

        return con.execute(f"""
            SELECT *
            FROM {_TABLE}
            WHERE {where}
            ORDER BY loan_amount DESC NULLS LAST
            LIMIT ?
        """, params).fetchall()

    def _query_fuzzy(
        self,
        con: Any,
        owner_name: str,
        city: str | None,
        state: str | None,
        limit: int,
    ) -> list[tuple]:
        """Fuzzy owner name match, then exact lookup on best match."""
        # Pull candidate owner names
        if state:
            candidates = con.execute(f"""
                SELECT DISTINCT owner_name
                FROM {_TABLE}
                WHERE UPPER(state) = UPPER(?)
            """, [state]).fetchall()
        else:
            candidates = con.execute(f"""
                SELECT DISTINCT owner_name FROM {_TABLE}
            """).fetchall()

        name_lower = owner_name.lower()
        best_score = 0.0
        best_name: str | None = None

        for (cand,) in candidates:
            score = SequenceMatcher(None, name_lower, cand.lower()).ratio()
            if score > best_score:
                best_score = score
                best_name = cand

        if best_score < _FUZZY_MIN or best_name is None:
            return []

        log.debug(
            "hud: fuzzy match '%s' → '%s' (score=%.2f)",
            owner_name, best_name, best_score,
        )

        return self._query_exact(con, best_name, city, state, limit)

    def _column_names(self, con: Any) -> list[str]:
        """Return column names for the properties table."""
        rows = con.execute(f"DESCRIBE {_TABLE}").fetchall()
        return [r[0] for r in rows]

    def _row_to_dict(self, row: tuple) -> dict[str, Any]:
        """Convert a property row tuple to a dict using known column order."""
        # Column order matches the CREATE TABLE in hud.py ETL:
        # property_name, owner_name, city, state, zip_code,
        # loan_amount, loan_status, program_type, units, maturity_date
        cols = [
            "property_name", "owner_name", "city", "state", "zip_code",
            "loan_amount", "loan_status", "program_type", "units", "maturity_date",
        ]
        d = dict(zip(cols, row))
        # Normalize numeric types
        if d.get("loan_amount") is not None:
            try:
                d["loan_amount"] = float(d["loan_amount"])
            except (TypeError, ValueError):
                d["loan_amount"] = None
        if d.get("units") is not None:
            try:
                d["units"] = int(d["units"])
            except (TypeError, ValueError):
                d["units"] = None
        return d
