"""
osint/etl/runner.py

CLI entry point for bulk data ETL jobs.

Usage:
    python -m osint.etl.runner --sources fincen_ctr hud
    python -m osint.etl.runner --sources all
    python -m osint.etl.runner --sources icij_leaks --force-refresh
    python -m osint.etl.runner --list

Each source is an independently runnable module that implements:
    async def run(db_path: str, *, force_refresh: bool = False) -> ETLResult

ETLResult fields:
    source:       str          — Source name
    records_loaded: int        — Number of rows inserted/updated
    db_path:      str          — Path to resulting DuckDB file
    elapsed_sec:  float        — Wall-clock time
    error:        str | None   — Error message if failed

The DuckDB file path defaults to ~/.osint/etl/{source}.duckdb, but is
overridden by the ETL_DB_DIR environment variable if set.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Awaitable

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Result type shared by all ETL modules
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ETLResult:
    source:          str
    records_loaded:  int   = 0
    db_path:         str   = ""
    elapsed_sec:     float = 0.0
    error:           str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None


# ─────────────────────────────────────────────────────────────────────────────
# Registry of available ETL sources
# ─────────────────────────────────────────────────────────────────────────────

# Each value is a lazy import path; runner imports only the requested source(s).
_SOURCE_REGISTRY: dict[str, str] = {
    "fincen_ctr":   "osint.etl.us.fincen_ctr",
    "hud":          "osint.etl.us.hud",
    "icij_leaks":   "osint.etl.global.icij_leaks",
    # Future:
    # "irs_990":    "osint.etl.us.irs_990",
}


def _get_db_dir() -> Path:
    """Return the base directory for DuckDB files, creating it if needed."""
    db_dir = Path(os.environ.get("ETL_DB_DIR", Path.home() / ".osint" / "etl"))
    db_dir.mkdir(parents=True, exist_ok=True)
    return db_dir


def _db_path_for(source: str) -> str:
    return str(_get_db_dir() / f"{source}.duckdb")


async def _run_source(
    source: str,
    db_path: str,
    force_refresh: bool,
    manual_file_path: str | None = None,
) -> ETLResult:
    """
    Import and run a single ETL source module.

    manual_file_path is forwarded to ETL modules that accept it (fincen_ctr, hud).
    For sources that don't accept it, it is silently ignored.
    """
    import importlib
    import inspect

    module_path = _SOURCE_REGISTRY.get(source)
    if not module_path:
        return ETLResult(
            source=source,
            error=f"Unknown ETL source '{source}'. Run with --list to see available sources.",
        )

    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        return ETLResult(source=source, error=f"Import failed: {exc}")

    run_fn: Callable[[str], Awaitable[ETLResult]] = getattr(module, "run", None)
    if run_fn is None:
        return ETLResult(source=source, error=f"Module {module_path} has no run() function")

    # Only forward manual_file_path if the run() function accepts it
    run_sig = inspect.signature(run_fn)
    kwargs: dict = {"force_refresh": force_refresh}
    if "manual_file_path" in run_sig.parameters and manual_file_path:
        kwargs["manual_file_path"] = manual_file_path

    start = time.monotonic()
    try:
        result = await run_fn(db_path, **kwargs)
        result.elapsed_sec = time.monotonic() - start
        return result
    except Exception as exc:
        log.exception("ETL runner: source '%s' raised unhandled exception", source)
        return ETLResult(
            source=source,
            db_path=db_path,
            elapsed_sec=time.monotonic() - start,
            error=str(exc),
        )


async def _main(args: argparse.Namespace) -> int:
    """Async main; returns exit code."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if args.list:
        print("Available ETL sources:")
        for name, mod in _SOURCE_REGISTRY.items():
            print(f"  {name:<20}  {mod}")
        return 0

    sources = list(_SOURCE_REGISTRY.keys()) if "all" in args.sources else args.sources
    unknown = [s for s in sources if s not in _SOURCE_REGISTRY]
    if unknown:
        log.error("Unknown sources: %s", unknown)
        return 1

    # --file only makes sense with a single source
    manual_file = getattr(args, "file", None) or None
    if manual_file and len(sources) > 1:
        log.error("--file can only be used with a single source, not multiple.")
        return 1

    results: list[ETLResult] = []
    for source in sources:
        db_path = _db_path_for(source)
        log.info("ETL: starting '%s' → %s", source, db_path)
        result = await _run_source(
            source,
            db_path,
            force_refresh=args.force_refresh,
            manual_file_path=manual_file,
        )
        results.append(result)

        if result.ok:
            log.info(
                "ETL: '%s' complete — %d records in %.1fs",
                source, result.records_loaded, result.elapsed_sec,
            )
        else:
            log.error("ETL: '%s' FAILED — %s", source, result.error)

    # Summary
    print("\n── ETL Summary ──")
    ok_count = sum(1 for r in results if r.ok)
    for r in results:
        status = "OK" if r.ok else "FAIL"
        row = f"  [{status}] {r.source:<20}"
        if r.ok:
            row += f"  {r.records_loaded:>8,} records  {r.elapsed_sec:.1f}s  → {r.db_path}"
        else:
            row += f"  ERROR: {r.error}"
        print(row)
    print(f"\n{ok_count}/{len(results)} sources completed successfully.")

    return 0 if ok_count == len(results) else 1


def main() -> None:
    parser = argparse.ArgumentParser(
        description="OSINT ETL runner — bulk government data ingestion",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m osint.etl.runner --sources fincen_ctr hud
  python -m osint.etl.runner --sources all
  python -m osint.etl.runner --sources icij_leaks --force-refresh
  python -m osint.etl.runner --list

  # Manual file path (required for HUD and FinCEN CTR — URLs are unreliable):
  python -m osint.etl.runner --sources hud --file ~/Downloads/MFH_Discl_Detail.xlsx
  python -m osint.etl.runner --sources fincen_ctr --file ~/Downloads/CTRs_by_State.xlsx

  # Equivalent using environment variables:
  HUD_FILE=~/Downloads/file.csv python -m osint.etl.runner --sources hud
  FINCEN_CTR_FILE=~/Downloads/file.xlsx python -m osint.etl.runner --sources fincen_ctr
""",
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        default=[],
        metavar="SOURCE",
        help="ETL sources to run. Use 'all' to run all sources.",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        default=False,
        help="Re-download and reload even if DuckDB already exists.",
    )
    parser.add_argument(
        "--file",
        default=None,
        metavar="PATH",
        help=(
            "Path to a manually downloaded source file. "
            "Only valid when a single --source is specified. "
            "Required for HUD and FinCEN CTR (direct URLs are unreliable)."
        ),
    )
    parser.add_argument(
        "--list",
        action="store_true",
        default=False,
        help="List available ETL sources and exit.",
    )

    args = parser.parse_args()
    if not args.list and not args.sources:
        parser.print_help()
        raise SystemExit(1)

    raise SystemExit(asyncio.run(_main(args)))


if __name__ == "__main__":
    main()
