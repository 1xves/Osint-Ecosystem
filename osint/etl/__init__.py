"""
osint/etl/__init__.py

ETL (Extract-Transform-Load) package for bulk government data.

Design principle: ETL runs once per environment to download and index
bulk datasets into DuckDB. Runtime agents query the local DuckDB index —
they never perform bulk downloads at query time.

Subpackages:
    us/         — U.S. government bulk datasets (FinCEN, HUD, IRS 990)
    global/     — International bulk datasets (ICIJ Offshore Leaks)

Running the ETL:
    python -m osint.etl.runner --sources fincen_ctr hud
    python -m osint.etl.runner --sources all
    python -m osint.etl.runner --sources icij_leaks --force-refresh
"""

from __future__ import annotations
