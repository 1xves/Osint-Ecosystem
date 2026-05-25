"""
osint/core/source_registry.py

Source registry — the canonical mapping of (country_code, entity_type) → source_ids.

Design goals:
    - Agents never hardcode which clients to call. They query the registry.
    - Global expansion = add a registry entry + a new client. Zero agent changes.
    - Source IDs are stable string keys that match the DOMAIN constant in each
      client module and the keys in RATE_LIMITS (config.py).

Usage:
    from osint.core.source_registry import SourceRegistry

    # Get all source IDs for a US corporate entity
    sources = SourceRegistry.get_sources("US", "corporate")
    # → ["edgar", "form_d", "opencorporates", "bizapedia", "sos_us"]

    # Check if a specific source applies
    if SourceRegistry.has_source("US", "corporate", "form_d"):
        ...

    # Get all entity types a source covers
    entity_types = SourceRegistry.entity_types_for_source("littlesis", "US")

Implementation status per source_id:
    BUILT+WIRED     — client built AND wired into an agent
    BUILT           — client built, NOT yet wired
    PLANNED         — client not yet built

Each source_id maps to a module path and implementation status in SOURCE_METADATA.
"""

from __future__ import annotations

import logging
from typing import Literal

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Type aliases
# ─────────────────────────────────────────────────────────────────────────────

CountryCode = str  # ISO 3166-1 alpha-2 (e.g. "US", "GB", "GLOBAL")
EntityType  = str  # Matches EntityType enum values in enums.py


# ─────────────────────────────────────────────────────────────────────────────
# Core registry
# Maps (country_code, entity_type) → ordered list of source_ids.
# Order matters: sources listed earlier are queried before later ones.
# Primary sources (regulatory filings) before secondary (web scraping).
# ─────────────────────────────────────────────────────────────────────────────

_REGISTRY: dict[tuple[CountryCode, EntityType], list[str]] = {

    # ── US: Corporate ────────────────────────────────────────────────────────
    ("US", "corporate"): [
        "sec_edgar",            # BUILT+WIRED — EDGAR company filings
        "form_d",               # BUILT+WIRED — SEC Regulation D private raises
        "opencorporates",       # BUILT       — global corporate registry
        "bizapedia",            # BUILT+WIRED — registered agents, officers (scraper)
        "sos_us",               # BUILT+WIRED — secretary of state filings (PA, DE)
        "hud",                  # BUILT+WIRED — HUD multifamily property data (ETL)
        "fincen",               # BUILT+WIRED — FinCEN CTR aggregate data (ETL)
        "wayback",              # BUILT+WIRED — historical website scraping (archived)
    ],

    # ── US: Executive / High-Net-Worth Individual ─────────────────────────────
    ("US", "executive_hnw"): [
        "form_d",               # BUILT+WIRED — Related Persons in Reg D filings
        "littlesis",            # BUILT+WIRED — power network relationships
        "proxycurl",            # BUILT+WIRED — LinkedIn profile enrichment
        "sec_edgar",            # BUILT+WIRED — DEF 14A proxy compensation
        "followthemoney",       # BUILT       — state campaign finance donations
        "fec_api",              # BUILT       — federal campaign finance
        "patent_view",          # BUILT       — patent inventor records
        "courtlistener",        # BUILT       — litigation as party
    ],

    # ── US: HNWI (high-net-worth individual, not necessarily executive) ──────
    ("US", "hnwi"): [
        "littlesis",            # BUILT+WIRED
        "proxycurl",            # BUILT+WIRED
        "followthemoney",       # BUILT
        "fec_api",              # BUILT
        "courtlistener",        # BUILT
    ],

    # ── US: Nonprofit ─────────────────────────────────────────────────────────
    ("US", "nonprofit"): [
        "propublica_nonprofit", # BUILT+WIRED — ProPublica Nonprofit Explorer
        "irs_990_xml",          # PLANNED     — IRS S3 full 990 XML (ETL-dependent)
        "usaspending",          # BUILT       — federal grants received
        "courtlistener",        # BUILT       — litigation
    ],

    # ── US: Political (committees, PACs, agencies) ───────────────────────────
    ("US", "political"): [
        "fec_api",              # BUILT       — FEC campaign finance
        "opensecrets",          # BUILT+WIRED — OpenSecrets revolving door + stats
        "lda",                  # BUILT+WIRED — lobbying disclosure
        "followthemoney",       # BUILT       — state campaign finance
        "usaspending",          # BUILT       — contracts/grants awarded
    ],

    # ── US: Investor (VC, PE, family office) ─────────────────────────────────
    ("US", "investor"): [
        "sec_edgar",            # BUILT+WIRED — Form D as lead investor
        "form_d",               # BUILT+WIRED — Reg D filing participation
        "littlesis",            # BUILT+WIRED — power network
        "proxycurl",            # BUILT+WIRED — LinkedIn profile
        "opencorporates",       # BUILT       — fund entity registrations
    ],

    # ── US: Philanthropic ────────────────────────────────────────────────────
    ("US", "philanthropic"): [
        "propublica_nonprofit", # BUILT+WIRED — foundation 990 data
        "irs_990_xml",          # PLANNED     — full 990 XML
        "usaspending",          # BUILT       — grant-making through federal programs
        "littlesis",            # BUILT+WIRED — foundation network
    ],

    # ── US: Real Estate ───────────────────────────────────────────────────────
    ("US", "real_estate"): [
        "hud",                  # BUILT+WIRED — HUD multifamily portfolio (ETL)
        "sos_us",               # PLANNED     — entity registration
        "opencorporates",       # BUILT       — corporate registration
    ],

    # ── US: Community Leader ─────────────────────────────────────────────────
    ("US", "community_leader"): [
        "littlesis",            # BUILT+WIRED — civic connections
        "gdelt",                # BUILT+WIRED — news mentions
        "eventbrite",           # BUILT       — events organized
        "meetup",               # BUILT       — groups organized
    ],

    # ── US: Politician ────────────────────────────────────────────────────────
    ("US", "politician"): [
        "congress",             # BUILT+WIRED — congressional membership
        "fec_api",              # BUILT       — campaign finance
        "opensecrets",          # BUILT+WIRED — donor network
        "followthemoney",       # BUILT       — state-level finance
        "courtlistener",        # BUILT       — litigation
        "lda",                  # BUILT+WIRED — lobbying connections
    ],

    # ── US: Illicit ───────────────────────────────────────────────────────────
    ("US", "illicit"): [
        "ofac",                 # BUILT+WIRED — OFAC SDN list
        "courtlistener",        # BUILT       — criminal/civil cases
        "icij",                 # BUILT       — offshore leaks database
    ],

    # ── GLOBAL: Corporate ─────────────────────────────────────────────────────
    ("GLOBAL", "corporate"): [
        "opencorporates",       # BUILT       — multi-jurisdiction registry
        "icij",                 # BUILT       — offshore leaks
    ],

    # ── GLOBAL: Sanctions ─────────────────────────────────────────────────────
    ("GLOBAL", "sanctions"): [
        "ofac",                 # BUILT+WIRED — US OFAC SDN
        "un_sanctions",         # BUILT+WIRED — UN consolidated list
        "bis_entity_list",      # BUILT+WIRED — BIS export controls
        "eu_sanctions",         # BUILT+WIRED — EU consolidated list
    ],

    # ── GLOBAL: Illicit ───────────────────────────────────────────────────────
    ("GLOBAL", "illicit"): [
        "ofac",                 # BUILT+WIRED
        "un_sanctions",         # BUILT+WIRED
        "bis_entity_list",      # BUILT+WIRED
        "eu_sanctions",         # BUILT+WIRED
        "icij",                 # BUILT
        "courtlistener",        # BUILT       — US courts only; expand for GLOBAL later
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# Source metadata
# Canonical record for each source_id.
# "status" is informational — used for logging and auditing, not routing logic.
# ─────────────────────────────────────────────────────────────────────────────

ImplementationStatus = Literal[
    "BUILT+WIRED",   # Client built AND wired into at least one agent
    "BUILT",         # Client built, NOT wired into any agent yet
    "PLANNED",       # Client not yet built
]

SOURCE_METADATA: dict[str, dict] = {
    "sec_edgar":            {"module": "osint.clients.edgar",           "status": "BUILT+WIRED", "country": "US"},
    "form_d":               {"module": "osint.clients.form_d",          "status": "BUILT+WIRED", "country": "US"},
    "littlesis":            {"module": "osint.clients.littlesis",       "status": "BUILT+WIRED", "country": "US"},
    "proxycurl":            {"module": "osint.clients.proxycurl",       "status": "BUILT+WIRED", "country": "US"},
    "propublica_nonprofit": {"module": "osint.clients.propublica",      "status": "BUILT+WIRED", "country": "US"},
    "opensecrets":          {"module": "osint.clients.opensecrets",     "status": "BUILT+WIRED", "country": "US"},
    "lda":                  {"module": "osint.clients.lda",             "status": "BUILT+WIRED", "country": "US"},
    "congress":             {"module": "osint.clients.congress",        "status": "BUILT+WIRED", "country": "US"},
    "gdelt":                {"module": "osint.clients.gdelt",           "status": "BUILT+WIRED", "country": "GLOBAL"},
    "ofac":                 {"module": "osint.clients.ofac",            "status": "BUILT+WIRED", "country": "GLOBAL"},
    "un_sanctions":         {"module": "osint.clients.un_sanctions",    "status": "BUILT+WIRED", "country": "GLOBAL"},
    "bis_entity_list":      {"module": "osint.clients.bis_entity_list", "status": "BUILT+WIRED", "country": "GLOBAL"},
    "eu_sanctions":         {"module": "osint.clients.eu_sanctions",    "status": "BUILT+WIRED", "country": "GLOBAL"},
    "opencorporates":       {"module": "osint.clients.opencorporates",          "status": "BUILT+WIRED", "country": "GLOBAL"},
    "followthemoney":       {"module": "osint.clients.followthemoney",          "status": "BUILT+WIRED", "country": "US"},
    "fec_api":              {"module": "osint.clients.fec",                     "status": "BUILT",       "country": "US"},
    "usaspending":          {"module": "osint.clients.usaspending",             "status": "BUILT",       "country": "US"},
    "patent_view":          {"module": "osint.clients.patent_view",             "status": "BUILT+WIRED", "country": "US"},
    "courtlistener":        {"module": "osint.clients.courtlistener",           "status": "BUILT",       "country": "US"},
    "icij":                 {"module": "osint.clients.icij",                    "status": "BUILT+WIRED", "country": "GLOBAL"},
    "wayback":              {"module": "osint.clients.wayback",                 "status": "BUILT+WIRED", "country": "GLOBAL"},
    "eventbrite":           {"module": "osint.clients.eventbrite",              "status": "BUILT+WIRED", "country": "US"},
    "meetup":               {"module": "osint.clients.meetup",                  "status": "BUILT+WIRED", "country": "US"},
    "bizapedia":            {"module": "osint.clients.scrapers.bizapedia",      "status": "BUILT+WIRED", "country": "US"},
    "sos_us":               {"module": "osint.clients.scrapers.sos",            "status": "BUILT+WIRED", "country": "US"},
    "hud":                  {"module": "osint.clients.hud",             "status": "BUILT+WIRED", "country": "US"},
    "fincen":               {"module": "osint.clients.fincen",          "status": "BUILT+WIRED", "country": "US"},
    "irs_990_xml":          {"module": "osint.clients.irs_990",         "status": "PLANNED",     "country": "US"},
}


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

class SourceRegistry:
    """
    Static helper class — all methods are class methods, no instantiation needed.
    """

    @classmethod
    def get_sources(
        cls,
        country_code: CountryCode,
        entity_type: EntityType,
        *,
        status_filter: ImplementationStatus | None = None,
    ) -> list[str]:
        """
        Return the ordered list of source_ids for a (country_code, entity_type) pair.

        Args:
            country_code:   ISO 3166-1 alpha-2, or "GLOBAL".
            entity_type:    Entity type string (matches EntityType enum values).
            status_filter:  If set, only return sources with this status.
                            Useful for limiting to "BUILT+WIRED" only during production.

        Returns:
            Ordered list of source_ids. Empty list if no sources registered.

        Example:
            SourceRegistry.get_sources("US", "corporate")
            → ["sec_edgar", "form_d", "opencorporates", ...]

            SourceRegistry.get_sources("US", "corporate", status_filter="BUILT+WIRED")
            → ["sec_edgar", "form_d"]  # Only wired sources
        """
        sources = _REGISTRY.get((country_code, entity_type), [])

        if status_filter is not None:
            sources = [
                s for s in sources
                if SOURCE_METADATA.get(s, {}).get("status") == status_filter
            ]

        if not sources:
            log.debug(
                "source_registry: no sources for (%s, %s) [status_filter=%s]",
                country_code, entity_type, status_filter,
            )

        return sources

    @classmethod
    def has_source(
        cls,
        country_code: CountryCode,
        entity_type: EntityType,
        source_id: str,
    ) -> bool:
        """Check if a specific source_id is registered for this (country, entity_type)."""
        return source_id in _REGISTRY.get((country_code, entity_type), [])

    @classmethod
    def entity_types_for_source(
        cls,
        source_id: str,
        country_code: CountryCode | None = None,
    ) -> list[tuple[CountryCode, EntityType]]:
        """
        Return all (country_code, entity_type) pairs that include a given source_id.
        Useful for understanding the blast radius of a source being unavailable.

        Args:
            source_id:      The source to look up.
            country_code:   If provided, filter to only entries for this country.

        Returns:
            List of (country_code, entity_type) tuples.
        """
        results = []
        for (cc, et), sources in _REGISTRY.items():
            if source_id in sources:
                if country_code is None or cc == country_code:
                    results.append((cc, et))
        return results

    @classmethod
    def get_status(cls, source_id: str) -> ImplementationStatus | None:
        """Return the implementation status of a source_id."""
        meta = SOURCE_METADATA.get(source_id)
        if meta is None:
            return None
        return meta.get("status")  # type: ignore[return-value]

    @classmethod
    def register(
        cls,
        country_code: CountryCode,
        entity_type: EntityType,
        source_id: str,
        *,
        position: int | None = None,
        module: str = "",
        status: ImplementationStatus = "PLANNED",
    ) -> None:
        """
        Dynamically register a new source at runtime.

        This is the extension point for global expansion — new country clients
        call this in their module __init__ rather than modifying this file.

        Args:
            country_code:   Country code for this source.
            entity_type:    Entity type this source covers.
            source_id:      Unique source identifier string.
            position:       Insert at this position in the list (None = append).
            module:         Python import path for the client module.
            status:         Implementation status.

        Example (in osint/clients/uk/companies_house.py):
            from osint.core.source_registry import SourceRegistry
            SourceRegistry.register("GB", "corporate", "companies_house",
                                    module="osint.clients.uk.companies_house",
                                    status="BUILT+WIRED")
        """
        key = (country_code, entity_type)
        if key not in _REGISTRY:
            _REGISTRY[key] = []

        if source_id not in _REGISTRY[key]:
            if position is not None:
                _REGISTRY[key].insert(position, source_id)
            else:
                _REGISTRY[key].append(source_id)

        SOURCE_METADATA[source_id] = {
            "module": module,
            "status": status,
            "country": country_code,
        }

        log.debug(
            "source_registry: registered source '%s' for (%s, %s) [status=%s]",
            source_id, country_code, entity_type, status,
        )

    @classmethod
    def audit(cls) -> dict[str, list[str]]:
        """
        Return a summary of all sources by implementation status.
        Useful for logging at startup to see what's wired vs. planned.

        Returns:
            {"BUILT+WIRED": [...], "BUILT": [...], "PLANNED": [...]}
        """
        result: dict[str, list[str]] = {"BUILT+WIRED": [], "BUILT": [], "PLANNED": []}
        seen: set[str] = set()
        for sources in _REGISTRY.values():
            for source_id in sources:
                if source_id in seen:
                    continue
                seen.add(source_id)
                status = SOURCE_METADATA.get(source_id, {}).get("status", "PLANNED")
                result.setdefault(status, []).append(source_id)
        return result
