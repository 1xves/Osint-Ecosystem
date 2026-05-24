"""
osint/export/stix21.py

STIX 2.1 Exporter — converts the illicit-layer pipeline output into a
standards-compliant STIX 2.1 bundle for threat intelligence sharing.

STIX 2.1 (Structured Threat Information Expression) is the MITRE/OASIS
standard for exchanging threat intelligence. The format is accepted by:
  - Threat intel platforms (MISP, OpenCTI, ThreatConnect, Anomali)
  - ISAC/ISAO sharing communities
  - Government TAXII feeds
  - Security operations tooling

Reference: https://docs.oasis-open.org/cti/stix/v2.1/stix-v2.1.html

What this exporter covers:
  - Illicit entities (OFAC sanctioned, organized crime, defendants, etc.)
    → STIX threat-actor SDOs
  - Non-illicit entities they are connected to (investors, corporates, etc.)
    → STIX identity SDOs
  - Relationships between them (based on relationship_type mapping below)
    → STIX relationship SROs
  - OFAC SDN and court-filing evidence
    → STIX indicator SDOs
  - Everything wrapped in a STIX bundle

What this exporter does NOT cover:
  - Non-illicit entities with no illicit-layer connections
    (the bundle scope is intentionally limited to the illicit subgraph)
  - Scoring data, briefing output, or analytical assessments
    (those are operational outputs, not threat intelligence)

Sensitivity:
  All objects in this bundle are presumed sensitive. The bundle itself
  carries a data_marking AMBER (TLP:AMBER — share within your organization
  and with partners under NDA). Callers may downgrade to TLP:GREEN or
  upgrade to TLP:RED before distribution.

Usage:
    from osint.export.stix21 import STIX21Exporter

    exporter = STIX21Exporter()
    bundle = exporter.export(state)             # Returns dict
    exporter.export_to_file(state, "/out/bundle.json")  # Writes JSON

    # Or export a specific verified entity set only:
    bundle = exporter.export(state, entity_ids=verified_entity_ids)
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

STIX_SPEC_VERSION = "2.1"

# TLP marking definition IDs (canonical STIX UUIDs)
TLP_WHITE_ID  = "marking-definition--613f2e26-407d-48c7-9eca-b8e91df99dc9"
TLP_GREEN_ID  = "marking-definition--34098fce-860f-48ae-8e10-f67b0b41f570"
TLP_AMBER_ID  = "marking-definition--f88d31f6-1208-47b8-8c13-1706eb90e3fd"
TLP_RED_ID    = "marking-definition--5e57c739-391a-4eb3-b6be-7d15ca92d5ed"

# Default marking for this bundle
DEFAULT_TLP = TLP_AMBER_ID

# Confidence mapping: our string → STIX integer (0-100)
CONFIDENCE_MAP: dict[str, int] = {
    "high":   85,
    "medium": 50,
    "low":    20,
}

# Map our entity_subtype to STIX threat-actor types
# https://docs.oasis-open.org/cti/stix/v2.1/cs01/stix-v2.1-cs01.html#_k017w16zutw
SUBTYPE_TO_THREAT_ACTOR_TYPES: dict[str, list[str]] = {
    "ofac_sanctioned":    ["criminal", "nation-state"],
    "federal_defendant":  ["criminal"],
    "organized_crime":    ["criminal", "crime-syndicate"],
    "money_laundering":   ["criminal", "financial-crime"],
    "fraud":              ["criminal", "financial-crime"],
    "corruption":         ["criminal", "insider-threat"],
    "human_trafficking":  ["criminal"],
    "drug_trafficking":   ["criminal"],
}

# Map our relationship types to STIX relationship types
# STIX has a defined vocabulary; unmapped types fall back to "related-to"
REL_TYPE_TO_STIX: dict[str, str] = {
    "INVESTED_IN":              "targets",
    "FOUNDED":                  "attributed-to",
    "EMPLOYED_BY":              "related-to",
    "SITS_ON_BOARD_OF":         "related-to",
    "DONATED_TO":               "targets",
    "SUBSIDIARY_OF":            "consists-of",
    "LITIGATION_AGAINST":       "targets",
    "MENTIONED_WITH":           "related-to",
    "POLITICALLY_CONNECTED_TO": "related-to",
    "OFFSHORE_ENTITY_LINKED_TO": "uses",
    "BENEFICIAL_OWNER_OF":      "owns",
    "BOARD_INTERLOCKED_WITH":   "related-to",
    "CO_INVESTED_WITH":         "cooperates-with",
    "CO_FOUNDED_WITH":          "cooperates-with",
    "ADVISED_BY":               "related-to",
    "REGULATORY_OVERSIGHT":     "related-to",
}

# Map our entity_type to STIX identity_class
# https://docs.oasis-open.org/cti/stix/v2.1/cs01/stix-v2.1-cs01.html#_be1dktvcmyu
ENTITY_TYPE_TO_IDENTITY_CLASS: dict[str, str] = {
    "investor":         "organization",
    "corporate":        "organization",
    "nonprofit":        "organization",
    "philanthropic":    "organization",
    "political":        "organization",
    "government":       "organization",
    "executive_hnw":    "individual",
    "politician":       "individual",
    "hnwi":             "individual",
    "community_leader": "individual",
    "illicit":          "individual",   # May be individual or org — default individual
}

# Map our entity_type to STIX industry sectors (for identity objects)
ENTITY_TYPE_TO_SECTORS: dict[str, list[str]] = {
    "investor":         ["financial-services"],
    "corporate":        ["technology"],
    "nonprofit":        ["non-profit"],
    "philanthropic":    ["non-profit"],
    "political":        ["government"],
    "government":       ["government"],
}


# ─────────────────────────────────────────────────────────────────────────────
# Exporter
# ─────────────────────────────────────────────────────────────────────────────

class STIX21Exporter:
    """
    Converts OSINT pipeline state to a STIX 2.1 bundle.

    Scope: illicit-layer entities and their first-degree connections.
    Output: STIX 2.1 bundle dict ready for serialization or TAXII push.

    Thread/coroutine safety: stateless — no instance state mutated during export.
    Multiple concurrent exports are safe.
    """

    # ─────────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────────

    def export(
        self,
        state: dict[str, Any],
        entity_ids: list[str] | None = None,
        tlp_marking: str = DEFAULT_TLP,
    ) -> dict[str, Any]:
        """
        Build a STIX 2.1 bundle from pipeline state.

        Args:
            state:          Full pipeline state dict (or a subset with the
                            keys: scored_entities, relationships_draft,
                            verification_results, run_id, city_name).
            entity_ids:     Optional allowlist of entity_ids to include.
                            If None, all illicit-type entities are exported.
                            Use verified_entity_ids to restrict to gate-passed entities.
            tlp_marking:    TLP marking definition ID to apply to all objects.
                            Defaults to TLP:AMBER.

        Returns:
            STIX 2.1 bundle dict. Serialize with json.dumps() for file output.
        """
        run_id    = state.get("run_id", str(uuid.uuid4()))
        city_name = state.get("city_name", "Unknown")
        entities  = (
            state.get("scored_entities")
            or state.get("enriched_entities")
            or state.get("canonical_entities", [])
        )
        relationships = state.get("relationships_draft", [])

        # ── Filter: illicit entities only (or from entity_ids allowlist) ──────
        illicit_entities = [
            e for e in entities
            if e.get("entity_type") == "illicit"
            and (entity_ids is None or e.get("entity_id") in set(entity_ids))
        ]

        if not illicit_entities:
            log.info(
                "stix21_exporter: no illicit entities found in state — "
                "exporting empty bundle for run_id=%s", run_id,
            )
            return self._empty_bundle(run_id, city_name)

        illicit_ids = {e["entity_id"] for e in illicit_entities if e.get("entity_id")}

        # ── Find first-degree connections ─────────────────────────────────────
        # Collect all relationships where at least one endpoint is illicit.
        # This also determines which non-illicit entities need identity SDOs.
        illicit_edges = [
            edge for edge in relationships
            if (
                edge.get("source_entity_id") in illicit_ids
                or edge.get("target_entity_id") in illicit_ids
            )
        ]

        # Collect entity_ids of non-illicit connected entities
        connected_ids: set[str] = set()
        for edge in illicit_edges:
            for eid_key in ("source_entity_id", "target_entity_id"):
                eid = edge.get(eid_key)
                if eid and eid not in illicit_ids:
                    connected_ids.add(eid)

        connected_entities = [
            e for e in entities
            if e.get("entity_id") in connected_ids
        ]

        # ── Build entity_id → entity lookup ───────────────────────────────────
        entity_lookup: dict[str, dict[str, Any]] = {
            e["entity_id"]: e
            for e in entities
            if e.get("entity_id")
        }

        # ── Build STIX objects ─────────────────────────────────────────────────
        stix_objects: list[dict[str, Any]] = []
        # Map: our entity_id → STIX object id (e.g. "threat-actor--<uuid>")
        entity_to_stix_id: dict[str, str] = {}

        # 1. Threat-actor SDOs for illicit entities
        for entity in illicit_entities:
            ta = self._entity_to_threat_actor(entity, tlp_marking)
            entity_to_stix_id[entity["entity_id"]] = ta["id"]
            stix_objects.append(ta)

            # Indicator SDO if entity has OFAC / court evidence
            indicators = self._build_indicators(entity, ta["id"], tlp_marking)
            stix_objects.extend(indicators)

        # 2. Identity SDOs for connected non-illicit entities
        for entity in connected_entities:
            identity = self._entity_to_identity(entity, tlp_marking)
            entity_to_stix_id[entity["entity_id"]] = identity["id"]
            stix_objects.append(identity)

        # 3. Relationship SROs for illicit edges
        for edge in illicit_edges:
            src_eid = edge.get("source_entity_id", "")
            tgt_eid = edge.get("target_entity_id", "")

            src_stix = entity_to_stix_id.get(src_eid)
            tgt_stix = entity_to_stix_id.get(tgt_eid)

            if not src_stix or not tgt_stix:
                # One endpoint is not in our export scope — skip
                log.debug(
                    "stix21_exporter: skipping edge %s→%s — endpoint not in scope",
                    src_eid[:8], tgt_eid[:8],
                )
                continue

            rel_sro = self._edge_to_relationship(edge, src_stix, tgt_stix, tlp_marking)
            stix_objects.append(rel_sro)

        # ── Build bundle ───────────────────────────────────────────────────────
        bundle = self._build_bundle(
            run_id=run_id,
            city_name=city_name,
            stix_objects=stix_objects,
        )

        log.info(
            "stix21_exporter: exported %d illicit entities, %d connected entities, "
            "%d relationships, %d total objects (run_id=%s, city=%s)",
            len(illicit_entities), len(connected_entities),
            len(illicit_edges), len(stix_objects),
            run_id, city_name,
        )

        return bundle

    def export_to_file(
        self,
        state: dict[str, Any],
        output_path: str | Path,
        entity_ids: list[str] | None = None,
        tlp_marking: str = DEFAULT_TLP,
        indent: int = 2,
    ) -> Path:
        """
        Export STIX 2.1 bundle to a JSON file.

        Args:
            state:          Pipeline state dict.
            output_path:    Destination path for the .json file.
            entity_ids:     Optional entity_id allowlist.
            tlp_marking:    TLP marking to apply.
            indent:         JSON indentation (2 for human-readable, 0 for compact).

        Returns:
            Resolved Path of the written file.
        """
        bundle = self.export(state, entity_ids=entity_ids, tlp_marking=tlp_marking)

        out_path = Path(output_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps(bundle, indent=indent, default=str),
            encoding="utf-8",
        )
        log.info("stix21_exporter: bundle written to %s", out_path)
        return out_path

    # ─────────────────────────────────────────────────────────────────────────
    # SDO / SRO builders
    # ─────────────────────────────────────────────────────────────────────────

    def _entity_to_threat_actor(
        self,
        entity: dict[str, Any],
        tlp_marking: str,
    ) -> dict[str, Any]:
        """
        Convert an illicit OSINT entity to a STIX 2.1 threat-actor SDO.

        Spec: https://docs.oasis-open.org/cti/stix/v2.1/cs01/stix-v2.1-cs01.html#_k017w16zutw
        """
        now        = _now_iso()
        stix_id    = f"threat-actor--{uuid.uuid4()}"
        subtype    = entity.get("entity_subtype", "")
        confidence = CONFIDENCE_MAP.get(entity.get("overall_confidence", "medium"), 50)

        # threat_actor_types from subtype
        threat_actor_types = (
            SUBTYPE_TO_THREAT_ACTOR_TYPES.get(subtype, ["criminal"])
        )

        # Build aliases list
        aliases: list[str] = list(entity.get("aliases", []))
        for key in ("legal_name", "org_name", "person_name"):
            val = entity.get(key)
            if val and val not in aliases and val != entity.get("canonical_name"):
                aliases.append(val)

        # Labels from sanction status, ICIJ match, etc.
        labels: list[str] = [subtype.replace("_", "-")] if subtype else []
        cat = entity.get("category_fields", {})
        if cat.get("sanction_status"):
            labels.append("ofac-sanctioned")
        if cat.get("icij_offshore_match"):
            labels.append("offshore-entity-linked")
        if cat.get("court_cases"):
            labels.append("federal-defendant")

        # External references from evidence sources
        external_refs = _build_external_references(entity)

        # Description — combine entity description + key flags
        desc_parts = []
        if entity.get("description"):
            desc_parts.append(str(entity["description"])[:500])
        sanction_status = cat.get("sanction_status")
        if sanction_status:
            desc_parts.append(f"OFAC sanction status: {sanction_status}")
        if cat.get("icij_offshore_match"):
            desc_parts.append(
                "Appears in ICIJ Offshore Leaks database. "
                "Presence does NOT constitute proof of wrongdoing."
            )

        description = " ".join(desc_parts) if desc_parts else (
            f"Illicit-layer entity: {entity.get('canonical_name', 'Unknown')}. "
            f"Subtype: {subtype or 'unclassified'}."
        )

        return {
            "type":                "threat-actor",
            "spec_version":        STIX_SPEC_VERSION,
            "id":                  stix_id,
            "created":             now,
            "modified":            now,
            "name":                entity.get("canonical_name", "Unknown"),
            "description":         description,
            "aliases":             aliases,
            "threat_actor_types":  threat_actor_types,
            "labels":              labels,
            "confidence":          confidence,
            "external_references": external_refs,
            "object_marking_refs": [tlp_marking],
            # OSINT-system extensions (stored in custom properties — prefix x_)
            "x_osint_entity_id":   entity.get("entity_id"),
            "x_osint_entity_type": entity.get("entity_type"),
            "x_osint_run_id":      entity.get("source_run_ids", [None])[0],
            "x_osint_city":        entity.get("primary_city"),
            "x_needs_human_review": True,   # Always true for illicit entities
        }

    def _entity_to_identity(
        self,
        entity: dict[str, Any],
        tlp_marking: str,
    ) -> dict[str, Any]:
        """
        Convert a non-illicit OSINT entity to a STIX 2.1 identity SDO.
        Used for entities connected to illicit actors (investors, corporations, etc.).

        Spec: https://docs.oasis-open.org/cti/stix/v2.1/cs01/stix-v2.1-cs01.html#_wh296fiwpahp
        """
        now         = _now_iso()
        stix_id     = f"identity--{uuid.uuid4()}"
        entity_type = entity.get("entity_type", "unknown")
        confidence  = CONFIDENCE_MAP.get(entity.get("overall_confidence", "medium"), 50)

        identity_class = ENTITY_TYPE_TO_IDENTITY_CLASS.get(entity_type, "unknown")
        sectors        = ENTITY_TYPE_TO_SECTORS.get(entity_type, [])

        external_refs = _build_external_references(entity)

        contact_info_parts: list[str] = []
        if entity.get("website_url"):
            contact_info_parts.append(f"Web: {entity['website_url']}")
        if entity.get("linkedin_url"):
            contact_info_parts.append(f"LinkedIn: {entity['linkedin_url']}")

        return {
            "type":                "identity",
            "spec_version":        STIX_SPEC_VERSION,
            "id":                  stix_id,
            "created":             now,
            "modified":            now,
            "name":                entity.get("canonical_name", entity.get("name", "Unknown")),
            "description":         (entity.get("description") or "")[:500],
            "identity_class":      identity_class,
            "sectors":             sectors if sectors else None,
            "contact_information": " | ".join(contact_info_parts) or None,
            "confidence":          confidence,
            "external_references": external_refs,
            "object_marking_refs": [tlp_marking],
            # Custom extensions
            "x_osint_entity_id":   entity.get("entity_id"),
            "x_osint_entity_type": entity_type,
            "x_osint_city":        entity.get("primary_city"),
        }

    def _edge_to_relationship(
        self,
        edge: dict[str, Any],
        source_stix_id: str,
        target_stix_id: str,
        tlp_marking: str,
    ) -> dict[str, Any]:
        """
        Convert a pipeline relationship edge to a STIX 2.1 relationship SRO.

        Spec: https://docs.oasis-open.org/cti/stix/v2.1/cs01/stix-v2.1-cs01.html#_e9da3c8t9u7b
        """
        now          = _now_iso()
        our_rel_type = edge.get("relationship_type", "MENTIONED_WITH")
        stix_rel     = REL_TYPE_TO_STIX.get(our_rel_type, "related-to")
        confidence   = CONFIDENCE_MAP.get(edge.get("confidence", "medium"), 50)

        description = (edge.get("evidence_snippets") or [""])[0][:500]
        if not description:
            description = (
                f"{our_rel_type.replace('_', ' ').lower()} relationship "
                f"(confidence: {edge.get('confidence', 'medium')})"
            )

        external_refs: list[dict[str, str]] = []
        for snippet in edge.get("evidence_snippets", [])[:3]:
            if snippet:
                external_refs.append({
                    "source_name": "OSINT pipeline evidence",
                    "description": str(snippet)[:200],
                })

        return {
            "type":                "relationship",
            "spec_version":        STIX_SPEC_VERSION,
            "id":                  f"relationship--{uuid.uuid4()}",
            "created":             now,
            "modified":            now,
            "relationship_type":   stix_rel,
            "source_ref":          source_stix_id,
            "target_ref":          target_stix_id,
            "description":         description,
            "confidence":          confidence,
            "external_references": external_refs if external_refs else None,
            "object_marking_refs": [tlp_marking],
            # Custom extensions
            "x_osint_relationship_type":   our_rel_type,
            "x_osint_relationship_id":     edge.get("relationship_id"),
            "x_osint_relationship_strength": edge.get("relationship_strength"),
            "x_osint_sensitive":           edge.get("sensitive_claim", True),
        }

    def _build_indicators(
        self,
        entity: dict[str, Any],
        threat_actor_stix_id: str,
        tlp_marking: str,
    ) -> list[dict[str, Any]]:
        """
        Build STIX indicator SDOs for OFAC SDN listings and court case filings
        associated with this illicit entity.

        Each indicator represents a specific, verifiable claim (SDN entry,
        court case number) rather than the entity itself.

        Spec: https://docs.oasis-open.org/cti/stix/v2.1/cs01/stix-v2.1-cs01.html#_muftrcpnf89v
        """
        indicators: list[dict[str, Any]] = []
        cat  = entity.get("category_fields", {})
        now  = _now_iso()
        name = entity.get("canonical_name", "Unknown")

        # ── OFAC SDN indicator ─────────────────────────────────────────────────
        sanction_status = cat.get("sanction_status")
        if sanction_status:
            stix_pattern = f"[threat-actor:name = '{_escape_stix_pattern(name)}']"
            indicator = {
                "type":         "indicator",
                "spec_version": STIX_SPEC_VERSION,
                "id":           f"indicator--{uuid.uuid4()}",
                "created":      now,
                "modified":     now,
                "name":         f"OFAC SDN: {name}",
                "description":  (
                    f"{name} appears on the OFAC Specially Designated Nationals (SDN) list. "
                    f"Sanction status: {sanction_status}. "
                    f"Sanctions do not by themselves constitute criminal guilt."
                ),
                "pattern_type": "stix",
                "pattern":      stix_pattern,
                "valid_from":   now,
                "indicator_types": ["malicious-activity", "attribution"],
                "labels":          ["ofac-sdn", "treasury-sanction"],
                "confidence":   CONFIDENCE_MAP.get(entity.get("overall_confidence", "high"), 85),
                "external_references": [{
                    "source_name": "OFAC SDN List",
                    "url":         "https://sanctionssearch.ofac.treas.gov/",
                    "description": f"SDN entry for {name}",
                }],
                "object_marking_refs": [tlp_marking],
                # Relationship to threat-actor (STIX indicator → "indicates" → threat-actor)
                # We store as custom field; caller should create a "indicates" relationship SRO
                # if their TAXII platform requires explicit indicator→threat-actor links.
                "x_indicates_ref": threat_actor_stix_id,
            }
            indicators.append(indicator)

            # Relationship: indicator → indicates → threat-actor
            indicators.append({
                "type":                "relationship",
                "spec_version":        STIX_SPEC_VERSION,
                "id":                  f"relationship--{uuid.uuid4()}",
                "created":             now,
                "modified":            now,
                "relationship_type":   "indicates",
                "source_ref":          indicator["id"],
                "target_ref":          threat_actor_stix_id,
                "description":         f"OFAC SDN listing indicates threat actor {name}",
                "object_marking_refs": [tlp_marking],
            })

        # ── Court case indicators ──────────────────────────────────────────────
        court_cases = cat.get("court_cases")
        if court_cases and isinstance(court_cases, list):
            for case in court_cases[:3]:   # Cap at 3 case indicators per entity
                if not case:
                    continue
                case_str = str(case)
                # Try to extract a case number or reference
                case_number = _extract_case_number(case_str)
                case_label  = case_number or case_str[:50]

                stix_pattern = (
                    f"[threat-actor:name = '{_escape_stix_pattern(name)}']"
                )
                ind = {
                    "type":         "indicator",
                    "spec_version": STIX_SPEC_VERSION,
                    "id":           f"indicator--{uuid.uuid4()}",
                    "created":      now,
                    "modified":     now,
                    "name":         f"Court filing: {case_label}",
                    "description":  (
                        f"{name} is a party to federal court case: {case_str[:300]}."
                    ),
                    "pattern_type": "stix",
                    "pattern":      stix_pattern,
                    "valid_from":   now,
                    "indicator_types": ["malicious-activity"],
                    "labels":          ["federal-court-case"],
                    "confidence":   CONFIDENCE_MAP.get(entity.get("overall_confidence", "high"), 85),
                    "external_references": [{
                        "source_name": "CourtListener",
                        "url":         "https://www.courtlistener.com/",
                        "description": f"Federal court filing: {case_label}",
                    }],
                    "object_marking_refs": [tlp_marking],
                    "x_indicates_ref": threat_actor_stix_id,
                }
                indicators.append(ind)
                indicators.append({
                    "type":                "relationship",
                    "spec_version":        STIX_SPEC_VERSION,
                    "id":                  f"relationship--{uuid.uuid4()}",
                    "created":             now,
                    "modified":            now,
                    "relationship_type":   "indicates",
                    "source_ref":          ind["id"],
                    "target_ref":          threat_actor_stix_id,
                    "description":         f"Court filing indicates involvement of {name}",
                    "object_marking_refs": [tlp_marking],
                })

        return indicators

    # ─────────────────────────────────────────────────────────────────────────
    # Bundle assembly
    # ─────────────────────────────────────────────────────────────────────────

    def _build_bundle(
        self,
        run_id: str,
        city_name: str,
        stix_objects: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """
        Wrap STIX objects in a STIX 2.1 bundle.

        Strips None-valued keys from all objects before bundling (STIX spec
        does not permit null-value properties; omit them instead).
        """
        # Strip None values recursively (top level only — nested structs kept)
        clean_objects = [
            {k: v for k, v in obj.items() if v is not None}
            for obj in stix_objects
        ]

        return {
            "type":         "bundle",
            "id":           f"bundle--{uuid.uuid4()}",
            "spec_version": STIX_SPEC_VERSION,
            "objects":      clean_objects,
            # Custom extension: bundle metadata
            "x_osint_run_id":    run_id,
            "x_osint_city":      city_name,
            "x_osint_exported":  _now_iso(),
            "x_osint_object_count": len(clean_objects),
        }

    def _empty_bundle(self, run_id: str, city_name: str) -> dict[str, Any]:
        """Return an empty but valid STIX 2.1 bundle."""
        return self._build_bundle(
            run_id=run_id,
            city_name=city_name,
            stix_objects=[],
        )


# ─────────────────────────────────────────────────────────────────────────────
# Module helpers
# ─────────────────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    """Return current UTC timestamp in STIX 2.1 format (ms precision)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _build_external_references(entity: dict[str, Any]) -> list[dict[str, str]]:
    """
    Build STIX external_references from entity source_urls and external_ids.
    Each reference is a dict with source_name and url.
    """
    refs: list[dict[str, str]] = []
    seen_urls: set[str] = set()

    # Source URLs → external references
    for url in (entity.get("source_urls") or [])[:5]:
        if url and url not in seen_urls:
            seen_urls.add(url)
            refs.append({
                "source_name": _url_to_source_name(url),
                "url":         url,
            })

    # External IDs (e.g. EIN, Crunchbase ID, FEC ID) → external references
    ext_ids = entity.get("external_ids") or {}
    if isinstance(ext_ids, dict):
        for key, val in ext_ids.items():
            if val:
                refs.append({
                    "source_name":  key.replace("_", " ").title(),
                    "external_id":  str(val),
                    "description":  f"{key}: {val}",
                })

    return refs if refs else []


def _url_to_source_name(url: str) -> str:
    """Extract a human-readable source name from a URL."""
    _domain_map = {
        "crunchbase.com":   "Crunchbase",
        "sec.gov":          "SEC EDGAR",
        "opensecrets.org":  "OpenSecrets",
        "courtlistener.com": "CourtListener",
        "propublica.org":   "ProPublica",
        "usaspending.gov":  "USASpending",
        "opencorporates.com": "OpenCorporates",
        "ofac.treas.gov":   "OFAC",
        "sanctionssearch.ofac.treas.gov": "OFAC SDN",
        "fec.gov":          "FEC",
        "irs.gov":          "IRS",
        "web.archive.org":  "Internet Archive (Wayback Machine)",
    }
    try:
        from urllib.parse import urlparse
        host = urlparse(url).netloc.lstrip("www.")
        for key, name in _domain_map.items():
            if host.endswith(key):
                return name
        return host or "external source"
    except Exception:
        return "external source"


def _escape_stix_pattern(name: str) -> str:
    """
    Escape a name for use in a STIX pattern string.
    Escapes single quotes and backslashes per the STIX pattern spec.
    """
    return name.replace("\\", "\\\\").replace("'", "\\'")


def _extract_case_number(case_str: str) -> str | None:
    """
    Attempt to extract a federal court case number from a case description string.
    Federal case numbers typically look like: 1:21-cr-00001, 2:20-cv-12345, etc.
    Returns None if no case number pattern found.
    """
    # Standard US federal case number pattern
    pattern = r"\d{1,2}:\d{2}-(cr|cv|crim|civ)-\d{4,6}"
    match = re.search(pattern, case_str, re.IGNORECASE)
    if match:
        return match.group(0)
    return None
