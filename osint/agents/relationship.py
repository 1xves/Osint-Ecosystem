"""
osint/agents/relationship.py

Relationship Mapping Agent — runs after enrichment, before scoring.

Infers edges between canonical entities using two strategies:

  1. Structural extraction  (no LLM)
     Reads category_fields data directly. High confidence.
     Covers the most important relationship types:
       INVESTED_IN           investor → corporate/nonprofit
       CO_INVESTED_WITH      investor ↔ investor (shared portfolio company)
       PEER_INVESTOR_IN      investor ↔ investor (same funding round)
       FOUNDED               executive_hnw → corporate
       CO_FOUNDED_WITH       executive_hnw ↔ executive_hnw (same company)
       EMPLOYED_BY           executive_hnw → corporate
       SITS_ON_BOARD_OF      executive_hnw/politician → corporate/nonprofit
       DONATED_TO            any → political
       RECEIVED_GRANT_FROM   nonprofit → philanthropic
       FUNDED_BY             nonprofit/corporate → philanthropic
       LITIGATION_AGAINST    any → any (CourtListener cases)
       SUBSIDIARY_OF         corporate → corporate (parent company)

  2. LLM inference  (qwen3:14b, one call per entity)
     Reads entity descriptions, news snippets, and co-mentions.
     Produces MENTIONED_WITH and POLITICALLY_CONNECTED_TO edges.
     Only called for entities with rich description data.
     NOTE: LLM-inferred edges are tagged with claim_type="inferred"
     and get lower relationship_strength scores.

Hard rules:
  - Every relationship MUST have ≥ 1 evidence_id. Edges without evidence
    go to rejected_items with rejection_reason="no_evidence".
  - Self-loops are silently discarded.
  - Duplicate edges (same source, target, type) are silently discarded
    (DB has ON CONFLICT DO NOTHING).
  - sensitive_claim=True for LITIGATION_AGAINST, POLITICALLY_CONNECTED_TO,
    and any relationship involving an illicit entity.

Neo4j:
  Relationships are written to Postgres (relationships table) with neo4j_synced=False.
  A separate sync job (or the graph router) syncs to Neo4j on demand.

Output state fields:
    relationships_draft    list of all inferred relationship dicts (pre-verification)
    relationships_rejected list of relationships that lacked evidence
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Any

from osint.agents.base import BaseAgent

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

AGENT_NAME    = "relationship_agent"
AGENT_VERSION = "1.0"

# Minimum name similarity to accept a name-based match (same threshold as Layer 2 resolution)
NAME_MATCH_MIN = 0.82   # Slightly lower than resolution — relationship inference can tolerate
                         # slightly looser matches since we're looking for signal, not merging

# Source quality for relationship strength computation
SOURCE_QUALITY_MAP = {
    "crunchbase":          "primary",
    "fec_api":             "primary",
    "sec_edgar":           "primary",
    "propublica_nonprofit": "primary",
    "usaspending":         "primary",
    "courtlistener":       "primary",
    "opencorporates":      "secondary",
    "serpapi":             "tertiary",
    "gdelt":               "tertiary",
    "inferred":            "tertiary",
}

# Canonical source URLs for structured data sources (used as evidence source_url)
SOURCE_URL_FALLBACKS = {
    "crunchbase":   "https://www.crunchbase.com",
    "fec_api":      "https://api.open.fec.gov/v1/",
    "sec_edgar":    "https://www.sec.gov/cgi-bin/browse-edgar",
    "propublica":   "https://projects.propublica.org/nonprofits/",
    "usaspending":  "https://api.usaspending.gov/api/v2/",
    "courtlistener": "https://www.courtlistener.com/",
    "opencorporates": "https://opencorporates.com",
}

# Relationship types that require sensitive_claim=True
SENSITIVE_REL_TYPES = {
    "LITIGATION_AGAINST",
    "POLITICALLY_CONNECTED_TO",
    "DONATED_TO",
    "REGULATORY_OVERSIGHT",
}

# Directed vs undirected for each relationship type
RELATIONSHIP_DIRECTION = {
    "INVESTED_IN":              "directed",
    "CO_INVESTED_WITH":         "undirected",
    "PEER_INVESTOR_IN":         "undirected",
    "FOUNDED":                  "directed",
    "CO_FOUNDED_WITH":          "undirected",
    "EMPLOYED_BY":              "directed",
    "SITS_ON_BOARD_OF":         "directed",
    "DONATED_TO":               "directed",
    "RECEIVED_GRANT_FROM":      "directed",
    "FUNDED_BY":                "directed",
    "LITIGATION_AGAINST":       "directed",
    "SUBSIDIARY_OF":            "directed",
    "MENTIONED_WITH":           "undirected",
    "POLITICALLY_CONNECTED_TO": "undirected",
    "ADVISED_BY":               "directed",
    "ALUMNI_OF":                "directed",
    "AWARDED_CONTRACT_TO":      "directed",
    "REGULATORY_OVERSIGHT":     "directed",
}

# LLM system prompt for relationship inference
LLM_SYSTEM_PROMPT = (
    "You are an OSINT analyst. Identify relationships between entities based on "
    "their descriptions and any co-mention signals. "
    "Only infer relationships supported by the text provided. "
    "Respond with valid JSON only."
)

LLM_INFERENCE_PROMPT = """\
Given the following entity profiles, identify any MENTIONED_WITH or POLITICALLY_CONNECTED_TO
relationships between them.

{entity_summaries}

For each relationship found, provide:
{{
  "relationships": [
    {{
      "source_name": "<entity A name>",
      "target_name": "<entity B name>",
      "relationship_type": "MENTIONED_WITH | POLITICALLY_CONNECTED_TO",
      "evidence_snippet": "<one sentence describing the connection>",
      "confidence": "low | medium"
    }}
  ]
}}

If no relationships are evident, return {{"relationships": []}}"""


# ─────────────────────────────────────────────────────────────────────────────
# Relationship strength formula (from spec §4.2)
# ─────────────────────────────────────────────────────────────────────────────

def _relationship_strength(
    evidence_count: int,
    source_quality: str,
    relationship_type: str,
    is_direct: bool,
) -> float:
    base = {"primary": 1.0, "secondary": 0.7, "tertiary": 0.4}.get(source_quality, 0.4)
    if not is_direct:
        base *= 0.5
    # Weak type caps
    if relationship_type == "MENTIONED_WITH":
        base = min(base, 0.4)
    if relationship_type == "POLITICALLY_CONNECTED_TO":
        base = min(base, 0.6)
    multiplier = min(1.0 + (evidence_count - 1) * 0.1, 1.5)
    return round(min(base * multiplier, 1.0), 4)


# ─────────────────────────────────────────────────────────────────────────────
# Name matching helpers
# ─────────────────────────────────────────────────────────────────────────────

_NAME_STRIP = str.maketrans("", "", ".,;:'-&/")

def _norm(name: str) -> str:
    return " ".join(name.lower().translate(_NAME_STRIP).split())


def _find_by_name(
    name_index: dict[str, list[dict[str, Any]]],
    query: str,
    entity_type_filter: str | None = None,
) -> dict[str, Any] | None:
    """
    Find the best-matching entity for a query name.
    Returns None if no match above NAME_MATCH_MIN.
    Prefers exact match > fuzzy match within same entity type.
    """
    query_norm = _norm(query)
    if not query_norm:
        return None

    # Exact match first (fast path)
    exact = name_index.get(query_norm)
    if exact:
        if entity_type_filter:
            typed = [e for e in exact if e.get("entity_type") == entity_type_filter]
            if typed:
                return typed[0]
        return exact[0]

    # Fuzzy scan
    best_score = 0.0
    best_entity = None
    for norm_name, entities in name_index.items():
        score = SequenceMatcher(None, query_norm, norm_name).ratio()
        if score >= NAME_MATCH_MIN and score > best_score:
            if entity_type_filter:
                typed = [e for e in entities if e.get("entity_type") == entity_type_filter]
                if typed:
                    best_score = score
                    best_entity = typed[0]
            else:
                best_score = score
                best_entity = entities[0]

    return best_entity


def _build_name_index(
    entities: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Build normalized_name → [entity, ...] index for fast lookups."""
    index: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for e in entities:
        name = e.get("canonical_name") or e.get("name") or ""
        if name:
            index[_norm(name)].append(e)
        # Also index aliases
        for alias in e.get("aliases", []):
            if alias:
                index[_norm(alias)].append(e)
    return dict(index)


# ─────────────────────────────────────────────────────────────────────────────
# Relationship builder helper
# ─────────────────────────────────────────────────────────────────────────────

class _RelBuilder:
    """
    Collects relationship drafts and helps write them to DB.
    Each _RelBuilder instance is reused across the full relationship extraction pass.
    """

    def __init__(self) -> None:
        self.draft: list[dict[str, Any]] = []
        self.rejected: list[dict[str, Any]] = []
        # Track (source_id, target_id, rel_type) to deduplicate
        self._seen: set[tuple[str, str, str]] = set()

    def add(
        self,
        source_entity: dict[str, Any],
        target_entity: dict[str, Any],
        rel_type: str,
        evidence_snippet: str,
        source_url: str,
        source_quality: str = "secondary",
        confidence: str = "medium",
        run_id: str = "",
        valid_from: str | None = None,
    ) -> None:
        """
        Add a candidate relationship to the draft list.
        Silently drops self-loops and duplicates.
        """
        source_id = source_entity.get("entity_id", "")
        target_id = target_entity.get("entity_id", "")

        if not source_id or not target_id:
            return
        if source_id == target_id:
            return

        # Normalise undirected edge key (smaller id first)
        if RELATIONSHIP_DIRECTION.get(rel_type) == "undirected":
            key = (min(source_id, target_id), max(source_id, target_id), rel_type)
        else:
            key = (source_id, target_id, rel_type)

        if key in self._seen:
            return
        self._seen.add(key)

        # Determine sensitive_claim
        sensitive = (
            rel_type in SENSITIVE_REL_TYPES
            or source_entity.get("entity_type") == "illicit"
            or target_entity.get("entity_type") == "illicit"
        )

        strength = _relationship_strength(
            evidence_count=1,
            source_quality=source_quality,
            relationship_type=rel_type,
            is_direct=source_quality in ("primary", "secondary"),
        )

        self.draft.append({
            "_source_entity": source_entity,   # Temporary — removed before DB write
            "relationship_id": str(uuid.uuid4()),
            "run_id":          run_id,
            "source_entity_id": source_id,
            "target_entity_id": target_id,
            "relationship_type": rel_type,
            "direction":        RELATIONSHIP_DIRECTION.get(rel_type, "directed"),
            "evidence_ids":     [],            # Filled during _write_relationships
            "evidence_snippets": [evidence_snippet[:500]],
            "_evidence_snippet": evidence_snippet[:500],
            "_source_url":      source_url,
            "_source_quality":  source_quality,
            "confidence":       confidence,
            "relationship_strength": strength,
            "sensitive_claim":  sensitive,
            "verified":         False,
            "valid_from":       valid_from,
            "valid_to":         None,
        })


# ─────────────────────────────────────────────────────────────────────────────
# Agent
# ─────────────────────────────────────────────────────────────────────────────

class RelationshipAgent(BaseAgent):
    """
    Infers relationships between canonical entities from enriched category_fields data.
    """

    AGENT_NAME = AGENT_NAME
    AGENT_VERSION = AGENT_VERSION

    async def run(self, state: dict[str, Any]) -> dict[str, Any]:
        run_id  = state["run_id"]
        entities: list[dict[str, Any]] = state.get("enriched_entities") or state.get("canonical_entities", [])

        log.info(
            "relationship_agent: inferring relationships for %d entities", len(entities)
        )

        if not entities:
            return self._empty_patch(state)

        # ── Build name index ──────────────────────────────────────────────────
        name_index = _build_name_index(entities)

        # ── Group entities by type for targeted extraction ────────────────────
        by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for e in entities:
            if e.get("entity_type"):
                by_type[e["entity_type"]].append(e)

        # ── Build relationship drafts ─────────────────────────────────────────
        rb = _RelBuilder()

        self._extract_invested_in(by_type, name_index, rb, run_id)
        self._extract_founded(by_type, name_index, rb, run_id)
        self._extract_employed_by(by_type, name_index, rb, run_id)
        self._extract_sits_on_board_of(by_type, name_index, rb, run_id)
        self._extract_donated_to(by_type, name_index, rb, run_id)
        self._extract_received_grant_from(by_type, name_index, rb, run_id)
        self._extract_litigation_against(by_type, name_index, rb, run_id)
        self._extract_subsidiary_of(by_type, name_index, rb, run_id)
        self._extract_co_investments(rb, run_id)
        self._extract_co_founders(rb, run_id)

        log.info(
            "relationship_agent: structural extraction found %d candidate edges",
            len(rb.draft),
        )

        # ── LLM inference for MENTIONED_WITH / POLITICALLY_CONNECTED_TO ───────
        await self._llm_inference_pass(entities, name_index, rb, run_id)

        # ── Write evidence + relationships to DB ──────────────────────────────
        written, rejected = await self._write_relationships(rb.draft, run_id)

        # Write rejected (no evidence or error) to rejected_items
        for rej in rejected:
            try:
                await self.write_rejected_item(
                    stage="relationship_mapping",
                    item_type="relationship_candidate",
                    item_snapshot=rej,
                    rejection_reason=rej.get("_rejection_reason", "no_evidence"),
                    rejection_detail=rej.get("_rejection_detail"),
                )
            except Exception as exc:
                log.warning("relationship_agent: failed to write rejected edge: %s", exc)

        log.info(
            "relationship_agent: %d edges written, %d rejected",
            len(written), len(rejected),
        )

        # ── Neo4j sync ────────────────────────────────────────────────────────
        # Sync canonical entity nodes and relationship edges to Neo4j
        await self._sync_to_neo4j(entities, written)

        return {
            "relationships_draft":    written,
            "relationships_rejected": rejected,
            "current_phase":          "SCORING",
            **self.agent_status_patch(
                "success", state.get("agent_statuses", {}),
            ),
            **self.token_count_patch(
                state.get("total_tokens_in", 0),
                state.get("total_tokens_out", 0),
                state.get("agent_token_counts", {}),
            ),
            **self.entity_count_patch(state.get("agent_entity_counts", {})),
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Structural extraction methods
    # ─────────────────────────────────────────────────────────────────────────

    def _extract_invested_in(
        self,
        by_type: dict[str, list[dict[str, Any]]],
        name_index: dict[str, list[dict[str, Any]]],
        rb: _RelBuilder,
        run_id: str,
    ) -> None:
        """investor → corporate/nonprofit: from portfolio_companies."""
        for investor in by_type.get("investor", []):
            cat = investor.get("category_fields", {})
            portfolio = cat.get("portfolio_companies") or []
            if not isinstance(portfolio, list):
                continue

            source_urls = investor.get("source_urls", [])
            source_url  = source_urls[0] if source_urls else SOURCE_URL_FALLBACKS["crunchbase"]

            for company_name in portfolio:
                if not company_name:
                    continue
                # Try corporate first, then nonprofit
                target = (
                    _find_by_name(name_index, company_name, "corporate")
                    or _find_by_name(name_index, company_name, "nonprofit")
                    or _find_by_name(name_index, company_name, "philanthropic")
                )
                if target:
                    rb.add(
                        source_entity=investor,
                        target_entity=target,
                        rel_type="INVESTED_IN",
                        evidence_snippet=(
                            f"{investor.get('canonical_name')} invested in "
                            f"{target.get('canonical_name')} (portfolio company)"
                        ),
                        source_url=source_url,
                        source_quality="primary",
                        confidence="high",
                        run_id=run_id,
                    )

    def _extract_founded(
        self,
        by_type: dict[str, list[dict[str, Any]]],
        name_index: dict[str, list[dict[str, Any]]],
        rb: _RelBuilder,
        run_id: str,
    ) -> None:
        """executive_hnw → corporate: from corporate.founder_names."""
        # From corporate entity perspective
        for corp in by_type.get("corporate", []):
            cat = corp.get("category_fields", {})
            founder_names = cat.get("founder_names") or []
            if not isinstance(founder_names, list):
                continue

            source_urls = corp.get("source_urls", [])
            source_url  = source_urls[0] if source_urls else SOURCE_URL_FALLBACKS["crunchbase"]

            for fname in founder_names:
                if not fname:
                    continue
                founder = _find_by_name(name_index, fname, "executive_hnw")
                if founder:
                    rb.add(
                        source_entity=founder,
                        target_entity=corp,
                        rel_type="FOUNDED",
                        evidence_snippet=(
                            f"{founder.get('canonical_name')} co-founded "
                            f"{corp.get('canonical_name')}"
                        ),
                        source_url=source_url,
                        source_quality="primary",
                        confidence="high",
                        run_id=run_id,
                    )

        # From pipeline_agent output: founders list
        for exec_entity in by_type.get("executive_hnw", []):
            cat = exec_entity.get("category_fields", {})
            if cat.get("is_founder") and cat.get("primary_employer"):
                employer_name = cat["primary_employer"]
                corp = _find_by_name(name_index, employer_name, "corporate")
                if corp:
                    source_urls = exec_entity.get("source_urls", [])
                    rb.add(
                        source_entity=exec_entity,
                        target_entity=corp,
                        rel_type="FOUNDED",
                        evidence_snippet=(
                            f"{exec_entity.get('canonical_name')} is founder of "
                            f"{corp.get('canonical_name')}"
                        ),
                        source_url=(source_urls[0] if source_urls else SOURCE_URL_FALLBACKS["crunchbase"]),
                        source_quality="primary",
                        confidence="medium",
                        run_id=run_id,
                    )

    def _extract_employed_by(
        self,
        by_type: dict[str, list[dict[str, Any]]],
        name_index: dict[str, list[dict[str, Any]]],
        rb: _RelBuilder,
        run_id: str,
    ) -> None:
        """executive_hnw → corporate: from current_employer."""
        for exec_entity in by_type.get("executive_hnw", []):
            cat = exec_entity.get("category_fields", {})
            employer = cat.get("current_employer") or cat.get("primary_employer") or cat.get("employer_name")
            if not employer:
                continue

            corp = (
                _find_by_name(name_index, employer, "corporate")
                or _find_by_name(name_index, employer, "investor")
                or _find_by_name(name_index, employer, "nonprofit")
            )
            if not corp:
                continue

            source_urls = exec_entity.get("source_urls", [])
            rb.add(
                source_entity=exec_entity,
                target_entity=corp,
                rel_type="EMPLOYED_BY",
                evidence_snippet=(
                    f"{exec_entity.get('canonical_name')} is employed by "
                    f"{corp.get('canonical_name')} "
                    f"(title: {cat.get('primary_role', 'unknown')})"
                ),
                source_url=(source_urls[0] if source_urls else SOURCE_URL_FALLBACKS["crunchbase"]),
                source_quality="primary" if exec_entity.get("overall_confidence") == "high" else "secondary",
                confidence=exec_entity.get("overall_confidence", "medium"),
                run_id=run_id,
            )

    def _extract_sits_on_board_of(
        self,
        by_type: dict[str, list[dict[str, Any]]],
        name_index: dict[str, list[dict[str, Any]]],
        rb: _RelBuilder,
        run_id: str,
    ) -> None:
        """executive_hnw/politician → corporate/nonprofit: from board_seats."""
        for exec_entity in list(by_type.get("executive_hnw", [])) + list(by_type.get("politician", [])):
            cat = exec_entity.get("category_fields", {})
            board_seats = cat.get("board_seats") or []
            if not isinstance(board_seats, list):
                continue

            source_urls = exec_entity.get("source_urls", [])
            for org_name in board_seats:
                if not org_name:
                    continue
                org = (
                    _find_by_name(name_index, org_name, "corporate")
                    or _find_by_name(name_index, org_name, "nonprofit")
                    or _find_by_name(name_index, org_name, "philanthropic")
                )
                if org:
                    rb.add(
                        source_entity=exec_entity,
                        target_entity=org,
                        rel_type="SITS_ON_BOARD_OF",
                        evidence_snippet=(
                            f"{exec_entity.get('canonical_name')} sits on board of "
                            f"{org.get('canonical_name')}"
                        ),
                        source_url=(source_urls[0] if source_urls else SOURCE_URL_FALLBACKS["propublica"]),
                        source_quality="secondary",
                        confidence="medium",
                        run_id=run_id,
                    )

    def _extract_donated_to(
        self,
        by_type: dict[str, list[dict[str, Any]]],
        name_index: dict[str, list[dict[str, Any]]],
        rb: _RelBuilder,
        run_id: str,
    ) -> None:
        """
        any → political: from FEC contribution data.
        Political entity category_fields may have donor_names.
        """
        for pol in list(by_type.get("political", [])) + list(by_type.get("politician", [])):
            cat = pol.get("category_fields", {})
            donor_names = cat.get("top_donors") or cat.get("donor_names") or []
            if not isinstance(donor_names, list):
                continue

            source_urls = pol.get("source_urls", [])
            source_url  = source_urls[0] if source_urls else SOURCE_URL_FALLBACKS["fec_api"]

            for donor_name in donor_names:
                if not donor_name:
                    continue
                donor = (
                    _find_by_name(name_index, donor_name, "hnwi")
                    or _find_by_name(name_index, donor_name, "investor")
                    or _find_by_name(name_index, donor_name, "corporate")
                    or _find_by_name(name_index, donor_name, "executive_hnw")
                )
                if donor:
                    rb.add(
                        source_entity=donor,
                        target_entity=pol,
                        rel_type="DONATED_TO",
                        evidence_snippet=(
                            f"{donor.get('canonical_name')} made political contributions to "
                            f"{pol.get('canonical_name')} (FEC record)"
                        ),
                        source_url=source_url,
                        source_quality="primary",
                        confidence="high",
                        run_id=run_id,
                    )

    def _extract_received_grant_from(
        self,
        by_type: dict[str, list[dict[str, Any]]],
        name_index: dict[str, list[dict[str, Any]]],
        rb: _RelBuilder,
        run_id: str,
    ) -> None:
        """nonprofit → philanthropic: grant recipient → funder."""
        # From nonprofit's perspective: who funded them
        for npo in by_type.get("nonprofit", []):
            cat = npo.get("category_fields", {})
            funder_names = cat.get("grant_funders") or cat.get("primary_funder") or []
            if isinstance(funder_names, str):
                funder_names = [funder_names]
            if not isinstance(funder_names, list):
                continue

            source_urls = npo.get("source_urls", [])
            source_url  = source_urls[0] if source_urls else SOURCE_URL_FALLBACKS["propublica"]

            for funder_name in funder_names:
                if not funder_name:
                    continue
                funder = (
                    _find_by_name(name_index, funder_name, "philanthropic")
                    or _find_by_name(name_index, funder_name, "corporate")
                )
                if funder:
                    rb.add(
                        source_entity=npo,
                        target_entity=funder,
                        rel_type="RECEIVED_GRANT_FROM",
                        evidence_snippet=(
                            f"{npo.get('canonical_name')} received grant from "
                            f"{funder.get('canonical_name')} (990/ProPublica record)"
                        ),
                        source_url=source_url,
                        source_quality="primary",
                        confidence="high",
                        run_id=run_id,
                    )

        # From philanthropic's perspective: who they funded
        for phil in by_type.get("philanthropic", []):
            cat = phil.get("category_fields", {})
            grantee_names = cat.get("grantee_names") or []
            if not isinstance(grantee_names, list):
                continue

            source_urls = phil.get("source_urls", [])
            source_url  = source_urls[0] if source_urls else SOURCE_URL_FALLBACKS["propublica"]

            for grantee_name in grantee_names:
                if not grantee_name:
                    continue
                grantee = (
                    _find_by_name(name_index, grantee_name, "nonprofit")
                    or _find_by_name(name_index, grantee_name, "corporate")
                )
                if grantee:
                    rb.add(
                        source_entity=grantee,
                        target_entity=phil,
                        rel_type="RECEIVED_GRANT_FROM",
                        evidence_snippet=(
                            f"{grantee.get('canonical_name')} received grant from "
                            f"{phil.get('canonical_name')} (990 Schedule I)"
                        ),
                        source_url=source_url,
                        source_quality="primary",
                        confidence="high",
                        run_id=run_id,
                    )

    def _extract_litigation_against(
        self,
        by_type: dict[str, list[dict[str, Any]]],
        name_index: dict[str, list[dict[str, Any]]],
        rb: _RelBuilder,
        run_id: str,
    ) -> None:
        """
        any → any: from illicit entity court cases.
        Illicit entities have court_cases in category_fields with party names.
        """
        for illicit in by_type.get("illicit", []):
            cat = illicit.get("category_fields", {})
            cases = cat.get("court_cases") or []
            if not isinstance(cases, list):
                continue

            for case in cases:
                if not isinstance(case, dict):
                    continue
                plaintiff = case.get("plaintiff_name") or case.get("plaintiff")
                defendant = case.get("defendant_name") or case.get("defendant")

                if not plaintiff or not defendant:
                    continue

                # Try to match plaintiff and defendant to known entities
                plaintiff_entity = _find_by_name(name_index, plaintiff)
                defendant_entity = _find_by_name(name_index, defendant)

                # Write relationship in both directions if both matched
                source_url = SOURCE_URL_FALLBACKS["courtlistener"]
                case_name  = case.get("case_name") or case.get("case_title", "court case")

                if plaintiff_entity and defendant_entity:
                    rb.add(
                        source_entity=plaintiff_entity,
                        target_entity=defendant_entity,
                        rel_type="LITIGATION_AGAINST",
                        evidence_snippet=(
                            f"{plaintiff_entity.get('canonical_name')} vs "
                            f"{defendant_entity.get('canonical_name')}: {case_name}"
                        ),
                        source_url=source_url,
                        source_quality="primary",
                        confidence="high",
                        run_id=run_id,
                    )
                elif defendant_entity:
                    # Match illicit entity (plaintiff might be US Government)
                    rb.add(
                        source_entity=illicit,
                        target_entity=defendant_entity,
                        rel_type="LITIGATION_AGAINST",
                        evidence_snippet=(
                            f"Federal litigation: {case_name}. "
                            f"{defendant_entity.get('canonical_name')} is defendant."
                        ),
                        source_url=source_url,
                        source_quality="primary",
                        confidence="medium",
                        run_id=run_id,
                    )

    def _extract_subsidiary_of(
        self,
        by_type: dict[str, list[dict[str, Any]]],
        name_index: dict[str, list[dict[str, Any]]],
        rb: _RelBuilder,
        run_id: str,
    ) -> None:
        """corporate → corporate: from parent_company_name."""
        for corp in by_type.get("corporate", []):
            cat = corp.get("category_fields", {})
            parent_name = cat.get("parent_company_name") or cat.get("parent_company")
            if not parent_name:
                continue

            parent = _find_by_name(name_index, parent_name, "corporate")
            if parent:
                source_urls = corp.get("source_urls", [])
                rb.add(
                    source_entity=corp,
                    target_entity=parent,
                    rel_type="SUBSIDIARY_OF",
                    evidence_snippet=(
                        f"{corp.get('canonical_name')} is a subsidiary of "
                        f"{parent.get('canonical_name')}"
                    ),
                    source_url=(source_urls[0] if source_urls else SOURCE_URL_FALLBACKS["opencorporates"]),
                    source_quality="secondary",
                    confidence="medium",
                    run_id=run_id,
                )

    def _extract_co_investments(self, rb: _RelBuilder, run_id: str) -> None:
        """
        Derive CO_INVESTED_WITH from INVESTED_IN edges already in rb.draft.
        Two investors who both INVESTED_IN the same target entity
        have a CO_INVESTED_WITH relationship.
        """
        # Build: target_entity_id → list of source entities (investors)
        target_to_investors: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for edge in rb.draft:
            if edge["relationship_type"] == "INVESTED_IN":
                target_id = edge["target_entity_id"]
                source    = edge.get("_source_entity")
                if source:
                    target_to_investors[target_id].append(source)

        # For each target with 2+ investors, create CO_INVESTED_WITH edges
        for target_id, investors in target_to_investors.items():
            if len(investors) < 2:
                continue
            for i in range(len(investors)):
                for j in range(i + 1, len(investors)):
                    inv_a = investors[i]
                    inv_b = investors[j]
                    # Find a common portfolio company name for the evidence snippet
                    target_name = target_id  # fallback
                    for edge in rb.draft:
                        if (edge["relationship_type"] == "INVESTED_IN"
                                and edge["target_entity_id"] == target_id
                                and edge["source_entity_id"] == inv_a.get("entity_id")):
                            target_name = edge["evidence_snippets"][0] if edge["evidence_snippets"] else target_id
                            break

                    rb.add(
                        source_entity=inv_a,
                        target_entity=inv_b,
                        rel_type="CO_INVESTED_WITH",
                        evidence_snippet=(
                            f"{inv_a.get('canonical_name')} and "
                            f"{inv_b.get('canonical_name')} are co-investors "
                            f"in the same portfolio company (entity_id: {target_id})"
                        ),
                        source_url=SOURCE_URL_FALLBACKS["crunchbase"],
                        source_quality="primary",
                        confidence="high",
                        run_id=run_id,
                    )

    def _extract_co_founders(self, rb: _RelBuilder, run_id: str) -> None:
        """
        Derive CO_FOUNDED_WITH from FOUNDED edges.
        Two executives who both FOUNDED the same company → CO_FOUNDED_WITH.
        """
        company_to_founders: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for edge in rb.draft:
            if edge["relationship_type"] == "FOUNDED":
                company_id = edge["target_entity_id"]
                source     = edge.get("_source_entity")
                if source:
                    company_to_founders[company_id].append(source)

        for company_id, founders in company_to_founders.items():
            if len(founders) < 2:
                continue
            for i in range(len(founders)):
                for j in range(i + 1, len(founders)):
                    f_a = founders[i]
                    f_b = founders[j]
                    rb.add(
                        source_entity=f_a,
                        target_entity=f_b,
                        rel_type="CO_FOUNDED_WITH",
                        evidence_snippet=(
                            f"{f_a.get('canonical_name')} and {f_b.get('canonical_name')} "
                            f"co-founded the same company (entity_id: {company_id})"
                        ),
                        source_url=SOURCE_URL_FALLBACKS["crunchbase"],
                        source_quality="primary",
                        confidence="medium",
                        run_id=run_id,
                    )

    # ─────────────────────────────────────────────────────────────────────────
    # LLM inference pass
    # ─────────────────────────────────────────────────────────────────────────

    async def _llm_inference_pass(
        self,
        entities: list[dict[str, Any]],
        name_index: dict[str, list[dict[str, Any]]],
        rb: _RelBuilder,
        run_id: str,
    ) -> None:
        """
        Use qwen3:14b to infer MENTIONED_WITH and POLITICALLY_CONNECTED_TO edges
        from entity descriptions and news co-mentions.

        Strategy: group community_leader and politician entities (most likely
        to have co-mention signals) into batches of 10, call LLM once per batch.
        """
        # Only run for community_leader and politician entities
        target_entities = [
            e for e in entities
            if e.get("entity_type") in ("community_leader", "politician")
            and (e.get("description") or e.get("category_fields", {}).get("bio"))
        ]

        if not target_entities:
            log.debug("relationship_agent: no LLM inference candidates")
            return

        # Process in batches of 8 (to stay within context window)
        BATCH_SIZE = 8
        for batch_start in range(0, len(target_entities), BATCH_SIZE):
            batch = target_entities[batch_start:batch_start + BATCH_SIZE]
            await self._llm_infer_batch(batch, name_index, rb, run_id)

    async def _llm_infer_batch(
        self,
        batch: list[dict[str, Any]],
        name_index: dict[str, list[dict[str, Any]]],
        rb: _RelBuilder,
        run_id: str,
    ) -> None:
        """Run one LLM inference call for a batch of entities."""
        entity_summaries = []
        for e in batch:
            cat  = e.get("category_fields", {})
            desc = e.get("description") or cat.get("bio") or ""
            entity_summaries.append(
                f"Name: {e.get('canonical_name')}\n"
                f"Type: {e.get('entity_type')}\n"
                f"Description: {desc[:300]}"
            )

        prompt = LLM_INFERENCE_PROMPT.format(
            entity_summaries="\n\n---\n\n".join(entity_summaries)
        )

        try:
            result, _meta = await self.llm_generate_json(
                task_type="relationship_inference",
                prompt=prompt,
                system=LLM_SYSTEM_PROMPT,
            )
        except Exception as exc:
            log.warning("relationship_agent: LLM inference failed: %s", exc)
            return

        if not isinstance(result, dict):
            return

        relationships = result.get("relationships", [])
        if not isinstance(relationships, list):
            return

        for rel in relationships:
            if not isinstance(rel, dict):
                continue
            source_name  = rel.get("source_name", "")
            target_name  = rel.get("target_name", "")
            rel_type     = rel.get("relationship_type", "MENTIONED_WITH")
            snippet      = rel.get("evidence_snippet", "LLM-inferred co-mention")
            confidence   = rel.get("confidence", "low")

            if rel_type not in ("MENTIONED_WITH", "POLITICALLY_CONNECTED_TO"):
                continue
            if not source_name or not target_name:
                continue

            source_entity = _find_by_name(name_index, source_name)
            target_entity = _find_by_name(name_index, target_name)
            if not source_entity or not target_entity:
                continue

            rb.add(
                source_entity=source_entity,
                target_entity=target_entity,
                rel_type=rel_type,
                evidence_snippet=str(snippet)[:500],
                source_url="https://web.archive.org/",    # LLM-inferred; no specific URL
                source_quality="tertiary",
                confidence=confidence if confidence in ("low", "medium", "high") else "low",
                run_id=run_id,
            )

    # ─────────────────────────────────────────────────────────────────────────
    # DB write: evidence + relationships
    # ─────────────────────────────────────────────────────────────────────────

    async def _write_relationships(
        self,
        draft: list[dict[str, Any]],
        run_id: str,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """
        For each draft relationship:
        1. Write an entity_evidence record for the source entity.
        2. Use the returned link_id as evidence_id.
        3. Write the relationship record.

        Returns (written, rejected).
        """
        written:  list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []

        for edge in draft:
            source_entity = edge.pop("_source_entity", {})
            source_url    = edge.pop("_source_url", "")
            evidence_snip = edge.pop("_evidence_snippet", "")
            source_quality = edge.pop("_source_quality", "secondary")

            # Evidence record
            source_entity_id = edge.get("source_entity_id")
            if not source_entity_id or not source_url:
                edge["_rejection_reason"] = "missing_evidence_source"
                rejected.append(edge)
                continue

            try:
                link_id = await self.write_evidence({
                    "link_id":         str(uuid.uuid4()),
                    "entity_id":       source_entity_id,
                    "run_id":          run_id,
                    "supported_field": f"relationship_{edge['relationship_type'].lower()}",
                    "source_url":      source_url,
                    "source_type":     "structured_data",
                    "evidence_snippet": evidence_snip,
                    "claim_type":      "direct_statement",
                    "confidence":      edge.get("confidence", "medium"),
                    "agent_name":      self.AGENT_NAME,
                    "prompt_version":  self.AGENT_VERSION,
                })
                edge["evidence_ids"] = [link_id]
            except Exception as exc:
                log.warning(
                    "relationship_agent: evidence write failed for %s→%s (%s): %s",
                    source_entity_id, edge.get("target_entity_id"),
                    edge.get("relationship_type"), exc,
                )
                edge["_rejection_reason"] = "evidence_write_failed"
                edge["_rejection_detail"] = str(exc)
                rejected.append(edge)
                continue

            # Relationship record
            try:
                relationship_id = await self.write_relationship(edge)
                edge["relationship_id"] = relationship_id
                written.append(edge)
            except Exception as exc:
                log.warning(
                    "relationship_agent: relationship write failed %s→%s (%s): %s",
                    source_entity_id, edge.get("target_entity_id"),
                    edge.get("relationship_type"), exc,
                )
                edge["_rejection_reason"] = "relationship_write_failed"
                edge["_rejection_detail"] = str(exc)
                rejected.append(edge)

        return written, rejected

    # ─────────────────────────────────────────────────────────────────────────
    # Neo4j sync
    # ─────────────────────────────────────────────────────────────────────────

    async def _sync_to_neo4j(
        self,
        entities: list[dict[str, Any]],
        written_edges: list[dict[str, Any]],
    ) -> None:
        """
        Sync canonical entity nodes and written edges to Neo4j.
        Failures are logged but non-fatal — Neo4j is a secondary index.
        """
        # Sync entity nodes
        for entity in entities:
            entity_id = entity.get("entity_id")
            if not entity_id:
                continue
            try:
                await self._neo4j.upsert_node(entity)
            except Exception as exc:
                log.warning(
                    "relationship_agent: Neo4j node upsert failed for '%s': %s",
                    entity.get("canonical_name"), exc,
                )

        # Sync relationship edges
        for edge in written_edges:
            try:
                await self._neo4j.upsert_edge(edge)
            except Exception as exc:
                log.warning(
                    "relationship_agent: Neo4j edge upsert failed %s→%s (%s): %s",
                    edge.get("source_entity_id"), edge.get("target_entity_id"),
                    edge.get("relationship_type"), exc,
                )

        # Mark as synced in Postgres
        synced_ids = [e["relationship_id"] for e in written_edges if e.get("relationship_id")]
        if synced_ids:
            try:
                await self._db.mark_neo4j_synced(synced_ids)
            except Exception as exc:
                log.warning("relationship_agent: mark_neo4j_synced failed: %s", exc)

    # ─────────────────────────────────────────────────────────────────────────
    # Utility
    # ─────────────────────────────────────────────────────────────────────────

    def _empty_patch(self, state: dict[str, Any]) -> dict[str, Any]:
        return {
            "relationships_draft":    [],
            "relationships_rejected": [],
            "current_phase":          "SCORING",
            **self.agent_status_patch("success", state.get("agent_statuses", {})),
            **self.token_count_patch(
                state.get("total_tokens_in", 0),
                state.get("total_tokens_out", 0),
                state.get("agent_token_counts", {}),
            ),
            **self.entity_count_patch(state.get("agent_entity_counts", {})),
        }
