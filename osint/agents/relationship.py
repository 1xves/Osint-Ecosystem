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
from osint.clients.littlesis import LittleSisClient, RELATIONSHIP_TO_PIPELINE_TYPE

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
    "sec_form_d":          "primary",    # Regulatory filing — mandatory disclosure
    "propublica_nonprofit": "primary",
    "usaspending":         "primary",
    "courtlistener":       "primary",
    "followthemoney":      "primary",    # State campaign finance filings — mandatory disclosure
    "patent_view":         "primary",    # USPTO official patent database
    "littlesis":           "secondary",  # Curated human-edited DB — not primary source
    "opencorporates":      "secondary",  # Aggregated corporate registry — secondary source
    "serpapi":             "tertiary",
    "gdelt":               "tertiary",
    "eventbrite":          "tertiary",
    "meetup":              "tertiary",
    "bizapedia":           "secondary",  # Aggregated SoS data — secondary source
    "sos_pa":              "primary",    # PA SoS filing — official state record
    "sos_de":              "primary",    # DE SoS filing — official state record
    "wayback":             "tertiary",   # Archived web page — historical, may be stale
    "inferred":            "tertiary",
    # Phase 8 — ETL bulk data
    "hud":                 "primary",    # HUD FHA-insured loan portfolio — official government data
    "fincen_ctr":          "primary",    # FinCEN CTR aggregate data — official government data
    # Phase 9 — ICIJ Offshore Leaks
    "icij_leaks":          "secondary",  # Leaked documents — high signal, not officially verified
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
    # Phase 8
    "hud":          "https://www.hud.gov/program_offices/housing/mfh/exp/mfhdiscl",
    "fincen_ctr":   "https://www.fincen.gov/financial-crimes-enforcement-network",
    # Phase 9
    "icij_leaks":   "https://offshoreleaks.icij.org",
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
    # Phase 6 — web scrapers
    "FORMERLY_EMPLOYED_BY":     "directed",   # Historical; from Wayback archived pages
    # Phase 8 — ETL bulk data
    "OWNS":                     "directed",   # Entity owns real estate (HUD insured properties)
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
# Relationship strength formula v2 (Phase 11.2 — multi-factor confidence scoring)
# ─────────────────────────────────────────────────────────────────────────────

def _relationship_strength_v2(
    evidence_count: int,
    source_quality: str,
    relationship_type: str,
    is_direct: bool,
    recency_days: int | None = None,
    is_inferred: bool = False,
) -> tuple[float, float]:
    """
    Multi-factor relationship strength + confidence score computation.

    Returns:
        (relationship_strength, confidence_score)
        Both floats in [0.0, 1.0].

    Factors:
        1. Source quality tier:    primary=1.0, secondary=0.7, tertiary=0.4
        2. Directness penalty:     indirect edges halved
        3. Relationship type caps: MENTIONED_WITH ≤ 0.4, POLITICALLY_CONNECTED_TO ≤ 0.6
        4. Evidence count boost:   +10% per additional source, capped at 1.5×
        5. Recency decay:          evidence > 5 years old → 0.85×, > 10 years → 0.70×
        6. Inference penalty:      inferred (not directly observed) → 0.60×, floor at 0.35

    Separation of strength vs. confidence:
        - relationship_strength: overall combined score for ranking/display
        - confidence_score: float for DB storage + downstream ML features.
          The confidence_score applies the inference penalty; relationship_strength
          applies the evidence_count multiplier. They diverge for inferred edges
          with multiple corroborating sources.
    """
    _QUALITY_BASE = {"primary": 1.0, "secondary": 0.7, "tertiary": 0.4}
    base = _QUALITY_BASE.get(source_quality, 0.4)

    if not is_direct:
        base *= 0.5

    # Type-level caps (weak relationship types are capped regardless of source quality)
    if relationship_type == "MENTIONED_WITH":
        base = min(base, 0.4)
    elif relationship_type == "POLITICALLY_CONNECTED_TO":
        base = min(base, 0.6)
    elif relationship_type == "WORKS_UNDER":
        # Always inferred; cap at 0.65 even with strong base
        base = min(base, 0.65)

    # Evidence count multiplier — corroboration from multiple sources increases confidence
    multiplier = min(1.0 + (evidence_count - 1) * 0.1, 1.5)

    # Recency factor — older evidence is less reliable
    if recency_days is not None:
        if recency_days > 3650:     # > 10 years
            base *= 0.70
        elif recency_days > 1825:   # > 5 years
            base *= 0.85

    # Inference penalty — inferred chains are inherently less certain
    # Floor prevents inferred edges from reaching implausibly low scores
    if is_inferred:
        base = max(base * 0.60, 0.35)

    strength       = round(min(base * multiplier, 1.0), 4)
    confidence_score = round(min(base, 1.0), 4)   # score before multiplier (source-level certainty)

    return strength, confidence_score


def _relationship_strength(
    evidence_count: int,
    source_quality: str,
    relationship_type: str,
    is_direct: bool,
) -> float:
    """
    Legacy wrapper — preserved for any remaining call sites.
    Phase 11 migrated all internal callers to _relationship_strength_v2().
    """
    strength, _ = _relationship_strength_v2(
        evidence_count=evidence_count,
        source_quality=source_quality,
        relationship_type=relationship_type,
        is_direct=is_direct,
    )
    return strength


# ─────────────────────────────────────────────────────────────────────────────
# Name matching helpers
# ─────────────────────────────────────────────────────────────────────────────

_NAME_STRIP = str.maketrans("", "", ".,;:'-&/")

def _norm(name: str) -> str:
    return " ".join(name.lower().translate(_NAME_STRIP).split())


def _icij_source_label(source_id: str) -> str:
    """Convert an ICIJ sourceID to a human-readable label for evidence snippets."""
    _labels = {
        "panama_papers":   "Panama Papers 2016",
        "paradise_papers": "Paradise Papers 2017",
        "pandora_papers":  "Pandora Papers 2021",
        "offshore_leaks":  "ICIJ Offshore Leaks",
    }
    return _labels.get((source_id or "").lower(), source_id or "ICIJ")


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
        recency_days: int | None = None,
        is_inferred: bool = False,
        sensitive_claim: bool | None = None,
    ) -> None:
        """
        Add a candidate relationship to the draft list.
        Silently drops self-loops and duplicates.

        Args:
            recency_days:   Age of the evidence in days (for recency decay scoring).
                            None means no decay is applied.
            is_inferred:    True for 2-hop inferred edges (WORKS_UNDER). These
                            receive a confidence penalty and are labeled "inferred".
            sensitive_claim: Override for sensitive_claim flag. If None, inferred
                            from rel_type and entity types.
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
        if sensitive_claim is None:
            sensitive_claim = (
                rel_type in SENSITIVE_REL_TYPES
                or source_entity.get("entity_type") == "illicit"
                or target_entity.get("entity_type") == "illicit"
            )

        # Phase 11.2 — multi-factor confidence scoring
        strength, confidence_score = _relationship_strength_v2(
            evidence_count=1,
            source_quality=source_quality,
            relationship_type=rel_type,
            is_direct=source_quality in ("primary", "secondary"),
            recency_days=recency_days,
            is_inferred=is_inferred,
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
            "confidence_score": confidence_score,  # Phase 11.2 — float for DB
            "relationship_strength": strength,
            "sensitive_claim":  sensitive_claim,
            "verified":         False,
            "is_inferred":      is_inferred,
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
        # LittleSis pre-built relationship edges — stored in enrichment phase
        self._extract_littlesis_relationships(entities, name_index, rb, run_id)
        # FollowTheMoney donation edges
        self._extract_ftm_relationships(by_type, name_index, rb, run_id)
        # OpenCorporates officer edges
        self._extract_opencorporates_relationships(by_type, name_index, rb, run_id)
        # Bizapedia officer / registered-agent edges
        self._extract_bizapedia_relationships(by_type, name_index, rb, run_id)
        # Wayback Machine historical executive edges
        self._extract_wayback_relationships(by_type, name_index, rb, run_id)
        # CourtListener litigation edges (Phase 7) — all entity types
        self._extract_courtlistener_litigation(entities, name_index, rb, run_id)
        # EDGAR document extraction officer/director edges (Phase 7)
        self._extract_edgar_doc_relationships(by_type, name_index, rb, run_id)
        # HUD multifamily property OWNS edges (Phase 8 — ETL bulk data)
        self._extract_hud_relationships(by_type, name_index, rb, run_id)
        # ICIJ offshore entity links (Phase 9 — Neo4j subgraph)
        self._extract_icij_relationships(by_type, name_index, rb, run_id)

        log.info(
            "relationship_agent: structural extraction found %d candidate edges",
            len(rb.draft),
        )

        # ── LLM inference for MENTIONED_WITH / POLITICALLY_CONNECTED_TO ───────
        await self._llm_inference_pass(entities, name_index, rb, run_id)

        # ── Phase 11.3 — 2-hop WORKS_UNDER inference ─────────────────────────
        self._infer_works_under(entities, rb, run_id)

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

        # From executive_hnw entities whose subtype indicates founder.
        # Field name mapping:
        #   - Old pipeline_agent path: is_founder (bool) + primary_employer
        #   - executive_hnw.py (Crunchbase): executive_subtype in ("founder","serial_founder") + current_company
        #   - Form D stubs: no explicit founder flag (use is_executive flag instead)
        _FOUNDER_SUBTYPES = {"founder", "serial_founder"}
        for exec_entity in by_type.get("executive_hnw", []):
            cat = exec_entity.get("category_fields", {})

            # Path 1: legacy is_founder bool field
            if cat.get("is_founder"):
                employer_name = cat.get("primary_employer") or cat.get("current_employer") or cat.get("current_company")
            # Path 2: Crunchbase executive_subtype
            elif cat.get("executive_subtype") in _FOUNDER_SUBTYPES:
                employer_name = cat.get("current_company") or cat.get("current_employer") or cat.get("primary_employer")
            else:
                employer_name = None

            if not employer_name:
                continue

            corp = (
                _find_by_name(name_index, employer_name, "corporate")
                or _find_by_name(name_index, employer_name, "investor")
            )
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
        """executive_hnw → corporate: from current_employer or current_company.

        Field name mapping:
          - Form D stubs (corporate.py)  → category_fields["current_employer"]
          - Crunchbase people (executive_hnw.py) → category_fields["current_company"]
          - SerpAPI fallback (executive_hnw.py)  → category_fields["current_company"]
        We check both so either path produces EMPLOYED_BY edges.
        """
        for exec_entity in by_type.get("executive_hnw", []):
            cat = exec_entity.get("category_fields", {})
            # Check all known field names — collection agents use different keys
            employer = (
                cat.get("current_employer")
                or cat.get("primary_employer")
                or cat.get("employer_name")
                or cat.get("current_company")   # executive_hnw.py (Crunchbase + Proxycurl)
            )
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
            title = (
                cat.get("primary_role")
                or cat.get("current_title")   # executive_hnw.py uses current_title
                or "unknown"
            )
            rb.add(
                source_entity=exec_entity,
                target_entity=corp,
                rel_type="EMPLOYED_BY",
                evidence_snippet=(
                    f"{exec_entity.get('canonical_name')} is employed by "
                    f"{corp.get('canonical_name')} "
                    f"(title: {title})"
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
        """executive_hnw/politician → corporate/nonprofit: from board_seats.

        Two paths:
          1. Explicit board_seats list (Form D stubs + enrichment sources)
          2. executive_subtype == "board_member" + current_company (Crunchbase bulk search)
             — Crunchbase people search infers "board_member" subtype when title contains
               "board member / board director / chairman". The primary_organization
               is stored as current_company. Use it to derive a SITS_ON_BOARD_OF edge.
        """
        _BOARD_SUBTYPES = {"board_member"}

        for exec_entity in list(by_type.get("executive_hnw", [])) + list(by_type.get("politician", [])):
            cat = exec_entity.get("category_fields", {})
            source_urls = exec_entity.get("source_urls", [])

            # Path 1: explicit board_seats list (Form D stubs, enrichment)
            board_seats = cat.get("board_seats") or []
            if not isinstance(board_seats, list):
                board_seats = []
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

            # Path 2: executive_subtype == "board_member" + current_company (Crunchbase bulk)
            if cat.get("executive_subtype") in _BOARD_SUBTYPES:
                org_name = cat.get("current_company") or cat.get("current_employer")
                if org_name:
                    org = (
                        _find_by_name(name_index, org_name, "corporate")
                        or _find_by_name(name_index, org_name, "nonprofit")
                        or _find_by_name(name_index, org_name, "philanthropic")
                    )
                    if org:
                        title = cat.get("current_title", "board member")
                        rb.add(
                            source_entity=exec_entity,
                            target_entity=org,
                            rel_type="SITS_ON_BOARD_OF",
                            evidence_snippet=(
                                f"{exec_entity.get('canonical_name')} serves as {title} "
                                f"of {org.get('canonical_name')} (Crunchbase)"
                            ),
                            source_url=(source_urls[0] if source_urls else SOURCE_URL_FALLBACKS["crunchbase"]),
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

    def _extract_ftm_relationships(
        self,
        by_type: dict[str, list[dict[str, Any]]],
        name_index: dict[str, list[dict[str, Any]]],
        rb: _RelBuilder,
        run_id: str,
    ) -> None:
        """
        Create DONATED_TO edges from FollowTheMoney data stored by enrichment_agent.

        Two data shapes:
          1. Politician/political entities have category_fields["ftm_contributions"] —
             a list of donors who gave to this politician. Each donor is resolved
             to a pipeline entity and a DONATED_TO edge is created.

          2. Executive_hnw/hnwi entities have category_fields["ftm_donation_targets"] —
             a list of political entities they donated to. Each target is resolved
             and a DONATED_TO edge is created (donor → target direction).

        Both shapes produce the same DONATED_TO edge type, just from different
        perspectives.
        """
        ftm_url = "https://www.followthemoney.org"
        edges_added = 0

        # Shape 1: politician/political received donations from named donors
        for pol in list(by_type.get("politician", [])) + list(by_type.get("political", [])):
            cat = pol.get("category_fields", {})
            contributions = cat.get("ftm_contributions")
            if not contributions or not isinstance(contributions, list):
                continue

            for contrib in contributions:
                donor_name = contrib.get("donor_name", "")
                if not donor_name:
                    continue
                amount = contrib.get("amount", 0)
                year   = contrib.get("year", "")

                donor = (
                    _find_by_name(name_index, donor_name, "executive_hnw")
                    or _find_by_name(name_index, donor_name, "hnwi")
                    or _find_by_name(name_index, donor_name, "corporate")
                    or _find_by_name(name_index, donor_name, "investor")
                )
                if not donor:
                    continue

                snippet = (
                    f"{donor.get('canonical_name')} donated ${amount:,} to "
                    f"{pol.get('canonical_name')} (FollowTheMoney{', ' + str(year) if year else ''})"
                )
                rb.add(
                    source_entity=donor,
                    target_entity=pol,
                    rel_type="DONATED_TO",
                    evidence_snippet=snippet[:500],
                    source_url=ftm_url,
                    source_quality="primary",
                    confidence="high",
                    run_id=run_id,
                )
                edges_added += 1

        # Shape 2: executive_hnw/hnwi made donations to named political targets
        for exec_entity in list(by_type.get("executive_hnw", [])) + list(by_type.get("hnwi", [])):
            cat = exec_entity.get("category_fields", {})
            targets = cat.get("ftm_donation_targets")
            if not targets or not isinstance(targets, list):
                continue

            for t in targets:
                target_name = t.get("target_name", "")
                if not target_name:
                    continue
                amount = t.get("amount", 0)
                year   = t.get("year", "")

                pol = (
                    _find_by_name(name_index, target_name, "politician")
                    or _find_by_name(name_index, target_name, "political")
                )
                if not pol:
                    continue

                snippet = (
                    f"{exec_entity.get('canonical_name')} donated ${amount:,} to "
                    f"{pol.get('canonical_name')} (FollowTheMoney{', ' + str(year) if year else ''})"
                )
                rb.add(
                    source_entity=exec_entity,
                    target_entity=pol,
                    rel_type="DONATED_TO",
                    evidence_snippet=snippet[:500],
                    source_url=ftm_url,
                    source_quality="primary",
                    confidence="high",
                    run_id=run_id,
                )
                edges_added += 1

        if edges_added:
            log.info(
                "relationship_agent: FollowTheMoney extraction added %d DONATED_TO edges",
                edges_added,
            )

    def _extract_opencorporates_relationships(
        self,
        by_type: dict[str, list[dict[str, Any]]],
        name_index: dict[str, list[dict[str, Any]]],
        rb: _RelBuilder,
        run_id: str,
    ) -> None:
        """
        Create EMPLOYED_BY edges from OpenCorporates officer data stored by enrichment_agent.

        enrichment_agent._run_opencorporates_enrichment() stores officer lists in
        category_fields["opencorporates_officers"] for corporate entities. Each officer
        entry has {name, position, start_date}.

        This method resolves officer names to pipeline entities and creates EMPLOYED_BY
        or SITS_ON_BOARD_OF edges based on the position string.

        Only creates edges when the officer name matches a known pipeline entity —
        we don't create stub entities here (that's done via the Form D stub pattern).
        """
        oc_url = "https://opencorporates.com"
        edges_added = 0

        for corp in by_type.get("corporate", []):
            cat = corp.get("category_fields", {})
            officers = cat.get("opencorporates_officers")
            if not officers or not isinstance(officers, list):
                continue

            corp_name = corp.get("canonical_name", "")
            oc_data = cat.get("opencorporates_data", {})
            company_url = oc_data.get("opencorporates_url", oc_url)

            for officer in officers:
                officer_name = officer.get("name", "")
                position     = officer.get("position", "").lower()
                start_date   = officer.get("start_date", "")

                if not officer_name:
                    continue

                person = (
                    _find_by_name(name_index, officer_name, "executive_hnw")
                    or _find_by_name(name_index, officer_name, "hnwi")
                    or _find_by_name(name_index, officer_name, "investor")
                )
                if not person:
                    continue

                # Determine relationship type from position string
                board_keywords = ("director", "trustee", "board", "governor", "chairman", "chair")
                exec_keywords  = ("ceo", "cfo", "coo", "president", "officer", "executive", "managing")

                if any(kw in position for kw in board_keywords):
                    rel_type = "SITS_ON_BOARD_OF"
                elif any(kw in position for kw in exec_keywords):
                    rel_type = "EMPLOYED_BY"
                else:
                    # Registered agents, generic officers → EMPLOYED_BY
                    rel_type = "EMPLOYED_BY"

                snippet = (
                    f"{person.get('canonical_name')} is {officer.get('position', 'officer')} "
                    f"of {corp_name} (OpenCorporates{', since ' + start_date if start_date else ''})"
                )
                rb.add(
                    source_entity=person,
                    target_entity=corp,
                    rel_type=rel_type,
                    evidence_snippet=snippet[:500],
                    source_url=company_url,
                    source_quality="secondary",
                    confidence="medium",
                    run_id=run_id,
                    valid_from=start_date or None,
                )
                edges_added += 1

        if edges_added:
            log.info(
                "relationship_agent: OpenCorporates extraction added %d officer edges",
                edges_added,
            )

    def _extract_bizapedia_relationships(
        self,
        by_type: dict[str, list[dict[str, Any]]],
        name_index: dict[str, list[dict[str, Any]]],
        rb: _RelBuilder,
        run_id: str,
    ) -> None:
        """
        Create EMPLOYED_BY edges from Bizapedia officer data stored by enrichment_agent.

        enrichment_agent._run_bizapedia_enrichment() stores:
            category_fields["bizapedia_data"]["officers"] = [{name, title}]

        Resolves officer names to pipeline entities and creates edges.
        Also creates IS_REGISTERED_AGENT_OF edges if the registered agent
        matches a known entity (useful for shell company network analysis).
        """
        edges_added = 0

        for corp in by_type.get("corporate", []) + by_type.get("investor", []):
            cat      = corp.get("category_fields", {})
            biz_data = cat.get("bizapedia_data")
            if not biz_data or not isinstance(biz_data, dict):
                continue

            corp_name  = corp.get("canonical_name", "")
            source_url = biz_data.get("source_url", "https://www.bizapedia.com")

            # ── Officer edges ──────────────────────────────────────────────────
            for officer in biz_data.get("officers", []):
                officer_name = officer.get("name", "")
                title        = officer.get("title", "")

                if not officer_name:
                    continue

                person = (
                    _find_by_name(name_index, officer_name, "executive_hnw")
                    or _find_by_name(name_index, officer_name, "hnwi")
                    or _find_by_name(name_index, officer_name, "investor")
                )
                if not person:
                    continue

                title_lower = title.lower()
                board_kws   = ("director", "trustee", "board", "governor", "chairman", "chair")
                if any(kw in title_lower for kw in board_kws):
                    rel_type = "SITS_ON_BOARD_OF"
                else:
                    rel_type = "EMPLOYED_BY"

                snippet = (
                    f"{person.get('canonical_name')} is {title or 'officer'} "
                    f"of {corp_name} (Bizapedia)"
                )
                rb.add(
                    source_entity=person,
                    target_entity=corp,
                    rel_type=rel_type,
                    evidence_snippet=snippet[:500],
                    source_url=source_url,
                    source_quality="secondary",
                    confidence="medium",
                    run_id=run_id,
                )
                edges_added += 1

        if edges_added:
            log.info(
                "relationship_agent: Bizapedia extraction added %d officer edges",
                edges_added,
            )

    def _extract_wayback_relationships(
        self,
        by_type: dict[str, list[dict[str, Any]]],
        name_index: dict[str, list[dict[str, Any]]],
        rb: _RelBuilder,
        run_id: str,
    ) -> None:
        """
        Create FORMERLY_EMPLOYED_BY edges from Wayback Machine historical executive
        data stored by enrichment_agent.

        enrichment_agent._run_wayback_enrichment() stores:
            category_fields["wayback_executives"] = [
                {name, title, snapshot_url, snapshot_date, source}
            ]

        Uses lower confidence than current employment edges — archived pages may
        be outdated and the person may no longer be with the company.
        """
        edges_added = 0

        for corp in by_type.get("corporate", []) + by_type.get("investor", []):
            cat              = corp.get("category_fields", {})
            wayback_execs    = cat.get("wayback_executives")
            if not wayback_execs or not isinstance(wayback_execs, list):
                continue

            corp_name = corp.get("canonical_name", "")

            for exec_entry in wayback_execs:
                exec_name    = exec_entry.get("name", "")
                title        = exec_entry.get("title", "")
                snapshot_url = exec_entry.get("snapshot_url", "")
                snap_date    = exec_entry.get("snapshot_date", "")

                if not exec_name:
                    continue

                person = (
                    _find_by_name(name_index, exec_name, "executive_hnw")
                    or _find_by_name(name_index, exec_name, "hnwi")
                    or _find_by_name(name_index, exec_name, "investor")
                )
                if not person:
                    continue

                date_note = f" as of {snap_date}" if snap_date else ""
                snippet   = (
                    f"{person.get('canonical_name')} was {title or 'executive'} "
                    f"of {corp_name}{date_note} (Wayback Machine archive)"
                )
                rb.add(
                    source_entity=person,
                    target_entity=corp,
                    rel_type="FORMERLY_EMPLOYED_BY",
                    evidence_snippet=snippet[:500],
                    source_url=snapshot_url or "https://web.archive.org",
                    source_quality="tertiary",
                    confidence="low",       # Historical — may be stale
                    run_id=run_id,
                    valid_from=snap_date or None,
                )
                edges_added += 1

        if edges_added:
            log.info(
                "relationship_agent: Wayback extraction added %d historical exec edges",
                edges_added,
            )

    def _extract_courtlistener_litigation(
        self,
        entities: list[dict[str, Any]],
        name_index: dict[str, list[dict[str, Any]]],
        rb: _RelBuilder,
        run_id: str,
    ) -> None:
        """
        Create LITIGATION_AGAINST edges from CourtListener data stored by enrichment_agent.

        enrichment_agent._run_litigation_enrichment() stores:
            category_fields["litigation"] = [
                {case_name, case_number, court, filing_date, case_type,
                 plaintiffs, defendants, outcome, monetary_judgment, summary, docket_url}
            ]

        Runs across ALL entity types (corporate, investor, executive_hnw, illicit) —
        more comprehensive than the existing _extract_litigation_against which only
        reads from illicit entities.

        Strategy:
          - If the case has a structured plaintiffs/defendants list, try to resolve each
            to a known pipeline entity and create directed LITIGATION_AGAINST edges.
          - If only the searched entity name is available (no party resolution possible),
            create a self-referential sentinel edge noting the entity was a party.
          - monetary_judgment is stored in edge metadata for financial risk scoring.
        """
        source_url = SOURCE_URL_FALLBACKS["courtlistener"]
        edges_added = 0

        for entity in entities:
            cat        = entity.get("category_fields", {})
            litigation = cat.get("litigation")
            if not litigation or not isinstance(litigation, list):
                continue

            entity_name = entity.get("canonical_name", "")

            for case in litigation:
                if not isinstance(case, dict):
                    continue

                case_name       = case.get("case_name") or case.get("name", "Unknown case")
                court           = case.get("court", "")
                filing_date     = case.get("filing_date") or case.get("date_filed")
                outcome         = case.get("outcome")
                monetary_value  = case.get("monetary_judgment")
                summary         = case.get("summary", "")
                docket_url      = case.get("docket_url", source_url)

                plaintiffs  = case.get("plaintiffs", []) or []
                defendants  = case.get("defendants", []) or []

                # Try to resolve listed parties to pipeline entities
                resolved_plaintiffs  = [e for p in plaintiffs for e in [_find_by_name(name_index, p)] if e]
                resolved_defendants  = [e for d in defendants for e in [_find_by_name(name_index, d)] if e]

                outcome_note = f" (outcome: {outcome})" if outcome else ""
                money_note   = f" (${monetary_value:,})" if monetary_value else ""
                snippet_base = (
                    f"{case_name} — {court}{outcome_note}{money_note}. "
                    f"{summary[:200]}"
                ).strip()

                if resolved_plaintiffs and resolved_defendants:
                    # Full resolution: write P→D edges
                    for p_ent in resolved_plaintiffs:
                        for d_ent in resolved_defendants:
                            rb.add(
                                source_entity=p_ent,
                                target_entity=d_ent,
                                rel_type="LITIGATION_AGAINST",
                                evidence_snippet=snippet_base[:500],
                                source_url=docket_url,
                                source_quality="primary",
                                confidence="high",
                                run_id=run_id,
                                valid_from=filing_date,
                            )
                            edges_added += 1
                else:
                    # Partial resolution: entity is a party, create edge to itself as note
                    # or to any resolved counterparty
                    counterparties = resolved_defendants or resolved_plaintiffs
                    for counter in counterparties:
                        if counter.get("entity_id") == entity.get("entity_id"):
                            continue
                        rb.add(
                            source_entity=entity,
                            target_entity=counter,
                            rel_type="LITIGATION_AGAINST",
                            evidence_snippet=snippet_base[:500],
                            source_url=docket_url,
                            source_quality="primary",
                            confidence="medium",
                            run_id=run_id,
                            valid_from=filing_date,
                        )
                        edges_added += 1

        if edges_added:
            log.info(
                "relationship_agent: CourtListener litigation added %d edges",
                edges_added,
            )

    def _extract_edgar_doc_relationships(
        self,
        by_type: dict[str, list[dict[str, Any]]],
        name_index: dict[str, list[dict[str, Any]]],
        rb: _RelBuilder,
        run_id: str,
    ) -> None:
        """
        Create EMPLOYED_BY and SITS_ON_BOARD_OF edges from EDGAR document extraction
        data stored by enrichment_agent._run_edgar_doc_enrichment().

        Reads three fields:
          category_fields["exec_compensation"]     — from DEF 14A proxy statements
          category_fields["annual_report_officers"] — from 10-K officer section
          category_fields["annual_report_directors"] — from 10-K director section

        Name resolution: tries executive_hnw first, then hnwi, then investor.
        Only creates edges for names that resolve to known pipeline entities.
        """
        source_url = SOURCE_URL_FALLBACKS["sec_edgar"]
        edges_added = 0

        all_corps = by_type.get("corporate", []) + by_type.get("investor", [])

        for corp in all_corps:
            cat       = corp.get("category_fields", {})
            corp_name = corp.get("canonical_name", "")

            # ── DEF 14A executive compensation ────────────────────────────────
            for exec_rec in cat.get("exec_compensation", []):
                if not isinstance(exec_rec, dict):
                    continue
                exec_name = exec_rec.get("name", "")
                title     = exec_rec.get("title", "")
                total_comp = exec_rec.get("total_compensation")
                year      = exec_rec.get("fiscal_year")

                if not exec_name:
                    continue

                person = (
                    _find_by_name(name_index, exec_name, "executive_hnw")
                    or _find_by_name(name_index, exec_name, "hnwi")
                )
                if not person:
                    continue

                comp_note = f" (total comp: ${total_comp:,})" if total_comp else ""
                year_note = f" in FY{year}" if year else ""
                snippet   = (
                    f"{person.get('canonical_name')} served as {title or 'executive'} "
                    f"of {corp_name}{year_note}{comp_note} (DEF 14A proxy statement)"
                )
                rb.add(
                    source_entity=person,
                    target_entity=corp,
                    rel_type="EMPLOYED_BY",
                    evidence_snippet=snippet[:500],
                    source_url=source_url,
                    source_quality="primary",
                    confidence="high",
                    run_id=run_id,
                )
                edges_added += 1

            # ── 10-K annual report officers ───────────────────────────────────
            for officer in cat.get("annual_report_officers", []):
                if not isinstance(officer, dict):
                    continue
                officer_name = officer.get("name", "")
                title        = officer.get("title", "")
                tenure_start = officer.get("tenure_start_year")

                if not officer_name:
                    continue

                person = (
                    _find_by_name(name_index, officer_name, "executive_hnw")
                    or _find_by_name(name_index, officer_name, "hnwi")
                )
                if not person:
                    continue

                snippet = (
                    f"{person.get('canonical_name')} is {title or 'officer'} "
                    f"of {corp_name}"
                    + (f" since {tenure_start}" if tenure_start else "")
                    + " (SEC 10-K annual report)"
                )
                rb.add(
                    source_entity=person,
                    target_entity=corp,
                    rel_type="EMPLOYED_BY",
                    evidence_snippet=snippet[:500],
                    source_url=source_url,
                    source_quality="primary",
                    confidence="high",
                    run_id=run_id,
                )
                edges_added += 1

            # ── 10-K annual report directors ──────────────────────────────────
            for director in cat.get("annual_report_directors", []):
                if not isinstance(director, dict):
                    continue
                dir_name     = director.get("name", "")
                title        = director.get("title", "")
                independence = director.get("independence", "")

                if not dir_name:
                    continue

                person = (
                    _find_by_name(name_index, dir_name, "executive_hnw")
                    or _find_by_name(name_index, dir_name, "hnwi")
                    or _find_by_name(name_index, dir_name, "investor")
                )
                if not person:
                    continue

                indep_note = f" ({independence})" if independence else ""
                snippet    = (
                    f"{person.get('canonical_name')} serves as {title or 'director'}"
                    f"{indep_note} of {corp_name} (SEC 10-K annual report)"
                )
                rb.add(
                    source_entity=person,
                    target_entity=corp,
                    rel_type="SITS_ON_BOARD_OF",
                    evidence_snippet=snippet[:500],
                    source_url=source_url,
                    source_quality="primary",
                    confidence="high",
                    run_id=run_id,
                )
                edges_added += 1

        if edges_added:
            log.info(
                "relationship_agent: EDGAR document extraction added %d officer/director edges",
                edges_added,
            )

    # ─────────────────────────────────────────────────────────────────────────
    # HUD Multifamily Property OWNS edges (Phase 8)
    # ─────────────────────────────────────────────────────────────────────────

    def _extract_hud_relationships(
        self,
        by_type: dict[str, list[dict[str, Any]]],
        name_index: dict[str, list[dict[str, Any]]],
        rb: _RelBuilder,
        run_id: str,
    ) -> None:
        """
        Create OWNS edges from HUD multifamily property data stored by
        enrichment_agent._run_hud_enrichment().

        enrichment_agent stores:
            category_fields["hud_properties"] = [
                {property_name, owner_name, city, state, zip_code,
                 loan_amount, loan_status, program_type, units, maturity_date}
            ]

        For each property, we create:
            entity → OWNS → synthetic property node (if no match in pipeline)
                OR
            entity → OWNS → matched real_estate entity (if name resolves)

        Property nodes that don't match existing pipeline entities are created
        as synthetic MENTIONED_WITH targets — this is intentional, since the
        pipeline may not have seeded all properties as entities.

        Synthetic property evidence snippet includes: property name, city,
        state, loan amount (if available), HUD program type.
        """
        source_url = SOURCE_URL_FALLBACKS["hud"]
        edges_added = 0

        # Eligible entity types for HUD ownership
        eligible = by_type.get("corporate", []) + by_type.get("real_estate", []) + by_type.get("investor", [])

        for entity in eligible:
            cat        = entity.get("category_fields", {})
            properties = cat.get("hud_properties")
            if not properties or not isinstance(properties, list):
                continue

            for prop in properties:
                if not isinstance(prop, dict):
                    continue

                prop_name  = prop.get("property_name", "")
                city       = prop.get("city", "")
                state      = prop.get("state", "")
                loan_amt   = prop.get("loan_amount")
                program    = prop.get("program_type", "")
                status     = prop.get("loan_status", "")
                units      = prop.get("units")

                if not prop_name:
                    continue

                # Try to resolve property to an existing pipeline entity
                target = _find_by_name(name_index, prop_name, "real_estate")

                if target is None:
                    # Property not in pipeline — create a synthetic minimal entity
                    # to represent it as a target of the OWNS edge.
                    # Use the property's ZIP as the entity_id seed for uniqueness.
                    zip_code = prop.get("zip_code", "")
                    prop_id  = f"hud_prop_{prop_name[:40].lower().replace(' ', '_')}_{zip_code}"
                    target = {
                        "entity_id":     prop_id,
                        "canonical_name": prop_name,
                        "entity_type":   "real_estate",
                        "primary_city":  city,
                        "primary_state": state,
                    }

                # Build evidence snippet
                loc_parts = [p for p in [city, state] if p]
                location  = ", ".join(loc_parts)
                loan_str  = f" (FHA loan: ${loan_amt:,.0f})" if loan_amt else ""
                prog_str  = f" [{program}]" if program else ""
                units_str = f", {units} units" if units else ""
                snippet   = (
                    f"{entity.get('canonical_name')} owns {prop_name}"
                    f"{(' in ' + location) if location else ''}"
                    f"{units_str}{loan_str}{prog_str} — HUD FHA-insured multifamily property"
                )

                rb.add(
                    source_entity=entity,
                    target_entity=target,
                    rel_type="OWNS",
                    evidence_snippet=snippet[:500],
                    source_url=source_url,
                    source_quality="primary",
                    confidence="high",
                    run_id=run_id,
                )
                edges_added += 1

        if edges_added:
            log.info(
                "relationship_agent: HUD property data added %d OWNS edges",
                edges_added,
            )

    def _extract_icij_relationships(
        self,
        by_type: dict[str, list[dict[str, Any]]],
        name_index: dict[str, list[dict[str, Any]]],
        rb: _RelBuilder,
        run_id: str,
    ) -> None:
        """
        Emit OFFSHORE_ENTITY_LINKED_TO edges between pipeline entities that
        share ICIJ node IDs in their offshore shell chains.

        enrichment_agent stores:
            category_fields["icij_nodes"]       — direct ICIJ matches for this entity
            category_fields["icij_shell_chain"] — connected ICIJ nodes (up to depth 4)
            category_fields["offshore_flag"]    — True if any match found

        Logic:
            1. Build an index: icij_id → [pipeline_entities that reference it]
               (includes both direct matches and shell chain nodes)
            2. For each icij_id referenced by 2+ pipeline entities, emit
               OFFSHORE_ENTITY_LINKED_TO between each pair.

        Edge confidence: "medium" — ICIJ data is from leaked documents, not
        officially verified. Evidence snippet includes the shared ICIJ node name
        and the source leak (Panama Papers, etc.).

        Sensitive: all ICIJ-sourced edges carry sensitive_claim=True.
        """
        source_url = SOURCE_URL_FALLBACKS.get("icij_leaks", "https://offshoreleaks.icij.org")
        edges_added = 0

        # Eligible entity types for ICIJ screening
        eligible_types = {"corporate", "investor", "illicit"}
        eligible: list[dict[str, Any]] = []
        for et in eligible_types:
            eligible.extend(by_type.get(et, []))

        if len(eligible) < 2:
            return  # Need at least 2 entities to form a connection

        # ── Build index: icij_id → [{entity, icij_node_info}] ───────────────
        icij_to_entities: dict[str, list[dict[str, Any]]] = {}

        for entity in eligible:
            cat = entity.get("category_fields", {})
            if not cat.get("offshore_flag"):
                continue

            # Collect all ICIJ node IDs referenced by this entity
            referenced_ids: dict[str, dict] = {}

            # Direct matches
            for match in cat.get("icij_nodes", []):
                icij_id = match.get("icij_id", "")
                if icij_id:
                    referenced_ids[icij_id] = match

            # Shell chain nodes (indirect connections)
            for chain_node in cat.get("icij_shell_chain", []):
                icij_id = chain_node.get("icij_id", "")
                if icij_id and icij_id not in referenced_ids:
                    referenced_ids[icij_id] = chain_node

            for icij_id, icij_info in referenced_ids.items():
                if icij_id not in icij_to_entities:
                    icij_to_entities[icij_id] = []
                icij_to_entities[icij_id].append({
                    "entity":    entity,
                    "icij_info": icij_info,
                })

        # ── Emit OFFSHORE_ENTITY_LINKED_TO for each shared ICIJ node ────────
        emitted_pairs: set[frozenset] = set()

        for icij_id, refs in icij_to_entities.items():
            if len(refs) < 2:
                continue

            for i in range(len(refs)):
                for j in range(i + 1, len(refs)):
                    ent_a    = refs[i]["entity"]
                    ent_b    = refs[j]["entity"]
                    info     = refs[i]["icij_info"]  # use first ref's ICIJ info for snippet

                    pair = frozenset([
                        ent_a.get("entity_id", ""),
                        ent_b.get("entity_id", ""),
                    ])
                    if pair in emitted_pairs:
                        continue
                    emitted_pairs.add(pair)

                    icij_name    = info.get("name", icij_id)
                    source_label = _icij_source_label(info.get("source_dataset", ""))
                    snippet = (
                        f"{ent_a.get('canonical_name')} and "
                        f"{ent_b.get('canonical_name')} are linked via "
                        f"ICIJ offshore entity '{icij_name}' ({source_label})"
                    )

                    rb.add(
                        source_entity=ent_a,
                        target_entity=ent_b,
                        rel_type="OFFSHORE_ENTITY_LINKED_TO",
                        evidence_snippet=snippet[:500],
                        source_url=source_url,
                        source_quality="secondary",
                        confidence="medium",
                        sensitive_claim=True,
                        run_id=run_id,
                    )
                    edges_added += 1

        if edges_added:
            log.info(
                "relationship_agent: ICIJ data added %d OFFSHORE_ENTITY_LINKED_TO edges",
                edges_added,
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
    # LittleSis pre-built relationship edges
    # ─────────────────────────────────────────────────────────────────────────

    def _extract_littlesis_relationships(
        self,
        entities: list[dict[str, Any]],
        name_index: dict[str, list[dict[str, Any]]],
        rb: _RelBuilder,
        run_id: str,
    ) -> None:
        """
        Create relationship edges from LittleSis relationships stored by enrichment_agent.

        enrichment_agent._run_littlesis_enrichment() stores a list of raw LittleSis
        relationship attribute dicts in entity["category_fields"]["littlesis_relationships"].
        Each dict has: entity1_id, entity2_id, category_id, description1,
        is_current, start_date, end_date, label, littlesis_url.

        We need to resolve both endpoints to pipeline entities. Since LittleSis
        uses its own numeric IDs, we resolve via name matching:
          1. Build a map from littlesis_id → pipeline entity for all enriched entities.
          2. For each relationship, look up both endpoints by littlesis_id.
          3. If both endpoints are in the pipeline: add edge to _RelBuilder.
          4. If one endpoint is missing: skip for now (stub entity pattern comes later).

        Only handles relationship categories that map to known pipeline types
        (Position, Donation, Lobbying, Ownership, Professional). Social and
        Family relationships are skipped — too noisy without additional verification.
        """
        # ── Build littlesis_id → pipeline entity map ──────────────────────────
        ls_id_to_entity: dict[str | int, dict[str, Any]] = {}
        for entity in entities:
            cat = entity.get("category_fields", {})
            ls_id = cat.get("littlesis_id")
            if ls_id is not None:
                ls_id_to_entity[ls_id] = entity

        if not ls_id_to_entity:
            return  # No LittleSis-enriched entities — nothing to do

        # ── Process each entity's LittleSis relationships ─────────────────────
        edges_added = 0
        for entity in entities:
            cat = entity.get("category_fields", {})
            ls_rels = cat.get("littlesis_relationships")
            if not ls_rels or not isinstance(ls_rels, list):
                continue

            for rel in ls_rels:
                category_id = rel.get("category_id")
                if category_id is None:
                    continue

                # Skip categories that don't map to pipeline relationship types
                pipeline_type = LittleSisClient.get_pipeline_relationship_type(category_id)
                if not pipeline_type:
                    continue

                entity1_id = rel.get("entity1_id")
                entity2_id = rel.get("entity2_id")
                if not entity1_id or not entity2_id:
                    continue

                # Resolve both endpoints to pipeline entities
                source_entity = ls_id_to_entity.get(entity1_id)
                target_entity = ls_id_to_entity.get(entity2_id)

                if not source_entity or not target_entity:
                    # At least one endpoint not in pipeline — skip for now
                    # The stub entity pattern will handle this in a future iteration
                    continue

                # Map LittleSis pipeline type to the OSINT relationship enum
                rel_type_map = {
                    "board_membership":          "SITS_ON_BOARD_OF",
                    "donation":                  "DONATED_TO",
                    "lobbying":                  "LOBBYING_FOR",
                    "business_ownership":        "SUBSIDIARY_OF",
                    "professional_collaboration": "MENTIONED_WITH",
                    "membership":                "MENTIONED_WITH",
                }
                osint_type = rel_type_map.get(pipeline_type, "MENTIONED_WITH")

                # Skip undirected weak type for Position — use SITS_ON_BOARD_OF
                if category_id == 1:   # Position
                    role_desc = rel.get("description1") or rel.get("label", "")
                    if role_desc and any(t in role_desc.lower() for t in ("board", "director", "trustee", "governor")):
                        osint_type = "SITS_ON_BOARD_OF"
                    elif role_desc and any(t in role_desc.lower() for t in ("ceo", "cfo", "president", "executive", "officer")):
                        osint_type = "EMPLOYED_BY"
                    else:
                        osint_type = "SITS_ON_BOARD_OF"  # Default Position → board seat

                ls_url = rel.get("littlesis_url", "https://littlesis.org")
                description1 = LittleSisClient.get_relationship_description(rel) or ""
                is_current   = LittleSisClient.relationship_is_current(rel)
                start_date   = rel.get("start_date")

                evidence = (
                    f"LittleSis: {source_entity.get('canonical_name')} "
                    f"{description1 or 'is related to'} "
                    f"{target_entity.get('canonical_name')}"
                )
                if not is_current:
                    evidence += " (historical)"

                rb.add(
                    source_entity=source_entity,
                    target_entity=target_entity,
                    rel_type=osint_type,
                    evidence_snippet=evidence[:500],
                    source_url=ls_url,
                    source_quality="secondary",
                    confidence="medium" if is_current else "low",
                    run_id=run_id,
                    valid_from=start_date,
                )
                edges_added += 1

        log.info(
            "relationship_agent: LittleSis extraction added %d candidate edges "
            "(%d entities had LittleSis data)",
            edges_added, len(ls_id_to_entity),
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Phase 11.3 — 2-hop WORKS_UNDER inference
    # ─────────────────────────────────────────────────────────────────────────

    def _infer_works_under(
        self,
        entities: list[dict[str, Any]],
        rb: _RelBuilder,
        run_id: str,
    ) -> None:
        """
        Infer WORKS_UNDER(person → officer/director) edges via 2-hop chain:

            Person A --EMPLOYED_BY--> Company X --SITS_ON_BOARD_OF (reverse)--> Person B

        Semantics: if A works for X, and B sits on X's board, then A works under B's oversight.

        This is a structural inference — it is always labeled is_inferred=True and receives
        a confidence penalty from _relationship_strength_v2() with a minimum floor of 0.35.
        The inference is only emitted when BOTH:
          1. An EMPLOYED_BY edge A→X is in the current draft
          2. A SITS_ON_BOARD_OF edge B→X is also in the draft

        Why operating on rb.draft (not the DB):
            Draft edges are drawn from enriched_entities in the same run. Using the draft
            avoids cross-run pollution and is consistent with how board_interlock edges work.

        Limitations:
            - Only sees edges built in this run — does not query historical edges from DB.
            - Does not infer WORKS_UNDER for FORMERLY_EMPLOYED_BY (historical employment).
            - Person-to-person only: both source and target must have entity_type in
              PERSON_TYPES to avoid inferring officer-of-officer chains through corporate
              holding structures.
        """
        _PERSON_TYPES = {
            "person", "executive_hnw", "hnwi",
            "politician", "community_leader",
        }

        # Build entity_id → entity dict for fast lookup
        entity_map: dict[str, dict[str, Any]] = {
            e.get("entity_id", ""): e
            for e in entities
            if e.get("entity_id")
        }

        # Build two indices from the current draft edges
        # employed_by[employee_id] = set of company_ids they EMPLOYED_BY
        # board_members[company_id] = set of person_ids that SITS_ON_BOARD_OF that company
        employed_by:  dict[str, set[str]] = {}
        board_members: dict[str, set[str]] = {}

        for edge in rb.draft:
            rel_type   = edge.get("relationship_type", "")
            source_id  = edge.get("source_entity_id", "")
            target_id  = edge.get("target_entity_id", "")

            if rel_type == "EMPLOYED_BY":
                employed_by.setdefault(source_id, set()).add(target_id)
            elif rel_type == "SITS_ON_BOARD_OF":
                board_members.setdefault(target_id, set()).add(source_id)

        if not employed_by or not board_members:
            return

        inferred_count = 0
        for employee_id, company_ids in employed_by.items():
            employee = entity_map.get(employee_id)
            if not employee:
                continue
            if employee.get("entity_type", "") not in _PERSON_TYPES:
                continue

            for company_id in company_ids:
                officers = board_members.get(company_id, set())
                company  = entity_map.get(company_id)
                company_name = (
                    (company.get("canonical_name") or company.get("name") or "")
                    if company else ""
                )

                for officer_id in officers:
                    if officer_id == employee_id:
                        continue
                    officer = entity_map.get(officer_id)
                    if not officer:
                        continue
                    if officer.get("entity_type", "") not in _PERSON_TYPES:
                        continue

                    snippet = (
                        f"{employee.get('canonical_name', employee_id)} is employed by "
                        f"{company_name or company_id}, where "
                        f"{officer.get('canonical_name', officer_id)} sits on the board "
                        f"(inferred WORKS_UNDER chain via EMPLOYED_BY + SITS_ON_BOARD_OF)"
                    )

                    rb.add(
                        source_entity=employee,
                        target_entity=officer,
                        rel_type="WORKS_UNDER",
                        evidence_snippet=snippet,
                        source_url="",            # no external URL — inferred chain
                        source_quality="tertiary",
                        confidence="low",         # inferred — always low string tier
                        run_id=run_id,
                        is_inferred=True,
                        sensitive_claim=False,
                    )
                    inferred_count += 1

        if inferred_count:
            log.info(
                "relationship_agent: _infer_works_under emitted %d WORKS_UNDER edges",
                inferred_count,
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
        # Run LLM inference for all entity types that have a canonical name.
        # Previously restricted to community_leader/politician + bio — too narrow:
        # most entities lack description/bio in category_fields after collection,
        # causing LLM inference to skip entirely and produce 0 relationships.
        target_entities = [
            e for e in entities
            if e.get("canonical_name")
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
            is_inferred = edge.get("is_inferred", False)

            # Inferred edges (WORKS_UNDER chains) have no external URL.
            # Use internal sentinel URL so the evidence record is still written.
            if is_inferred and not source_url:
                source_url = "internal://inferred"

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
                    "source_type":     "internal_database" if is_inferred else "structured_data",
                    "evidence_snippet": evidence_snip,
                    "claim_type":      "inferred" if is_inferred else "direct_statement",
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
