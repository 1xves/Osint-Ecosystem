"""
osint/clients/icij.py

ICIJ Offshore Leaks Database client.

The International Consortium of Investigative Journalists maintains the Offshore
Leaks Database — a public graph database of entities, officers, and intermediaries
from leaked documents including:
    - Panama Papers (2016)     — 11.5M documents, Mossack Fonseca
    - Paradise Papers (2017)   — 13.4M documents, Appleby
    - Pandora Papers (2021)    — 11.9M documents, 14 offshore service providers
    - Offshore Leaks (ongoing) — additional structured offshore data

API: https://offshoreleaks.icij.org/api/v1/
Documentation: https://offshoreleaks.icij.org/pages/api
Authentication: No API key required. Public API. Be respectful.

Key endpoints used:
    /api/v1/nodes?q=            — search all entity types
    /api/v1/edges?from=&to=     — relationships between two nodes
    /api/v1/nodes/{id}          — single node detail

Data model:
    Nodes: Entity | Officer | Intermediary | Address | Other
    Edges: registered_address | officer_of | intermediary_of | connected_to

Warning:
    ICIJ data is from leaked documents. The presence of an entity in this
    database does NOT constitute proof of wrongdoing. All matches must be
    written with sensitivity_tier='restricted' and needs_review=True.
    Enrichment agent handles the sensitivity flagging.

Rate limits:
    Undocumented but enforced. Keep to ≤ 20 req/min.
"""

from __future__ import annotations

import logging
from typing import Any

from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

DOMAIN = "icij"
BASE_URL = "https://offshoreleaks.icij.org/api/v1"


class ICIJClient:
    """
    Async client for the ICIJ Offshore Leaks public API.

    All public methods return raw API response dicts.
    The enrichment agent handles sensitivity classification — this client
    is intentionally neutral: it fetches data and returns it unchanged.
    """

    def __init__(self, rate_limiter: RateLimiter) -> None:
        self._rl = rate_limiter

    # ─────────────────────────────────────────────────────────────────────────
    # Node search
    # ─────────────────────────────────────────────────────────────────────────

    async def search(
        self,
        query: str,
        node_type: str | None = None,
        jurisdiction: str | None = None,
        country_codes: list[str] | None = None,
        limit: int = 25,
        offset: int = 0,
    ) -> dict[str, Any]:
        """
        Search the ICIJ Offshore Leaks database for nodes matching a name.

        Args:
            query:          Name or partial name to search.
            node_type:      Filter by node type: 'Entity', 'Officer',
                            'Intermediary', 'Address'. None = all types.
            jurisdiction:   Filter by jurisdiction code (e.g. 'BVI', 'PAN').
            country_codes:  Filter by country codes (e.g. ['US', 'UK']).
            limit:          Max results (default 25).
            offset:         Pagination offset.

        Returns:
            API response with 'nodes' list and 'total' count.
            Each node has: node_id, name, node_type, sourceID, jurisdiction,
            jurisdiction_description, country_codes, incorporation_date,
            inactivation_date, struck_off_date, status, company_type.
        """
        params: dict[str, Any] = {"q": query, "limit": min(limit, 100), "offset": offset}
        if node_type:
            params["node_type"] = node_type
        if jurisdiction:
            params["jurisdiction"] = jurisdiction
        if country_codes:
            params["country_codes"] = ",".join(country_codes)

        return await self._rl.get(DOMAIN, f"{BASE_URL}/nodes", params=params)

    # ─────────────────────────────────────────────────────────────────────────
    # Node detail
    # ─────────────────────────────────────────────────────────────────────────

    async def get_node(self, node_id: str) -> dict[str, Any]:
        """
        Fetch full detail for a single ICIJ node by ID.

        Returns the node's full profile including all linked officers,
        registered addresses, and connected entities.
        """
        return await self._rl.get(DOMAIN, f"{BASE_URL}/nodes/{node_id}")

    # ─────────────────────────────────────────────────────────────────────────
    # Edge traversal
    # ─────────────────────────────────────────────────────────────────────────

    async def get_edges(
        self,
        node_id: str,
        direction: str = "both",
        edge_type: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        """
        Fetch relationships (edges) connected to a specific node.

        Args:
            node_id:    ICIJ node_id to fetch edges for.
            direction:  'from', 'to', or 'both'. 'from' = outbound only.
            edge_type:  Filter edge type: 'officer_of', 'registered_address',
                        'intermediary_of', 'connected_to'. None = all.
            limit:      Max edges to return.

        Returns:
            API response with 'edges' list. Each edge has:
            start_id, end_id, rel_type, sourceID.
        """
        params: dict[str, Any] = {
            "direction": direction,
            "limit": min(limit, 100),
        }
        if edge_type:
            params["rel_type"] = edge_type

        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/nodes/{node_id}/relationships",
            params=params,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Convenience: search + de-risk noise
    # ─────────────────────────────────────────────────────────────────────────

    async def search_officers(
        self,
        name: str,
        country_codes: list[str] | None = None,
        limit: int = 10,
    ) -> dict[str, Any]:
        """
        Convenience wrapper: search specifically for Officers (individuals
        who appear in offshore structures as beneficial owners, signatories, etc).

        Officers are the most relevant node type for individual entity screening.
        """
        return await self.search(
            query=name,
            node_type="Officer",
            country_codes=country_codes,
            limit=limit,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Static helpers
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def extract_nodes(response: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract nodes list from a search response."""
        nodes = response.get("nodes", [])
        if not isinstance(nodes, list):
            return []
        return nodes

    @staticmethod
    def extract_edges(response: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract edges list from a relationship response."""
        edges = response.get("edges", [])
        if not isinstance(edges, list):
            return []
        return edges

    @staticmethod
    def source_label(source_id: str) -> str:
        """
        Convert ICIJ sourceID to a human-readable leak source label.
        Source IDs from the API are like 'panama_papers', 'paradise_papers', etc.
        """
        label_map = {
            "panama_papers":    "Panama Papers (2016)",
            "paradise_papers":  "Paradise Papers (2017)",
            "pandora_papers":   "Pandora Papers (2021)",
            "offshore_leaks":   "ICIJ Offshore Leaks",
        }
        return label_map.get(source_id.lower(), source_id)
