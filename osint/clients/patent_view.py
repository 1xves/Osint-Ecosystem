"""
osint/clients/patent_view.py

USPTO PatentsView API client.

PatentsView is the USPTO's official data dissemination API. It provides
structured, searchable patent data: inventors, assignees, technology
classifications, co-inventors, and citation networks.

For OSINT purposes, patent data reveals:
    - What technology domains a corporate entity is active in
    - Inventor networks (PATENT_CO_INVENTOR relationships)
    - Whether an executive has foundational IP in a domain
    - Company technology strategy (what they protect vs. what they don't)
    - Patent portfolio strength as a proxy for competitive moat

API: https://api.patentsview.org/
Documentation: https://patentsview.org/apis/api-endpoints/patents
Authentication: None required. Free public API. API key optional — not required.

Key endpoints used:
    /patents/query      — search patents by inventor, assignee, CPC class, date
    /inventors/query    — search inventors by name, location
    /assignees/query    — search assignees (companies) by name

Rate limits:
    ~45 req/min. Our config sets 45 req/min.

Notes:
    - PatentsView uses a custom JSON query language (not standard REST params).
    - All queries are POST requests with a JSON body.
    - Fields to request are specified explicitly in the 'f' array.
    - Pagination via 'o' (options) with 'per_page' and 'page'.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

DOMAIN = "patentsview"
BASE_URL = "https://api.patentsview.org"

# Default patent fields to retrieve
PATENT_FIELDS = [
    "patent_id", "patent_title", "patent_type", "patent_date",
    "patent_abstract", "patent_num_claims",
    "assignee_id", "assignee_organization", "assignee_city", "assignee_state",
    "inventor_id", "inventor_first_name", "inventor_last_name",
    "inventor_city", "inventor_state",
    "cpc_section_id", "cpc_subsection_id", "cpc_group_id",
]

# Default inventor fields
INVENTOR_FIELDS = [
    "inventor_id", "inventor_first_name", "inventor_last_name",
    "inventor_city", "inventor_state", "inventor_country",
    "patent_id", "patent_title", "patent_date",
    "assignee_organization",
]

# Default assignee fields
ASSIGNEE_FIELDS = [
    "assignee_id", "assignee_organization", "assignee_type",
    "assignee_city", "assignee_state", "assignee_country",
    "patent_id", "patent_title", "patent_date", "patent_num_claims",
]


class PatentViewClient:
    """
    Async client for USPTO PatentsView API.

    All queries use POST with JSON body — PatentsView's custom query language.
    """

    def __init__(self, rate_limiter: RateLimiter) -> None:
        self._rl = rate_limiter

    # ─────────────────────────────────────────────────────────────────────────
    # Patent search
    # ─────────────────────────────────────────────────────────────────────────

    async def search_by_assignee(
        self,
        company_name: str,
        state: str | None = None,
        limit: int = 25,
        page: int = 1,
    ) -> dict[str, Any]:
        """
        Search patents by assignee (company) name.

        Args:
            company_name:   Partial or full company name.
            state:          Two-letter state code filter (US only).
            limit:          Results per page (default 25, max 100).
            page:           Page number for pagination.

        Returns:
            PatentsView response with 'patents' list, 'count', 'total_patent_count'.
        """
        q: dict[str, Any] = {"_begins": {"assignee_organization": company_name}}
        if state:
            q = {"_and": [q, {"assignee_state": state.upper()}]}

        return await self._post(
            "/patents/query",
            q=q,
            f=PATENT_FIELDS,
            o={"per_page": min(limit, 100), "page": page},
        )

    async def search_by_inventor(
        self,
        first_name: str,
        last_name: str,
        city: str | None = None,
        state: str | None = None,
        limit: int = 25,
        page: int = 1,
    ) -> dict[str, Any]:
        """
        Search patents by inventor name.

        Args:
            first_name:  Inventor first name (can be partial).
            last_name:   Inventor last name.
            city:        City filter.
            state:       Two-letter state code filter.
            limit:       Results per page.
            page:        Page number.

        Returns:
            PatentsView response with 'patents' list.
        """
        q: dict[str, Any] = {
            "_and": [
                {"_begins": {"inventor_first_name": first_name}},
                {"_begins": {"inventor_last_name": last_name}},
            ]
        }
        if city:
            q["_and"].append({"_begins": {"inventor_city": city}})
        if state:
            q["_and"].append({"inventor_state": state.upper()})

        return await self._post(
            "/patents/query",
            q=q,
            f=PATENT_FIELDS,
            o={"per_page": min(limit, 100), "page": page},
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Inventor search (by person)
    # ─────────────────────────────────────────────────────────────────────────

    async def search_inventors(
        self,
        last_name: str,
        first_name: str | None = None,
        city: str | None = None,
        state: str | None = None,
        limit: int = 25,
    ) -> dict[str, Any]:
        """
        Search inventors directly (useful for finding all patents by a person).

        Returns inventor records with patent history included.
        """
        conditions: list[dict[str, Any]] = [
            {"_begins": {"inventor_last_name": last_name}}
        ]
        if first_name:
            conditions.append({"_begins": {"inventor_first_name": first_name}})
        if city:
            conditions.append({"_begins": {"inventor_city": city}})
        if state:
            conditions.append({"inventor_state": state.upper()})

        q = {"_and": conditions} if len(conditions) > 1 else conditions[0]

        return await self._post(
            "/inventors/query",
            q=q,
            f=INVENTOR_FIELDS,
            o={"per_page": min(limit, 100)},
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Assignee search (by company)
    # ─────────────────────────────────────────────────────────────────────────

    async def search_assignees(
        self,
        company_name: str,
        state: str | None = None,
        limit: int = 25,
    ) -> dict[str, Any]:
        """
        Search assignees (organizations holding patents) by name.
        Returns patent portfolio summary per assignee.
        """
        q: dict[str, Any] = {"_begins": {"assignee_organization": company_name}}
        if state:
            q = {"_and": [q, {"assignee_state": state.upper()}]}

        return await self._post(
            "/assignees/query",
            q=q,
            f=ASSIGNEE_FIELDS,
            o={"per_page": min(limit, 100)},
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Internal: POST helper
    # ─────────────────────────────────────────────────────────────────────────

    async def _post(
        self,
        path: str,
        q: dict[str, Any],
        f: list[str],
        o: dict[str, Any] | None = None,
        s: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """
        POST to a PatentsView endpoint.

        Args:
            path:   API path (e.g. '/patents/query').
            q:      Query filter dict (PatentsView JSON query language).
            f:      Fields to return (list of field name strings).
            o:      Options (per_page, page, include_subentity_total_counts).
            s:      Sort order (list of {field: direction} dicts).
        """
        body: dict[str, Any] = {"q": q, "f": f}
        if o:
            body["o"] = o
        if s:
            body["s"] = s

        return await self._rl.post(
            DOMAIN,
            f"{BASE_URL}{path}",
            json_body=body,
            headers={"Content-Type": "application/json"},
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Static helpers
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def extract_patents(response: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract patents list from a /patents/query response."""
        patents = response.get("patents", [])
        if not isinstance(patents, list):
            return []
        return patents

    @staticmethod
    def extract_co_inventors(patents: list[dict[str, Any]], target_inventor_name: str) -> list[str]:
        """
        Given a list of patents and a target inventor name, return all
        co-inventor names found on those patents.

        Args:
            patents:                List of patent dicts from /patents/query.
            target_inventor_name:   Normalized full name of the target inventor.

        Returns:
            Deduplicated list of co-inventor full names (not including target).
        """
        co_inventors: set[str] = set()
        target_norm = target_inventor_name.lower().strip()

        for patent in patents:
            inventors = patent.get("inventors", [])
            if not isinstance(inventors, list):
                continue
            # Check if target is on this patent
            on_patent = any(
                target_norm in f"{inv.get('inventor_first_name', '')} {inv.get('inventor_last_name', '')}".lower()
                for inv in inventors
            )
            if on_patent:
                for inv in inventors:
                    full_name = f"{inv.get('inventor_first_name', '')} {inv.get('inventor_last_name', '')}".strip()
                    if full_name.lower() != target_norm:
                        co_inventors.add(full_name)

        return sorted(co_inventors)

    @staticmethod
    def extract_cpc_domains(patents: list[dict[str, Any]]) -> list[str]:
        """
        Extract unique CPC section codes from a patent list.
        Returns tech domain codes (A-H + Y: A=Human Necessities, G=Physics, H=Electricity, etc.)
        """
        domains: set[str] = set()
        for patent in patents:
            cpcs = patent.get("cpcs", [])
            if isinstance(cpcs, list):
                for cpc in cpcs:
                    section = cpc.get("cpc_section_id")
                    if section:
                        domains.add(section)
        return sorted(domains)
