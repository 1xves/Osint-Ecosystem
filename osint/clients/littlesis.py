"""
osint/clients/littlesis.py

LittleSis power network database client.

LittleSis (https://littlesis.org) is a free, publicly-accessible database of
connections between powerful US people and organizations. It is maintained by
Public Accountability Initiative and contains:
  - ~200,000 US entities (people and organizations)
  - ~500,000+ documented relationships with source citations
  - Focus on corporate boards, political donors, lobbying, and financial ties

This client provides the most valuable free data source for:
  - Board memberships (Position relationships)
  - Executive officer history
  - Political donation networks
  - Lobbying relationships
  - Investment and ownership ties
  - Think tank and foundation board interlocks

API: https://api.littlesis.org/
Documentation: https://littlesis.org/api
Authentication:
  - No API key required for public endpoints
  - Rate limit: ~60 req/min (undocumented; be conservative at 30)
  - User-Agent should identify the application

Endpoints used:
  GET /entities/search?q=&num=&page=   — search entities by name
  GET /entities/{id}                    — entity detail with extensions
  GET /entities/{id}/relationships      — paginated relationships for an entity
  GET /relationships/{id}               — single relationship detail
  GET /lists/{id}/entities              — entities in a LittleSis "list" (named group)

Data model:
  Entity: Person or Organization with typed extensions
  Relationship: Typed edge between two entities
    category_id → relationship type (see RELATIONSHIP_CATEGORIES below)
    entity1_id  → subject
    entity2_id  → object
    is_current  → whether relationship is still active
    start_date / end_date → temporal bounds

Response format: JSON:API (https://jsonapi.org/)
  { "data": [...], "meta": {...} }
  Each item has { "id", "type", "attributes", "relationships" }

Rate limits:
  Configured as domain "littlesis" — 30 req/min
"""

from __future__ import annotations

import logging
from typing import Any

from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

BASE_URL = "https://api.littlesis.org"
DOMAIN   = "littlesis"

# User-Agent per LittleSis fair-use guidance
HEADERS: dict[str, str] = {
    "User-Agent": "OSINT-System research@osint-system.local",
    "Accept":     "application/json",
}

# ─────────────────────────────────────────────────────────────────────────────
# LittleSis relationship category IDs → meaningful names
# Source: https://api.littlesis.org/relationships
# ─────────────────────────────────────────────────────────────────────────────
RELATIONSHIP_CATEGORIES: dict[int, str] = {
    1:  "Position",          # Person holds position at organization (board, exec, staff)
    2:  "Education",         # Person educated at institution
    3:  "Membership",        # Person/org is member of organization
    4:  "Family",            # Family relationship
    5:  "Donation",          # Person/org donated to person/org
    6:  "Transaction",       # Financial transaction between entities
    7:  "Lobbying",          # Lobbying firm/lobbyist represents client
    8:  "Social",            # General social/personal connection
    9:  "Professional",      # Professional collaboration/partnership
    10: "Ownership",         # Person/org owns or controls org
    11: "Other",             # Catch-all
    12: "Generic",           # Generic undifferentiated relationship
}

# Categories that map to OSINT relationship types in the pipeline
RELATIONSHIP_TO_PIPELINE_TYPE: dict[int, str] = {
    1:  "board_membership",          # Position → board member or executive
    5:  "donation",                  # Donation → political/charitable donation
    7:  "lobbying",                  # Lobbying → represents client
    10: "business_ownership",        # Ownership → controls
    9:  "professional_collaboration", # Professional → collaboration
    3:  "membership",                # Membership → belongs to
}


class LittleSisClient:
    """
    Async client for the LittleSis US power network API.

    All public methods return raw API response dicts or parsed lists.
    The enrichment agent is responsible for:
      - Entity resolution (matching LittleSis entities to pipeline entities)
      - Writing relationship records to the database
      - Deduplication of edges already in the graph

    Key workflow in enrichment_agent:
      1. For each resolved entity, call search() with canonical_name
      2. If match found with sufficient name similarity, call get_relationships()
      3. For each relationship, determine if both endpoints are in the pipeline
      4. If yes: write relationship record directly (no LLM inference needed)
      5. If one endpoint is missing: create stub entity (stub entity pattern)
    """

    def __init__(self, rate_limiter: RateLimiter) -> None:
        self._rl = rate_limiter

    # ─────────────────────────────────────────────────────────────────────────
    # Entity search
    # ─────────────────────────────────────────────────────────────────────────

    async def search(
        self,
        query: str,
        num: int = 10,
        page: int = 1,
    ) -> dict[str, Any]:
        """
        Search LittleSis for entities matching a name.

        LittleSis search is name-only full-text. It matches aliases,
        former names, and alternate spellings. Best practice: use the
        entity's canonical name and check name similarity on results.

        Args:
            query:  Name to search. Works for both persons and organizations.
            num:    Results per page (max ~25 practical; API accepts higher).
            page:   Page number for pagination.

        Returns:
            JSON:API response:
            {
              "data": [
                {
                  "id": 12345,
                  "type": "entities",
                  "attributes": {
                    "name": "John Smith",
                    "blurb": "CEO of Acme Corp",
                    "website": null,
                    "primary_ext": "Person",    # "Person" or "Org"
                    "updated_at": "2024-01-01T00:00:00Z",
                    "start_date": null,
                    "end_date": null,
                    "aliases": ["John D. Smith"],
                    "types": ["Person", "BusinessPerson"],
                    "links": {"self": "https://littlesis.org/person/12345"}
                  }
                }
              ],
              "meta": {"total": 5, "page": 1, "per_page": 10}
            }
        """
        params: dict[str, Any] = {"q": query, "num": num, "page": page}
        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/entities/search",
            params=params,
            headers=HEADERS,
            timeout=10.0,  # fail fast if API is down; was 30s default
        )

    async def get_entity(self, entity_id: int | str) -> dict[str, Any]:
        """
        Fetch full detail for a single LittleSis entity by ID.

        Returns the entity's full profile including extensions (Person or Org
        specific fields), aliases, and entity types.

        Args:
            entity_id: LittleSis numeric entity ID.

        Returns:
            JSON:API response with single entity in `data` field.
            Person attributes include: gender, birthdate, nationality.
            Org attributes include: org_type, state, employees.
        """
        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/entities/{entity_id}",
            headers=HEADERS,
            timeout=10.0,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Relationship traversal
    # ─────────────────────────────────────────────────────────────────────────

    async def get_relationships(
        self,
        entity_id: int | str,
        category_id: int | None = None,
        num: int = 25,
        page: int = 1,
    ) -> dict[str, Any]:
        """
        Fetch paginated relationships for an entity.

        This is the core method for extracting power network edges from LittleSis.
        Returns all relationships where the entity is either endpoint (entity1 or
        entity2). The `entity1_id` is typically the "subject" and `entity2_id`
        the "object", but this is not enforced by LittleSis.

        Args:
            entity_id:   LittleSis entity ID.
            category_id: Filter by relationship type. None = all types.
                         See RELATIONSHIP_CATEGORIES for values.
                         Most useful: 1 (Position), 5 (Donation), 7 (Lobbying),
                                      10 (Ownership), 9 (Professional)
            num:         Results per page (default 25; max ~100).
            page:        Page number.

        Returns:
            JSON:API response:
            {
              "data": [
                {
                  "id": 67890,
                  "type": "relationships",
                  "attributes": {
                    "entity1_id": 12345,
                    "entity2_id": 54321,
                    "category_id": 1,
                    "category_name": "Position",
                    "description1": "Board Member",    # role of entity1 at entity2
                    "description2": null,
                    "is_current": true,
                    "start_date": "2010-01-01",
                    "end_date": null,
                    "label": "is a Board Member of",
                    "links": {
                      "self": "https://littlesis.org/relationships/67890"
                    }
                  }
                }
              ],
              "meta": {"total": 42, "page": 1, "per_page": 25}
            }
        """
        params: dict[str, Any] = {"num": num, "page": page}
        if category_id is not None:
            params["category_id"] = category_id

        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/entities/{entity_id}/relationships",
            params=params,
            headers=HEADERS,
            timeout=10.0,
        )

    async def get_all_relationships(
        self,
        entity_id: int | str,
        category_id: int | None = None,
        max_pages: int = 4,
        per_page: int = 25,
    ) -> list[dict[str, Any]]:
        """
        Paginate through all relationships for an entity.

        Automatically fetches up to `max_pages` pages and returns the
        combined list of relationship data items. Stops early if a page
        returns fewer items than `per_page` (last page).

        Args:
            entity_id:   LittleSis entity ID.
            category_id: Optional relationship type filter.
            max_pages:   Max pages to fetch (caps at max_pages * per_page items).
                         Default 4 pages × 25 = 100 relationships max.
            per_page:    Items per page.

        Returns:
            Combined list of relationship attribute dicts (not JSON:API wrappers).
            Each dict is the `attributes` from the JSON:API response item,
            with `id` added from the item's `id` field.
        """
        all_relationships: list[dict[str, Any]] = []

        for page_num in range(1, max_pages + 1):
            try:
                response = await self.get_relationships(
                    entity_id=entity_id,
                    category_id=category_id,
                    num=per_page,
                    page=page_num,
                )
            except Exception as e:
                log.warning(
                    "LittleSisClient.get_all_relationships failed on page %d for entity %s: %s",
                    page_num, entity_id, e,
                )
                break

            items = response.get("data", [])
            for item in items:
                attrs = item.get("attributes", {})
                attrs["id"] = item.get("id")   # inject relationship ID into attrs
                all_relationships.append(attrs)

            # Stop if this was the last page
            if len(items) < per_page:
                break

        return all_relationships

    async def get_relationship(self, relationship_id: int | str) -> dict[str, Any]:
        """
        Fetch a single relationship by ID with full sourcing information.

        Relationship detail includes source citations — each relationship in
        LittleSis is documented with at least one source (usually a news article,
        SEC filing, or government document). Use this when writing evidence
        records: the source URL here is far better than "littlesis.org" generically.

        Returns:
            JSON:API response with single relationship in `data` field.
            Attributes include `sources` array — each with `url`, `name`,
            `publication_date`.
        """
        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/relationships/{relationship_id}",
            headers=HEADERS,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Board interlocks — specialized query for the pipeline
    # ─────────────────────────────────────────────────────────────────────────

    async def get_board_memberships(
        self,
        entity_id: int | str,
        current_only: bool = False,
    ) -> list[dict[str, Any]]:
        """
        Convenience wrapper: fetch all Position (category 1) relationships.

        Position relationships include board memberships, executive positions,
        staff roles, and advisory positions. These are the most valuable
        relationship type for the OSINT pipeline — they directly populate
        board_membership and employment edges.

        Args:
            entity_id:    LittleSis entity ID.
            current_only: If True, filter to relationships where is_current=True.

        Returns:
            List of relationship attribute dicts with `id` field added.
            Each dict includes: entity1_id, entity2_id, description1 (role title),
            is_current, start_date, end_date, label.
        """
        relationships = await self.get_all_relationships(
            entity_id=entity_id,
            category_id=1,     # Position
            max_pages=4,
        )

        if current_only:
            relationships = [r for r in relationships if r.get("is_current")]

        return relationships

    async def get_donations(
        self,
        entity_id: int | str,
    ) -> list[dict[str, Any]]:
        """
        Convenience wrapper: fetch Donation (category 5) relationships.

        Returns donation relationships where this entity is either donor
        or recipient. Most commonly used to find political donation networks
        for high-net-worth individuals and corporate PACs.
        """
        return await self.get_all_relationships(
            entity_id=entity_id,
            category_id=5,
            max_pages=4,
        )

    async def get_lobbying_relationships(
        self,
        entity_id: int | str,
    ) -> list[dict[str, Any]]:
        """
        Convenience wrapper: fetch Lobbying (category 7) relationships.

        Returns lobbying relationships — either the firm/lobbyist doing the
        lobbying (entity1) or the client being represented (entity2).
        """
        return await self.get_all_relationships(
            entity_id=entity_id,
            category_id=7,
            max_pages=4,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Static helpers — parse JSON:API responses
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def extract_entities(response: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Extract entity list from a search response.

        Returns a flattened list of entity dicts with `id` field injected
        from the JSON:API item id. The raw `attributes` are spread into the
        dict. Each dict has:
            id, name, blurb, website, primary_ext, updated_at,
            start_date, end_date, aliases, types, littlesis_url
        """
        entities: list[dict[str, Any]] = []
        for item in response.get("data", []):
            attrs = dict(item.get("attributes", {}))
            attrs["id"] = item.get("id")
            # Extract canonical LittleSis URL from links
            links = attrs.pop("links", {})
            attrs["littlesis_url"] = links.get("self", f"https://littlesis.org/entities/{attrs['id']}")
            entities.append(attrs)
        return entities

    @staticmethod
    def extract_relationships(response: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Extract relationship list from a get_relationships() response.

        Returns flattened list with `id` injected. Each dict has:
            id, entity1_id, entity2_id, category_id, category_name,
            description1, description2, is_current, start_date, end_date,
            label, littlesis_url
        """
        relationships: list[dict[str, Any]] = []
        for item in response.get("data", []):
            attrs = dict(item.get("attributes", {}))
            attrs["id"] = item.get("id")
            links = attrs.pop("links", {})
            attrs["littlesis_url"] = links.get("self", f"https://littlesis.org/relationships/{attrs['id']}")
            relationships.append(attrs)
        return relationships

    @staticmethod
    def get_pipeline_relationship_type(category_id: int) -> str | None:
        """
        Map a LittleSis category_id to the pipeline's relationship_type enum value.

        Returns None for categories that don't map cleanly to pipeline types.
        Callers should use the LittleSis category_name as fallback.
        """
        return RELATIONSHIP_TO_PIPELINE_TYPE.get(category_id)

    @staticmethod
    def format_entity_name(entity_attrs: dict[str, Any]) -> str | None:
        """Extract the canonical name from an entity attributes dict."""
        return entity_attrs.get("name")

    @staticmethod
    def is_person(entity_attrs: dict[str, Any]) -> bool:
        """True if this LittleSis entity is a Person (vs. Organization)."""
        return entity_attrs.get("primary_ext", "").lower() == "person"

    @staticmethod
    def is_org(entity_attrs: dict[str, Any]) -> bool:
        """True if this LittleSis entity is an Organization (vs. Person)."""
        return entity_attrs.get("primary_ext", "").lower() in ("org", "organization")

    @staticmethod
    def get_relationship_description(rel_attrs: dict[str, Any]) -> str | None:
        """
        Build a human-readable description from a relationship's attributes.
        Prefers description1 (subject's role), falls back to label.
        """
        desc = rel_attrs.get("description1")
        if desc:
            return desc
        return rel_attrs.get("label")

    @staticmethod
    def relationship_is_current(rel_attrs: dict[str, Any]) -> bool:
        """
        Determine if a relationship is currently active.

        LittleSis uses is_current=True for active relationships and
        is_current=False for historical. None means unknown — treat as
        potentially current for pipeline purposes (avoids discarding valid data).
        """
        is_current = rel_attrs.get("is_current")
        if is_current is None:
            return True   # Unknown → assume current
        return bool(is_current)
