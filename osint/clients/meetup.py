"""
osint/clients/meetup.py

Meetup.com API client (GraphQL v3).

Meetup is the primary source for tech group organizers — the people running
weekly/monthly startup meetups, dev groups, and entrepreneurship circles.
Group organizers are strong community_leader candidates: sustained commitment,
grassroots networks, proven convening power.

API: https://api.meetup.com/gql (GraphQL)
Documentation: https://www.meetup.com/api/guide/
Authentication: OAuth 2.0 access token. Required.
                Get a private key from: https://secure.meetup.com/meetup_api/key/

Key operations used:
    keywordSearch       — search groups and events by keyword + location
    groupByUrlname      — get full group detail including organizer
    upcomingEvents      — events near a location with group info

Rate limits:
    ~30 req/min on standard access.

Notes:
    - Meetup's REST API was deprecated. All queries use GraphQL.
    - Group organizers have a 'organizer' field: name, id, photo.
    - The API uses 'urlname' (slug) as the primary group identifier.
    - Group 'category' field: categories 34=tech, 2=career&business are most relevant.
"""

from __future__ import annotations

import logging
from typing import Any

from osint.core.config import settings
from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

DOMAIN = "meetup"
GRAPHQL_URL = "https://api.meetup.com/gql"

# GraphQL queries
_KEYWORD_SEARCH_QUERY = """
query KeywordSearch($query: String!, $lat: Float!, $lon: Float!, $radius: Int!, $first: Int!) {
  keywordSearch(
    input: { first: $first }
    filter: {
      query: $query
      lat: $lat
      lon: $lon
      radius: $radius
    }
  ) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id
        result {
          ... on Group {
            id
            name
            urlname
            description
            city
            state
            country
            lat
            lon
            membersCount
            topicsCount
            organizer {
              id
              name
              bio
              memberUrl
            }
            groupPhoto { baseUrl }
            upcomingEvents { totalCount }
          }
        }
      }
    }
  }
}
"""

_GROUP_DETAIL_QUERY = """
query GroupDetail($urlname: String!) {
  groupByUrlname(urlname: $urlname) {
    id
    name
    urlname
    description
    city
    state
    country
    lat
    lon
    membersCount
    foundedDate
    organizer {
      id
      name
      bio
      memberUrl
      photo { baseUrl }
    }
    pastEvents(input: { first: 5 }) {
      edges {
        node { id title dateTime attendeeCount venue { city } }
      }
    }
    upcomingEvents { totalCount }
  }
}
"""

_UPCOMING_EVENTS_QUERY = """
query UpcomingEvents($lat: Float!, $lon: Float!, $radius: Int!, $first: Int!) {
  upcomingEvents(
    filter: { lat: $lat, lon: $lon, radius: $radius }
    input: { first: $first }
  ) {
    pageInfo { hasNextPage }
    edges {
      node {
        id
        title
        dateTime
        going
        group {
          id
          name
          urlname
          membersCount
          organizer { id name bio memberUrl }
        }
      }
    }
  }
}
"""


class MeetupClient:
    """
    Async GraphQL client for Meetup.com API v3.

    All methods POST to the GraphQL endpoint and return the 'data' dict from
    the response. Callers parse the nested GraphQL response structure.
    """

    def __init__(self, rate_limiter: RateLimiter) -> None:
        self._rl = rate_limiter

    def _auth_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {settings.meetup_api_key}",
            "Content-Type": "application/json",
        }

    async def _graphql(
        self,
        query: str,
        variables: dict[str, Any],
    ) -> dict[str, Any]:
        """Execute a GraphQL query. Returns response['data'] or raises."""
        payload = {"query": query, "variables": variables}
        response = await self._rl.post(
            DOMAIN,
            GRAPHQL_URL,
            json_body=payload,
            headers=self._auth_headers(),
        )
        errors = response.get("errors")
        if errors:
            log.warning("meetup GraphQL errors: %s", errors)
        return response.get("data", {})

    # ─────────────────────────────────────────────────────────────────────────
    # Keyword search for tech groups
    # ─────────────────────────────────────────────────────────────────────────

    async def search_groups(
        self,
        query: str,
        lat: float,
        lon: float,
        radius_miles: int = 25,
        limit: int = 20,
    ) -> dict[str, Any]:
        """
        Search for Meetup groups matching a keyword near a location.

        Args:
            query:          Search string (e.g. 'startup', 'tech', 'entrepreneur').
            lat/lon:        Geographic center of the city.
            radius_miles:   Search radius in miles (default 25).
            limit:          Max results (default 20, max 50).

        Returns:
            GraphQL data dict with 'keywordSearch.edges' list.
            Each edge.node.result (when it's a Group) has:
            name, urlname, organizer, membersCount, upcomingEvents.totalCount.
        """
        return await self._graphql(
            _KEYWORD_SEARCH_QUERY,
            {
                "query": query,
                "lat": lat,
                "lon": lon,
                "radius": radius_miles,
                "first": min(limit, 50),
            },
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Group detail
    # ─────────────────────────────────────────────────────────────────────────

    async def get_group(self, urlname: str) -> dict[str, Any]:
        """
        Fetch full profile for a Meetup group by its URL slug.

        Returns groupByUrlname with organizer profile and event history.
        """
        return await self._graphql(_GROUP_DETAIL_QUERY, {"urlname": urlname})

    # ─────────────────────────────────────────────────────────────────────────
    # Upcoming events near location
    # ─────────────────────────────────────────────────────────────────────────

    async def upcoming_events(
        self,
        lat: float,
        lon: float,
        radius_miles: int = 25,
        limit: int = 30,
    ) -> dict[str, Any]:
        """
        Fetch upcoming events near a location. Includes group organizer info.
        Useful for discovering active organizers in a city without knowing
        group names in advance.
        """
        return await self._graphql(
            _UPCOMING_EVENTS_QUERY,
            {
                "lat": lat,
                "lon": lon,
                "radius": radius_miles,
                "first": min(limit, 50),
            },
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Static helpers
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def extract_groups_from_search(data: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Extract Group nodes from a keywordSearch response.
        Filters to result type == Group (search can return mixed types).
        """
        edges = (
            data.get("keywordSearch", {})
            .get("edges", [])
        )
        groups = []
        for edge in edges:
            node = edge.get("node", {})
            result = node.get("result")
            if result and result.get("urlname"):   # Groups have urlname; events don't
                groups.append(result)
        return groups

    @staticmethod
    def extract_organizers_from_events(data: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Extract unique organizer profiles from an upcomingEvents response.
        Deduplicates by organizer id.
        """
        edges = data.get("upcomingEvents", {}).get("edges", [])
        seen_ids: set[str] = set()
        organizers = []
        for edge in edges:
            node = edge.get("node", {})
            group = node.get("group", {})
            organizer = group.get("organizer", {})
            org_id = organizer.get("id")
            if org_id and org_id not in seen_ids:
                seen_ids.add(org_id)
                # Attach group context
                organizer["_group_name"] = group.get("name", "")
                organizer["_group_urlname"] = group.get("urlname", "")
                organizer["_group_members"] = group.get("membersCount", 0)
                organizers.append(organizer)
        return organizers
