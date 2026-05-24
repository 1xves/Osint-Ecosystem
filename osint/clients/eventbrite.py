"""
osint/clients/eventbrite.py

Eventbrite API client.

Eventbrite is the primary source for discovering tech event organizers — the
people who run startup meetups, hackathons, pitch competitions, and investor
networking events. Event organizers are prime community_leader candidates:
they hold convening power without formal titles.

API: https://www.eventbriteapi.com/v3/
Documentation: https://www.eventbrite.com/platform/api
Authentication: Bearer token (OAuth private token). Required. Free registration.

Key endpoints used:
    /events/search/             — discover events by location and category
    /organizations/{id}/events/ — list events for a known organizer
    /events/{id}/               — single event detail
    /events/{id}/organizer/     — organizer profile for a specific event

The token is stored as EVENTBRITE_API_KEY in settings (it's a private token,
not a true OAuth bearer in developer testing mode).

Rate limits:
    50 req/s per key (generous). Our config caps at 50 req/min to be safe.

Notes:
    - Eventbrite returns paginated results via 'pagination' key with 'continuation' tokens.
    - Category 102 = Business & Professional. 101 = Music (avoid). 108 = Science & Technology.
    - Formats: 'seminar', 'conference', 'networking', 'class,training'.
"""

from __future__ import annotations

import logging
from typing import Any

from osint.core.config import settings
from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

DOMAIN = "eventbrite"
BASE_URL = "https://www.eventbriteapi.com/v3"

# Eventbrite category IDs for tech / startup / professional events
TECH_CATEGORY_IDS = ["102", "108"]          # Business & Professional, Science & Tech
STARTUP_FORMATS = ["networking", "conference", "seminar", "class,training"]


class EventbriteClient:
    """
    Async client for Eventbrite API v3.

    All methods return raw API response dicts.
    Uses Bearer token authentication.
    """

    def __init__(self, rate_limiter: RateLimiter) -> None:
        self._rl = rate_limiter

    def _auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {settings.eventbrite_api_key}"}

    # ─────────────────────────────────────────────────────────────────────────
    # Event search
    # ─────────────────────────────────────────────────────────────────────────

    async def search_events(
        self,
        city: str,
        state_code: str | None = None,
        country: str = "US",
        keywords: str | None = None,
        category_id: str | None = None,
        max_results: int = 50,
        page_continuation: str | None = None,
    ) -> dict[str, Any]:
        """
        Search for tech/professional events in a city.

        Args:
            city:               City name (e.g. 'Austin').
            state_code:         Two-letter state (e.g. 'TX'). US only.
            country:            ISO-2 country code (default 'US').
            keywords:           Optional search query string.
            category_id:        Eventbrite category ID (default: tech categories).
            max_results:        Max events per page (max 50).
            page_continuation:  Continuation token for pagination.

        Returns:
            API response with 'events' list and 'pagination' dict.
            Each event has: id, name, description, organizer_id, venue,
            start, end, category_id, format_id, is_free, capacity, is_online.
        """
        params: dict[str, Any] = {
            "location.address": f"{city}{', ' + state_code if state_code else ''}",
            "location.within": "25mi",
            "expand": "organizer,venue",
            "page_size": min(max_results, 50),
        }
        if keywords:
            params["q"] = keywords
        if category_id:
            params["categories"] = category_id
        else:
            params["categories"] = ",".join(TECH_CATEGORY_IDS)
        if page_continuation:
            params["continuation"] = page_continuation

        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/events/search/",
            params=params,
            headers=self._auth_headers(),
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Event detail
    # ─────────────────────────────────────────────────────────────────────────

    async def get_event(self, event_id: str) -> dict[str, Any]:
        """
        Fetch full detail for a single Eventbrite event.
        Includes organizer profile and venue.
        """
        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/events/{event_id}/",
            params={"expand": "organizer,venue,ticket_availability"},
            headers=self._auth_headers(),
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Organizer profile
    # ─────────────────────────────────────────────────────────────────────────

    async def get_organizer(self, organizer_id: str) -> dict[str, Any]:
        """
        Fetch an organizer's public profile.

        Returns: id, name, description, website, logo, num_followers,
        url (Eventbrite profile page).
        """
        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/organizers/{organizer_id}/",
            headers=self._auth_headers(),
        )

    async def get_organizer_events(
        self,
        organizer_id: str,
        order_by: str = "start_desc",
        max_results: int = 20,
    ) -> dict[str, Any]:
        """
        Fetch all events hosted by a specific organizer.
        Useful for establishing an organizer's event history.
        """
        return await self._rl.get(
            DOMAIN,
            f"{BASE_URL}/organizers/{organizer_id}/events/",
            params={
                "order_by": order_by,
                "page_size": min(max_results, 50),
                "expand": "venue",
            },
            headers=self._auth_headers(),
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Static helpers
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def extract_events(response: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract events list from a search response."""
        events = response.get("events", [])
        if not isinstance(events, list):
            return []
        return events

    @staticmethod
    def extract_organizer(event: dict[str, Any]) -> dict[str, Any] | None:
        """Extract organizer sub-dict from an expanded event object."""
        return event.get("organizer") or None

    @staticmethod
    def organizer_to_candidate(
        organizer: dict[str, Any],
        city_name: str,
        event_count: int = 0,
    ) -> dict[str, Any]:
        """
        Convert an Eventbrite organizer object to a minimal candidate dict
        for the community_leader agent to build an entity from.

        Returns a dict with fields the agent expects — NOT a full entity dict;
        the agent constructs the entity from this.
        """
        return {
            "name": organizer.get("name", ""),
            "description": organizer.get("description", {}).get("text", "") if isinstance(organizer.get("description"), dict) else organizer.get("description", ""),
            "website_url": organizer.get("website", ""),
            "eventbrite_url": organizer.get("url", ""),
            "eventbrite_organizer_id": organizer.get("id", ""),
            "num_followers": organizer.get("num_followers", 0),
            "event_count": event_count,
            "primary_city": city_name,
            "source": "eventbrite",
        }
