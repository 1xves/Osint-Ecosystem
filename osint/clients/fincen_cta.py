"""
osint/clients/fincen_cta.py

FinCEN Corporate Transparency Act (CTA) Beneficial Ownership stub client.

The CTA (effective January 1, 2024) requires most US companies to file
Beneficial Ownership Information (BOI) reports with FinCEN. This creates the
first-ever federal database mapping companies to their actual human owners —
closing the shell company anonymity gap.

Current status (as of 2025-2026):
    - The FinCEN BOI database exists and is being populated.
    - The public API for law enforcement / authorized users is partially available.
    - General public access API: NOT YET available.
    - Rule status: courts have issued conflicting rulings; FinCEN is currently
      not enforcing for many entity types while litigation plays out.
    - Expected timeline: phased access rollout in 2025-2026.

This client is a STUB — it establishes the interface, logs appropriately,
and returns empty results until FinCEN opens the API. When the API is available,
implement the actual HTTP calls following FinCEN's published spec.

References:
    https://www.fincen.gov/boi
    https://www.fincen.gov/beneficial-ownership-information-reporting-rule-frequently-asked-questions
    https://www.fincen.gov/boi-api (placeholder — not yet live)

Authentication (when available):
    FinCEN will use OAuth 2.0 client credentials with client_id/client_secret.
    These will be stored as FINCEN_CTA_API_KEY (or separate fields).
"""

from __future__ import annotations

import logging
from typing import Any

from osint.core.config import settings
from osint.core.rate_limiter import RateLimiter

log = logging.getLogger(__name__)

DOMAIN = "fincen_cta"
BASE_URL = "https://api.fincen.gov/boi/v1"   # Placeholder — not yet live

# API availability flag — flip to True when FinCEN opens the API
API_AVAILABLE = False


class FinCENCTAClient:
    """
    Stub client for the FinCEN Corporate Transparency Act BOI database.

    All methods currently return empty results with a structured warning log.
    The interface is intentionally production-ready so the enrichment agent
    can call this today and get real data when the API opens — with no
    code changes in the caller.

    When FinCEN opens the API:
        1. Set API_AVAILABLE = True
        2. Implement _request() method
        3. Implement actual parsing in search() and lookup()
        4. Add FINCEN_CTA credentials to Settings
    """

    def __init__(self, rate_limiter: RateLimiter) -> None:
        self._rl = rate_limiter

    # ─────────────────────────────────────────────────────────────────────────
    # Beneficial owner search by company
    # ─────────────────────────────────────────────────────────────────────────

    async def search_by_company(
        self,
        company_name: str,
        state: str | None = None,
        ein: str | None = None,
    ) -> dict[str, Any]:
        """
        Search for a company's beneficial owners by company name or EIN.

        When live, this will return:
            - Beneficial owners (name, DOB, address, ID type/number)
            - Company applicants
            - FinCEN ID (company identifier)

        Args:
            company_name:   Legal company name.
            state:          State of formation (two-letter code).
            ein:            Employer Identification Number (optional).

        Returns:
            Structured response with 'beneficial_owners' list and
            'reporting_company' dict. Empty when API_AVAILABLE=False.
        """
        if not API_AVAILABLE:
            log.info(
                "fincen_cta: API not yet available — '%s' skipped. "
                "Monitor https://www.fincen.gov/boi for API launch.",
                company_name,
            )
            return _empty_boi_response(company_name, reason="api_not_yet_available")

        if not settings.fincen_cta_api_key:
            log.warning("fincen_cta: API key not configured — skipping '%s'", company_name)
            return _empty_boi_response(company_name, reason="api_key_not_configured")

        # TODO: implement when API is live
        # params = {"company_name": company_name}
        # if state:
        #     params["state"] = state.upper()
        # if ein:
        #     params["ein"] = ein
        # return await self._rl.get(DOMAIN, f"{BASE_URL}/search", params=params,
        #                           headers=self._auth_headers())
        return _empty_boi_response(company_name, reason="not_implemented")

    # ─────────────────────────────────────────────────────────────────────────
    # Beneficial owner lookup by individual
    # ─────────────────────────────────────────────────────────────────────────

    async def search_by_individual(
        self,
        name: str,
        state: str | None = None,
    ) -> dict[str, Any]:
        """
        Search for companies where a named individual is a beneficial owner.

        When live, this will reveal all companies a person is a 25%+ owner
        or has substantial control over — revealing hidden corporate networks.

        Returns:
            'companies' list (company name, EIN, state) where individual
            appears as beneficial owner. Empty when API_AVAILABLE=False.
        """
        if not API_AVAILABLE:
            log.info(
                "fincen_cta: API not yet available — individual search for '%s' skipped.",
                name,
            )
            return {"companies": [], "_api_status": "not_yet_available", "_name": name}

        if not settings.fincen_cta_api_key:
            log.warning("fincen_cta: API key not configured")
            return {"companies": [], "_api_status": "api_key_not_configured"}

        # TODO: implement when API is live
        return {"companies": [], "_api_status": "not_implemented"}

    # ─────────────────────────────────────────────────────────────────────────
    # Auth helper (placeholder)
    # ─────────────────────────────────────────────────────────────────────────

    def _auth_headers(self) -> dict[str, str]:
        """Build FinCEN authentication headers (OAuth 2.0 bearer)."""
        return {"Authorization": f"Bearer {settings.fincen_cta_api_key}"}

    # ─────────────────────────────────────────────────────────────────────────
    # Static: API availability check
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def is_available() -> bool:
        """Return True when the FinCEN BOI API is available and configured."""
        return API_AVAILABLE and bool(settings.fincen_cta_api_key)


# ─────────────────────────────────────────────────────────────────────────────
# Module helpers
# ─────────────────────────────────────────────────────────────────────────────

def _empty_boi_response(company_name: str, reason: str) -> dict[str, Any]:
    """Return a structured empty response for caller consistency."""
    return {
        "reporting_company": None,
        "beneficial_owners": [],
        "company_applicants": [],
        "_company_name": company_name,
        "_api_status": reason,
    }
