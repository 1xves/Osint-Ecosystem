"""
osint/agents/pipeline.py

Pipeline Intelligence Agent — Phase 1 collection agent.

Calls the localhost:5050 research pipeline service to collect startup and
venture deal intelligence. This pipeline is a separate system that tracks
active startups, recent funding rounds, deal flow, and emerging companies
not yet in major databases.

The pipeline service is an internal tool — it is assumed to be running locally
and accessible at the configured endpoint. If it is not available, this agent
produces zero entities (non-blocking, logged as warning).

Data sources:
1. Internal pipeline at http://localhost:5050 (configurable via settings)
   - /api/v1/companies?city=<city> → active startup profiles
   - /api/v1/deals?city=<city>     → recent funding rounds
   - /api/v1/founders?city=<city>  → founder profiles

Output:
- Appends to state["raw_entities"] — raw pre-resolution entity dicts
- Writes evidence records for every sourced field
- Writes search records for every search attempt
- Does NOT resolve or deduplicate

Entity types produced:
- "investor"        → VCs/angels from pipeline deal data
- "executive_hnw"   → Founders from pipeline founder data
- "corporate"       → Startup companies from pipeline company data

Note on entity_types: pipeline output is mapped to existing entity types.
The pipeline agent does NOT produce a new entity type — it produces entities
of the same types as other agents (investor, executive_hnw, corporate).

Notes:
- The pipeline service must implement the API contract described in PIPELINE_API.md
- If the service returns 404 or connection refused, the agent skips gracefully
- Pipeline data is treated as "medium" confidence by default — it's proprietary
  but subject to data entry errors and may be ahead of public announcements
- All pipeline entities have source_type="internal_pipeline"
"""

from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import httpx

from osint.agents.base import BaseAgent
from osint.core.config import settings

log = logging.getLogger(__name__)

# Pipeline API endpoint (configurable via settings)
# Default: localhost:5050 (local research pipeline)
PIPELINE_BASE_URL = settings.research_pipeline_url
PIPELINE_TIMEOUT_SECONDS = 30.0

# Maximum entities per endpoint
MAX_COMPANIES = 50
MAX_DEALS = 30
MAX_FOUNDERS = 30


class PipelineAgent(BaseAgent):
    """
    Phase 1 collection agent that ingests data from the internal research pipeline.
    Falls back gracefully if the pipeline service is unavailable.
    """

    AGENT_NAME = "pipeline_agent"
    AGENT_VERSION = "1.0"

    async def run(self, state: dict[str, Any]) -> dict[str, Any]:
        city_name = state["city_name"]
        run_id = state["run_id"]
        existing_raw_entities = state.get("raw_entities", [])
        pass_number = state.get("pass_number", 1)

        log.info("PipelineAgent: collecting from %s for %s (pass %d)",
                 PIPELINE_BASE_URL, city_name, pass_number)

        new_raw_entities: list[dict[str, Any]] = []

        # ── Check if pipeline is available ────────────────────────────────────
        pipeline_available = await self._check_pipeline_health()
        if not pipeline_available:
            log.warning(
                "PipelineAgent: pipeline at %s is not available — skipping. "
                "This is non-blocking; start the pipeline service to enable this agent.",
                PIPELINE_BASE_URL
            )
            await self.write_search_record(
                source_searched="internal_pipeline",
                query_used=f"pipeline health check {PIPELINE_BASE_URL}",
                result_found=False,
                entity_type="corporate",
                failure_reason=f"Pipeline service not available at {PIPELINE_BASE_URL}",
            )
            return {
                "raw_entities": existing_raw_entities,
                **self.agent_status_patch("success", state.get("agent_statuses", {})),
                **self.token_count_patch(
                    state.get("total_tokens_in", 0),
                    state.get("total_tokens_out", 0),
                    state.get("agent_token_counts", {}),
                ),
                **self.entity_count_patch(state.get("agent_entity_counts", {})),
            }

        # ── Source 1: Companies ───────────────────────────────────────────────
        company_entities = await self._collect_companies(city_name, run_id)
        new_raw_entities.extend(company_entities)
        log.info("PipelineAgent: companies endpoint yielded %d entities", len(company_entities))

        # ── Source 2: Deals (investors) ───────────────────────────────────────
        deal_entities = await self._collect_deals(city_name, run_id)
        new_raw_entities.extend(deal_entities)
        log.info("PipelineAgent: deals endpoint yielded %d entities", len(deal_entities))

        # ── Source 3: Founders (executives) ───────────────────────────────────
        founder_entities = await self._collect_founders(city_name, run_id)
        new_raw_entities.extend(founder_entities)
        log.info("PipelineAgent: founders endpoint yielded %d entities", len(founder_entities))

        log.info("PipelineAgent: %d total raw entities from pipeline", len(new_raw_entities))

        patch: dict[str, Any] = {
            "raw_entities": existing_raw_entities + new_raw_entities,
            **self.agent_status_patch("success", state.get("agent_statuses", {})),
            **self.token_count_patch(
                state.get("total_tokens_in", 0),
                state.get("total_tokens_out", 0),
                state.get("agent_token_counts", {}),
            ),
            **self.entity_count_patch(state.get("agent_entity_counts", {})),
        }
        return patch

    async def _check_pipeline_health(self) -> bool:
        """Ping the pipeline health endpoint. Returns True if available."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{PIPELINE_BASE_URL}/health")
                return resp.status_code == 200
        except (httpx.ConnectError, httpx.TimeoutException, OSError):
            return False
        except Exception as e:
            log.debug("PipelineAgent: health check unexpected error: %s", e)
            return False

    async def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        """
        Make a GET request to the pipeline API.
        Returns empty dict on failure. Does NOT raise — agents must be resilient.
        """
        url = f"{PIPELINE_BASE_URL}{path}"
        try:
            async with httpx.AsyncClient(timeout=PIPELINE_TIMEOUT_SECONDS) as client:
                resp = await client.get(url, params=params)
                resp.raise_for_status()
                return resp.json()
        except httpx.HTTPStatusError as e:
            log.warning("PipelineAgent: %s returned %d: %s", path, e.response.status_code, e)
            return {}
        except (httpx.ConnectError, httpx.TimeoutException) as e:
            log.warning("PipelineAgent: %s connection failed: %s", path, e)
            return {}
        except Exception as e:
            log.warning("PipelineAgent: %s unexpected error: %s", path, e)
            return {}

    # ─────────────────────────────────────────────────────────────────────────
    # Companies endpoint
    # ─────────────────────────────────────────────────────────────────────────

    async def _collect_companies(
        self, city_name: str, run_id: str
    ) -> list[dict[str, Any]]:
        """
        Fetch startup companies from the pipeline /api/v1/companies endpoint.
        Maps to entity_type="corporate" (startup subtype).
        """
        search_start = time.monotonic()
        response = await self._get(
            "/api/v1/companies",
            {"city": city_name, "limit": MAX_COMPANIES, "active": True},
        )
        elapsed_ms = int((time.monotonic() - search_start) * 1000)

        companies = response.get("companies", response.get("results", []))

        await self.write_search_record(
            source_searched="internal_pipeline",
            query_used=f"active companies in {city_name}",
            result_found=bool(companies),
            entity_type="corporate",
            result_count=len(companies),
            response_time_ms=elapsed_ms,
        )

        entities = []
        now_iso = datetime.now(timezone.utc).isoformat()

        for company in companies:
            name = (company.get("name") or company.get("company_name") or "").strip()
            if not name:
                continue

            company_id = str(company.get("id") or company.get("company_id") or "")
            source_url = company.get("url") or company.get("website") or f"{PIPELINE_BASE_URL}/companies/{company_id}"

            stage = company.get("stage") or company.get("funding_stage") or "early"
            founded_year = company.get("founded_year") or company.get("founded")

            category_fields: dict[str, Any] = {
                "corporate_subtype": "startup",
                "corporate_subtype_status": "REPORTED",
                "pipeline_company_id": company_id,
                "funding_stage": stage,
                "funding_stage_status": "REPORTED" if stage else "NOT_COLLECTED",
                "founded_year": str(founded_year) if founded_year else None,
                "industry": company.get("industry") or company.get("sector"),
                "industry_status": "REPORTED" if company.get("industry") or company.get("sector") else "NOT_COLLECTED",
                "employee_count_range": company.get("employee_count") or company.get("headcount"),
                "total_funding": company.get("total_funding") or company.get("funding_total"),
                "total_funding_status": "REPORTED" if (company.get("total_funding") or company.get("funding_total")) else "NOT_COLLECTED",
                "last_funding_date": company.get("last_funding_date"),
                "investors": company.get("investors", []),
            }

            entity: dict[str, Any] = {
                "entity_id": None,
                "canonical_name": name,
                "entity_type": "corporate",
                "entity_subtype": "startup",
                "aliases": [],
                "valid_from": now_iso,
                "valid_to": None,
                "superseded_by": None,

                "primary_city": company.get("city") or city_name,
                "primary_city_status": "REPORTED" if company.get("city") else "NOT_COLLECTED",
                "primary_state": company.get("state"),
                "primary_state_status": "REPORTED" if company.get("state") else "NOT_COLLECTED",
                "primary_country": "United States",
                "primary_country_status": "REPORTED",

                "website_url": company.get("website") or company.get("url"),
                "website_url_status": "REPORTED" if (company.get("website") or company.get("url")) else "NOT_COLLECTED",
                "linkedin_url": company.get("linkedin_url"),
                "linkedin_url_status": "REPORTED" if company.get("linkedin_url") else "NOT_COLLECTED",
                "twitter_handle": company.get("twitter_handle"),
                "twitter_handle_status": "REPORTED" if company.get("twitter_handle") else "NOT_COLLECTED",

                "description": company.get("description") or company.get("one_liner"),
                "description_status": "REPORTED" if (company.get("description") or company.get("one_liner")) else "NOT_COLLECTED",
                "description_source_url": source_url,

                "external_ids": {"pipeline_company_id": company_id} if company_id else {},
                "source_agent": self.AGENT_NAME,
                "source_run_ids": [run_id],
                "merge_provenance": [],
                "source_urls": [source_url] if source_url else [],
                "last_seen": now_iso,
                "last_verified": None,

                "overall_confidence": "medium",
                "source_count": 1,
                "corroboration_count": 0,

                "partner_candidate": False,
                "competitor_candidate": False,
                "blocker_candidate": False,
                "investment_candidate": True,   # Startups are investment candidates by definition
                "support_candidate": False,
                "recruiter_candidate": False,
                "top_influencer": False,

                "score_influence": 0,
                "score_startup_relevance": 0,
                "score_partner_potential": 0,
                "score_supporter_potential": 0,
                "score_competitor_potential": 0,
                "score_blocker_risk": 0,
                "score_investment_potential": 0,
                "score_support_target": 0,
                "score_recruiting_potential": 0,

                "needs_review": False,
                "sensitivity_tier": "standard",

                "category_fields": category_fields,

                "_raw_entity_id": str(uuid.uuid4()),
                "_source": "internal_pipeline",
                "_pending_evidence": [
                    {
                        "entity_id": None,
                        "run_id": run_id,
                        "supported_field": "canonical_name",
                        "supported_value": name,
                        "source_url": source_url,
                        "source_type": "internal_database",
                        "source_api": "internal_pipeline",
                        "retrieved_at": now_iso,
                        "evidence_snippet": (
                            f"Internal pipeline: {name} is an active startup in {company.get('city') or city_name}"
                            f"{f', stage: {stage}' if stage else ''}"
                            + (f", industry: {category_fields.get('industry')}" if category_fields.get('industry') else "")
                        ),
                        "claim_type": "direct_statement",
                        "confidence": "medium",
                        "agent_name": self.AGENT_NAME,
                        "prompt_version": self.AGENT_VERSION,
                    }
                ],
            }
            entities.append(entity)

        return entities

    # ─────────────────────────────────────────────────────────────────────────
    # Deals endpoint
    # ─────────────────────────────────────────────────────────────────────────

    async def _collect_deals(
        self, city_name: str, run_id: str
    ) -> list[dict[str, Any]]:
        """
        Fetch recent funding deals from /api/v1/deals.
        Maps investors to entity_type="investor".
        """
        search_start = time.monotonic()
        response = await self._get(
            "/api/v1/deals",
            {"city": city_name, "limit": MAX_DEALS},
        )
        elapsed_ms = int((time.monotonic() - search_start) * 1000)

        deals = response.get("deals", response.get("results", []))

        await self.write_search_record(
            source_searched="internal_pipeline",
            query_used=f"recent deals in {city_name}",
            result_found=bool(deals),
            entity_type="investor",
            result_count=len(deals),
            response_time_ms=elapsed_ms,
        )

        entities = []
        seen_investors: set[str] = set()
        now_iso = datetime.now(timezone.utc).isoformat()

        for deal in deals:
            # Extract investors from deal data
            investors = deal.get("investors", []) or deal.get("lead_investors", [])
            if isinstance(investors, str):
                investors = [investors]

            for investor_name in investors:
                if not investor_name or investor_name in seen_investors:
                    continue
                seen_investors.add(investor_name)

                source_url = f"{PIPELINE_BASE_URL}/deals/{deal.get('id', '')}" if deal.get("id") else ""
                amount = deal.get("amount") or deal.get("funding_amount")
                stage = deal.get("stage") or deal.get("round_type")

                category_fields: dict[str, Any] = {
                    "investor_subtype": "vc",
                    "investor_subtype_status": "NOT_COLLECTED",
                    "pipeline_deal_source": True,
                    "last_deal_stage": stage,
                    "last_deal_amount": amount,
                }

                entity: dict[str, Any] = {
                    "entity_id": None,
                    "canonical_name": investor_name,
                    "entity_type": "investor",
                    "entity_subtype": "vc",
                    "aliases": [],
                    "valid_from": now_iso,
                    "valid_to": None,
                    "superseded_by": None,

                    "primary_city": city_name,
                    "primary_city_status": "NOT_COLLECTED",
                    "primary_state": None,
                    "primary_state_status": "NOT_COLLECTED",
                    "primary_country": "United States",
                    "primary_country_status": "NOT_COLLECTED",

                    "website_url": None,
                    "website_url_status": "NOT_COLLECTED",
                    "linkedin_url": None,
                    "linkedin_url_status": "NOT_COLLECTED",
                    "twitter_handle": None,
                    "twitter_handle_status": "NOT_COLLECTED",

                    "description": None,
                    "description_status": "NOT_COLLECTED",
                    "description_source_url": source_url,

                    "external_ids": {},
                    "source_agent": self.AGENT_NAME,
                    "source_run_ids": [run_id],
                    "merge_provenance": [],
                    "source_urls": [source_url] if source_url else [],
                    "last_seen": now_iso,
                    "last_verified": None,

                    "overall_confidence": "medium",
                    "source_count": 1,
                    "corroboration_count": 0,

                    "partner_candidate": False,
                    "competitor_candidate": False,
                    "blocker_candidate": False,
                    "investment_candidate": False,
                    "support_candidate": False,
                    "recruiter_candidate": False,
                    "top_influencer": False,

                    "score_influence": 0,
                    "score_startup_relevance": 0,
                    "score_partner_potential": 0,
                    "score_supporter_potential": 0,
                    "score_competitor_potential": 0,
                    "score_blocker_risk": 0,
                    "score_investment_potential": 0,
                    "score_support_target": 0,
                    "score_recruiting_potential": 0,

                    "needs_review": False,
                    "sensitivity_tier": "standard",

                    "category_fields": category_fields,

                    "_raw_entity_id": str(uuid.uuid4()),
                    "_source": "internal_pipeline",
                    "_pending_evidence": [
                        {
                            "entity_id": None,
                            "run_id": run_id,
                            "supported_field": "canonical_name",
                            "supported_value": investor_name,
                            "source_url": source_url,
                            "source_type": "internal_database",
                            "source_api": "internal_pipeline",
                            "retrieved_at": now_iso,
                            "evidence_snippet": (
                                f"Pipeline deal: {investor_name} invested in "
                                f"{deal.get('company', 'a startup')} in {city_name}"
                                f"{f' ({stage})' if stage else ''}"
                                f"{f', ${amount:,}' if isinstance(amount, (int, float)) else f', {amount}' if amount else ''}"
                            ),
                            "claim_type": "direct_statement",
                            "confidence": "medium",
                            "agent_name": self.AGENT_NAME,
                            "prompt_version": self.AGENT_VERSION,
                        }
                    ],
                }
                entities.append(entity)

        return entities

    # ─────────────────────────────────────────────────────────────────────────
    # Founders endpoint
    # ─────────────────────────────────────────────────────────────────────────

    async def _collect_founders(
        self, city_name: str, run_id: str
    ) -> list[dict[str, Any]]:
        """
        Fetch founder profiles from /api/v1/founders.
        Maps to entity_type="executive_hnw" (founder subtype).
        """
        search_start = time.monotonic()
        response = await self._get(
            "/api/v1/founders",
            {"city": city_name, "limit": MAX_FOUNDERS},
        )
        elapsed_ms = int((time.monotonic() - search_start) * 1000)

        founders = response.get("founders", response.get("results", []))

        await self.write_search_record(
            source_searched="internal_pipeline",
            query_used=f"founders in {city_name}",
            result_found=bool(founders),
            entity_type="executive_hnw",
            result_count=len(founders),
            response_time_ms=elapsed_ms,
        )

        entities = []
        now_iso = datetime.now(timezone.utc).isoformat()

        for founder in founders:
            name = (
                founder.get("name") or
                f"{founder.get('first_name', '')} {founder.get('last_name', '')}".strip()
            )
            if not name:
                continue

            founder_id = str(founder.get("id") or "")
            company_name = founder.get("company") or founder.get("startup_name")
            title = founder.get("title") or "Founder"
            linkedin_url = founder.get("linkedin_url") or founder.get("linkedin")
            source_url = f"{PIPELINE_BASE_URL}/founders/{founder_id}" if founder_id else ""

            category_fields: dict[str, Any] = {
                "executive_subtype": "founder",
                "executive_subtype_status": "REPORTED",
                "current_title": title,
                "current_title_status": "REPORTED" if title else "NOT_COLLECTED",
                "current_company": company_name,
                "current_company_status": "REPORTED" if company_name else "NOT_COLLECTED",
                "pipeline_founder_id": founder_id,
            }

            entity: dict[str, Any] = {
                "entity_id": None,
                "canonical_name": name,
                "entity_type": "executive_hnw",
                "entity_subtype": "founder",
                "aliases": [],
                "valid_from": now_iso,
                "valid_to": None,
                "superseded_by": None,

                "primary_city": founder.get("city") or city_name,
                "primary_city_status": "REPORTED" if founder.get("city") else "NOT_COLLECTED",
                "primary_state": founder.get("state"),
                "primary_state_status": "REPORTED" if founder.get("state") else "NOT_COLLECTED",
                "primary_country": "United States",
                "primary_country_status": "REPORTED",

                "website_url": None,
                "website_url_status": "NOT_COLLECTED",
                "linkedin_url": linkedin_url,
                "linkedin_url_status": "REPORTED" if linkedin_url else "NOT_COLLECTED",
                "twitter_handle": founder.get("twitter"),
                "twitter_handle_status": "REPORTED" if founder.get("twitter") else "NOT_COLLECTED",

                "description": founder.get("bio") or founder.get("description"),
                "description_status": "REPORTED" if (founder.get("bio") or founder.get("description")) else "NOT_COLLECTED",
                "description_source_url": source_url,

                "external_ids": {"pipeline_founder_id": founder_id} if founder_id else {},
                "source_agent": self.AGENT_NAME,
                "source_run_ids": [run_id],
                "merge_provenance": [],
                "source_urls": [source_url] if source_url else [],
                "last_seen": now_iso,
                "last_verified": None,

                "overall_confidence": "medium",
                "source_count": 1,
                "corroboration_count": 0,

                "partner_candidate": False,
                "competitor_candidate": False,
                "blocker_candidate": False,
                "investment_candidate": False,
                "support_candidate": False,
                "recruiter_candidate": False,
                "top_influencer": False,

                "score_influence": 0,
                "score_startup_relevance": 0,
                "score_partner_potential": 0,
                "score_supporter_potential": 0,
                "score_competitor_potential": 0,
                "score_blocker_risk": 0,
                "score_investment_potential": 0,
                "score_support_target": 0,
                "score_recruiting_potential": 0,

                "needs_review": False,
                "sensitivity_tier": "standard",

                "category_fields": category_fields,

                "_raw_entity_id": str(uuid.uuid4()),
                "_source": "internal_pipeline",
                "_pending_evidence": [
                    {
                        "entity_id": None,
                        "run_id": run_id,
                        "supported_field": "canonical_name",
                        "supported_value": name,
                        "source_url": source_url,
                        "source_type": "internal_database",
                        "source_api": "internal_pipeline",
                        "retrieved_at": now_iso,
                        "evidence_snippet": (
                            f"Pipeline: {name} is a{' ' + title if title else ' founder'}"
                            f"{f' at {company_name}' if company_name else ''}"
                            f" in {founder.get('city') or city_name}"
                        ),
                        "claim_type": "direct_statement",
                        "confidence": "medium",
                        "agent_name": self.AGENT_NAME,
                        "prompt_version": self.AGENT_VERSION,
                    }
                ],
            }
            entities.append(entity)

        return entities
