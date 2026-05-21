"""
osint/schemas/entities.py

Python TypedDicts for all entity types.

These are the canonical in-memory representations used by agents.
Every field here maps 1:1 to the SQL schema (entities.category_fields JSONB
for category-specific fields, top-level columns for EntityBase fields).

Rules:
- Every nullable attribute field has a companion _status field (five-state null).
- Never add a field here without first updating OSINT_Schema_Spec.md.
- These TypedDicts are total=False — agents build them incrementally.
"""

from __future__ import annotations
from typing import TypedDict, Literal

# ─────────────────────────────────────────────────────────────────────────────
# Five-state null literal type
# ─────────────────────────────────────────────────────────────────────────────

FieldStatus = Literal[
    "REPORTED",
    "REPORTED_ABSENT",
    "NOT_COLLECTED",
    "NOT_REPORTED",
    "COLLECTED_UNREPORTED",
]

Confidence = Literal["high", "medium", "low"]

SensitivityTier = Literal["standard", "elevated", "restricted"]


# ─────────────────────────────────────────────────────────────────────────────
# Universal Entity Base
# All entity types extend this. Matches Part 2 of OSINT_Schema_Spec.md.
# ─────────────────────────────────────────────────────────────────────────────

class EntityBase(TypedDict, total=False):
    # Identity
    entity_id: str                      # UUID, assigned at resolution
    canonical_name: str                 # Normalized, deduplicated name
    entity_type: str                    # One of 10 types (EntityType enum values)
    entity_subtype: str                 # Type-specific subtype
    aliases: list[str]                  # All other names across sources

    # Temporal
    valid_from: str                     # ISO8601 when this record version became active
    valid_to: str | None                # ISO8601, None = currently active
    superseded_by: str | None           # entity_id of newer record version

    # Location (five-state null on all)
    primary_city: str | None
    primary_city_status: FieldStatus
    primary_state: str | None
    primary_state_status: FieldStatus
    primary_country: str | None
    primary_country_status: FieldStatus

    # Contact & Web
    website_url: str | None
    website_url_status: FieldStatus
    linkedin_url: str | None
    linkedin_url_status: FieldStatus
    twitter_handle: str | None
    twitter_handle_status: FieldStatus

    # Description
    description: str | None             # Factual, sourced — NOT LLM-generated
    description_status: FieldStatus
    description_source_url: str | None

    # External Identifiers (used for entity resolution Layer 1)
    external_ids: dict[str, str]        # {"crunchbase_id": "x", "ein": "y"}

    # Provenance
    source_agent: str                   # Agent that first produced this record
    source_run_ids: list[str]           # All run UUIDs contributing to this record
    merge_provenance: list[str]         # raw_entity_ids merged into this canonical record
    source_urls: list[str]              # All source URLs contributing to this record
    last_seen: str                      # ISO8601, most recent source retrieval
    last_verified: str | None           # ISO8601, most recent active verification

    # Confidence
    overall_confidence: Confidence
    source_count: int
    corroboration_count: int

    # Classification Flags (set by Analysis & Scoring Agent)
    partner_candidate: bool
    competitor_candidate: bool
    blocker_candidate: bool
    investment_candidate: bool
    support_candidate: bool
    recruiter_candidate: bool
    top_influencer: bool

    # 9-Dimension Scores (set by Analysis & Scoring Agent, 0–100)
    # Rationale lives in analytical_assessments table, not here
    score_influence: int
    score_startup_relevance: int
    score_partner_potential: int
    score_supporter_potential: int
    score_competitor_potential: int
    score_blocker_risk: int
    score_investment_potential: int
    score_support_target: int
    score_recruiting_potential: int

    # Sensitivity
    needs_review: bool
    sensitivity_tier: SensitivityTier

    # Category-specific fields (merged in at API layer, stored in category_fields JSONB)
    category_fields: dict


# ─────────────────────────────────────────────────────────────────────────────
# 3.1 Investor Fields
# ─────────────────────────────────────────────────────────────────────────────

class InvestorFields(TypedDict, total=False):
    investor_subtype: str               # vc | angel | family_office | pe | corporate_vc | syndicate

    # Fund details
    aum_usd: int | None
    aum_usd_status: FieldStatus
    fund_names: list[str] | None
    fund_names_status: FieldStatus
    fund_count: int | None
    fund_count_status: FieldStatus
    fund_vintage_years: list[int] | None
    fund_vintage_years_status: FieldStatus

    # Investment behavior
    investment_stage_focus: list[str] | None    # pre_seed|seed|series_a|series_b|growth|late_stage
    investment_stage_focus_status: FieldStatus
    investment_thesis: str | None
    investment_thesis_status: FieldStatus
    sector_focus: list[str] | None
    sector_focus_status: FieldStatus
    check_size_min_usd: int | None
    check_size_min_usd_status: FieldStatus
    check_size_max_usd: int | None
    check_size_max_usd_status: FieldStatus
    lead_investor: bool | None
    lead_investor_status: FieldStatus
    co_invest_openness: str | None              # open | selective | closed
    co_invest_openness_status: FieldStatus
    investment_geography_focus: list[str] | None
    investment_geography_focus_status: FieldStatus

    # Portfolio
    portfolio_companies_local: list[str] | None
    portfolio_companies_local_status: FieldStatus
    portfolio_count_total: int | None
    portfolio_count_total_status: FieldStatus
    notable_exits: list[str] | None
    notable_exits_status: FieldStatus

    # People
    named_partners_gps: list[str] | None
    named_partners_gps_status: FieldStatus
    managing_partner: str | None
    managing_partner_status: FieldStatus

    # External IDs
    crunchbase_id: str | None
    sec_crd_number: str | None
    sec_13f_filer: bool | None
    pitchbook_id: str | None


# ─────────────────────────────────────────────────────────────────────────────
# 3.2 Philanthropic Fields
# ─────────────────────────────────────────────────────────────────────────────

class PhilanthropicFields(TypedDict, total=False):
    philanthropic_subtype: str          # private_foundation|community_foundation|corporate_foundation|major_donor|giving_circle

    # Financial
    ein: str | None
    ein_status: FieldStatus
    annual_giving_usd: int | None
    annual_giving_usd_status: FieldStatus
    total_assets_usd: int | None
    total_assets_usd_status: FieldStatus
    source_990_year: int | None
    source_990_year_status: FieldStatus

    # Focus
    focus_areas: list[str] | None
    focus_areas_status: FieldStatus
    geographic_focus: list[str] | None
    geographic_focus_status: FieldStatus
    grant_types: list[str] | None       # general_operating|project|capital|capacity_building
    grant_types_status: FieldStatus

    # Grantmaking
    grant_recipients_local: list[dict] | None
    # Each: {name, ein|None, amount_usd, year, purpose}
    grant_recipients_local_status: FieldStatus
    average_grant_size_usd: int | None
    average_grant_size_usd_status: FieldStatus

    # Personnel
    key_personnel: list[dict] | None
    # Each: {name, title, compensation_usd|None}
    key_personnel_status: FieldStatus
    board_members: list[str] | None
    board_members_status: FieldStatus

    # Government
    government_funding_received_usd: int | None
    government_funding_received_usd_status: FieldStatus

    # External IDs
    candid_id: str | None
    guidestar_id: str | None


# ─────────────────────────────────────────────────────────────────────────────
# 3.3 Corporate Fields
# ─────────────────────────────────────────────────────────────────────────────

class CorporateFields(TypedDict, total=False):
    corporate_subtype: str              # major_employer|innovation_host|accelerator|strategic_investor|startup_studio

    # Company info
    industry: str | None
    industry_status: FieldStatus
    naics_code: str | None
    naics_code_status: FieldStatus
    founded_year: int | None
    founded_year_status: FieldStatus
    public_or_private: str | None       # public | private
    public_or_private_status: FieldStatus
    ticker_symbol: str | None
    ticker_symbol_status: FieldStatus
    stock_exchange: str | None
    stock_exchange_status: FieldStatus

    # Scale
    local_headcount_estimate: int | None
    local_headcount_estimate_status: FieldStatus
    total_headcount: int | None
    total_headcount_status: FieldStatus
    revenue_usd: int | None
    revenue_usd_status: FieldStatus
    revenue_year: int | None

    # Structure
    parent_company_name: str | None
    parent_company_name_status: FieldStatus
    subsidiary_names_local: list[str] | None
    subsidiary_names_local_status: FieldStatus

    # Innovation activity
    innovation_programs: list[dict] | None
    # Each: {name, focus, stage, cohort_size|None, equity_taken|None}
    innovation_programs_status: FieldStatus
    corporate_vc_arm_name: str | None
    corporate_vc_arm_name_status: FieldStatus
    corporate_vc_portfolio_local: list[str] | None
    corporate_vc_portfolio_local_status: FieldStatus
    accelerator_programs: list[str] | None
    accelerator_programs_status: FieldStatus

    # People
    key_executives: list[dict] | None
    # Each: {name, title, tenure_start|None}
    key_executives_status: FieldStatus
    local_site_leader: str | None
    local_site_leader_status: FieldStatus

    # External IDs
    crunchbase_id: str | None
    sec_cik: str | None
    opencorporates_id: str | None
    duns_number: str | None


# ─────────────────────────────────────────────────────────────────────────────
# 3.4 Political Fields (Office or Body)
# ─────────────────────────────────────────────────────────────────────────────

class PoliticalFields(TypedDict, total=False):
    political_subtype: str              # elected_official|appointed_official|regulatory_body|government_agency|political_committee

    # Office
    title_office: str | None
    title_office_status: FieldStatus
    jurisdiction_level: str | None      # federal | state | local
    jurisdiction_level_status: FieldStatus
    jurisdiction_name: str | None
    jurisdiction_name_status: FieldStatus
    party_affiliation: str | None
    party_affiliation_status: FieldStatus
    term_start_date: str | None
    term_start_date_status: FieldStatus
    term_end_date: str | None
    term_end_date_status: FieldStatus

    # Committees & Policy
    committee_assignments: list[str] | None
    committee_assignments_status: FieldStatus
    subcommittee_assignments: list[str] | None
    policy_focus_areas: list[str] | None
    policy_focus_areas_status: FieldStatus
    notable_legislation_sponsored: list[dict] | None
    # Each: {bill_name, bill_number, status, year}
    notable_legislation_sponsored_status: FieldStatus

    # Finance & Influence
    total_campaign_raised_usd: int | None
    total_campaign_raised_usd_status: FieldStatus
    top_donor_industries: list[str] | None
    top_donor_industries_status: FieldStatus
    outside_spending_received_usd: int | None
    outside_spending_received_usd_status: FieldStatus
    lobbying_expenditures_received_usd: int | None
    lobbying_expenditures_received_usd_status: FieldStatus

    # Government contracting & grants
    government_contracts_awarded_count: int | None
    government_contracts_awarded_count_status: FieldStatus
    government_contracts_awarded_usd: int | None
    government_contracts_awarded_usd_status: FieldStatus
    government_grants_awarded_count: int | None
    government_grants_awarded_count_status: FieldStatus

    # External IDs
    fec_candidate_id: str | None
    fec_committee_id: str | None
    opensecrets_id: str | None
    bioguide_id: str | None


# ─────────────────────────────────────────────────────────────────────────────
# 3.5 Nonprofit & Civil Society Fields
# ─────────────────────────────────────────────────────────────────────────────

class NonprofitFields(TypedDict, total=False):
    nonprofit_subtype: str              # incubator|accelerator|workforce_dev|advocacy|community_org|ngo|university_program|professional_association

    # Identity
    ein: str | None
    ein_status: FieldStatus
    ntee_code: str | None
    ntee_code_status: FieldStatus
    founding_year: int | None
    founding_year_status: FieldStatus

    # Financial
    annual_revenue_usd: int | None
    annual_revenue_usd_status: FieldStatus
    total_assets_usd: int | None
    total_assets_usd_status: FieldStatus
    source_990_year: int | None
    source_990_year_status: FieldStatus

    # Mission & Programs
    mission_statement: str | None
    mission_statement_status: FieldStatus
    programs_offered: list[dict] | None
    # Each: {name, description, target_audience, cohort_size|None}
    programs_offered_status: FieldStatus
    ecosystem_builder_role: str | None
    ecosystem_builder_role_status: FieldStatus

    # People
    key_personnel: list[dict] | None
    # Each: {name, title, compensation_usd|None}
    key_personnel_status: FieldStatus
    board_members: list[str] | None
    board_members_status: FieldStatus

    # Funding
    primary_funders: list[str] | None
    primary_funders_status: FieldStatus
    government_grants_received_usd: int | None
    government_grants_received_usd_status: FieldStatus
    corporate_sponsors: list[str] | None
    corporate_sponsors_status: FieldStatus

    # Grantmaking
    grant_recipients: list[dict] | None
    # Each: {name, amount_usd, year, purpose}
    grant_recipients_status: FieldStatus

    # Reach
    members_or_alumni_count: int | None
    members_or_alumni_count_status: FieldStatus
    companies_graduated: list[str] | None
    companies_graduated_status: FieldStatus


# ─────────────────────────────────────────────────────────────────────────────
# 3.6 Executive & High-Net-Worth Individual Fields
# ─────────────────────────────────────────────────────────────────────────────

class ExecutiveHNWFields(TypedDict, total=False):
    individual_subtype: str             # founder|ceo|cto|coo|cfo|vp|partner|gp|board_chair|advisor

    # Current role
    primary_role: str | None
    primary_role_status: FieldStatus
    primary_employer: str | None
    primary_employer_status: FieldStatus
    primary_employer_entity_id: str | None  # FK to corporate/investor entity if resolved

    # Board & advisory
    board_seats: list[dict] | None
    # Each: {org_name, org_entity_id|None, role, start_date|None, end_date|None, is_current}
    board_seats_status: FieldStatus
    advisory_roles: list[dict] | None
    # Each: {org_name, org_entity_id|None, start_date|None}
    advisory_roles_status: FieldStatus

    # Founding history
    co_founded_companies: list[dict] | None
    # Each: {company_name, founded_year|None, outcome|None}
    co_founded_companies_status: FieldStatus

    # Investment activity
    known_investments: list[dict] | None
    # Each: {company_name, round|None, year|None, amount_usd|None}
    known_investments_status: FieldStatus
    angel_investor: bool | None
    angel_investor_status: FieldStatus
    estimated_portfolio_size: int | None
    estimated_portfolio_size_status: FieldStatus

    # Wealth
    estimated_net_worth_category: str | None    # under_1m|1m_10m|10m_100m|over_100m
    estimated_net_worth_category_status: FieldStatus
    primary_wealth_source: str | None           # founder_exit|investment_returns|executive_compensation|inheritance|other
    primary_wealth_source_status: FieldStatus

    # Philanthropic
    philanthropic_giving_focus: list[str] | None
    philanthropic_giving_focus_status: FieldStatus
    known_donations_local: list[dict] | None
    # Each: {recipient, amount_usd|None, year|None}
    known_donations_local_status: FieldStatus

    # Political
    political_donation_history: list[dict] | None
    # Each: {recipient, party|None, amount_usd, year, fec_record_id|None}
    political_donation_history_status: FieldStatus

    # Background
    education: list[dict] | None
    # Each: {institution, degree|None, year|None}
    education_status: FieldStatus
    prior_companies: list[dict] | None
    # Each: {company_name, role, start_year|None, end_year|None}
    prior_companies_status: FieldStatus

    # External IDs
    crunchbase_person_id: str | None
    sec_cik: str | None
    proxycurl_retrieved: bool
    proxycurl_retrieved_at: str | None


# ─────────────────────────────────────────────────────────────────────────────
# 3.7 Community Leader Fields
# Always produces low confidence — enforced at schema and agent level.
# ─────────────────────────────────────────────────────────────────────────────

class CommunityLeaderFields(TypedDict, total=False):
    community_subtype: str              # civic_leader|ecosystem_connector|media_influencer|cultural_leader|religious_leader|labor_leader

    # Influence profile
    sphere_of_influence: str | None
    sphere_of_influence_status: FieldStatus
    estimated_reach: str | None         # local | regional | national
    estimated_reach_status: FieldStatus
    influence_mechanisms: list[str] | None  # media|events|org_leadership|network_brokering
    influence_mechanisms_status: FieldStatus

    # Affiliations
    affiliated_organizations: list[dict] | None
    # Each: {org_name, role|None, is_formal}
    affiliated_organizations_status: FieldStatus

    # Evidence of influence
    visibility_signals: list[dict] | None
    # Each: {signal_type, description, date|None, source_url}
    visibility_signals_status: FieldStatus
    notable_achievements: list[str] | None
    notable_achievements_status: FieldStatus

    # Source transparency — deliberately surfaced
    source_evidence_count: int

    # Mandatory: always "low" — non-negotiable
    confidence_override: Literal["low"]


# ─────────────────────────────────────────────────────────────────────────────
# 3.8 Politician (Individual) Fields
# ─────────────────────────────────────────────────────────────────────────────

class PoliticianFields(TypedDict, total=False):
    # Current office
    elected_office: str | None
    elected_office_status: FieldStatus
    jurisdiction_level: str | None      # federal | state | local
    jurisdiction_name: str | None
    party: str | None
    party_status: FieldStatus
    term_start: str | None
    term_start_status: FieldStatus
    term_end: str | None
    term_end_status: FieldStatus
    seeking_reelection: bool | None
    seeking_reelection_status: FieldStatus

    # Legislative activity
    committee_memberships: list[str] | None
    committee_memberships_status: FieldStatus
    bills_sponsored: list[dict] | None
    # Each: {bill_name, bill_number, status, year, tech_relevant}
    bills_sponsored_status: FieldStatus
    votes_with_party_pct: float | None
    votes_with_party_pct_status: FieldStatus

    # Campaign finance
    total_raised_usd: int | None
    total_raised_usd_status: FieldStatus
    total_spent_usd: int | None
    total_spent_usd_status: FieldStatus
    top_donor_industries: list[str] | None
    top_donor_industries_status: FieldStatus
    top_donors_named: list[dict] | None
    # Each: {name, amount_usd, year}
    top_donors_named_status: FieldStatus
    outside_spending_usd: int | None
    outside_spending_usd_status: FieldStatus

    # Connections
    known_ecosystem_connections: list[str] | None
    known_ecosystem_connections_status: FieldStatus
    tech_stance: str | None             # supportive | neutral | skeptical | hostile
    tech_stance_status: FieldStatus

    # External IDs
    fec_candidate_id: str | None
    bioguide_id: str | None
    opensecrets_id: str | None
    votesmart_id: str | None


# ─────────────────────────────────────────────────────────────────────────────
# 3.9 HNWI Fields
# ─────────────────────────────────────────────────────────────────────────────

class HNWIFields(TypedDict, total=False):
    # Wealth profile
    estimated_net_worth_category: str | None    # 1m_10m|10m_100m|100m_500m|over_500m
    estimated_net_worth_category_status: FieldStatus
    primary_wealth_source: str | None           # inheritance|real_estate|entrepreneurship|investment|other
    primary_wealth_source_status: FieldStatus
    wealth_generation: str | None               # first_gen|second_gen|third_gen_plus
    wealth_generation_status: FieldStatus

    # Assets (public signals only)
    known_real_estate_local: list[dict] | None
    # Each: {property_type, description, source}
    known_real_estate_local_status: FieldStatus
    known_business_interests: list[dict] | None
    # Each: {business_name, role|None, sector|None}
    known_business_interests_status: FieldStatus

    # Philanthropic
    philanthropic_activities: list[dict] | None
    # Each: {org_name, role|None, focus_area|None}
    philanthropic_activities_status: FieldStatus
    known_donations_usd_range: str | None       # under_10k|10k_100k|100k_1m|over_1m
    known_donations_usd_range_status: FieldStatus

    # Community & political
    community_roles: list[str] | None
    community_roles_status: FieldStatus
    political_donation_history: list[dict] | None
    # Each: {recipient, party|None, amount_usd, year}
    political_donation_history_status: FieldStatus

    # Family connections
    family_connections_local: list[dict] | None
    # Each: {name, relationship, entity_id|None}
    family_connections_local_status: FieldStatus

    # External IDs
    sec_cik: str | None
    proxycurl_retrieved: bool


# ─────────────────────────────────────────────────────────────────────────────
# 3.10 Illicit Actor Fields
# MANDATORY: needs_review=True, sensitivity_tier="restricted", confidence_required="high"
# ─────────────────────────────────────────────────────────────────────────────

class IllicitFields(TypedDict, total=False):
    illicit_subtype: str                # sanctions_target|fraud_actor|money_laundering|organized_crime|regulatory_violator|known_associate

    # Sanctions
    ofac_listed: bool | None
    ofac_listed_status: FieldStatus
    ofac_list_types: list[str] | None   # SDN | OFSI | UN | EU
    ofac_list_types_status: FieldStatus
    ofac_program_names: list[str] | None
    ofac_program_names_status: FieldStatus
    ofac_entry_date: str | None
    other_sanctions_lists: list[str] | None
    other_sanctions_lists_status: FieldStatus

    # Legal
    court_cases: list[dict] | None
    # Each: {case_number, court, charge_type, status, outcome|None, year, source_url}
    court_cases_status: FieldStatus
    sec_enforcement_actions: list[dict] | None
    # Each: {action_type, description, year, outcome|None, source_url}
    sec_enforcement_actions_status: FieldStatus
    fincen_advisories: list[str] | None
    fincen_advisories_status: FieldStatus

    # Network
    known_associates: list[dict] | None
    # Each: {name, entity_id|None, relationship_description, source_url}
    known_associates_status: FieldStatus
    known_shell_companies: list[str] | None
    known_shell_companies_status: FieldStatus

    # Ecosystem relevance
    ecosystem_connection_description: str | None
    ecosystem_connection_description_status: FieldStatus
    connection_evidence_urls: list[str] | None

    # Mandatory overrides — enforced at agent level, not optional
    needs_review: Literal[True]
    sensitivity_tier: Literal["restricted"]
    confidence_required: Literal["high"]


# ─────────────────────────────────────────────────────────────────────────────
# Composite: full entity as used in agent output (base + category fields merged)
# ─────────────────────────────────────────────────────────────────────────────

class InvestorEntity(EntityBase, InvestorFields):
    pass

class PhilanthropicEntity(EntityBase, PhilanthropicFields):
    pass

class CorporateEntity(EntityBase, CorporateFields):
    pass

class PoliticalEntity(EntityBase, PoliticalFields):
    pass

class NonprofitEntity(EntityBase, NonprofitFields):
    pass

class ExecutiveHNWEntity(EntityBase, ExecutiveHNWFields):
    pass

class CommunityLeaderEntity(EntityBase, CommunityLeaderFields):
    pass

class PoliticianEntity(EntityBase, PoliticianFields):
    pass

class HNWIEntity(EntityBase, HNWIFields):
    pass

class IllicitEntity(EntityBase, IllicitFields):
    pass
