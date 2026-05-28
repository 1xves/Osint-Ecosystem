"""
osint/seeds/philadelphia.py

Curated seed entities for the Philadelphia OSINT pipeline.

PURPOSE
-------
Seed entities solve two interconnected problems:

  1. Entity quality — SerpAPI-based discovery produces garbage names
     ("Top 10 VC Investors in Philadelphia"). Seeds are real named entities
     with known identifiers (CIK, EIN, FEC ID, etc.).

  2. Relationship bootstrapping — the relationship agent can only form edges
     between entities that co-exist in the same run. Seeds provide pre-populated
     category_fields (current_company, founder_names, board_seats, etc.) so that
     cross-entity edges form immediately without waiting for enrichment.

HOW IT WORKS
------------
Each seed dict is structured identically to a collected entity:
  - entity_type / entity_subtype
  - canonical_name + known aliases
  - External IDs (sec_cik, ein, fec_candidate_id, crunchbase_id)
  - category_fields pre-populated with relationship-relevant fields

During collection, each agent calls load_seeds(entity_type) and upserts the
seeded entities BEFORE running its discovery passes. This guarantees that a
baseline of real, named entities with known cross-links enters every run.

MAINTENANCE
-----------
Add new entities here as the pipeline expands. Keep category_fields minimal —
only pre-populate fields you know with high confidence. The enrichment agent
will fill in the rest during the run.

Fields marked with _status keys follow the pipeline convention:
  "REPORTED"     — taken from a reliable source but not yet API-verified
  "NOT_COLLECTED" — not attempted yet (enrichment will fill this)
"""

from __future__ import annotations
from typing import Any

# ─────────────────────────────────────────────────────────────────────────────
# Corporate seeds — major Philadelphia employers with SEC CIKs
# ─────────────────────────────────────────────────────────────────────────────

_CORPORATE: list[dict[str, Any]] = [
    {
        "canonical_name": "Comcast Corporation",
        "entity_type": "corporate",
        "entity_subtype": "public_company",
        "aliases": ["Comcast", "CMCSA"],
        "sec_cik": "0001166691",
        "website_url": "https://corporate.comcast.com",
        "description": "Global media and technology company; largest cable operator in the US. Headquartered in Philadelphia, PA.",
        "category_fields": {
            "corporate_subtype": "public_company",
            "industry": "Media & Telecommunications",
            "ticker_symbol": "CMCSA",
            "is_sec_registrant": True,
            "employee_count_range": "100000+",
            "founder_names": ["Ralph Roberts", "Daniel Aaron", "Julian Brodsky"],
            "founder_names_status": "REPORTED",
            "corporate_subtype_status": "REPORTED",
            "industry_status": "REPORTED",
        },
    },
    {
        "canonical_name": "Aramark Corporation",
        "entity_type": "corporate",
        "entity_subtype": "public_company",
        "aliases": ["Aramark", "ARMK"],
        "sec_cik": "0001659166",
        "website_url": "https://www.aramark.com",
        "description": "Food services, facilities management, and uniform services company. Headquartered in Philadelphia, PA.",
        "category_fields": {
            "corporate_subtype": "public_company",
            "industry": "Food Services",
            "ticker_symbol": "ARMK",
            "is_sec_registrant": True,
            "employee_count_range": "50000-100000",
            "corporate_subtype_status": "REPORTED",
            "industry_status": "REPORTED",
        },
    },
    {
        "canonical_name": "Lincoln Financial Group",
        "entity_type": "corporate",
        "entity_subtype": "public_company",
        "aliases": ["Lincoln Financial", "Lincoln National Corporation", "LNC"],
        "sec_cik": "0000059558",
        "website_url": "https://www.lfg.com",
        "description": "Fortune 500 financial services company providing insurance, retirement, and investment management. Headquartered in Radnor, PA (Philadelphia metro).",
        "category_fields": {
            "corporate_subtype": "public_company",
            "industry": "Financial Services / Insurance",
            "ticker_symbol": "LNC",
            "is_sec_registrant": True,
            "corporate_subtype_status": "REPORTED",
            "industry_status": "REPORTED",
        },
    },
    {
        "canonical_name": "Crown Holdings Inc",
        "entity_type": "corporate",
        "entity_subtype": "public_company",
        "aliases": ["Crown Holdings", "Crown Cork & Seal", "CCK"],
        "sec_cik": "0000023675",
        "website_url": "https://www.crowncork.com",
        "description": "Global packaging manufacturer. Headquartered in Yardley, PA (Philadelphia metro).",
        "category_fields": {
            "corporate_subtype": "public_company",
            "industry": "Packaging",
            "ticker_symbol": "CCK",
            "is_sec_registrant": True,
            "corporate_subtype_status": "REPORTED",
            "industry_status": "REPORTED",
        },
    },
    {
        "canonical_name": "Independence Blue Cross",
        "entity_type": "corporate",
        "entity_subtype": "private_company",
        "aliases": ["IBX", "Independence Blue Cross LLC"],
        "ein": "23-1731498",
        "website_url": "https://www.ibx.com",
        "description": "Pennsylvania's largest health insurer. Headquartered in Philadelphia, PA.",
        "category_fields": {
            "corporate_subtype": "private_company",
            "industry": "Health Insurance",
            "employee_count_range": "5000-10000",
            "corporate_subtype_status": "REPORTED",
            "industry_status": "REPORTED",
        },
    },
    {
        "canonical_name": "Thomas Jefferson University",
        "entity_type": "corporate",
        "entity_subtype": "university",
        "aliases": ["Jefferson Health", "Jefferson", "TJU"],
        "ein": "23-1352674",
        "website_url": "https://www.jefferson.edu",
        "description": "Academic health center and university. Includes Jefferson Health hospital system. Headquartered in Philadelphia, PA.",
        "category_fields": {
            "corporate_subtype": "university",
            "industry": "Healthcare / Education",
            "employee_count_range": "30000-50000",
            "corporate_subtype_status": "REPORTED",
            "industry_status": "REPORTED",
        },
    },
]

# ─────────────────────────────────────────────────────────────────────────────
# Executive / HNWI seeds — named individuals with known employers
# ─────────────────────────────────────────────────────────────────────────────

_EXECUTIVE_HNW: list[dict[str, Any]] = [
    {
        "canonical_name": "Brian L. Roberts",
        "entity_type": "executive_hnw",
        "entity_subtype": "ceo",
        "aliases": ["Brian Roberts"],
        "description": "Chairman and CEO of Comcast Corporation. Son of Comcast founder Ralph Roberts.",
        "category_fields": {
            "current_title": "Chairman and Chief Executive Officer",
            "current_company": "Comcast Corporation",
            "executive_subtype": "ceo",
            "is_founder": False,
            "current_title_status": "REPORTED",
            "current_company_status": "REPORTED",
            "executive_subtype_status": "REPORTED",
        },
    },
    {
        "canonical_name": "David Cohen",
        "entity_type": "executive_hnw",
        "entity_subtype": "executive",
        "aliases": [],
        "description": "Senior Executive Vice President and Chief Diversity Officer at Comcast Corporation.",
        "category_fields": {
            "current_title": "Senior Executive Vice President",
            "current_company": "Comcast Corporation",
            "executive_subtype": "executive",
            "current_title_status": "REPORTED",
            "current_company_status": "REPORTED",
            "executive_subtype_status": "REPORTED",
        },
    },
    {
        "canonical_name": "Michael Rashid",
        "entity_type": "executive_hnw",
        "entity_subtype": "ceo",
        "aliases": [],
        "description": "President and CEO of Independence Blue Cross.",
        "category_fields": {
            "current_title": "President and Chief Executive Officer",
            "current_company": "Independence Blue Cross",
            "executive_subtype": "ceo",
            "current_title_status": "REPORTED",
            "current_company_status": "REPORTED",
            "executive_subtype_status": "REPORTED",
        },
    },
    {
        "canonical_name": "John Fry",
        "entity_type": "executive_hnw",
        "entity_subtype": "president",
        "aliases": [],
        "description": "President of Drexel University. Former president of Franklin & Marshall College.",
        "category_fields": {
            "current_title": "President",
            "current_company": "Drexel University",
            "executive_subtype": "president",
            "board_seats": ["Philadelphia Industrial Development Corporation", "Navy Yard"],
            "current_title_status": "REPORTED",
            "current_company_status": "REPORTED",
            "board_seats_status": "REPORTED",
            "executive_subtype_status": "REPORTED",
        },
    },
    {
        "canonical_name": "Allan Domb",
        "entity_type": "hnwi",
        "entity_subtype": "real_estate",
        "aliases": ["Michael Allan Domb"],
        "description": "Philadelphia real estate developer and former city councilmember. Known as 'The Condo King'.",
        "category_fields": {
            "hnwi_subtype": "real_estate",
            "wealth_source": "real estate development",
            "real_estate_focus": True,
            "hnwi_subtype_status": "REPORTED",
            "wealth_source_status": "REPORTED",
        },
    },
    {
        "canonical_name": "Josh Kopelman",
        "entity_type": "executive_hnw",
        "entity_subtype": "founder",
        "aliases": [],
        "description": "Founding partner of First Round Capital. Previously founded Half.com (acquired by eBay). Philadelphia-based venture capitalist.",
        "category_fields": {
            "current_title": "Partner",
            "current_company": "First Round Capital",
            "executive_subtype": "founder",
            "is_founder": True,
            "notable_companies_founded": ["Half.com", "First Round Capital", "Infonautics"],
            "current_title_status": "REPORTED",
            "current_company_status": "REPORTED",
            "executive_subtype_status": "REPORTED",
        },
    },
    {
        "canonical_name": "Jeff Fluhr",
        "entity_type": "executive_hnw",
        "entity_subtype": "founder",
        "aliases": [],
        "description": "Co-founder of StubHub; partner at First Round Capital.",
        "category_fields": {
            "current_title": "Partner",
            "current_company": "First Round Capital",
            "executive_subtype": "founder",
            "is_founder": True,
            "current_title_status": "REPORTED",
            "current_company_status": "REPORTED",
            "executive_subtype_status": "REPORTED",
        },
    },
]

# ─────────────────────────────────────────────────────────────────────────────
# Investor seeds — named Philadelphia VC / PE / family office firms
# ─────────────────────────────────────────────────────────────────────────────

_INVESTOR: list[dict[str, Any]] = [
    {
        "canonical_name": "First Round Capital",
        "entity_type": "investor",
        "entity_subtype": "vc",
        "aliases": ["First Round"],
        "website_url": "https://firstround.com",
        "description": "Seed-stage venture capital firm founded in Philadelphia. Portfolio includes Uber, Square, Warby Parker.",
        "category_fields": {
            "investor_subtype": "vc",
            "investment_stage_focus": ["seed", "pre-seed"],
            "sector_focus": ["technology", "software", "consumer"],
            "managing_partner": "Josh Kopelman",
            "portfolio_companies": ["Uber", "Square", "Warby Parker", "Roblox", "Notion"],
            "investor_subtype_status": "REPORTED",
            "investment_stage_focus_status": "REPORTED",
            "sector_focus_status": "REPORTED",
            "managing_partner_status": "REPORTED",
            "portfolio_companies_status": "REPORTED",
        },
    },
    {
        "canonical_name": "Ben Franklin Technology Partners of Southeastern Pennsylvania",
        "entity_type": "investor",
        "entity_subtype": "angel",
        "aliases": ["Ben Franklin Tech Partners", "BFTP/SEP", "Ben Franklin Technology Partners"],
        "website_url": "https://www.sep.benfranklin.org",
        "ein": "23-2456888",
        "description": "Pennsylvania state-funded seed stage technology investor and accelerator. One of the oldest early-stage technology investors in the US.",
        "category_fields": {
            "investor_subtype": "angel",
            "investment_stage_focus": ["seed", "early stage"],
            "sector_focus": ["technology", "life sciences", "advanced manufacturing"],
            "investor_subtype_status": "REPORTED",
            "investment_stage_focus_status": "REPORTED",
            "sector_focus_status": "REPORTED",
        },
    },
    {
        "canonical_name": "NewSpring Capital",
        "entity_type": "investor",
        "entity_subtype": "pe",
        "aliases": ["NewSpring", "NewSpring Holdings"],
        "website_url": "https://www.newspringcapital.com",
        "description": "Multi-strategy private equity and growth capital firm. Headquartered in Radnor, PA.",
        "category_fields": {
            "investor_subtype": "pe",
            "investment_stage_focus": ["growth equity", "buyout"],
            "sector_focus": ["healthcare", "technology", "business services"],
            "investor_subtype_status": "REPORTED",
            "investment_stage_focus_status": "REPORTED",
            "sector_focus_status": "REPORTED",
        },
    },
    {
        "canonical_name": "Dreamit Ventures",
        "entity_type": "investor",
        "entity_subtype": "vc",
        "aliases": ["Dreamit"],
        "website_url": "https://www.dreamit.com",
        "description": "Venture accelerator with deep Philadelphia roots. Focus on urban tech, health tech, and security.",
        "category_fields": {
            "investor_subtype": "vc",
            "investment_stage_focus": ["seed", "accelerator"],
            "sector_focus": ["urban tech", "health tech", "security"],
            "investor_subtype_status": "REPORTED",
            "investment_stage_focus_status": "REPORTED",
            "sector_focus_status": "REPORTED",
        },
    },
    {
        "canonical_name": "Safeguard Scientifics",
        "entity_type": "investor",
        "entity_subtype": "vc",
        "aliases": ["Safeguard", "SFE"],
        "sec_cik": "0000086955",
        "website_url": "https://www.safeguard.com",
        "description": "Wayne, PA-based growth-stage capital provider focused on technology and life sciences companies.",
        "category_fields": {
            "investor_subtype": "vc",
            "investment_stage_focus": ["growth stage"],
            "sector_focus": ["technology", "life sciences"],
            "is_sec_registrant": True,
            "investor_subtype_status": "REPORTED",
            "investment_stage_focus_status": "REPORTED",
            "sector_focus_status": "REPORTED",
        },
    },
]

# ─────────────────────────────────────────────────────────────────────────────
# Nonprofit seeds — major Philadelphia nonprofits with known EINs
# ─────────────────────────────────────────────────────────────────────────────

_NONPROFIT: list[dict[str, Any]] = [
    {
        "canonical_name": "United Way of Greater Philadelphia and Southern New Jersey",
        "entity_type": "nonprofit",
        "entity_subtype": "social_services",
        "aliases": ["United Way of Greater Philadelphia", "UWGPSNJ"],
        "ein": "23-1365983",
        "website_url": "https://www.unitedforimpact.org",
        "description": "Regional United Way affiliate serving Greater Philadelphia and Southern New Jersey.",
        "category_fields": {
            "nonprofit_subtype": "social_services",
            "focus_areas": ["poverty", "education", "health", "financial stability"],
            "government_funded": False,
            "nonprofit_subtype_status": "REPORTED",
            "focus_areas_status": "REPORTED",
        },
    },
    {
        "canonical_name": "Children's Hospital of Philadelphia",
        "entity_type": "nonprofit",
        "entity_subtype": "hospital",
        "aliases": ["CHOP", "CHOP Foundation"],
        "ein": "23-1352153",
        "website_url": "https://www.chop.edu",
        "description": "One of the top-ranked children's hospitals in the US. Philadelphia, PA.",
        "category_fields": {
            "nonprofit_subtype": "hospital",
            "focus_areas": ["pediatric medicine", "medical research"],
            "government_funded": False,
            "nonprofit_subtype_status": "REPORTED",
            "focus_areas_status": "REPORTED",
        },
    },
    {
        "canonical_name": "Chamber of Commerce for Greater Philadelphia",
        "entity_type": "nonprofit",
        "entity_subtype": "economic_development",
        "aliases": ["Greater Philadelphia Chamber of Commerce", "GPCC"],
        "ein": "23-0431010",
        "website_url": "https://chamberphl.com",
        "description": "Business association representing Greater Philadelphia region employers and driving regional economic development.",
        "category_fields": {
            "nonprofit_subtype": "economic_development",
            "focus_areas": ["economic development", "business advocacy", "workforce"],
            "government_funded": False,
            "nonprofit_subtype_status": "REPORTED",
            "focus_areas_status": "REPORTED",
        },
    },
]

# ─────────────────────────────────────────────────────────────────────────────
# Philanthropic seeds — major Philadelphia foundations
# ─────────────────────────────────────────────────────────────────────────────

_PHILANTHROPIC: list[dict[str, Any]] = [
    {
        "canonical_name": "William Penn Foundation",
        "entity_type": "philanthropic",
        "entity_subtype": "private_foundation",
        "aliases": [],
        "ein": "23-6250803",
        "website_url": "https://www.williampennfoundation.org",
        "description": "One of Philadelphia's largest private foundations. Focus on arts, environment, children and families.",
        "category_fields": {
            "philanthropic_subtype": "private_foundation",
            "primary_cause_areas": ["arts", "environment", "children", "families"],
            "total_assets_text": "$1.9 billion",
            "annual_giving_text": "$100 million+",
            "philanthropic_subtype_status": "REPORTED",
            "primary_cause_areas_status": "REPORTED",
            "total_assets_text_status": "REPORTED",
        },
    },
    {
        "canonical_name": "Philadelphia Foundation",
        "entity_type": "philanthropic",
        "entity_subtype": "community_foundation",
        "aliases": [],
        "ein": "23-6024535",
        "website_url": "https://www.philafound.org",
        "description": "Greater Philadelphia community foundation managing over $700M in charitable assets.",
        "category_fields": {
            "philanthropic_subtype": "community_foundation",
            "primary_cause_areas": ["community development", "education", "health"],
            "total_assets_text": "$700 million+",
            "philanthropic_subtype_status": "REPORTED",
            "primary_cause_areas_status": "REPORTED",
        },
    },
    {
        "canonical_name": "Philanthropy Network Greater Philadelphia",
        "entity_type": "philanthropic",
        "entity_subtype": "donor_network",
        "aliases": ["Philanthropy Network"],
        "ein": "31-1591503",
        "website_url": "https://philanthropynetwork.org",
        "description": "Membership organization for grantmakers in the Greater Philadelphia region.",
        "category_fields": {
            "philanthropic_subtype": "donor_network",
            "primary_cause_areas": ["community development", "civic engagement"],
            "philanthropic_subtype_status": "REPORTED",
            "primary_cause_areas_status": "REPORTED",
        },
    },
]

# ─────────────────────────────────────────────────────────────────────────────
# Politician seeds — named Philadelphia-area elected officials
# ─────────────────────────────────────────────────────────────────────────────

_POLITICIAN: list[dict[str, Any]] = [
    {
        "canonical_name": "Cherelle Parker",
        "entity_type": "politician",
        "entity_subtype": "mayor",
        "aliases": [],
        "description": "Mayor of Philadelphia, elected November 2023. Former PA State Representative and Philadelphia City Councilmember.",
        "category_fields": {
            "title": "Mayor",
            "office_held": "Mayor of Philadelphia",
            "is_current": True,
            "party_affiliation": "Democratic",
            "politician_subtype": "mayor",
            "title_status": "REPORTED",
            "party_affiliation_status": "REPORTED",
            "politician_subtype_status": "REPORTED",
        },
    },
    {
        "canonical_name": "Kenyatta Johnson",
        "entity_type": "politician",
        "entity_subtype": "city_council",
        "aliases": [],
        "fec_candidate_id": "P60025028",
        "description": "Philadelphia City Council member, 2nd District. Longest-serving member of Council.",
        "category_fields": {
            "title": "City Council Member",
            "office_held": "Philadelphia City Council, 2nd District",
            "is_current": True,
            "party_affiliation": "Democratic",
            "district": "2nd District",
            "politician_subtype": "city_council",
            "title_status": "REPORTED",
            "party_affiliation_status": "REPORTED",
            "politician_subtype_status": "REPORTED",
        },
    },
    {
        "canonical_name": "Bob Brady",
        "entity_type": "politician",
        "entity_subtype": "us_representative",
        "aliases": ["Robert Brady"],
        "fec_candidate_id": "H8PA01076",
        "description": "Former U.S. Representative for Pennsylvania's 1st congressional district (1998–2019). Former chairman of Philadelphia Democratic Party.",
        "category_fields": {
            "title": "Former U.S. Representative",
            "office_held": "U.S. House of Representatives, PA-1",
            "is_current": False,
            "party_affiliation": "Democratic",
            "district": "PA-1",
            "politician_subtype": "us_representative",
            "title_status": "REPORTED",
            "party_affiliation_status": "REPORTED",
            "politician_subtype_status": "REPORTED",
        },
    },
]

# ─────────────────────────────────────────────────────────────────────────────
# Community leader seeds
# ─────────────────────────────────────────────────────────────────────────────

_COMMUNITY_LEADER: list[dict[str, Any]] = [
    {
        "canonical_name": "Sheila Hess",
        "entity_type": "community_leader",
        "entity_subtype": "civic_connector",
        "aliases": [],
        "description": "Executive Director of the Urban Affairs Coalition, Philadelphia's largest civic coalition.",
        "category_fields": {
            "title_or_role": "Executive Director",
            "affiliated_organization": "Urban Affairs Coalition",
            "cause_or_focus": "civic development and community services",
            "community_leader_subtype": "civic_connector",
            "influence_type": "leadership",
            "title_or_role_status": "REPORTED",
            "affiliated_organization_status": "REPORTED",
        },
    },
]

# ─────────────────────────────────────────────────────────────────────────────
# Master registry and loader
# ─────────────────────────────────────────────────────────────────────────────

_ALL_SEEDS: dict[str, list[dict[str, Any]]] = {
    "corporate":       _CORPORATE,
    "executive_hnw":   _EXECUTIVE_HNW,
    "hnwi":            [e for e in _EXECUTIVE_HNW if e["entity_type"] == "hnwi"],
    "investor":        _INVESTOR,
    "nonprofit":       _NONPROFIT,
    "philanthropic":   _PHILANTHROPIC,
    "politician":      _POLITICIAN,
    "community_leader": _COMMUNITY_LEADER,
}


def load_seeds(
    entity_type: str,
    city_name: str = "Philadelphia",
) -> list[dict[str, Any]]:
    """
    Return a copy of all seed entities for the given entity_type.

    Each returned dict is safe to modify — it's a shallow copy.
    The caller (collection agent) should inject run_id, entity_id, and
    source_run_ids before writing to the DB.

    Args:
        entity_type: One of the pipeline entity type strings.
        city_name:   Used to set primary_city on seeds that don't have it.
                     Allows future city-parameterised seed files.

    Returns:
        List of entity dicts, or [] if this entity_type has no seeds.
    """
    seeds = _ALL_SEEDS.get(entity_type, [])
    result = []
    for seed in seeds:
        if seed.get("entity_type") != entity_type:
            continue
        copy = dict(seed)
        copy["primary_city"]          = copy.get("primary_city", city_name)
        copy["primary_city_status"]   = copy.get("primary_city_status", "REPORTED")
        copy["primary_country"]       = copy.get("primary_country", "United States")
        copy["primary_country_status"] = copy.get("primary_country_status", "REPORTED")
        copy["overall_confidence"]    = copy.get("overall_confidence", "medium")
        copy["source_count"]          = copy.get("source_count", 1)
        copy["corroboration_count"]   = copy.get("corroboration_count", 0)
        copy["_is_seed"]              = True   # flag for downstream dedup
        # Ensure category_fields exists
        if "category_fields" not in copy:
            copy["category_fields"] = {}
        result.append(copy)
    return result


def all_seed_entity_names() -> set[str]:
    """Return the set of all canonical_names across all seed types."""
    names: set[str] = set()
    for seeds in _ALL_SEEDS.values():
        for s in seeds:
            if n := s.get("canonical_name"):
                names.add(n)
            for alias in s.get("aliases", []):
                names.add(alias)
    return names
