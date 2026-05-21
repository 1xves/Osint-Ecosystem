"""
osint/core/enums.py

All enumeration types for the OSINT system.
Adapted from rdi/enums.py — the five-state null pattern is shared infrastructure.
Every enum value is stored as its string value for readability in the database.
"""

import enum


# ─────────────────────────────────────────────────────────────────────────────
# Five-State Null — the most important enum in the system.
# Applied to every attribute field on every entity.
# These five states must never be collapsed into a single null.
# ─────────────────────────────────────────────────────────────────────────────

class FieldStatus(str, enum.Enum):
    """
    Epistemic status of any data field on any entity record.

    REPORTED            — A source explicitly states this value with a
                          retrievable citation.

    REPORTED_ABSENT     — A source explicitly states this field does not
                          apply to this entity (e.g., "no investment arm").

    NOT_COLLECTED       — This field was out of scope for this run.
                          We did not search for it. Says nothing about
                          whether the information exists.

    NOT_REPORTED        — We searched one or more sources and found nothing.
                          This IS intelligence — absence after an active
                          search is a different signal than never looking.

    COLLECTED_UNREPORTED — Found but not surfacing due to sensitivity tier
                           (e.g., restricted content on illicit entities).
    """
    REPORTED             = "REPORTED"
    REPORTED_ABSENT      = "REPORTED_ABSENT"
    NOT_COLLECTED        = "NOT_COLLECTED"
    NOT_REPORTED         = "NOT_REPORTED"
    COLLECTED_UNREPORTED = "COLLECTED_UNREPORTED"


# ─────────────────────────────────────────────────────────────────────────────
# Entity Types — the 10 categories the system tracks
# ─────────────────────────────────────────────────────────────────────────────

class EntityType(str, enum.Enum):
    INVESTOR          = "investor"
    PHILANTHROPIC     = "philanthropic"
    CORPORATE         = "corporate"
    POLITICAL         = "political"
    NONPROFIT         = "nonprofit"
    EXECUTIVE_HNW     = "executive_hnw"
    COMMUNITY_LEADER  = "community_leader"
    POLITICIAN        = "politician"
    HNWI              = "hnwi"
    ILLICIT           = "illicit"


# ─────────────────────────────────────────────────────────────────────────────
# Entity Subtypes — per category
# ─────────────────────────────────────────────────────────────────────────────

class InvestorSubtype(str, enum.Enum):
    VC            = "vc"
    ANGEL         = "angel"
    FAMILY_OFFICE = "family_office"
    PE            = "pe"
    CORPORATE_VC  = "corporate_vc"
    SYNDICATE     = "syndicate"

class PhilanthropicSubtype(str, enum.Enum):
    PRIVATE_FOUNDATION   = "private_foundation"
    COMMUNITY_FOUNDATION = "community_foundation"
    CORPORATE_FOUNDATION = "corporate_foundation"
    MAJOR_DONOR          = "major_donor"
    GIVING_CIRCLE        = "giving_circle"

class CorporateSubtype(str, enum.Enum):
    MAJOR_EMPLOYER      = "major_employer"
    INNOVATION_HOST     = "innovation_host"
    ACCELERATOR         = "accelerator"
    STRATEGIC_INVESTOR  = "strategic_investor"
    STARTUP_STUDIO      = "startup_studio"

class PoliticalSubtype(str, enum.Enum):
    ELECTED_OFFICIAL   = "elected_official"
    APPOINTED_OFFICIAL = "appointed_official"
    REGULATORY_BODY    = "regulatory_body"
    GOVERNMENT_AGENCY  = "government_agency"
    POLITICAL_COMMITTEE = "political_committee"

class NonprofitSubtype(str, enum.Enum):
    INCUBATOR              = "incubator"
    ACCELERATOR            = "accelerator"
    WORKFORCE_DEV          = "workforce_dev"
    ADVOCACY               = "advocacy"
    COMMUNITY_ORG          = "community_org"
    NGO                    = "ngo"
    UNIVERSITY_PROGRAM     = "university_program"
    PROFESSIONAL_ASSOC     = "professional_association"

class ExecutiveHNWSubtype(str, enum.Enum):
    FOUNDER       = "founder"
    CEO           = "ceo"
    CTO           = "cto"
    COO           = "coo"
    CFO           = "cfo"
    VP            = "vp"
    PARTNER       = "partner"
    GP            = "gp"
    BOARD_CHAIR   = "board_chair"
    ADVISOR       = "advisor"

class CommunityLeaderSubtype(str, enum.Enum):
    CIVIC_LEADER        = "civic_leader"
    ECOSYSTEM_CONNECTOR = "ecosystem_connector"
    MEDIA_INFLUENCER    = "media_influencer"
    CULTURAL_LEADER     = "cultural_leader"
    RELIGIOUS_LEADER    = "religious_leader"
    LABOR_LEADER        = "labor_leader"

class IllicitSubtype(str, enum.Enum):
    SANCTIONS_TARGET    = "sanctions_target"
    FRAUD_ACTOR         = "fraud_actor"
    MONEY_LAUNDERING    = "money_laundering"
    ORGANIZED_CRIME     = "organized_crime"
    REGULATORY_VIOLATOR = "regulatory_violator"
    KNOWN_ASSOCIATE     = "known_associate"


# ─────────────────────────────────────────────────────────────────────────────
# Run & Agent Status
# ─────────────────────────────────────────────────────────────────────────────

class RunStatus(str, enum.Enum):
    PENDING  = "pending"
    RUNNING  = "running"
    COMPLETE = "complete"
    FAILED   = "failed"
    PARTIAL  = "partial"

class AgentStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    ERROR   = "error"
    TIMEOUT = "timeout"
    SKIPPED = "skipped"

class PipelinePhase(str, enum.Enum):
    INIT               = "INIT"
    COLLECTION_PASS1   = "COLLECTION_PASS1"
    GAP_ANALYSIS       = "GAP_ANALYSIS"
    COLLECTION_PASS2   = "COLLECTION_PASS2"
    RESOLUTION         = "RESOLUTION"
    ENRICHMENT         = "ENRICHMENT"
    RELATIONSHIP       = "RELATIONSHIP"
    SCORING            = "SCORING"
    VERIFICATION       = "VERIFICATION"
    BRIEFING           = "BRIEFING"
    DONE               = "DONE"


# ─────────────────────────────────────────────────────────────────────────────
# Confidence
# ─────────────────────────────────────────────────────────────────────────────

class Confidence(str, enum.Enum):
    HIGH   = "high"
    MEDIUM = "medium"
    LOW    = "low"


# ─────────────────────────────────────────────────────────────────────────────
# Relationship Types — all 18
# ─────────────────────────────────────────────────────────────────────────────

class RelationshipType(str, enum.Enum):
    INVESTED_IN              = "INVESTED_IN"
    CO_INVESTED_WITH         = "CO_INVESTED_WITH"
    SITS_ON_BOARD_OF         = "SITS_ON_BOARD_OF"
    EMPLOYED_BY              = "EMPLOYED_BY"
    FOUNDED                  = "FOUNDED"
    ADVISED_BY               = "ADVISED_BY"
    FUNDED_BY                = "FUNDED_BY"
    DONATED_TO               = "DONATED_TO"
    RECEIVED_GRANT_FROM      = "RECEIVED_GRANT_FROM"
    AWARDED_CONTRACT_TO      = "AWARDED_CONTRACT_TO"
    POLITICALLY_CONNECTED_TO = "POLITICALLY_CONNECTED_TO"
    ALUMNI_OF                = "ALUMNI_OF"
    CO_FOUNDED_WITH          = "CO_FOUNDED_WITH"
    SUBSIDIARY_OF            = "SUBSIDIARY_OF"
    MENTIONED_WITH           = "MENTIONED_WITH"
    REGULATORY_OVERSIGHT     = "REGULATORY_OVERSIGHT"
    LITIGATION_AGAINST       = "LITIGATION_AGAINST"
    PEER_INVESTOR_IN         = "PEER_INVESTOR_IN"


# ─────────────────────────────────────────────────────────────────────────────
# Evidence & Source Quality
# ─────────────────────────────────────────────────────────────────────────────

class SourceType(str, enum.Enum):
    API_RESPONSE       = "api_response"
    PDF_DOCUMENT       = "pdf_document"
    WEB_PAGE           = "web_page"
    NEWS_ARTICLE       = "news_article"
    REGULATORY_FILING  = "regulatory_filing"
    DATABASE_RECORD    = "database_record"

class ClaimType(str, enum.Enum):
    DIRECT_STATEMENT = "direct_statement"
    INFERRED         = "inferred"
    COMPUTED         = "computed"

class AssessmentType(str, enum.Enum):
    SCORE_RATIONALE              = "score_rationale"
    RELATIONSHIP_INFERENCE       = "relationship_inference"
    BRIEFING_CLAIM               = "briefing_claim"
    ENTITY_RESOLUTION_DECISION   = "entity_resolution_decision"
    GAP_ANALYSIS                 = "gap_analysis"
    FRAMING                      = "framing"


# ─────────────────────────────────────────────────────────────────────────────
# Sensitivity
# ─────────────────────────────────────────────────────────────────────────────

class SensitivityTier(str, enum.Enum):
    STANDARD   = "standard"
    ELEVATED   = "elevated"
    RESTRICTED = "restricted"


# ─────────────────────────────────────────────────────────────────────────────
# Rejection
# ─────────────────────────────────────────────────────────────────────────────

class RejectionStage(str, enum.Enum):
    EXTRACTION   = "extraction"
    RESOLUTION   = "resolution"
    ENRICHMENT   = "enrichment"
    RELATIONSHIP = "relationship"
    SCORING      = "scoring"
    VERIFICATION = "verification"

class RejectedItemType(str, enum.Enum):
    ENTITY          = "entity"
    RELATIONSHIP    = "relationship"
    ENRICHMENT      = "enrichment"
    CLAIM           = "claim"
    MERGE_DECISION  = "merge_decision"


# ─────────────────────────────────────────────────────────────────────────────
# Framing
# ─────────────────────────────────────────────────────────────────────────────

class FramingType(str, enum.Enum):
    MAINSTREAM     = "mainstream"
    HETERODOX      = "heterodox"
    ADJACENT       = "adjacent_domain"
    PRACTITIONER   = "practitioner"


# ─────────────────────────────────────────────────────────────────────────────
# Storage
# ─────────────────────────────────────────────────────────────────────────────

class StorageLocation(str, enum.Enum):
    LOCAL_NVME_HOT    = "local_nvme_hot"
    LOCAL_EXTERNAL    = "local_external_ssd"
    DOCKER_VOLUME     = "docker_volume"
    OBJECT_STORAGE    = "object_storage"
    REMOTE_URL_ONLY   = "remote_url_only"

class UsageContext(str, enum.Enum):
    SOURCE_DOCUMENT  = "source_document"
    EVIDENCE_ARCHIVE = "evidence_archive"
    PROFILE_PHOTO    = "profile_photo"
    EXPORT_ARTIFACT  = "export_artifact"
