-- ─────────────────────────────────────────────────────────────────────────────
-- OSINT Ecosystem Intelligence System — Initial Schema
-- Migration: 001_initial_schema.sql
-- Run against: Supabase PostgreSQL project
-- Order matters — agent_runs must exist before tables that FK into it.
-- ─────────────────────────────────────────────────────────────────────────────

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE: agent_runs
-- One record per full pipeline execution. Top-level run record.
-- Created first — all other tables FK into it.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE agent_runs (
    run_id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    city_name                   TEXT NOT NULL,
    country_or_region           TEXT NOT NULL DEFAULT 'United States',
    city_key                    TEXT NOT NULL,          -- normalized: 'austin_us'

    -- Status
    run_status                  TEXT NOT NULL DEFAULT 'pending'
                                    CHECK (run_status IN ('pending','running','complete','failed','partial')),
    failure_reason              TEXT,

    -- Models used this run
    model_default               TEXT NOT NULL DEFAULT 'qwen3:14b',
    model_escalation            TEXT NOT NULL DEFAULT 'qwen3:22b',

    -- Results summary (updated at run completion)
    total_entities_found        INTEGER DEFAULT 0,
    total_relationships_found   INTEGER DEFAULT 0,
    total_claims_verified       INTEGER DEFAULT 0,
    total_claims_failed         INTEGER DEFAULT 0,
    total_items_rejected        INTEGER DEFAULT 0,
    overall_confidence          TEXT CHECK (overall_confidence IN ('high','medium','low')),

    -- Scope
    pass_count                  SMALLINT DEFAULT 0,
    gap_fill_triggered          BOOLEAN DEFAULT FALSE,
    categories_thin             TEXT[],                -- categories below coverage threshold

    -- Timing
    started_at                  TIMESTAMPTZ DEFAULT NOW(),
    completed_at                TIMESTAMPTZ,
    duration_seconds            INTEGER,

    -- Trigger
    triggered_by                TEXT,                  -- user_id or 'scheduler'
    trigger_type                TEXT CHECK (trigger_type IN ('manual','scheduled','delta')),
    is_delta_run                BOOLEAN DEFAULT FALSE,
    previous_run_id             UUID REFERENCES agent_runs(run_id)
);

CREATE INDEX idx_runs_city        ON agent_runs(city_key);
CREATE INDEX idx_runs_status      ON agent_runs(run_status);
CREATE INDEX idx_runs_started     ON agent_runs(started_at DESC);
CREATE INDEX idx_runs_triggered   ON agent_runs(triggered_by);


-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE: entities
-- Canonical entity registry. One record per resolved entity per version.
-- Contains INTELLIGENCE_RECORD data only — no LLM assessments.
-- Temporal versioning: never UPDATE, INSERT new record with new valid_from.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE entities (
    -- Identity
    entity_id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_name              TEXT NOT NULL,
    entity_type                 TEXT NOT NULL CHECK (entity_type IN (
                                    'investor','philanthropic','corporate','political',
                                    'nonprofit','executive_hnw','community_leader',
                                    'politician','hnwi','illicit'
                                )),
    entity_subtype              TEXT,

    -- Aliases (other names seen across sources)
    aliases                     TEXT[] DEFAULT '{}',

    -- Temporal versioning
    valid_from                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    valid_to                    TIMESTAMPTZ,           -- NULL = currently active
    superseded_by               UUID REFERENCES entities(entity_id),

    -- External identifiers (used for entity resolution)
    crunchbase_id               TEXT,
    ein                         TEXT,                  -- IRS Employer Identification Number
    fec_candidate_id            TEXT,
    fec_committee_id            TEXT,
    sec_crd_number              TEXT,
    sec_cik                     TEXT,
    opencorporates_id           TEXT,
    bioguide_id                 TEXT,
    opensecrets_id              TEXT,
    wikidata_id                 TEXT,

    -- Location (five-state null pattern)
    primary_city                TEXT,
    primary_city_status         TEXT NOT NULL DEFAULT 'NOT_COLLECTED'
                                    CHECK (primary_city_status IN (
                                        'REPORTED','REPORTED_ABSENT','NOT_COLLECTED',
                                        'NOT_REPORTED','COLLECTED_UNREPORTED'
                                    )),
    primary_state               TEXT,
    primary_state_status        TEXT NOT NULL DEFAULT 'NOT_COLLECTED'
                                    CHECK (primary_state_status IN (
                                        'REPORTED','REPORTED_ABSENT','NOT_COLLECTED',
                                        'NOT_REPORTED','COLLECTED_UNREPORTED'
                                    )),
    primary_country             TEXT DEFAULT 'United States',
    primary_country_status      TEXT NOT NULL DEFAULT 'NOT_COLLECTED'
                                    CHECK (primary_country_status IN (
                                        'REPORTED','REPORTED_ABSENT','NOT_COLLECTED',
                                        'NOT_REPORTED','COLLECTED_UNREPORTED'
                                    )),

    -- Web presence
    website_url                 TEXT,
    website_url_status          TEXT NOT NULL DEFAULT 'NOT_COLLECTED'
                                    CHECK (website_url_status IN (
                                        'REPORTED','REPORTED_ABSENT','NOT_COLLECTED',
                                        'NOT_REPORTED','COLLECTED_UNREPORTED'
                                    )),
    linkedin_url                TEXT,
    linkedin_url_status         TEXT NOT NULL DEFAULT 'NOT_COLLECTED'
                                    CHECK (linkedin_url_status IN (
                                        'REPORTED','REPORTED_ABSENT','NOT_COLLECTED',
                                        'NOT_REPORTED','COLLECTED_UNREPORTED'
                                    )),

    -- Description
    description                 TEXT,
    description_status          TEXT NOT NULL DEFAULT 'NOT_COLLECTED'
                                    CHECK (description_status IN (
                                        'REPORTED','REPORTED_ABSENT','NOT_COLLECTED',
                                        'NOT_REPORTED','COLLECTED_UNREPORTED'
                                    )),

    -- Provenance
    source_agent                TEXT NOT NULL,
    source_run_ids              UUID[] DEFAULT '{}',
    merge_provenance            JSONB DEFAULT '[]',    -- list of raw_entity_ids merged in
    source_urls                 TEXT[] DEFAULT '{}',
    last_seen                   TIMESTAMPTZ,
    last_verified               TIMESTAMPTZ,

    -- Confidence
    overall_confidence          TEXT CHECK (overall_confidence IN ('high','medium','low')),
    source_count                INTEGER DEFAULT 0,
    corroboration_count         INTEGER DEFAULT 0,

    -- ─── Classification Flags (set by Analysis & Scoring Agent) ───────────
    partner_candidate           BOOLEAN DEFAULT FALSE,
    competitor_candidate        BOOLEAN DEFAULT FALSE,
    blocker_candidate           BOOLEAN DEFAULT FALSE,
    investment_candidate        BOOLEAN DEFAULT FALSE,
    support_candidate           BOOLEAN DEFAULT FALSE,
    recruiter_candidate         BOOLEAN DEFAULT FALSE,
    top_influencer              BOOLEAN DEFAULT FALSE,

    -- ─── 9-Dimension Scores (set by Analysis & Scoring Agent) ────────────
    -- Full rationale lives in analytical_assessments, not here.
    score_influence             SMALLINT DEFAULT 0 CHECK (score_influence BETWEEN 0 AND 100),
    score_startup_relevance     SMALLINT DEFAULT 0 CHECK (score_startup_relevance BETWEEN 0 AND 100),
    score_partner_potential     SMALLINT DEFAULT 0 CHECK (score_partner_potential BETWEEN 0 AND 100),
    score_supporter_potential   SMALLINT DEFAULT 0 CHECK (score_supporter_potential BETWEEN 0 AND 100),
    score_competitor_potential  SMALLINT DEFAULT 0 CHECK (score_competitor_potential BETWEEN 0 AND 100),
    score_blocker_risk          SMALLINT DEFAULT 0 CHECK (score_blocker_risk BETWEEN 0 AND 100),
    score_investment_potential  SMALLINT DEFAULT 0 CHECK (score_investment_potential BETWEEN 0 AND 100),
    score_support_target        SMALLINT DEFAULT 0 CHECK (score_support_target BETWEEN 0 AND 100),
    score_recruiting_potential  SMALLINT DEFAULT 0 CHECK (score_recruiting_potential BETWEEN 0 AND 100),

    -- ─── Sensitivity ──────────────────────────────────────────────────────
    needs_review                BOOLEAN DEFAULT FALSE,
    sensitivity_tier            TEXT NOT NULL DEFAULT 'standard'
                                    CHECK (sensitivity_tier IN ('standard','elevated','restricted')),

    -- ─── Category-specific fields (JSONB — schema enforced at app layer) ──
    -- Schema per entity_type is defined in osint/schemas/entities.py
    category_fields             JSONB DEFAULT '{}',

    -- Cost tracking for paid APIs
    proxycurl_retrieved         BOOLEAN DEFAULT FALSE,
    proxycurl_retrieved_at      TIMESTAMPTZ,

    created_at                  TIMESTAMPTZ DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_entities_type        ON entities(entity_type);
CREATE INDEX idx_entities_city        ON entities(primary_city);
CREATE INDEX idx_entities_valid       ON entities(valid_from, valid_to);
CREATE INDEX idx_entities_active      ON entities(valid_to) WHERE valid_to IS NULL;
CREATE INDEX idx_entities_scores      ON entities(score_influence DESC, score_partner_potential DESC);
CREATE INDEX idx_entities_flags       ON entities(partner_candidate, competitor_candidate, blocker_candidate)
                                        WHERE partner_candidate = TRUE OR competitor_candidate = TRUE OR blocker_candidate = TRUE;
CREATE INDEX idx_entities_review      ON entities(needs_review) WHERE needs_review = TRUE;
CREATE INDEX idx_entities_ein         ON entities(ein) WHERE ein IS NOT NULL;
CREATE INDEX idx_entities_crunchbase  ON entities(crunchbase_id) WHERE crunchbase_id IS NOT NULL;
CREATE INDEX idx_entities_fec         ON entities(fec_candidate_id) WHERE fec_candidate_id IS NOT NULL;
CREATE INDEX idx_entities_sec_cik     ON entities(sec_cik) WHERE sec_cik IS NOT NULL;
CREATE INDEX idx_entities_run         ON entities USING GIN(source_run_ids);


-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE: entity_evidence
-- Per-field evidence linking. Every claim about every entity field is
-- individually sourced here. This is what makes the system auditable at the
-- field level, not just at the entity level.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE entity_evidence (
    link_id                     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id                   UUID NOT NULL REFERENCES entities(entity_id) ON DELETE CASCADE,
    run_id                      UUID NOT NULL REFERENCES agent_runs(run_id),

    -- What field on the entity this evidence supports
    supported_field             TEXT NOT NULL,         -- e.g., 'aum_usd', 'board_seats', 'grant_recipients_local'
    supported_value             TEXT,                  -- The specific value being evidenced (serialized to string)

    -- Source
    source_url                  TEXT NOT NULL,         -- MANDATORY
    source_type                 TEXT NOT NULL CHECK (source_type IN (
                                    'api_response','pdf_document','web_page',
                                    'news_article','regulatory_filing','database_record'
                                )),
    source_api                  TEXT,                  -- e.g., 'crunchbase', 'sec_edgar', 'propublica_nonprofit'
    archived_url                TEXT,                  -- local archive URL if stored
    sha256_hash                 TEXT,                  -- content hash for integrity
    retrieved_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Evidence content
    evidence_snippet            TEXT NOT NULL,         -- MANDATORY: exact text from source
    claim_type                  TEXT NOT NULL CHECK (claim_type IN (
                                    'direct_statement','inferred','computed'
                                )),
    confidence                  TEXT NOT NULL CHECK (confidence IN ('high','medium','low')),

    -- Provenance
    agent_name                  TEXT NOT NULL,
    prompt_version              TEXT,

    created_at                  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_evidence_entity      ON entity_evidence(entity_id);
CREATE INDEX idx_evidence_run         ON entity_evidence(run_id);
CREATE INDEX idx_evidence_field       ON entity_evidence(supported_field);
CREATE INDEX idx_evidence_source_url  ON entity_evidence(source_url);
CREATE INDEX idx_evidence_agent       ON entity_evidence(agent_name);


-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE: analytical_assessments
-- Every LLM-produced inference. NEVER mixed into the entities table.
-- This is the structural enforcement of the evidence/analytical split.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE analytical_assessments (
    assessment_id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id                   UUID REFERENCES entities(entity_id) ON DELETE CASCADE,
    run_id                      UUID NOT NULL REFERENCES agent_runs(run_id),

    -- Type
    assessment_type             TEXT NOT NULL CHECK (assessment_type IN (
                                    'score_rationale','relationship_inference','briefing_claim',
                                    'entity_resolution_decision','gap_analysis','framing'
                                )),

    -- Content
    claim_text                  TEXT NOT NULL,
    claim_json                  JSONB,                 -- structured form (e.g., scores object)

    -- Provenance — what makes this an ANALYTICAL_ASSESSMENT, not INTELLIGENCE_RECORD
    framework_name              TEXT NOT NULL,         -- e.g., 'entity_scoring_v2'
    framework_version           TEXT NOT NULL,
    derived_from                UUID[] DEFAULT '{}',   -- array of entity_evidence.link_id
    model_used                  TEXT NOT NULL,
    prompt_version              TEXT NOT NULL,

    -- Quality
    confidence                  TEXT CHECK (confidence IN ('high','medium','low')),
    needs_review                BOOLEAN DEFAULT FALSE,
    review_date                 TIMESTAMPTZ,
    reviewed_by                 TEXT,                  -- user_id if human reviewed

    -- Versioning (old assessments are retained, not deleted)
    superseded_by               UUID REFERENCES analytical_assessments(assessment_id),
    is_current                  BOOLEAN DEFAULT TRUE,

    created_at                  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_assessments_entity   ON analytical_assessments(entity_id);
CREATE INDEX idx_assessments_run      ON analytical_assessments(run_id);
CREATE INDEX idx_assessments_type     ON analytical_assessments(assessment_type);
CREATE INDEX idx_assessments_current  ON analytical_assessments(is_current) WHERE is_current = TRUE;
CREATE INDEX idx_assessments_review   ON analytical_assessments(needs_review) WHERE needs_review = TRUE;


-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE: osint_search_records
-- Proof-of-search audit trail.
-- Written by EVERY agent on EVERY search call, success or failure.
-- Makes the difference between NOT_REPORTED and NOT_COLLECTED visible.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE osint_search_records (
    search_id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id                      UUID NOT NULL REFERENCES agent_runs(run_id),
    agent_name                  TEXT NOT NULL,

    -- What was searched
    entity_type                 TEXT,
    entity_id                   UUID,                  -- nullable: entity may not exist yet at search time
    raw_entity_id               TEXT,                  -- temp ID from collection phase

    -- Search details
    source_searched             TEXT NOT NULL,         -- e.g., 'crunchbase_api', 'sec_edgar'
    query_used                  TEXT NOT NULL,         -- exact query string or JSON params
    search_framing              TEXT,                  -- which framing was active

    -- Result
    result_found                BOOLEAN NOT NULL,
    result_count                INTEGER,
    failure_reason              TEXT,                  -- null if result_found = TRUE
    http_status_code            INTEGER,
    response_time_ms            INTEGER,

    -- Cache
    served_from_cache           BOOLEAN DEFAULT FALSE,
    cache_key                   TEXT,

    timestamp                   TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_search_run           ON osint_search_records(run_id);
CREATE INDEX idx_search_entity        ON osint_search_records(entity_id) WHERE entity_id IS NOT NULL;
CREATE INDEX idx_search_source        ON osint_search_records(source_searched);
CREATE INDEX idx_search_result        ON osint_search_records(result_found);
CREATE INDEX idx_search_agent         ON osint_search_records(agent_name);
CREATE INDEX idx_search_timestamp     ON osint_search_records(timestamp DESC);


-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE: relationships
-- Authoritative edge list. Neo4j is a derived/materialized view of this.
-- Postgres is canonical. Every edge requires at least one evidence_id.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE relationships (
    relationship_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id                      UUID NOT NULL REFERENCES agent_runs(run_id),

    -- Edge endpoints
    source_entity_id            UUID NOT NULL REFERENCES entities(entity_id),
    target_entity_id            UUID NOT NULL REFERENCES entities(entity_id),

    -- Edge type
    relationship_type           TEXT NOT NULL CHECK (relationship_type IN (
                                    'INVESTED_IN','CO_INVESTED_WITH','SITS_ON_BOARD_OF',
                                    'EMPLOYED_BY','FOUNDED','ADVISED_BY','FUNDED_BY',
                                    'DONATED_TO','RECEIVED_GRANT_FROM','AWARDED_CONTRACT_TO',
                                    'POLITICALLY_CONNECTED_TO','ALUMNI_OF','CO_FOUNDED_WITH',
                                    'SUBSIDIARY_OF','MENTIONED_WITH','REGULATORY_OVERSIGHT',
                                    'LITIGATION_AGAINST','PEER_INVESTOR_IN'
                                )),
    direction                   TEXT NOT NULL CHECK (direction IN ('directed','undirected')),

    -- Evidence (MANDATORY: minimum 1 evidence_id — enforced at application layer)
    evidence_ids                UUID[] NOT NULL DEFAULT '{}',
    evidence_snippets           TEXT[] DEFAULT '{}',

    -- Quality
    confidence                  TEXT NOT NULL CHECK (confidence IN ('high','medium','low')),
    relationship_strength       NUMERIC(3,2) CHECK (relationship_strength BETWEEN 0 AND 1),
    sensitive_claim             BOOLEAN DEFAULT FALSE,
    verified                    BOOLEAN DEFAULT FALSE,
    verified_at                 TIMESTAMPTZ,

    -- Temporal
    valid_from                  DATE,
    valid_to                    DATE,                  -- null = ongoing

    -- Neo4j sync state
    neo4j_synced                BOOLEAN DEFAULT FALSE,
    neo4j_synced_at             TIMESTAMPTZ,

    created_at                  TIMESTAMPTZ DEFAULT NOW(),

    -- No self-loops
    CONSTRAINT no_self_loops CHECK (source_entity_id != target_entity_id),

    -- No duplicate edges of same type between same pair in same run
    CONSTRAINT unique_edge_per_run UNIQUE (run_id, source_entity_id, target_entity_id, relationship_type)
);

CREATE INDEX idx_rel_source           ON relationships(source_entity_id);
CREATE INDEX idx_rel_target           ON relationships(target_entity_id);
CREATE INDEX idx_rel_type             ON relationships(relationship_type);
CREATE INDEX idx_rel_run              ON relationships(run_id);
CREATE INDEX idx_rel_verified         ON relationships(verified) WHERE verified = TRUE;
CREATE INDEX idx_rel_neo4j_pending    ON relationships(neo4j_synced) WHERE neo4j_synced = FALSE;
CREATE INDEX idx_rel_evidence         ON relationships USING GIN(evidence_ids);


-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE: file_media_store
-- All files and cached web content retrieved during any run.
-- SHA-256 hashing enables deduplication and integrity verification.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE file_media_store (
    file_id                     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id                   UUID REFERENCES entities(entity_id),   -- nullable
    run_id                      UUID NOT NULL REFERENCES agent_runs(run_id),

    -- Source
    original_url                TEXT NOT NULL,
    file_name                   TEXT,
    mime_type                   TEXT,

    -- Content integrity
    hash_sha256                 TEXT NOT NULL,
    file_size_bytes             BIGINT,

    -- Storage location
    storage_location            TEXT NOT NULL CHECK (storage_location IN (
                                    'local_nvme_hot','local_external_ssd','docker_volume',
                                    'object_storage','remote_url_only'
                                )),
    storage_path                TEXT,                  -- null if remote_url_only

    -- Usage
    usage_context               TEXT NOT NULL CHECK (usage_context IN (
                                    'source_document','evidence_archive',
                                    'profile_photo','export_artifact'
                                )),
    attribution_required        BOOLEAN DEFAULT TRUE,

    retrieved_at                TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_files_entity         ON file_media_store(entity_id) WHERE entity_id IS NOT NULL;
CREATE INDEX idx_files_run            ON file_media_store(run_id);
CREATE INDEX idx_files_hash           ON file_media_store(hash_sha256);  -- deduplication
CREATE INDEX idx_files_url            ON file_media_store(original_url);


-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE: agent_outputs
-- One record per agent per run. Enables per-agent performance analysis,
-- model cost tracking, and debugging of which agent caused gaps or errors.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE agent_outputs (
    output_id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id                      UUID NOT NULL REFERENCES agent_runs(run_id),
    agent_name                  TEXT NOT NULL,

    -- Status
    agent_status                TEXT NOT NULL CHECK (agent_status IN (
                                    'success','error','timeout','skipped','partial'
                                )),
    error_message               TEXT,

    -- Model usage
    model_used                  TEXT NOT NULL,
    prompt_version              TEXT NOT NULL,
    tokens_in                   INTEGER DEFAULT 0,
    tokens_out                  INTEGER DEFAULT 0,
    llm_call_count              INTEGER DEFAULT 0,

    -- Performance
    latency_ms                  INTEGER,
    api_calls_made              INTEGER DEFAULT 0,
    api_calls_cached            INTEGER DEFAULT 0,

    -- Results
    entities_produced           INTEGER DEFAULT 0,
    relationships_produced      INTEGER DEFAULT 0,
    items_rejected              INTEGER DEFAULT 0,

    -- Archive
    output_snapshot_path        TEXT,                  -- path to frozen JSON output

    started_at                  TIMESTAMPTZ DEFAULT NOW(),
    completed_at                TIMESTAMPTZ
);

CREATE INDEX idx_outputs_run          ON agent_outputs(run_id);
CREATE INDEX idx_outputs_agent        ON agent_outputs(agent_name);
CREATE INDEX idx_outputs_status       ON agent_outputs(agent_status);


-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE: rejected_items
-- Every entity, relationship, enrichment, and claim discarded at any stage.
-- Primary audit and diagnostic tool. Required for legal defensibility.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE rejected_items (
    rejection_id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id                      UUID NOT NULL REFERENCES agent_runs(run_id),
    agent_name                  TEXT NOT NULL,

    -- Pipeline stage where rejection occurred
    stage                       TEXT NOT NULL CHECK (stage IN (
                                    'extraction','resolution','enrichment',
                                    'relationship','scoring','verification'
                                )),

    -- What was rejected
    item_type                   TEXT NOT NULL CHECK (item_type IN (
                                    'entity','relationship','enrichment','claim','merge_decision'
                                )),
    item_id                     TEXT,                  -- raw_entity_id, relationship_id, or temp ID
    item_snapshot               JSONB NOT NULL,        -- full data at time of rejection

    -- Why
    rejection_reason            TEXT NOT NULL,
    rejection_detail            TEXT,                  -- verbose explanation

    timestamp                   TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_rejected_run         ON rejected_items(run_id);
CREATE INDEX idx_rejected_stage       ON rejected_items(stage);
CREATE INDEX idx_rejected_type        ON rejected_items(item_type);
CREATE INDEX idx_rejected_reason      ON rejected_items(rejection_reason);
CREATE INDEX idx_rejected_timestamp   ON rejected_items(timestamp DESC);


-- ─────────────────────────────────────────────────────────────────────────────
-- Verification: confirm all 9 tables were created
-- ─────────────────────────────────────────────────────────────────────────────
DO $$
DECLARE
    expected_tables TEXT[] := ARRAY[
        'agent_runs', 'entities', 'entity_evidence', 'analytical_assessments',
        'osint_search_records', 'relationships', 'file_media_store',
        'agent_outputs', 'rejected_items'
    ];
    t TEXT;
    missing TEXT[] := '{}';
BEGIN
    FOREACH t IN ARRAY expected_tables LOOP
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = t
        ) THEN
            missing := array_append(missing, t);
        END IF;
    END LOOP;

    IF array_length(missing, 1) > 0 THEN
        RAISE EXCEPTION 'Missing tables: %', array_to_string(missing, ', ');
    ELSE
        RAISE NOTICE 'All 9 tables created successfully.';
    END IF;
END $$;
