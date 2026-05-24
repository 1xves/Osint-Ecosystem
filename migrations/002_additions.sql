-- ============================================================================
-- Migration 002: Additions for ICIJ, OFAC staging, PII vault, new relationship
--                types, and board interlock support.
--
-- Run after 001_initial_schema.sql.
-- All DDL is idempotent (IF NOT EXISTS / DO $$ blocks).
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. OFAC SDN Staging
--    Caches raw OFAC SDN bulk download rows before enrichment agent processes
--    them. Decouples the SDN bulk download from per-entity lookup logic.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ofac_sdn_staging (
    id                  BIGSERIAL PRIMARY KEY,
    uid                 TEXT        NOT NULL,         -- OFAC UID (unique per SDN entry)
    sdn_name            TEXT        NOT NULL,
    sdn_type            TEXT,                         -- individual / entity / vessel / aircraft
    programs            TEXT[],                       -- sanction programs (SDGT, IRAN, etc.)
    title               TEXT,
    call_sign           TEXT,
    vessel_type         TEXT,
    tonnage             TEXT,
    gross_registered_tonnage TEXT,
    vessel_flag         TEXT,
    vessel_owner        TEXT,
    remarks             TEXT,
    aka_names           TEXT[],                       -- all known aliases
    addresses           JSONB,
    nationalities       TEXT[],
    citizenships        TEXT[],
    date_of_birth       TEXT,
    place_of_birth      TEXT,
    -- Tracking
    bulk_download_date  DATE        NOT NULL DEFAULT CURRENT_DATE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_ofac_sdn_uid_date UNIQUE (uid, bulk_download_date)
);

CREATE INDEX IF NOT EXISTS idx_ofac_sdn_name
    ON ofac_sdn_staging USING GIN (to_tsvector('english', sdn_name));

CREATE INDEX IF NOT EXISTS idx_ofac_sdn_download_date
    ON ofac_sdn_staging (bulk_download_date);

COMMENT ON TABLE ofac_sdn_staging IS
    'Raw OFAC SDN bulk download rows. Refreshed daily by ofac_refresh job. '
    'Enrichment agent reads from this table rather than hitting the live API per entity.';


-- ----------------------------------------------------------------------------
-- 2. ICIJ Entity Staging
--    Stores ICIJ Offshore Leaks Database entries for Panama / Paradise /
--    Pandora Papers and the Offshore Leaks graph. Used by enrichment agent
--    to screen entities against known offshore structures.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS icij_entity_staging (
    id                  BIGSERIAL PRIMARY KEY,
    icij_node_id        TEXT        NOT NULL,         -- ICIJ internal node_id
    node_type           TEXT        NOT NULL,         -- Entity / Officer / Intermediary / Address
    name                TEXT        NOT NULL,
    jurisdiction        TEXT,
    jurisdiction_description TEXT,
    country_codes       TEXT[],
    incorporation_date  TEXT,
    inactivation_date   TEXT,
    struck_off_date     TEXT,
    status              TEXT,
    company_type        TEXT,
    service_provider    TEXT,
    source_id           TEXT,                         -- Panama / Paradise / Pandora / OffshoreLeaks
    -- Linked officers (denormalized for fast lookup)
    linked_officer_names TEXT[],
    -- Tracking
    bulk_import_date    DATE        NOT NULL DEFAULT CURRENT_DATE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_icij_node_id_date UNIQUE (icij_node_id, bulk_import_date)
);

CREATE INDEX IF NOT EXISTS idx_icij_name
    ON icij_entity_staging USING GIN (to_tsvector('english', name));

CREATE INDEX IF NOT EXISTS idx_icij_linked_officers
    ON icij_entity_staging USING GIN (linked_officer_names);

CREATE INDEX IF NOT EXISTS idx_icij_source_id
    ON icij_entity_staging (source_id);

COMMENT ON TABLE icij_entity_staging IS
    'ICIJ Offshore Leaks Database nodes (Panama Papers, Paradise Papers, Pandora Papers, '
    'Offshore Leaks). Bulk-imported periodically. Enrichment agent screens canonical entities '
    'against this table using fuzzy name matching.';


-- ----------------------------------------------------------------------------
-- 3. PII Vault
--    Isolates personal contact data (phone, email, home address, SSN/TIN) from
--    the main entities table. Separate access controls apply.
--    Agents write here instead of embedding PII into category_fields JSONB.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pii_vault (
    pii_id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id           UUID        NOT NULL,         -- FK → entities.entity_id
    run_id              UUID        NOT NULL,
    -- Contact data (all nullable — record still created even if partial)
    email_address       TEXT,                         -- Encrypted at rest (app-level AES-256)
    phone_number        TEXT,                         -- E.164 format
    home_address_line1  TEXT,
    home_address_line2  TEXT,
    home_city           TEXT,
    home_state          TEXT,
    home_postal_code    TEXT,
    home_country        TEXT,
    -- Identity numbers — store ONLY last 4 digits or a hashed value, never full
    ssn_last4           TEXT,                         -- Last 4 only — NEVER full SSN
    ein_number          TEXT,                         -- Tax EIN (semi-public)
    -- Source + access tracking
    source_type         TEXT        NOT NULL,         -- how this was obtained
    source_url          TEXT,
    collection_agent    TEXT        NOT NULL,
    access_log          JSONB       NOT NULL DEFAULT '[]'::JSONB,
    -- Temporal
    valid_from          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    valid_to            TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- FK constraint — soft: entity may be deleted without cascade
    CONSTRAINT fk_pii_entity FOREIGN KEY (entity_id)
        REFERENCES entities(entity_id) ON DELETE SET NULL
);

-- No full-text index on PII fields by design — prevents accidental exposure
-- in query logs.
CREATE INDEX IF NOT EXISTS idx_pii_vault_entity_id
    ON pii_vault (entity_id);

CREATE INDEX IF NOT EXISTS idx_pii_vault_run_id
    ON pii_vault (run_id);

-- Row-level security: only pii_reader role can SELECT; agents use pii_writer role
-- These roles must be created separately in your Supabase project.
-- ALTER TABLE pii_vault ENABLE ROW LEVEL SECURITY;
-- CREATE POLICY pii_read_policy ON pii_vault FOR SELECT USING (
--     current_role IN ('pii_reader', 'pii_writer', 'service_role')
-- );

COMMENT ON TABLE pii_vault IS
    'Isolated personal contact data. Separate from main entities table. '
    'Access controlled by pii_reader / pii_writer roles. '
    'Never embed full SSN — store last 4 only or hash. '
    'email_address and phone_number should be encrypted at application level.';


-- ----------------------------------------------------------------------------
-- 4. Extend relationship_type CHECK constraint
--    relationship_type is a TEXT column with a CHECK constraint (not a PG enum).
--    To add new allowed values, drop and recreate the constraint.
-- ----------------------------------------------------------------------------

ALTER TABLE relationships
    DROP CONSTRAINT IF EXISTS relationships_relationship_type_check;

ALTER TABLE relationships
    ADD CONSTRAINT relationships_relationship_type_check
    CHECK (relationship_type IN (
        -- Original values from 001_initial_schema.sql
        'INVESTED_IN','CO_INVESTED_WITH','SITS_ON_BOARD_OF',
        'EMPLOYED_BY','FOUNDED','ADVISED_BY','FUNDED_BY',
        'DONATED_TO','RECEIVED_GRANT_FROM','AWARDED_CONTRACT_TO',
        'POLITICALLY_CONNECTED_TO','ALUMNI_OF','CO_FOUNDED_WITH',
        'SUBSIDIARY_OF','MENTIONED_WITH','REGULATORY_OVERSIGHT',
        'LITIGATION_AGAINST','PEER_INVESTOR_IN',
        -- New values added in 002_additions.sql
        'OFFSHORE_ENTITY_LINKED_TO','BENEFICIAL_OWNER_OF',
        'COMPETITOR_OF','PATENT_CO_INVENTOR','BOARD_INTERLOCKED_WITH'
    ));


-- ----------------------------------------------------------------------------
-- 5. Board Interlock Index
--    Adds a partial index on relationships to accelerate board interlock
--    queries (relationship_agent derives BOARD_INTERLOCKED_WITH edges from
--    SITS_ON_BOARD_OF edges at runtime).
-- ----------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_relationships_sits_on_board
    ON relationships (source_entity_id, target_entity_id)
    WHERE relationship_type = 'SITS_ON_BOARD_OF';

CREATE INDEX IF NOT EXISTS idx_relationships_board_interlock
    ON relationships (relationship_type, source_entity_id, target_entity_id)
    WHERE relationship_type = 'BOARD_INTERLOCKED_WITH';


-- ----------------------------------------------------------------------------
-- 6. ICIJ hit log
--    Appended to whenever enrichment_agent finds an ICIJ match for a
--    canonical entity. Separate from entity_evidence for easy audit.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS icij_hits (
    hit_id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id           UUID        NOT NULL,
    run_id              UUID        NOT NULL,
    icij_node_id        TEXT        NOT NULL,
    icij_source         TEXT        NOT NULL,         -- Panama / Paradise / Pandora / OffshoreLeaks
    match_name          TEXT        NOT NULL,
    match_score         NUMERIC(5,4),                 -- fuzzy score 0.0–1.0
    node_type           TEXT,
    jurisdiction        TEXT,
    linked_officers     TEXT[],
    reviewed            BOOLEAN     NOT NULL DEFAULT FALSE,
    reviewed_at         TIMESTAMPTZ,
    reviewed_by         TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_icij_hit_entity FOREIGN KEY (entity_id)
        REFERENCES entities(entity_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_icij_hits_entity_id
    ON icij_hits (entity_id);

CREATE INDEX IF NOT EXISTS idx_icij_hits_unreviewed
    ON icij_hits (entity_id)
    WHERE reviewed = FALSE;

COMMENT ON TABLE icij_hits IS
    'Audit log of ICIJ Offshore Leaks matches found during enrichment. '
    'Every potential match is logged regardless of confidence — human review required.';


-- ----------------------------------------------------------------------------
-- Verification
-- ----------------------------------------------------------------------------

DO $$
DECLARE
    expected_tables TEXT[] := ARRAY[
        'ofac_sdn_staging',
        'icij_entity_staging',
        'pii_vault',
        'icij_hits'
    ];
    tbl TEXT;
    missing INT := 0;
BEGIN
    FOREACH tbl IN ARRAY expected_tables LOOP
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = tbl
        ) THEN
            RAISE WARNING 'Migration 002: table % was NOT created', tbl;
            missing := missing + 1;
        END IF;
    END LOOP;

    IF missing = 0 THEN
        RAISE NOTICE 'Migration 002: all 4 new tables confirmed. Enum additions applied.';
    ELSE
        RAISE EXCEPTION 'Migration 002: % table(s) missing — check above warnings', missing;
    END IF;
END $$;
