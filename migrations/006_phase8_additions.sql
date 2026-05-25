-- migrations/006_phase8_additions.sql
--
-- Phase 8 additions: Bulk Government Data ETL Layer (FinCEN CTR + HUD)
--
-- Changes:
--   1. Add OWNS to relationships.relationship_type CHECK constraint
--      (used for HUD → entity owns real estate property edges)
--   2. Add 'etl_bulk_data' to entity_evidence.source_type CHECK constraint
--      (used when evidence derives from ETL-ingested bulk datasets)
--   3. Create index for OWNS relationship queries
--
-- Run this migration against your Supabase project before the next pipeline run.
-- Safe to re-run — uses DROP CONSTRAINT IF EXISTS + CREATE INDEX IF NOT EXISTS.
-- ----------------------------------------------------------------------------


-- ----------------------------------------------------------------------------
-- 1. Expand relationships.relationship_type CHECK constraint
--    Full replacement (Postgres requires DROP + ADD for CHECK constraints).
--    Cumulative from 001 → 002 → 005 → 006.
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
        -- Added in 002_additions.sql
        'OFFSHORE_ENTITY_LINKED_TO','BENEFICIAL_OWNER_OF',
        'COMPETITOR_OF','PATENT_CO_INVENTOR','BOARD_INTERLOCKED_WITH',
        -- Added in 005_phase6_additions.sql (Phase 6 — web scrapers)
        'FORMERLY_EMPLOYED_BY',         -- Historical employment from archived pages
        -- Added in 006_phase8_additions.sql (Phase 8 — ETL bulk data)
        'OWNS'                          -- Entity owns real estate (HUD insured properties)
    ));


-- ----------------------------------------------------------------------------
-- 2. Expand entity_evidence.source_type CHECK constraint
--    Add 'etl_bulk_data' for evidence sourced from ETL-ingested datasets
--    (FinCEN CTR aggregate data, HUD property portfolio).
-- ----------------------------------------------------------------------------

ALTER TABLE entity_evidence
    DROP CONSTRAINT IF EXISTS entity_evidence_source_type_check;

ALTER TABLE entity_evidence
    ADD CONSTRAINT entity_evidence_source_type_check
    CHECK (source_type IN (
        -- Original values from 001_initial_schema.sql
        'api_response',
        'pdf_document',
        'web_page',
        'news_article',
        'regulatory_filing',
        'database_record',
        -- Added in 004_fix_enum_constraints.sql
        'court_record',
        'government_record',
        'structured_data',
        'internal_database',
        'government_database',
        'professional_network',
        -- Added in 005_phase6_additions.sql (Phase 6 — web scrapers)
        'archived_web_page',            -- Wayback Machine / Internet Archive snapshots
        -- Added in 006_phase8_additions.sql (Phase 8 — ETL bulk data)
        'etl_bulk_data'                 -- ETL-ingested government bulk datasets (FinCEN, HUD)
    ));


-- ----------------------------------------------------------------------------
-- 3. Indexes for Phase 8 relationship queries
-- ----------------------------------------------------------------------------

-- OWNS edges: frequently queried entity → property, and by source_entity
CREATE INDEX IF NOT EXISTS idx_relationships_owns
    ON relationships (source_entity_id, target_entity_id)
    WHERE relationship_type = 'OWNS';


-- ----------------------------------------------------------------------------
-- 4. Verification
-- ----------------------------------------------------------------------------

DO $$
DECLARE
    v_owns_ok   BOOLEAN;
    v_etl_ok    BOOLEAN;
BEGIN
    -- Check OWNS is accepted
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.check_constraints
        WHERE constraint_name = 'relationships_relationship_type_check'
          AND check_clause LIKE '%OWNS%'
    ) INTO v_owns_ok;

    -- Check etl_bulk_data is accepted
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.check_constraints
        WHERE constraint_name = 'entity_evidence_source_type_check'
          AND check_clause LIKE '%etl_bulk_data%'
    ) INTO v_etl_ok;

    IF v_owns_ok AND v_etl_ok THEN
        RAISE NOTICE 'Migration 006: OWNS and etl_bulk_data constraints updated successfully.';
    ELSE
        RAISE WARNING 'Migration 006: Constraint verification failed (owns_ok=%, etl_ok=%)',
            v_owns_ok, v_etl_ok;
    END IF;
END;
$$;
