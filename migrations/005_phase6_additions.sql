-- ============================================================================
-- Migration 005: Add Phase 6 relationship types and supporting indices.
--
-- Phase 6 (Web Scraping) introduces one new relationship type:
--   FORMERLY_EMPLOYED_BY — links a person to a company they worked for
--                          historically (sourced from Wayback Machine
--                          archived team/about pages). Lower confidence
--                          than EMPLOYED_BY; may be stale.
--
-- Also adds source_type values for new scraper evidence:
--   'archived_web_page'  — enrichment_agent: Wayback Machine archived pages
--                          (flagged distinctly from live 'web_page' to signal
--                          the record may be out of date)
--
-- Run after 004_fix_enum_constraints.sql.
-- All DDL is idempotent-safe (DROP IF EXISTS + ADD).
-- ============================================================================


-- ----------------------------------------------------------------------------
-- 1. Expand relationships.relationship_type CHECK constraint
--    Add FORMERLY_EMPLOYED_BY to the allowed set.
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
        'FORMERLY_EMPLOYED_BY'     -- Historical employment from archived pages
    ));


-- ----------------------------------------------------------------------------
-- 2. Expand entity_evidence.source_type CHECK constraint
--    Add 'archived_web_page' for Wayback Machine evidence.
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
        'archived_web_page'        -- Wayback Machine / Internet Archive snapshots
    ));


-- ----------------------------------------------------------------------------
-- 3. Index to accelerate FORMERLY_EMPLOYED_BY queries
--    (used in briefing to surface historical employment chains)
-- ----------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_relationships_formerly_employed
    ON relationships (source_entity_id, target_entity_id)
    WHERE relationship_type = 'FORMERLY_EMPLOYED_BY';


-- ----------------------------------------------------------------------------
-- Verification
-- ----------------------------------------------------------------------------

DO $$
DECLARE
    rel_constraint_count    INT;
    evidence_constraint_count INT;
BEGIN
    SELECT COUNT(*) INTO rel_constraint_count
    FROM information_schema.table_constraints
    WHERE table_name = 'relationships'
      AND constraint_name = 'relationships_relationship_type_check'
      AND constraint_type = 'CHECK';

    SELECT COUNT(*) INTO evidence_constraint_count
    FROM information_schema.table_constraints
    WHERE table_name = 'entity_evidence'
      AND constraint_name = 'entity_evidence_source_type_check'
      AND constraint_type = 'CHECK';

    IF rel_constraint_count = 1 AND evidence_constraint_count = 1 THEN
        RAISE NOTICE 'Migration 005: relationship_type and source_type constraints updated successfully.';
    ELSE
        RAISE EXCEPTION 'Migration 005: constraint verification failed (rel=%, evidence=%)',
            rel_constraint_count, evidence_constraint_count;
    END IF;
END $$;
