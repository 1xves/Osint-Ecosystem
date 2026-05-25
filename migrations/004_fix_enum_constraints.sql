-- ============================================================================
-- Migration 004: Expand entity_evidence.source_type and
--                analytical_assessments.assessment_type CHECK constraints.
--
-- Problem: Both constraints were defined with a subset of values, but agents
-- write additional types that weren't anticipated at schema design time. Every
-- agent that hits a missing value silently drops its DB write (constraint
-- violation), producing gaps in evidence trails and missing assessments.
--
-- Missing source_type values (entity_evidence):
--   'court_record'        — illicit_agent: CourtListener PACER records
--   'government_record'   — politician, illicit, corporate, nonprofit, political agents
--   'structured_data'     — relationship_agent: graph-derived evidence
--   'internal_database'   — pipeline_agent: cross-run entity lookups
--   'government_database' — enrichment_agent: OFAC / SAM.gov lookups
--   'professional_network'— enrichment_agent: LinkedIn-class profile data
--
-- Missing assessment_type values (analytical_assessments):
--   'claim_verification'  — verification_agent: per-entity LLM verification
--   'pass2_dispatch'      — pass2_dispatcher_agent: resolution dispatch records
--   'final_briefing'      — briefing_agent: per-section briefing records
--   'final_briefing_full' — briefing_agent: full sections JSON blob
--
-- Run after 003_fix_rejected_items_constraints.sql.
-- All DDL is idempotent-safe (DROP IF EXISTS + ADD).
-- ============================================================================


-- ----------------------------------------------------------------------------
-- 1. Fix entity_evidence.source_type CHECK constraint
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
        -- Additional types used by collection agents
        'court_record',           -- illicit_agent: CourtListener / PACER records
        'government_record',      -- politician / illicit / corporate / nonprofit / political agents
        'structured_data',        -- relationship_agent: graph-derived relationship evidence
        'internal_database',      -- pipeline_agent: evidence sourced from prior pipeline runs
        'government_database',    -- enrichment_agent: OFAC SDN, SAM.gov lookups
        'professional_network'    -- enrichment_agent: LinkedIn-class professional profile data
    ));


-- ----------------------------------------------------------------------------
-- 2. Fix analytical_assessments.assessment_type CHECK constraint
-- ----------------------------------------------------------------------------

ALTER TABLE analytical_assessments
    DROP CONSTRAINT IF EXISTS analytical_assessments_assessment_type_check;

ALTER TABLE analytical_assessments
    ADD CONSTRAINT analytical_assessments_assessment_type_check
    CHECK (assessment_type IN (
        -- Original values from 001_initial_schema.sql
        'score_rationale',              -- scoring_agent: per-entity score justification
        'relationship_inference',       -- relationship_agent: LLM-inferred edge reasoning
        'briefing_claim',               -- (reserved — not currently written by any agent)
        'entity_resolution_decision',   -- resolution_agent: merge/keep/reject decisions
        'gap_analysis',                 -- gap_analysis_agent: missing data assessments
        'framing',                      -- orchestrator: city framing / context records
        -- Additional types used by agents
        'claim_verification',           -- verification_agent: per-entity LLM fact checks
        'pass2_dispatch',               -- pass2_dispatcher_agent: resolution dispatch records
        'final_briefing',               -- briefing_agent: per-section narrative records
        'final_briefing_full'           -- briefing_agent: full sections JSON blob
    ));


-- ----------------------------------------------------------------------------
-- Verification
-- ----------------------------------------------------------------------------

DO $$
DECLARE
    source_type_count    INT;
    assessment_type_count INT;
BEGIN
    SELECT COUNT(*) INTO source_type_count
    FROM information_schema.table_constraints
    WHERE table_name = 'entity_evidence'
      AND constraint_name = 'entity_evidence_source_type_check'
      AND constraint_type = 'CHECK';

    SELECT COUNT(*) INTO assessment_type_count
    FROM information_schema.table_constraints
    WHERE table_name = 'analytical_assessments'
      AND constraint_name = 'analytical_assessments_assessment_type_check'
      AND constraint_type = 'CHECK';

    IF source_type_count = 1 AND assessment_type_count = 1 THEN
        RAISE NOTICE 'Migration 004: entity_evidence and analytical_assessments constraints updated successfully.';
    ELSE
        RAISE EXCEPTION 'Migration 004: constraint verification failed (source_type=%, assessment_type=%)',
            source_type_count, assessment_type_count;
    END IF;
END $$;
