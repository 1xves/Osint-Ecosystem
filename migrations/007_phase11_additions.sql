-- migrations/007_phase11_additions.sql
--
-- Phase 11 additions: Intelligence Quality Pass
--
-- Changes:
--   1. Add WORKS_UNDER to relationships.relationship_type CHECK constraint
--      (used for 2-hop inferred edges: EMPLOYED_BY + SITS_ON_BOARD_OF → WORKS_UNDER)
--   2. Add confidence_score REAL column to relationships table
--      (replaces single-factor string confidence with multi-factor float 0.0–1.0)
--   3. Create index for WORKS_UNDER relationship queries
--   4. Create index for confidence_score range queries
--
-- Run this migration against your Supabase project before the next pipeline run:
--   python3 run_migrations.py 007
--
-- Safe to re-run — uses DROP CONSTRAINT IF EXISTS + ADD COLUMN IF NOT EXISTS
--   + CREATE INDEX IF NOT EXISTS.
-- ----------------------------------------------------------------------------


-- ----------------------------------------------------------------------------
-- 1. Expand relationships.relationship_type CHECK constraint
--    Full replacement (Postgres requires DROP + ADD for CHECK constraints).
--    Cumulative from 001 → 002 → 005 → 006 → 007.
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
        'OWNS',                         -- Entity owns real estate (HUD insured properties)
        -- Added in 007_phase11_additions.sql (Phase 11 — intelligence quality pass)
        'WORKS_UNDER'                   -- Inferred chain: EMPLOYED_BY + SITS_ON_BOARD_OF → WORKS_UNDER
    ));


-- ----------------------------------------------------------------------------
-- 2. Add confidence_score REAL column to relationships
--
--    Previous design used a string confidence enum ('high','medium','low').
--    Phase 11 adds a continuous float score (0.0–1.0) as a separate column
--    so existing string values are preserved and backward-compatible.
--
--    The float is populated by _relationship_strength_v2() in the relationship
--    agent. String confidence is kept for human-readable display.
-- ----------------------------------------------------------------------------

ALTER TABLE relationships
    ADD COLUMN IF NOT EXISTS confidence_score REAL
        CHECK (confidence_score IS NULL OR (confidence_score >= 0.0 AND confidence_score <= 1.0));

-- Backfill existing rows from string confidence to float approximations.
-- This is a best-effort backfill — pipeline re-runs will overwrite with
-- precise computed values from _relationship_strength_v2().
UPDATE relationships
SET confidence_score = CASE confidence
    WHEN 'high'   THEN 0.80
    WHEN 'medium' THEN 0.50
    WHEN 'low'    THEN 0.25
    ELSE 0.40
END
WHERE confidence_score IS NULL;


-- ----------------------------------------------------------------------------
-- 3. Indexes for Phase 11 relationship queries
-- ----------------------------------------------------------------------------

-- WORKS_UNDER edges: primarily queried by source_entity_id (who works under whom)
CREATE INDEX IF NOT EXISTS idx_relationships_works_under
    ON relationships (source_entity_id, target_entity_id)
    WHERE relationship_type = 'WORKS_UNDER';

-- confidence_score range queries (e.g., fetch all edges above a threshold)
CREATE INDEX IF NOT EXISTS idx_relationships_confidence_score
    ON relationships (confidence_score)
    WHERE confidence_score IS NOT NULL;


-- ----------------------------------------------------------------------------
-- 4. Verification
-- ----------------------------------------------------------------------------

DO $$
DECLARE
    v_works_under_ok    BOOLEAN;
    v_conf_score_ok     BOOLEAN;
BEGIN
    -- Check WORKS_UNDER is in the constraint
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.check_constraints
        WHERE constraint_name = 'relationships_relationship_type_check'
          AND check_clause LIKE '%WORKS_UNDER%'
    ) INTO v_works_under_ok;

    -- Check confidence_score column exists
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name   = 'relationships'
          AND column_name  = 'confidence_score'
    ) INTO v_conf_score_ok;

    IF v_works_under_ok AND v_conf_score_ok THEN
        RAISE NOTICE 'Migration 007: WORKS_UNDER and confidence_score applied successfully.';
    ELSE
        RAISE WARNING 'Migration 007: Verification failed (works_under_ok=%, conf_score_ok=%)',
            v_works_under_ok, v_conf_score_ok;
    END IF;
END;
$$;
