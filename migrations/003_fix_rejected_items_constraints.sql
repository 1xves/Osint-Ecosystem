-- ============================================================================
-- Migration 003: Expand rejected_items CHECK constraints to match actual usage.
--
-- Problem: The stage and item_type CHECK constraints in rejected_items were
-- defined with coarse values, but agents write more granular stage names and
-- additional item_type categories. This causes constraint violations at runtime.
--
-- Missing stage values:
--   'entity_resolution_layer3' — resolution.py writes ambiguous merge records
--   'enrichment_ofac_screen'   — enrichment.py writes OFAC match rejections
--   'relationship_mapping'     — relationship.py writes rejected relationship candidates
--
-- Missing item_type values:
--   'ambiguous_entity_pair'    — resolution.py: entity pairs in 0.60–0.84 similarity range
--   'relationship_candidate'   — relationship.py: inferred edges that fail validation
--   'ofac_match'               — enrichment.py: OFAC hits written for human review
--
-- Run after 002_additions.sql.
-- All DDL is idempotent-safe (DROP IF EXISTS + ADD).
-- ============================================================================


-- ----------------------------------------------------------------------------
-- 1. Fix stage CHECK constraint
-- ----------------------------------------------------------------------------

ALTER TABLE rejected_items
    DROP CONSTRAINT IF EXISTS rejected_items_stage_check;

ALTER TABLE rejected_items
    ADD CONSTRAINT rejected_items_stage_check
    CHECK (stage IN (
        -- Original values from 001_initial_schema.sql
        'extraction',
        'resolution',
        'enrichment',
        'relationship',
        'scoring',
        'verification',
        -- Granular stage names used by agents
        'entity_resolution_layer3',     -- resolution_agent: ambiguous merge pairs
        'enrichment_ofac_screen',       -- enrichment_agent: OFAC screening results
        'relationship_mapping'          -- relationship_agent: rejected edge candidates
    ));


-- ----------------------------------------------------------------------------
-- 2. Fix item_type CHECK constraint
-- ----------------------------------------------------------------------------

ALTER TABLE rejected_items
    DROP CONSTRAINT IF EXISTS rejected_items_item_type_check;

ALTER TABLE rejected_items
    ADD CONSTRAINT rejected_items_item_type_check
    CHECK (item_type IN (
        -- Original values from 001_initial_schema.sql
        'entity',
        'relationship',
        'enrichment',
        'claim',
        'merge_decision',
        -- Additional types used by agents
        'ambiguous_entity_pair',        -- resolution_agent: 0.60–0.84 similarity pairs
        'relationship_candidate',       -- relationship_agent: rejected inferred edges
        'ofac_match'                    -- enrichment_agent: OFAC SDN hits for review
    ));


-- ----------------------------------------------------------------------------
-- Verification
-- ----------------------------------------------------------------------------

DO $$
DECLARE
    stage_count    INT;
    item_type_count INT;
BEGIN
    -- Verify stage constraint exists with correct name
    SELECT COUNT(*) INTO stage_count
    FROM information_schema.table_constraints
    WHERE table_name = 'rejected_items'
      AND constraint_name = 'rejected_items_stage_check'
      AND constraint_type = 'CHECK';

    -- Verify item_type constraint exists with correct name
    SELECT COUNT(*) INTO item_type_count
    FROM information_schema.table_constraints
    WHERE table_name = 'rejected_items'
      AND constraint_name = 'rejected_items_item_type_check'
      AND constraint_type = 'CHECK';

    IF stage_count = 1 AND item_type_count = 1 THEN
        RAISE NOTICE 'Migration 003: rejected_items constraints updated successfully.';
    ELSE
        RAISE EXCEPTION 'Migration 003: constraint verification failed (stage=%, item_type=%)',
            stage_count, item_type_count;
    END IF;
END $$;
