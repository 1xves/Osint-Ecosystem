-- migrations/009_fix_city_key_and_stale_runs.sql
--
-- 1. Fix city_key: "philadelphia_un" → "philadelphia_us"
--    Root cause: worker._city_key() used country_name[:2] ("Un" from "United States")
--    instead of the ISO 3166-1 alpha-2 code ("us"). Fixed in code; this migrates
--    existing rows so the API can find them with the correct key.
--
-- 2. Mark orphaned "running" runs as failed.
--    Any run stuck in "running" status for more than 3 hours is orphaned —
--    the worker died without updating the status. Clean them up so the API
--    doesn't surface them as active.
--
-- Run: Supabase SQL Editor → paste → Run
-- Safe to run multiple times (idempotent).

BEGIN;

-- ── 1. Fix city_key in agent_runs ─────────────────────────────────────────────
UPDATE agent_runs
SET city_key = REPLACE(city_key, '_un', '_us')
WHERE city_key LIKE '%_un'
  AND city_key NOT LIKE '%_un_%';   -- guard: don't touch keys like "pune_in"

-- Verify: should be 0 rows remaining after update
-- SELECT count(*) FROM agent_runs WHERE city_key LIKE '%_un';

-- ── 2. Mark orphaned "running" runs as failed ─────────────────────────────────
UPDATE agent_runs
SET
    run_status     = 'failed',
    failure_reason = 'Orphaned — run was killed mid-execution (status never updated)',
    completed_at   = NOW()
WHERE run_status = 'running'
  AND started_at < NOW() - INTERVAL '3 hours';

-- ── 3. Mark orphaned "pending" runs as failed ─────────────────────────────────
-- Pending runs older than 24 hours were never picked up by a worker (Redis queue
-- was cleared or the job expired). Mark them failed so they don't pollute queries.
UPDATE agent_runs
SET
    run_status     = 'failed',
    failure_reason = 'Orphaned — job was never picked up by a worker',
    completed_at   = NOW()
WHERE run_status = 'pending'
  AND started_at < NOW() - INTERVAL '24 hours';

COMMIT;

-- ── Verification queries (run separately after migration) ─────────────────────
-- SELECT city_key, count(*) FROM agent_runs GROUP BY city_key ORDER BY 1;
-- SELECT run_status, count(*) FROM agent_runs GROUP BY run_status ORDER BY 1;
