-- Migration 010: Continuous pipeline support
--
-- Adds columns that distinguish full discovery runs from lightweight
-- refresh runs triggered by the scheduler cron jobs:
--
--   run_mode  — "full" | "enrichment_refresh" | "discovery_pass"
--              Controls which graph path executes (skip collection in refresh).
--   run_type  — human-readable label stored for filtering in dashboards.
--   scheduled — True if triggered by cron scheduler vs. manual API call.
--
-- Also creates the continuous_schedule table so the operator can inspect
-- and override per-city cadences from the API without a code deploy.

-- ── 1. Add run_mode + run_type + scheduled to agent_runs ────────────────────

ALTER TABLE agent_runs
  ADD COLUMN IF NOT EXISTS run_mode    TEXT    NOT NULL DEFAULT 'full'
    CHECK (run_mode IN ('full', 'enrichment_refresh', 'discovery_pass')),
  ADD COLUMN IF NOT EXISTS run_type    TEXT    NOT NULL DEFAULT 'manual',
  ADD COLUMN IF NOT EXISTS scheduled   BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN agent_runs.run_mode IS
  'Pipeline execution mode: full=all phases, enrichment_refresh=skip collection, discovery_pass=full weekly discovery';
COMMENT ON COLUMN agent_runs.run_type IS
  'Human-readable label: manual | enrichment_refresh | weekly_discovery';
COMMENT ON COLUMN agent_runs.scheduled IS
  'True if this run was triggered automatically by the scheduler cron';

-- ── 2. Create continuous_schedule table ─────────────────────────────────────
-- Stores per-city cadence overrides. Defaults are coded in scheduler.py.
-- Operator can disable scheduling for a city without a code deploy.

CREATE TABLE IF NOT EXISTS continuous_schedule (
  city_key             TEXT        PRIMARY KEY,          -- e.g. "philadelphia_us"
  city_name            TEXT        NOT NULL,
  country_or_region    TEXT        NOT NULL DEFAULT 'United States',

  -- Enrichment refresh: re-enrich known entities, re-infer relationships
  enrichment_refresh_enabled   BOOLEAN   NOT NULL DEFAULT TRUE,
  enrichment_refresh_interval  INTEGER   NOT NULL DEFAULT 6,  -- hours

  -- Discovery pass: full collection + new entity search
  discovery_pass_enabled       BOOLEAN   NOT NULL DEFAULT TRUE,
  discovery_pass_weekday       SMALLINT  NOT NULL DEFAULT 0,   -- 0=Monday

  -- Operational metadata
  last_enrichment_refresh_at   TIMESTAMPTZ,
  last_discovery_pass_at       TIMESTAMPTZ,
  last_enrichment_run_id       UUID,
  last_discovery_run_id        UUID,

  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE continuous_schedule IS
  'Per-city scheduling configuration for the continuous OSINT pipeline';

-- Insert Philadelphia default schedule
INSERT INTO continuous_schedule (
  city_key, city_name, country_or_region,
  enrichment_refresh_enabled, enrichment_refresh_interval,
  discovery_pass_enabled, discovery_pass_weekday
) VALUES (
  'philadelphia_us', 'Philadelphia', 'United States',
  TRUE, 6,
  TRUE, 0
) ON CONFLICT (city_key) DO NOTHING;

-- ── 3. Index on (run_mode, scheduled) for dashboard queries ─────────────────
CREATE INDEX IF NOT EXISTS idx_agent_runs_run_mode
  ON agent_runs (run_mode, run_status);

CREATE INDEX IF NOT EXISTS idx_agent_runs_scheduled
  ON agent_runs (scheduled, triggered_at DESC);
