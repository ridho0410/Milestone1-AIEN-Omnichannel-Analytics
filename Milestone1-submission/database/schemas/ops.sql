CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    pipeline_run_id TEXT PRIMARY KEY,
    dag_id TEXT,
    triggered_by TEXT,
    env_target TEXT NOT NULL DEFAULT 'local',
    status TEXT NOT NULL DEFAULT 'running',
    started_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at_utc TIMESTAMPTZ,
    duration_ms BIGINT,
    current_stage TEXT,
    error_message TEXT,
    metrics JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE TABLE IF NOT EXISTS ops.pipeline_stages (
    stage_id BIGSERIAL PRIMARY KEY,
    pipeline_run_id TEXT NOT NULL,
    stage_name TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at_utc TIMESTAMPTZ,
    duration_ms BIGINT,
    rows_affected BIGINT,
    details JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_ops_pipeline_stages_run ON ops.pipeline_stages (pipeline_run_id, stage_name);
CREATE TABLE IF NOT EXISTS ops.quality_checks (
    check_id BIGSERIAL PRIMARY KEY,
    pipeline_run_id TEXT NOT NULL,
    check_name TEXT NOT NULL,
    layer TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'error',
    status TEXT NOT NULL,
    observed_value NUMERIC(18, 4),
    expected_value NUMERIC(18, 4),
    details TEXT,
    checked_at_utc TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ops_quality_checks_run ON ops.quality_checks (pipeline_run_id, layer, status);
CREATE TABLE IF NOT EXISTS ops.reconciliation_results (
    reconciliation_id BIGSERIAL PRIMARY KEY,
    pipeline_run_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    left_source TEXT NOT NULL,
    right_source TEXT NOT NULL,
    left_value NUMERIC(18, 4),
    right_value NUMERIC(18, 4),
    variance NUMERIC(18, 4),
    tolerance NUMERIC(18, 4) NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    checked_at_utc TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ops_reconciliation_run ON ops.reconciliation_results (pipeline_run_id, status);