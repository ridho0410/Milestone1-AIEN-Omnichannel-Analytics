CREATE SCHEMA IF NOT EXISTS bronze;
CREATE TABLE IF NOT EXISTS bronze.ingestion_runs (
    pipeline_run_id TEXT PRIMARY KEY,
    raw_root TEXT,
    manifest_present BOOLEAN NOT NULL DEFAULT FALSE,
    manifest_valid BOOLEAN NOT NULL DEFAULT FALSE,
    files_expected INTEGER,
    files_found INTEGER,
    missing_sources TEXT [],
    started_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at_utc TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'running',
    notes TEXT
);
CREATE TABLE IF NOT EXISTS bronze.source_files (
    source_file TEXT PRIMARY KEY,
    source_family TEXT NOT NULL,
    file_format TEXT NOT NULL,
    checksum_sha256 TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    manifest_checksum TEXT,
    manifest_status TEXT NOT NULL DEFAULT 'not_checked',
    pipeline_run_id TEXT NOT NULL,
    loaded_at_utc TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS bronze.raw_records (
    bronze_record_id BIGSERIAL PRIMARY KEY,
    pipeline_run_id TEXT NOT NULL,
    source_family TEXT NOT NULL,
    source_file TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    source_record_id TEXT,
    checksum_sha256 TEXT NOT NULL,
    payload JSONB NOT NULL,
    ingested_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT bronze_raw_records_file_row_unique UNIQUE (source_file, source_row_number)
);
CREATE INDEX IF NOT EXISTS idx_bronze_raw_records_file ON bronze.raw_records (source_file);
CREATE INDEX IF NOT EXISTS idx_bronze_raw_records_run ON bronze.raw_records (pipeline_run_id);