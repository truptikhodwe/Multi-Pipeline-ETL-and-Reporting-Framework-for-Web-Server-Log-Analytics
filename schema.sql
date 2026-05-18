-- =============================================================
-- ETL Project – PostgreSQL Schema
-- Multi-Pipeline Reporting Framework
-- =============================================================

-- Run metadata (one row per pipeline × query × batch execution)
CREATE TABLE IF NOT EXISTS runs (
    run_id                 SERIAL PRIMARY KEY,
    pipeline_name          VARCHAR(50)      NOT NULL,
    query_name             VARCHAR(50)      NOT NULL,
    batch_id               INT              NOT NULL,
    batch_size             INT              NOT NULL,
    avg_batch_size         DOUBLE PRECISION NOT NULL,
    records_processed      INT              NOT NULL,
    malformed_record_count INT              NOT NULL,
    runtime_ms             BIGINT           NOT NULL,
    executed_at            TIMESTAMP        NOT NULL DEFAULT NOW()
);

-- Per-batch source details
CREATE TABLE IF NOT EXISTS batch_metadata (
    id           SERIAL PRIMARY KEY,
    run_id       INT          NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    batch_id     INT          NOT NULL,
    batch_label  VARCHAR(100) NOT NULL,
    source_file  VARCHAR(255) NOT NULL,
    record_count INT          NOT NULL
);

-- Malformed record summary per run/batch
CREATE TABLE IF NOT EXISTS malformed_records_summary (
    id              SERIAL PRIMARY KEY,
    run_id          INT              NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    batch_id        INT              NOT NULL,
    malformed_count INT              NOT NULL DEFAULT 0,
    total_count     INT              NOT NULL DEFAULT 0,
    malformed_pct   DOUBLE PRECISION NOT NULL DEFAULT 0.0
);

-- Query 1: Daily Traffic Summary
CREATE TABLE IF NOT EXISTS query1_results (
    id            SERIAL PRIMARY KEY,
    run_id        INT              NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    pipeline_name VARCHAR(50)      NOT NULL,
    batch_id      INT              NOT NULL,
    executed_at   TIMESTAMP        NOT NULL,
    log_date      VARCHAR(20)      NOT NULL,
    status_code   INT              NOT NULL,
    request_count BIGINT           NOT NULL,
    total_bytes   BIGINT           NOT NULL
);

-- Query 2: Top 20 Requested Resources
CREATE TABLE IF NOT EXISTS query2_results (
    id                  SERIAL PRIMARY KEY,
    run_id              INT              NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    pipeline_name       VARCHAR(50)      NOT NULL,
    batch_id            INT              NOT NULL,
    executed_at         TIMESTAMP        NOT NULL,
    resource_path       VARCHAR(1024)    NOT NULL,
    request_count       BIGINT           NOT NULL,
    total_bytes         BIGINT           NOT NULL,
    distinct_host_count BIGINT           NOT NULL
);

-- Query 3: Hourly Error Analysis
CREATE TABLE IF NOT EXISTS query3_results (
    id                   SERIAL PRIMARY KEY,
    run_id               INT              NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    pipeline_name        VARCHAR(50)      NOT NULL,
    batch_id             INT              NOT NULL,
    executed_at          TIMESTAMP        NOT NULL,
    log_date             VARCHAR(20)      NOT NULL,
    log_hour             INT              NOT NULL,
    error_request_count  BIGINT           NOT NULL,
    total_request_count  BIGINT           NOT NULL,
    error_rate           DOUBLE PRECISION NOT NULL,
    distinct_error_hosts BIGINT           NOT NULL
);
