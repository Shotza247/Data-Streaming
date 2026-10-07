-- flink-jobs/job_01_raw_to_minio.sql
--
-- Job 01: Raw Archival  —  kafka:tax-applications  →  MinIO raw-tax/
--
-- PURPOSE  : Archive every inbound event as-is (schema-on-read lake layer).
--            No cleaning. No filtering. Partitioned by event date.
--
-- TIMESTAMP HANDLING (Flink SQL as cleaning layer):
--   Producer emits ISO-8601 with microseconds + UTC offset, e.g.
--     "2026-10-01T13:32:07.539165+00:00"
--   We use SUBSTRING(..., 1, 19) to strip microseconds and offset,
--   yielding a clean "yyyy-MM-dd'T'HH:mm:ss" string that
--   TO_TIMESTAMP() can parse deterministically.
--
-- Lineage:  kafka://tax-applications  →  minio://raw-tax/

SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode'     = 'EXACTLY_ONCE';
SET 'table.exec.source.idle-timeout'   = '10s';

DROP TABLE IF EXISTS kafka_tax_applications_raw;
DROP TABLE IF EXISTS minio_raw_tax;

-- ── Source: raw Kafka events (all fields kept as STRING where ambiguous) ──────
CREATE TABLE kafka_tax_applications_raw (
    application_id    STRING,
    customer_id       STRING,
    customer_name     STRING,
    email             STRING,
    country           STRING,
    province          STRING,
    taxable_income    DOUBLE,
    employment_type   STRING,
    employment_status STRING,
    submitted_date    STRING,
    tax_year          STRING,
    filing_status     STRING,
    is_fraud          BOOLEAN,
    _produced_at      STRING,
    -- Flink SQL cleans the timestamp: strip microseconds + TZ offset first
    event_time AS TO_TIMESTAMP(SUBSTRING(`_produced_at`, 1, 19), 'yyyy-MM-dd''T''HH:mm:ss'),
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'tax-applications',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id'          = 'flink-job01-raw',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true'
);

-- ── Sink: MinIO raw-tax bucket, partitioned by event date ────────────────────
CREATE TABLE minio_raw_tax (
    application_id    STRING,
    customer_id       STRING,
    customer_name     STRING,
    email             STRING,
    country           STRING,
    province          STRING,
    taxable_income    DOUBLE,
    employment_type   STRING,
    employment_status STRING,
    submitted_date    STRING,
    tax_year          STRING,
    filing_status     STRING,
    is_fraud          BOOLEAN,
    _produced_at      STRING,
    dt                STRING
) PARTITIONED BY (dt)
WITH (
    'connector'                             = 'filesystem',
    'path'                                  = 's3a://${MINIO_BUCKET_RAW}/',
    'format'                                = 'json',
    'sink.partition-commit.policy.kind'     = 'success-file',
    'sink.partition-commit.delay'           = '1 min'
);

-- ── Insert: archive with date partition derived from event_time ───────────────
INSERT INTO minio_raw_tax
SELECT
    application_id,
    customer_id,
    customer_name,
    email,
    country,
    province,
    taxable_income,
    employment_type,
    employment_status,
    submitted_date,
    tax_year,
    filing_status,
    is_fraud,
    _produced_at,
    -- partition key: first 10 chars of submitted_date (YYYY-MM-DD)
    SUBSTRING(submitted_date, 1, 10)  AS dt
FROM kafka_tax_applications_raw;
