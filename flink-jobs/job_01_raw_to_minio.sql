-- flink-jobs/job_01_raw_to_minio.sql
--
-- Job 01: Raw Archival — tax-applications → MinIO raw-tax bucket
--
-- Reads every event from the tax-applications Kafka topic and writes
-- it as-is to MinIO (raw-tax bucket) partitioned by submitted_date.
-- No transformations — pure archival for the data lake layer.
--
-- Lineage: kafka://tax-applications → minio://raw-tax/

-- ── Enable checkpointing so the filesystem/S3 sink actually flushes ────────
-- Without checkpoints the StreamingFileSink buffers forever and never commits.
SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

DROP TABLE IF EXISTS kafka_tax_applications_raw;
DROP TABLE IF EXISTS minio_raw_tax;

-- ── Kafka source ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS kafka_tax_applications_raw (
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
    -- Flink metadata
    event_time AS TO_TIMESTAMP(`_produced_at`, 'yyyy-MM-dd''T''HH:mm:ss'),
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

-- ── MinIO sink (filesystem connector, JSON, partitioned by date) ──────────────
CREATE TABLE IF NOT EXISTS minio_raw_tax (
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
    dt                STRING   -- partition column (date string)
) PARTITIONED BY (dt)
WITH (
    'connector'            = 'filesystem',
    'path'                 = 's3a://${MINIO_BUCKET_RAW}/',
    'format'               = 'json',
    'sink.partition-commit.policy.kind' = 'success-file',
    'sink.partition-commit.delay'       = '1 min'
);

-- ── Insert ────────────────────────────────────────────────────────────────────
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
    SUBSTRING(submitted_date, 1, 10) AS dt   -- YYYY-MM-DD partition
FROM kafka_tax_applications_raw;
