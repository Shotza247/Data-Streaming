-- flink-jobs/job_05_cleaned.sql
--
-- Job 05: Cleaned & Standardised Output  →  MinIO cleaned-tax/
--
-- PURPOSE  : This is the core DATA QUALITY job. Flink SQL is the
--            transformation layer — not just a pipe. Every field gets
--            type-safe parsing, canonical casing, and null guards
--            before landing in the data lake.
--
-- TRANSFORMATIONS APPLIED (Flink SQL as the cleaning engine):
--
--   TIMESTAMPS
--     Raw: "2026-10-01T13:32:07.539165+00:00"  (ISO-8601, microseconds, TZ offset)
--     Fix: SUBSTRING(_produced_at, 1, 19)       → "2026-10-01T13:32:07"
--          REPLACE(..., 'T', ' ')               → "2026-10-01 13:32:07"
--          stored as clean STRING 'yyyy-MM-dd HH:mm:ss'
--
--   DATES
--     Raw: "2026-03-30"  (may have trailing spaces or extra chars)
--     Fix: SUBSTRING(submitted_date, 1, 10)     → safe YYYY-MM-DD slice
--
--   STRINGS  (all fields)
--     TRIM()  strips leading/trailing whitespace
--
--   CASE NORMALISATION  (categorical fields → canonical UPPERCASE)
--     country, province, employment_type, employment_status, filing_status
--
--   EMAIL normalisation
--     LOWER(TRIM(email))  → enforces lowercase for deduplication
--
--   NUMERICS
--     taxable_income DOUBLE  → kept as-is (already clean from JSON)
--
--   NULL GUARDS
--     WHERE filters drop records with null primary keys or negative income
--
--   AUDIT COLUMN
--     flink_processed_at  — when Flink wrote this row (wall-clock time)
--
-- Lineage:  kafka://tax-applications  →  minio://cleaned-tax/

SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode'     = 'EXACTLY_ONCE';
SET 'table.exec.source.idle-timeout'   = '10s';

DROP TABLE IF EXISTS kafka_tax_applications_clean;
DROP TABLE IF EXISTS minio_cleaned_tax;

-- ── Source: raw Kafka stream ──────────────────────────────────────────────────
-- All raw fields ingested as-is; computed columns do the cleaning.
CREATE TABLE kafka_tax_applications_clean (
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
    -- ── Timestamp cleaning (computed column) ──────────────────────────────────
    -- Step 1: slice to 19 chars → "2026-10-01T13:32:07"
    -- Step 2: replace 'T' with space → "2026-10-01 13:32:07"
    -- Step 3: parse as TIMESTAMP for watermark arithmetic
    event_time AS TO_TIMESTAMP(
        REPLACE(SUBSTRING(`_produced_at`, 1, 19), 'T', ' '),
        'yyyy-MM-dd HH:mm:ss'
    ),
    WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'tax-applications',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id'          = 'flink-job05-clean',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true'
);

-- ── Sink: MinIO cleaned-tax bucket, partitioned by date ──────────────────────
CREATE TABLE minio_cleaned_tax (
    application_id     STRING,
    customer_id        STRING,
    customer_name      STRING,
    email              STRING,
    country            STRING,
    province           STRING,
    taxable_income     DOUBLE,
    employment_type    STRING,
    employment_status  STRING,
    submitted_date     STRING,
    tax_year           STRING,
    filing_status      STRING,
    is_fraud           BOOLEAN,
    produced_at        STRING,
    flink_processed_at STRING,
    dt                 STRING
) PARTITIONED BY (dt)
WITH (
    'connector'                             = 'filesystem',
    'path'                                  = 's3a://${MINIO_BUCKET_CLEANED}/',
    'format'                                = 'json',
    'sink.partition-commit.policy.kind'     = 'success-file',
    'sink.partition-commit.delay'           = '1 min'
);

-- ── Cleaning INSERT: every transformation is explicit and documented ──────────
INSERT INTO minio_cleaned_tax
SELECT
    -- Primary keys: trim only (preserve exact UUID)
    TRIM(application_id)                                                AS application_id,
    TRIM(customer_id)                                                   AS customer_id,

    -- Display name: trim whitespace
    TRIM(customer_name)                                                 AS customer_name,

    -- Email: lowercase + trim for deduplication consistency
    LOWER(TRIM(email))                                                  AS email,

    -- Geography: UPPER for canonical lookup (e.g. 'south africa' → 'SOUTH AFRICA')
    UPPER(TRIM(country))                                                AS country,
    UPPER(TRIM(province))                                               AS province,

    -- Income: kept as DOUBLE (already numeric from JSON decoder)
    taxable_income,

    -- Employment categoricals: UPPER for consistent enum values
    UPPER(TRIM(employment_type))                                        AS employment_type,
    UPPER(TRIM(employment_status))                                      AS employment_status,

    -- Date: safe YYYY-MM-DD slice (strips time if present, trims extras)
    SUBSTRING(TRIM(submitted_date), 1, 10)                             AS submitted_date,

    -- Fiscal year: trim only (e.g. '2024/25' kept as-is — SA standard)
    TRIM(tax_year)                                                      AS tax_year,

    -- Filing status: UPPER for canonical enum
    UPPER(TRIM(filing_status))                                          AS filing_status,

    -- Fraud flag: pass through
    is_fraud,

    -- Cleaned producer timestamp:
    --   Raw:   "2026-10-01T13:32:07.539165+00:00"
    --   Clean: "2026-10-01 13:32:07"  (19-char UTC, no microseconds, no offset)
    REPLACE(SUBSTRING(_produced_at, 1, 19), 'T', ' ')                  AS produced_at,

    -- Flink processing timestamp (wall-clock, when this row was transformed)
    CAST(CURRENT_TIMESTAMP AS STRING)                                   AS flink_processed_at,

    -- Partition key: YYYY-MM-DD from cleaned submitted_date
    SUBSTRING(TRIM(submitted_date), 1, 10)                             AS dt

FROM kafka_tax_applications_clean
WHERE
    -- Data quality gates: drop structurally invalid records
    application_id IS NOT NULL
    AND customer_id IS NOT NULL
    AND taxable_income IS NOT NULL
    AND taxable_income >= 0.0
    AND CHAR_LENGTH(TRIM(submitted_date)) >= 10;
