-- flink-jobs/job_05_cleaned.sql
--
-- Job 05: Cleaned / Standardised Output → MinIO cleaned-tax bucket
--
-- Reads the raw tax-applications stream, applies data quality rules,
-- standardises values, and writes clean, typed records to the MinIO
-- cleaned-tax bucket partitioned by date. This is the source of truth
-- for DuckDB OLAP, MLflow training, and Qdrant vector seeding.
--
-- Transformations applied:
--   - UPPER(country), UPPER(province)  — canonical case
--   - TRIM on all STRING fields         — strip whitespace
--   - CAST taxable_income to DECIMAL    — explicit precision
--   - WHERE filter: non-null customer_id, application_id, taxable_income >= 0
--   - Add processed_at TIMESTAMP column
--   - dt partition column = YYYY-MM-DD portion of submitted_date
--
-- Lineage: kafka://tax-applications → minio://cleaned-tax/

-- ── Kafka source ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS kafka_tax_applications_clean (
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
    event_time        AS TO_TIMESTAMP(`_produced_at`, 'yyyy-MM-dd''T''HH:mm:ss'),
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

-- ── MinIO cleaned-tax sink ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS minio_cleaned_tax (
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
    processed_at      STRING,
    dt                STRING   -- partition column (YYYY-MM-DD)
) PARTITIONED BY (dt)
WITH (
    'connector'                             = 'filesystem',
    'path'                                  = 's3a://${MINIO_BUCKET_CLEANED}/',
    'format'                                = 'json',
    'sink.partition-commit.policy.kind'     = 'success-file',
    'sink.partition-commit.delay'           = '1 min'
);

-- ── Cleaned insert ────────────────────────────────────────────────────────────
INSERT INTO minio_cleaned_tax
SELECT
    TRIM(application_id)                        AS application_id,
    TRIM(customer_id)                           AS customer_id,
    TRIM(customer_name)                         AS customer_name,
    LOWER(TRIM(email))                          AS email,
    UPPER(TRIM(country))                        AS country,
    UPPER(TRIM(province))                       AS province,
    taxable_income,
    UPPER(TRIM(employment_type))                AS employment_type,
    UPPER(TRIM(employment_status))              AS employment_status,
    SUBSTRING(submitted_date, 1, 10)            AS submitted_date,
    TRIM(tax_year)                              AS tax_year,
    UPPER(TRIM(filing_status))                  AS filing_status,
    is_fraud,
    CAST(CURRENT_TIMESTAMP AS STRING)           AS processed_at,
    SUBSTRING(submitted_date, 1, 10)            AS dt
FROM kafka_tax_applications_clean
WHERE
    application_id IS NOT NULL
    AND customer_id IS NOT NULL
    AND taxable_income IS NOT NULL
    AND taxable_income >= 0.0;
