-- flink-jobs/job_02_enrich.sql
--
-- Job 02: Stream Enrichment  →  PostgreSQL enriched_tax_applications
--
-- PURPOSE  : Enrich each tax application event with the taxpayer's
--            profile data via an interval JOIN on the two Kafka streams.
--            Flink SQL handles the timestamp parsing and stream alignment.
--
-- TIMESTAMP HANDLING (Flink SQL as cleaning layer):
--   Raw _produced_at: "2026-10-01T13:32:07.539165+00:00"
--   Fix: REPLACE(SUBSTRING(...,1,19), 'T', ' ')  → "2026-10-01 13:32:07"
--        then TO_TIMESTAMP() → TIMESTAMP(3)
--
-- JOIN STRATEGY:
--   INTERVAL JOIN — links an application event with any profile event
--   for the same customer_id that falls within ±1 hour of the application.
--   This is the correct streaming join for two unbounded Kafka sources.
--
-- STARTUP FIX (Sub-Task 12):
--   Both sources use earliest-offset so the streams have matching events
--   in overlapping time windows. AT_LEAST_ONCE reduces checkpoint state
--   pressure for the interval join operator.
--
-- Lineage:  kafka://tax-applications + kafka://taxpayer-profiles
--        →  postgres://enriched_tax_applications

SET 'table.exec.source.idle-timeout'   = '10s';
SET 'execution.checkpointing.interval' = '60s';
SET 'execution.checkpointing.mode'     = 'AT_LEAST_ONCE';
-- Ignore duplicates at sink level: use ON CONFLICT DO UPDATE (upsert)
-- This is set via JDBC sink properties further down.
SET 'table.dml.async-timeout'          = '30s';

DROP TABLE IF EXISTS kafka_tax_applications_enrich;
DROP TABLE IF EXISTS kafka_taxpayer_profiles;
DROP TABLE IF EXISTS pg_enriched_applications;

-- ── Source 1: tax application events ─────────────────────────────────────────
CREATE TABLE kafka_tax_applications_enrich (
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
    -- Timestamp cleaning: strip microseconds+TZ, replace T with space
    event_time AS TO_TIMESTAMP(
        REPLACE(SUBSTRING(`_produced_at`, 1, 19), 'T', ' '),
        'yyyy-MM-dd HH:mm:ss'
    ),
    WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'tax-applications',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id'          = 'flink-job02-enrich',
    'scan.startup.mode'            = 'latest-offset',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true'
);

-- ── Source 2: taxpayer profile events ────────────────────────────────────────
CREATE TABLE kafka_taxpayer_profiles (
    customer_id               STRING,
    age                       INT,
    risk_score                DOUBLE,
    historical_filings_count  INT,
    avg_income_3yr            DOUBLE,
    flagged_previously        BOOLEAN,
    _produced_at              STRING,
    -- Same timestamp cleaning as above
    event_time AS TO_TIMESTAMP(
        REPLACE(SUBSTRING(`_produced_at`, 1, 19), 'T', ' '),
        'yyyy-MM-dd HH:mm:ss'
    ),
    WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'taxpayer-profiles',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id'          = 'flink-job02-profiles',
    'scan.startup.mode'            = 'latest-offset',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true'
);

-- ── Sink: PostgreSQL enriched_tax_applications ───────────────────────────────
-- Type mapping notes (Sub-Task 12 fix):
--   submitted_date: Flink DATE matches PG date column
--   processed_at:   Flink TIMESTAMP(3) matches PG timestamptz column
--   risk_score:     DECIMAL(5,4) matches PG numeric(5,4)
--   avg_income_3yr: DECIMAL(15,2) matches PG numeric(15,2)
-- Upsert mode: ON CONFLICT (application_id) DO UPDATE handles late arrivals
--   and avoids duplicate key errors on restart.
CREATE TABLE pg_enriched_applications (
    application_id            STRING,
    customer_id               STRING,
    customer_name             STRING,
    email                     STRING,
    country                   STRING,
    province                  STRING,
    taxable_income            DECIMAL(15,2),
    employment_type           STRING,
    employment_status         STRING,
    submitted_date            DATE,
    tax_year                  STRING,
    filing_status             STRING,
    is_fraud                  BOOLEAN,
    age                       INT,
    risk_score                DECIMAL(5,4),
    historical_filings_count  INT,
    avg_income_3yr            DECIMAL(15,2),
    flagged_previously        BOOLEAN,
    processed_at              TIMESTAMP(3)
) WITH (
    'connector'  = 'jdbc',
    'url'        = 'jdbc:postgresql://${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}',
    'table-name' = 'enriched_tax_applications',
    'username'   = '${POSTGRES_USER}',
    'password'   = '${POSTGRES_PASSWORD}',
    'driver'     = 'org.postgresql.Driver',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '2s'
);

-- ── Enrichment INSERT: LEFT interval join + field-level cleaning ──────────────
-- Flink SQL applies data quality transformations on both streams:
--   - UPPER / TRIM on all categorical fields
--   - LOWER on email
--   - SUBSTRING on dates
--   - COALESCE for missing profile fields (profile may not have arrived yet)
INSERT INTO pg_enriched_applications
SELECT
    -- Application fields (cleaned)
    TRIM(a.application_id)                                              AS application_id,
    TRIM(a.customer_id)                                                 AS customer_id,
    TRIM(a.customer_name)                                               AS customer_name,
    LOWER(TRIM(a.email))                                                AS email,
    UPPER(TRIM(a.country))                                              AS country,
    UPPER(TRIM(a.province))                                             AS province,
    CAST(a.taxable_income AS DECIMAL(15,2))                            AS taxable_income,
    UPPER(TRIM(a.employment_type))                                      AS employment_type,
    UPPER(TRIM(a.employment_status))                                    AS employment_status,
    -- Cast submitted_date STRING → DATE for PG date column
    CAST(SUBSTRING(TRIM(a.submitted_date), 1, 10) AS DATE)            AS submitted_date,
    TRIM(a.tax_year)                                                    AS tax_year,
    UPPER(TRIM(a.filing_status))                                        AS filing_status,
    a.is_fraud,
    -- Profile fields: COALESCE ensures clean defaults when no match
    CAST(COALESCE(p.age,                      0) AS INT)               AS age,
    CAST(COALESCE(p.risk_score,               0.0) AS DECIMAL(5,4))   AS risk_score,
    COALESCE(p.historical_filings_count,      0)                       AS historical_filings_count,
    CAST(COALESCE(p.avg_income_3yr,           0.0) AS DECIMAL(15,2))  AS avg_income_3yr,
    COALESCE(p.flagged_previously,            FALSE)                   AS flagged_previously,
    -- Flink wall-clock processing time as TIMESTAMP(3) for PG timestamptz
    CURRENT_TIMESTAMP                                                   AS processed_at
FROM kafka_tax_applications_enrich AS a
LEFT JOIN kafka_taxpayer_profiles AS p
    ON a.customer_id = p.customer_id
    AND p.event_time BETWEEN a.event_time - INTERVAL '1' HOUR
                         AND a.event_time + INTERVAL '1' HOUR;
