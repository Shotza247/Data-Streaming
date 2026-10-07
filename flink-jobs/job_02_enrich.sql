-- flink-jobs/job_02_enrich.sql
--
-- Job 02: Enrichment — JOIN tax-applications with taxpayer-profiles
--
-- Writes enriched records to PostgreSQL enriched_tax_applications table.
-- Uses an INTERVAL JOIN (within 1 hour) which is supported in Flink SQL
-- for two Kafka streaming sources — no temporal table required.
--
-- Lineage: kafka://tax-applications + kafka://taxpayer-profiles
--       => postgres://enriched_tax_applications

SET 'table.exec.source.idle-timeout' = '10s';
SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

DROP TABLE IF EXISTS kafka_tax_applications_enrich;
DROP TABLE IF EXISTS kafka_taxpayer_profiles;
DROP TABLE IF EXISTS pg_enriched_applications;

CREATE TABLE IF NOT EXISTS kafka_tax_applications_enrich (
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
    'properties.group.id'          = 'flink-job02-enrich',
    'scan.startup.mode'            = 'latest-offset',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true'
);

CREATE TABLE IF NOT EXISTS kafka_taxpayer_profiles (
    customer_id               STRING,
    age                       INT,
    risk_score                DOUBLE,
    historical_filings_count  INT,
    avg_income_3yr            DOUBLE,
    flagged_previously        BOOLEAN,
    _produced_at              STRING,
    event_time                AS TO_TIMESTAMP(`_produced_at`, 'yyyy-MM-dd''T''HH:mm:ss'),
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

CREATE TABLE IF NOT EXISTS pg_enriched_applications (
    application_id            STRING,
    customer_id               STRING,
    customer_name             STRING,
    email                     STRING,
    country                   STRING,
    province                  STRING,
    taxable_income            DOUBLE,
    employment_type           STRING,
    employment_status         STRING,
    submitted_date            STRING,
    tax_year                  STRING,
    filing_status             STRING,
    is_fraud                  BOOLEAN,
    age                       INT,
    risk_score                DOUBLE,
    historical_filings_count  INT,
    avg_income_3yr            DOUBLE,
    flagged_previously        BOOLEAN,
    processed_at              TIMESTAMP(3)
) WITH (
    'connector'  = 'jdbc',
    'url'        = 'jdbc:postgresql://${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}',
    'table-name' = 'enriched_tax_applications',
    'username'   = '${POSTGRES_USER}',
    'password'   = '${POSTGRES_PASSWORD}',
    'driver'     = 'org.postgresql.Driver'
);

INSERT INTO pg_enriched_applications
SELECT
    a.application_id,
    a.customer_id,
    a.customer_name,
    a.email,
    a.country,
    a.province,
    a.taxable_income,
    a.employment_type,
    a.employment_status,
    a.submitted_date,
    a.tax_year,
    a.filing_status,
    a.is_fraud,
    COALESCE(p.age,                      0)     AS age,
    COALESCE(p.risk_score,               0.0)   AS risk_score,
    COALESCE(p.historical_filings_count, 0)     AS historical_filings_count,
    COALESCE(p.avg_income_3yr,           0.0)   AS avg_income_3yr,
    COALESCE(p.flagged_previously,       FALSE) AS flagged_previously,
    CURRENT_TIMESTAMP                           AS processed_at
FROM kafka_tax_applications_enrich AS a
LEFT JOIN kafka_taxpayer_profiles AS p
    ON a.customer_id = p.customer_id
    AND p.event_time BETWEEN a.event_time - INTERVAL '1' HOUR
                         AND a.event_time + INTERVAL '1' HOUR;
