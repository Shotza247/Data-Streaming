-- flink-jobs/job_02_enrich.sql
--
-- Job 02: Enrichment — JOIN tax-applications with taxpayer-profiles
--
-- Performs a temporal join (FOR SYSTEM_TIME AS OF) so each application
-- record is enriched with the taxpayer profile that was current at the
-- time the application was processed. Writes to PostgreSQL.
--
-- Lineage: kafka://tax-applications + kafka://taxpayer-profiles
--       → postgres://enriched_tax_applications

-- ── Tax applications source ───────────────────────────────────────────────────
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
    proc_time         AS PROCTIME()   -- processing-time attribute for temporal join
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'tax-applications',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id'          = 'flink-job02-enrich',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true'
);

-- ── Taxpayer profiles source (versioned lookup table via processing time) ─────
CREATE TABLE IF NOT EXISTS kafka_taxpayer_profiles (
    customer_id               STRING,
    age                       INT,
    risk_score                DOUBLE,
    historical_filings_count  INT,
    avg_income_3yr            DOUBLE,
    flagged_previously        BOOLEAN,
    _produced_at              STRING,
    proc_time                 AS PROCTIME()
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'taxpayer-profiles',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id'          = 'flink-job02-profiles',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true'
);

-- ── PostgreSQL sink ───────────────────────────────────────────────────────────
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

-- ── Insert: LEFT JOIN on customer_id ─────────────────────────────────────────
-- Using processing-time temporal join pattern — enriches each application
-- with the latest profile snapshot at the time of processing.
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
LEFT JOIN kafka_taxpayer_profiles FOR SYSTEM_TIME AS OF a.proc_time AS p
    ON a.customer_id = p.customer_id;
