-- flink-jobs/job_06_fraud_signals_to_pg.sql
--
-- Job 06: Fraud Signals Kafka  →  PostgreSQL fraud_detections
--
-- PURPOSE  : Consume the fraud-signals topic (populated by both:
--              a) the 4 producers (synthetic signals)
--              b) job_04 Flink-side pattern detection signals)
--            and sink every signal into the PostgreSQL fraud_detections
--            table for dashboard queries and audit.
--
-- SCHEMA MAPPING (Sub-Task 12):
--   fraud_detections PK = detection_id  (maps from signal_id)
--   source field = 'FLINK'              (satisfies CHECK constraint)
--   detected_at = TIMESTAMP from string normalisation
--   severity CHECK: LOW | MEDIUM | HIGH | CRITICAL
--
-- Lineage:  kafka://fraud-signals  →  postgres://fraud_detections
--
-- Sub-Task 12 — new job to populate fraud_detections table.

SET 'table.exec.source.idle-timeout'   = '10s';
SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode'     = 'EXACTLY_ONCE';

DROP TABLE IF EXISTS kafka_fraud_signals_in;
DROP TABLE IF EXISTS pg_fraud_detections_sink;

-- ── Source: fraud-signals Kafka topic ────────────────────────────────────────
CREATE TABLE kafka_fraud_signals_in (
    signal_id      STRING,
    application_id STRING,
    customer_id    STRING,
    signal_type    STRING,
    severity       STRING,
    detected_at    STRING,
    description    STRING
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'fraud-signals',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id'          = 'flink-job06-fraud-pg',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true'
);

-- ── Sink: PostgreSQL fraud_detections ────────────────────────────────────────
-- Columns must match the table DDL exactly:
--   detection_id  VARCHAR(128) PK
--   application_id, customer_id, signal_type, severity, source VARCHAR
--   description   TEXT
--   detected_at   TIMESTAMP (Flink JDBC maps TIMESTAMP(3) → timestamptz)
--   resolved      BOOLEAN (default false, omitted = use DB default)
CREATE TABLE pg_fraud_detections_sink (
    detection_id   STRING,
    application_id STRING,
    customer_id    STRING,
    signal_type    STRING,
    severity       STRING,
    source         STRING,
    description    STRING,
    detected_at    TIMESTAMP(3)
) WITH (
    'connector'  = 'jdbc',
    'url'        = 'jdbc:postgresql://${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}',
    'table-name' = 'fraud_detections',
    'username'   = '${POSTGRES_USER}',
    'password'   = '${POSTGRES_PASSWORD}',
    'driver'     = 'org.postgresql.Driver'
);

-- ── Stream all fraud signals from Kafka into PostgreSQL ───────────────────────
-- signal_id → detection_id (PK)
-- severity normalisation: map any unknown value to 'HIGH' (satisfies CHECK)
-- source is always 'FLINK' for this pipeline leg
INSERT INTO pg_fraud_detections_sink
SELECT
    -- detection_id: use signal_id, fallback to UUID-like concat
    COALESCE(TRIM(signal_id), CONCAT('flink-sig-', CAST(FLOOR(RAND() * 1000000) AS STRING))) AS detection_id,
    COALESCE(TRIM(application_id), 'UNKNOWN')                               AS application_id,
    COALESCE(TRIM(customer_id),    'UNKNOWN')                               AS customer_id,
    UPPER(COALESCE(TRIM(signal_type), 'UNKNOWN'))                           AS signal_type,
    -- Normalise severity to CHECK constraint values: LOW/MEDIUM/HIGH/CRITICAL
    CASE UPPER(TRIM(severity))
        WHEN 'LOW'      THEN 'LOW'
        WHEN 'MEDIUM'   THEN 'MEDIUM'
        WHEN 'CRITICAL' THEN 'CRITICAL'
        ELSE 'HIGH'
    END                                                                     AS severity,
    -- All signals from this job are FLINK-detected
    'FLINK'                                                                 AS source,
    COALESCE(TRIM(description), 'No description')                           AS description,
    -- Convert normalised string to TIMESTAMP(3)
    TO_TIMESTAMP(
        REPLACE(SUBSTRING(COALESCE(detected_at, '2000-01-01T00:00:00'), 1, 19), 'T', ' '),
        'yyyy-MM-dd HH:mm:ss'
    )                                                                       AS detected_at
FROM kafka_fraud_signals_in
WHERE signal_id IS NOT NULL
  AND customer_id IS NOT NULL;
