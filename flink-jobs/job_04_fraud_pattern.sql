-- flink-jobs/job_04_fraud_pattern.sql
--
-- Job 04: Flink-Side Fraud Pattern Detection  →  kafka://fraud-signals
--
-- PURPOSE  : Detect suspicious patterns directly in the stream using
--            Flink SQL windowed aggregations, then emit fraud signals
--            back to a Kafka topic for downstream consumption.
--
-- PATTERNS DETECTED:
--   A. HIGH_FREQUENCY_SUBMISSIONS
--      Same customer_id appears 3+ times within a 10-minute window.
--      Signals potential automated filing or identity fraud.
--
--   B. INCOME_SPIKE
--      Single application with taxable_income > 500,000.
--      Signals possible income inflation for fraudulent refund claims.
--
-- TIMESTAMP HANDLING (Flink SQL as cleaning layer):
--   Raw _produced_at: "2026-10-01T13:32:07.539165+00:00"
--   Fix: REPLACE(SUBSTRING(...,1,19), 'T', ' ')  → "2026-10-01 13:32:07"
--        then TO_TIMESTAMP() → TIMESTAMP(3) for windowing
--
-- Lineage:  kafka://tax-applications  →  kafka://fraud-signals

SET 'table.exec.source.idle-timeout'   = '10s';
SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode'     = 'EXACTLY_ONCE';

DROP TABLE IF EXISTS kafka_tax_applications_fraud;
DROP TABLE IF EXISTS kafka_fraud_signals_out;

-- ── Source: tax application stream ───────────────────────────────────────────
CREATE TABLE kafka_tax_applications_fraud (
    application_id    STRING,
    customer_id       STRING,
    province          STRING,
    taxable_income    DOUBLE,
    employment_type   STRING,
    is_fraud          BOOLEAN,
    _produced_at      STRING,
    -- Timestamp cleaning pipeline (same as all other jobs)
    event_time AS TO_TIMESTAMP(
        REPLACE(SUBSTRING(`_produced_at`, 1, 19), 'T', ' '),
        'yyyy-MM-dd HH:mm:ss'
    ),
    WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'tax-applications',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id'          = 'flink-job04-fraud',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true'
);

-- ── Sink: fraud-signals Kafka topic ──────────────────────────────────────────
CREATE TABLE kafka_fraud_signals_out (
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
    'format'                       = 'json'
);

-- ── Pattern A: High-frequency submitter (3+ submissions in 10 minutes) ────────
-- Group by customer in a 10-minute tumbling window.
-- Flink SQL cleans the detected_at timestamp inline.
INSERT INTO kafka_fraud_signals_out
SELECT
    CONCAT(
        'flink-hf-',
        REPLACE(CAST(TUMBLE_START(event_time, INTERVAL '10' MINUTE) AS STRING), ' ', 'T'),
        '-',
        customer_id
    )                                                    AS signal_id,
    'MULTIPLE'                                           AS application_id,
    customer_id,
    'HIGH_FREQUENCY_SUBMISSIONS'                         AS signal_type,
    'HIGH'                                               AS severity,
    -- detected_at: clean ISO-8601 string from window start
    REPLACE(
        CAST(TUMBLE_START(event_time, INTERVAL '10' MINUTE) AS STRING),
        ' ', 'T'
    )                                                    AS detected_at,
    CONCAT(
        'Customer submitted ',
        CAST(COUNT(*) AS STRING),
        ' applications in 10-min window starting ',
        CAST(TUMBLE_START(event_time, INTERVAL '10' MINUTE) AS STRING)
    )                                                    AS description
FROM kafka_tax_applications_fraud
GROUP BY customer_id, TUMBLE(event_time, INTERVAL '10' MINUTE)
HAVING COUNT(*) >= 3;

-- ── Pattern B: Income spike (taxable_income > 500,000) ────────────────────────
-- Row-level filter — no windowing needed.
-- detected_at is the cleaned producer timestamp.
INSERT INTO kafka_fraud_signals_out
SELECT
    CONCAT('flink-spike-', TRIM(application_id))        AS signal_id,
    TRIM(application_id)                                 AS application_id,
    TRIM(customer_id)                                    AS customer_id,
    'INCOME_SPIKE'                                       AS signal_type,
    'MEDIUM'                                             AS severity,
    -- Clean the raw timestamp: strip microseconds + TZ offset
    REPLACE(SUBSTRING(_produced_at, 1, 19), 'T', ' ')  AS detected_at,
    CONCAT(
        'Taxable income R',
        CAST(CAST(taxable_income AS DECIMAL(15,2)) AS STRING),
        ' exceeds high-value threshold of R500,000'
    )                                                    AS description
FROM kafka_tax_applications_fraud
WHERE taxable_income > 500000.0;
