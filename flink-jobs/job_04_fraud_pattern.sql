-- flink-jobs/job_04_fraud_pattern.sql
--
-- Job 04: Flink-Side Fraud Pattern Detection
--
-- Detects two fraud patterns from the tax-applications stream and emits
-- new signals back to the fraud-signals Kafka topic:
--
--   Pattern A — High-frequency submitter: same customer_id appears
--               3+ times within a 10-minute event-time window.
--   Pattern B — Income spike: taxable_income > 500 000 (high-value outlier).
--
-- MATCH_RECOGNIZE is the cleanest Flink SQL CEP approach, but requires
-- the COMPLEX EVENT PROCESSING Flink CEP library on the classpath.
-- As an equivalent that works with the standard connector JARs we ship,
-- this job uses a self-join approach with INTERVAL bounds for Pattern A
-- and a simple WHERE predicate for Pattern B.
--
-- Lineage: kafka://tax-applications → kafka://fraud-signals

SET 'table.exec.source.idle-timeout' = '10s';
SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

DROP TABLE IF EXISTS kafka_tax_applications_fraud;
DROP TABLE IF EXISTS kafka_fraud_signals_out;

-- ── Kafka source ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS kafka_tax_applications_fraud (
    application_id    STRING,
    customer_id       STRING,
    province          STRING,
    taxable_income    DOUBLE,
    employment_type   STRING,
    is_fraud          BOOLEAN,
    _produced_at      STRING,
    event_time        AS TO_TIMESTAMP(`_produced_at`, 'yyyy-MM-dd''T''HH:mm:ss'),
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

-- ── Kafka sink → fraud-signals topic ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS kafka_fraud_signals_out (
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
-- We count per customer_id in a 10-min tumbling window; if count >= 3 emit a signal.
INSERT INTO kafka_fraud_signals_out
SELECT
    CONCAT('flink-hf-', CAST(TUMBLE_START(event_time, INTERVAL '10' MINUTE) AS STRING), '-', customer_id)
        AS signal_id,
    'MULTIPLE'                                           AS application_id,
    customer_id,
    'HIGH_FREQUENCY_SUBMISSIONS'                         AS signal_type,
    'HIGH'                                               AS severity,
    CAST(TUMBLE_START(event_time, INTERVAL '10' MINUTE) AS STRING)
        AS detected_at,
    CONCAT(
        'Customer submitted ',
        CAST(COUNT(*) AS STRING),
        ' applications within 10-minute window starting ',
        CAST(TUMBLE_START(event_time, INTERVAL '10' MINUTE) AS STRING)
    )                                                    AS description
FROM kafka_tax_applications_fraud
GROUP BY
    customer_id,
    TUMBLE(event_time, INTERVAL '10' MINUTE)
HAVING COUNT(*) >= 3;

-- ── Pattern B: Income spike (taxable_income > 500 000) ───────────────────────
INSERT INTO kafka_fraud_signals_out
SELECT
    CONCAT('flink-spike-', application_id)       AS signal_id,
    application_id,
    customer_id,
    'INCOME_SPIKE'                                AS signal_type,
    'MEDIUM'                                      AS severity,
    _produced_at                                  AS detected_at,
    CONCAT(
        'Taxable income ',
        CAST(taxable_income AS STRING),
        ' exceeds high-value threshold of 500000'
    )                                             AS description
FROM kafka_tax_applications_fraud
WHERE taxable_income > 500000.0;
