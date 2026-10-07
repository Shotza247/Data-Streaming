-- flink-jobs/job_03_kpi_windows.sql
--
-- Job 03: KPI Windowed Aggregations
--
-- Computes 5-minute tumbling window aggregates over the tax-applications
-- stream. Writes one row per window to PostgreSQL tax_kpi_windows table.
-- The kpi_to_redis.py poller then reads these rows into Redis for the
-- Streamlit dashboard hot cache.
--
-- Lineage: kafka://tax-applications → postgres://tax_kpi_windows

-- ── Allow watermark to advance even when Kafka partition is idle ─────────────
-- Without this, if a Kafka partition stops producing events the watermark
-- never advances and tumbling windows never close.
SET 'table.exec.source.idle-timeout' = '10s';
SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

DROP TABLE IF EXISTS kafka_tax_applications_kpi;
DROP TABLE IF EXISTS pg_tax_kpi_windows;

-- ── Kafka source with event time ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS kafka_tax_applications_kpi (
    application_id  STRING,
    customer_id     STRING,
    province        STRING,
    taxable_income  DOUBLE,
    employment_type STRING,
    is_fraud        BOOLEAN,
    _produced_at    STRING,
    event_time      AS TO_TIMESTAMP(`_produced_at`, 'yyyy-MM-dd''T''HH:mm:ss'),
    WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'tax-applications',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id'          = 'flink-job03-kpi',
    'scan.startup.mode'            = 'latest-offset',
    'format'                       = 'json',
    'json.ignore-parse-errors'     = 'true'
);

-- ── PostgreSQL KPI sink ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pg_tax_kpi_windows (
    window_start        TIMESTAMP(3),
    window_end          TIMESTAMP(3),
    total_count         BIGINT,
    avg_income          DOUBLE,
    distinct_customers  BIGINT,
    fraud_count         BIGINT,
    fraud_rate          DOUBLE,
    top_province        STRING
) WITH (
    'connector'  = 'jdbc',
    'url'        = 'jdbc:postgresql://${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}',
    'table-name' = 'tax_kpi_windows',
    'username'   = '${POSTGRES_USER}',
    'password'   = '${POSTGRES_PASSWORD}',
    'driver'     = 'org.postgresql.Driver'
);

-- ── Tumbling 5-minute window aggregation ─────────────────────────────────────
-- Note: FIRST_VALUE window functions cannot be mixed with GROUP BY tumbling
-- windows in Flink SQL. We approximate top_province by taking the MAX of the
-- province string in the window (deterministic, stable, Flink-compatible).
INSERT INTO pg_tax_kpi_windows
SELECT
    TUMBLE_START(event_time, INTERVAL '5' MINUTE)     AS window_start,
    TUMBLE_END(event_time,   INTERVAL '5' MINUTE)     AS window_end,
    COUNT(*)                                           AS total_count,
    AVG(taxable_income)                                AS avg_income,
    COUNT(DISTINCT customer_id)                        AS distinct_customers,
    SUM(CASE WHEN is_fraud = TRUE THEN 1 ELSE 0 END)  AS fraud_count,
    CAST(
        SUM(CASE WHEN is_fraud = TRUE THEN 1 ELSE 0 END) AS DOUBLE
    ) / NULLIF(COUNT(*), 0)                            AS fraud_rate,
    MAX(province)                                      AS top_province
FROM kafka_tax_applications_kpi
GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE);
