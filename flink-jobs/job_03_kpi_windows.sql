-- flink-jobs/job_03_kpi_windows.sql
--
-- Job 03: Real-Time KPI Window Aggregations  →  PostgreSQL tax_kpi_windows
--
-- PURPOSE  : Compute 5-minute tumbling window KPIs over the live stream.
--            Results feed into Redis (via kpi_to_redis.py) and the
--            Streamlit dashboard.
--
-- TIMESTAMP HANDLING (Flink SQL as cleaning layer):
--   Raw _produced_at: "2026-10-01T13:32:07.539165+00:00"
--   Cleaned:
--     SUBSTRING(..., 1, 19)  → "2026-10-01T13:32:07"
--     REPLACE(..., 'T', ' ') → "2026-10-01 13:32:07"
--     TO_TIMESTAMP(...)      → TIMESTAMP(3) for windowing
--
--   idle-timeout: after 10s of no new Kafka events, the source is marked
--   idle and the watermark advances — prevents windows from never closing.
--
-- Lineage:  kafka://tax-applications  →  postgres://tax_kpi_windows

SET 'table.exec.source.idle-timeout'   = '10s';
SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode'     = 'EXACTLY_ONCE';

DROP TABLE IF EXISTS kafka_tax_applications_kpi;
DROP TABLE IF EXISTS pg_tax_kpi_windows;

-- ── Source: clean the timestamp inline as a computed column ───────────────────
CREATE TABLE kafka_tax_applications_kpi (
    application_id  STRING,
    customer_id     STRING,
    province        STRING,
    taxable_income  DOUBLE,
    employment_type STRING,
    is_fraud        BOOLEAN,
    _produced_at    STRING,
    -- Timestamp cleaning pipeline:
    --   "2026-10-01T13:32:07.539165+00:00"
    --     → SUBSTRING(...,1,19) → "2026-10-01T13:32:07"
    --     → REPLACE('T',' ')   → "2026-10-01 13:32:07"
    --     → TO_TIMESTAMP(...)  → TIMESTAMP(3)  ← used for windowing
    event_time AS TO_TIMESTAMP(
        REPLACE(SUBSTRING(`_produced_at`, 1, 19), 'T', ' '),
        'yyyy-MM-dd HH:mm:ss'
    ),
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

-- ── Sink: PostgreSQL tax_kpi_windows ─────────────────────────────────────────
CREATE TABLE pg_tax_kpi_windows (
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

-- ── 5-minute tumbling window aggregation ─────────────────────────────────────
-- Each window emits one row covering 5 minutes of real event time.
-- fraud_rate = fraud_count / total_count (safe division via NULLIF)
INSERT INTO pg_tax_kpi_windows
SELECT
    TUMBLE_START(event_time, INTERVAL '5' MINUTE)     AS window_start,
    TUMBLE_END(event_time,   INTERVAL '5' MINUTE)     AS window_end,
    COUNT(*)                                           AS total_count,
    AVG(taxable_income)                                AS avg_income,
    COUNT(DISTINCT customer_id)                        AS distinct_customers,
    SUM(CASE WHEN is_fraud = TRUE THEN 1 ELSE 0 END)  AS fraud_count,
    CAST(SUM(CASE WHEN is_fraud = TRUE THEN 1 ELSE 0 END) AS DOUBLE)
        / NULLIF(COUNT(*), 0)                          AS fraud_rate,
    MAX(province)                                      AS top_province
FROM kafka_tax_applications_kpi
GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE);
