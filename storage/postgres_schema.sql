-- storage/postgres_schema.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Full PostgreSQL schema for the tax analytics platform.
-- Run once against the taxdb database.
--
--   podman exec -i postgres psql -U taxuser -d taxdb < storage/postgres_schema.sql
--
-- Tables:
--   enriched_tax_applications  -- Flink job_02 output (temporal JOIN)
--   tax_kpi_windows            -- Flink job_03 output (tumbling window aggregates)
--   fraud_detections           -- Derived from fraud_signals topic + Flink job_04
--   audit_log                  -- Derived from audit-events Kafka topic
-- ─────────────────────────────────────────────────────────────────────────────

-- ── enriched_tax_applications ────────────────────────────────────────────────
-- Written by Flink job_02 via JDBC connector.
-- Combines tax application fields with taxpayer profile enrichment.

CREATE TABLE IF NOT EXISTS enriched_tax_applications (
    application_id           VARCHAR(64)     PRIMARY KEY,
    customer_id              VARCHAR(64)     NOT NULL,
    customer_name            VARCHAR(255),
    email                    VARCHAR(255),
    country                  VARCHAR(100),
    province                 VARCHAR(100),
    taxable_income           NUMERIC(15, 2),
    employment_type          VARCHAR(50),
    employment_status        VARCHAR(50),
    submitted_date           DATE,
    tax_year                 VARCHAR(10),
    filing_status            VARCHAR(50),
    is_fraud                 BOOLEAN         DEFAULT FALSE,
    -- Enrichment fields from taxpayer-profiles topic (may be NULL if no JOIN match)
    age                      SMALLINT,
    risk_score               NUMERIC(5, 4),
    historical_filings_count INTEGER,
    avg_income_3yr           NUMERIC(15, 2),
    flagged_previously       BOOLEAN,
    -- Metadata
    _produced_at             TIMESTAMPTZ,
    _enriched_at             TIMESTAMPTZ     DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_eta_customer_id
    ON enriched_tax_applications (customer_id);

CREATE INDEX IF NOT EXISTS idx_eta_submitted_date
    ON enriched_tax_applications (submitted_date);

CREATE INDEX IF NOT EXISTS idx_eta_province
    ON enriched_tax_applications (province);

CREATE INDEX IF NOT EXISTS idx_eta_is_fraud
    ON enriched_tax_applications (is_fraud)
    WHERE is_fraud = TRUE;

-- ── tax_kpi_windows ───────────────────────────────────────────────────────────
-- Written by Flink job_03 via JDBC connector.
-- One row per 5-minute tumbling window.

CREATE TABLE IF NOT EXISTS tax_kpi_windows (
    window_start         TIMESTAMPTZ     NOT NULL,
    window_end           TIMESTAMPTZ     NOT NULL,
    total_count          BIGINT          NOT NULL DEFAULT 0,
    avg_income           NUMERIC(15, 2),
    distinct_customers   BIGINT          NOT NULL DEFAULT 0,
    fraud_count          BIGINT          NOT NULL DEFAULT 0,
    fraud_rate           NUMERIC(8, 6),
    top_province         VARCHAR(100),
    PRIMARY KEY (window_start)
);

CREATE INDEX IF NOT EXISTS idx_kpi_window_start
    ON tax_kpi_windows (window_start DESC);

-- ── fraud_detections ─────────────────────────────────────────────────────────
-- Aggregated fraud signals. Can be populated by:
--   a) A consumer reading from the fraud-signals Kafka topic
--   b) Directly by the LangGraph fraud agent after inference
-- Used by the dashboard Fraud Intelligence tab.

CREATE TABLE IF NOT EXISTS fraud_detections (
    detection_id         VARCHAR(128)    PRIMARY KEY,
    application_id       VARCHAR(64),
    customer_id          VARCHAR(64),
    signal_type          VARCHAR(100)    NOT NULL,
    severity             VARCHAR(20)     NOT NULL
                         CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    source               VARCHAR(50)     NOT NULL
                         CHECK (source IN ('PRODUCER', 'FLINK', 'AGENT', 'MANUAL')),
    fraud_probability    NUMERIC(5, 4),
    description          TEXT,
    detected_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    resolved             BOOLEAN         DEFAULT FALSE,
    resolved_at          TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_fd_customer_id
    ON fraud_detections (customer_id);

CREATE INDEX IF NOT EXISTS idx_fd_severity
    ON fraud_detections (severity);

CREATE INDEX IF NOT EXISTS idx_fd_detected_at
    ON fraud_detections (detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_fd_signal_type
    ON fraud_detections (signal_type);

-- ── audit_log ─────────────────────────────────────────────────────────────────
-- Mirror of the audit-events Kafka topic for persistent query access.
-- Written by a Kafka consumer (or future Flink job) reading the audit-events topic.

CREATE TABLE IF NOT EXISTS audit_log (
    event_id             VARCHAR(64)     PRIMARY KEY,
    application_id       VARCHAR(64),
    actor                VARCHAR(100)    NOT NULL,
    action               VARCHAR(100)    NOT NULL,
    event_timestamp      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    ip_address           INET,
    session_id           VARCHAR(64),
    outcome              VARCHAR(20)
                         CHECK (outcome IN ('SUCCESS', 'FAILURE', 'PENDING', 'UNKNOWN')),
    metadata             JSONB,
    _produced_at         TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_al_application_id
    ON audit_log (application_id);

CREATE INDEX IF NOT EXISTS idx_al_actor
    ON audit_log (actor);

CREATE INDEX IF NOT EXISTS idx_al_event_timestamp
    ON audit_log (event_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_al_action
    ON audit_log (action);

-- ── Convenience view: recent fraud summary ────────────────────────────────────
CREATE OR REPLACE VIEW fraud_summary_by_province AS
SELECT
    e.province,
    COUNT(DISTINCT fd.detection_id)    AS total_signals,
    COUNT(DISTINCT fd.customer_id)     AS flagged_customers,
    SUM(CASE WHEN fd.severity = 'HIGH'     THEN 1 ELSE 0 END) AS high_count,
    SUM(CASE WHEN fd.severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_count,
    AVG(fd.fraud_probability)          AS avg_fraud_probability,
    MAX(fd.detected_at)                AS latest_detection
FROM fraud_detections fd
LEFT JOIN enriched_tax_applications e ON fd.application_id = e.application_id
GROUP BY e.province;

-- ── Convenience view: latest KPI snapshot ────────────────────────────────────
CREATE OR REPLACE VIEW latest_kpi_snapshot AS
SELECT *
FROM tax_kpi_windows
ORDER BY window_start DESC
LIMIT 1;

-- ── Done ──────────────────────────────────────────────────────────────────────
-- Apply with:
--   podman exec -i postgres psql -U taxuser -d taxdb < storage/postgres_schema.sql
