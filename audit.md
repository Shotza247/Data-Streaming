# Platform Build Audit Log

---

## Entry 001 — Sub-Task 1: Project Scaffold, Docker Compose Infrastructure, and Streamlit Dashboard

**Date:** 2026-07-08
**Status:** ✅ COMPLETE

### Files Created

#### Infrastructure
| File | Description |
|---|---|
| `docker-compose.yml` | Full 15-service stack on `streaming-net` network |
| `.env` | All environment variables: Kafka, Postgres, MinIO, Redis, MLflow, Marquez |
| `Makefile` | Targets: up, down, restart, logs, topic-list, submit-sql, seed, smoke-test, reset |
| `README.md` | Architecture overview, prerequisites, quickstart, port reference table |
| `docker/Dockerfile.flink` | Custom Flink 1.18 image with 4 pinned connector JARs (kafka, jdbc, s3, postgresql driver) |
| `docker/Dockerfile.mlflow` | Python 3.11-slim with mlflow + boto3, S3/MinIO artifact backend |
| `docker/Dockerfile.streamlit` | Python 3.11-slim with gcc/libpq/curl, healthcheck, .streamlit config baked in |
| `docker/init-postgres.sql` | Creates taxdb, marquez, mlflow databases + taxuser and marquez roles |
| `docker/minio-init.sh` | Creates buckets: raw-tax, cleaned-tax, mlflow-artifacts |

#### Directory Scaffolds (with .gitkeep)
`producers/`, `flink-jobs/`, `storage/`, `agents/`, `lineage/`, `mlflow/`, `scripts/`, `tests/`

#### Dashboard
| File | Description |
|---|---|
| `dashboard/app.py` | Main entry: 5-tab layout with custom CSS and header |
| `dashboard/requirements.txt` | Pinned: streamlit, redis, psycopg2-binary, duckdb, boto3, requests, pandas, plotly |
| `dashboard/.streamlit/config.toml` | Theme (primaryColor=#2563eb), runOnSave=true, port=8501 |
| `dashboard/tabs/__init__.py` | Package marker |
| `dashboard/tabs/tab_tax_kpi.py` | Tax KPIs: Redis hot cache + DuckDB trend chart + income histogram |
| `dashboard/tabs/tab_fraud.py` | Fraud Intelligence: PostgreSQL signals + DuckDB province summary + charts |
| `dashboard/tabs/tab_agent_chat.py` | AI Agent Chat: LangGraph invoke + demo mode fallback + session history |
| `dashboard/tabs/tab_lineage.py` | Data Lineage: Marquez API table + Marquez Web UI iframe embed |
| `dashboard/tabs/tab_mlflow.py` | MLflow Status: runs table + model stage badge + metrics bar chart |

#### Tests
| File | Description |
|---|---|
| `tests/test_subtask1.log` | File inventory, fallback strategy, validation notes |

### Services Configured in docker-compose.yml

| Service | Image | Port | Notes |
|---|---|---|---|
| zookeeper | confluentinc/cp-zookeeper:7.5.0 | — | Kafka dependency |
| kafka | confluentinc/cp-kafka:7.5.0 | 9092, 9094 | HOST listener on 9094 for local access |
| kafka-ui | provectuslabs/kafka-ui:latest | 8080 | |
| flink-jobmanager | custom Dockerfile.flink | 8081 | flink-jobs volume mounted |
| flink-taskmanager | custom Dockerfile.flink | — | |
| flink-sql-client | custom Dockerfile.flink | — | profile=tools, one-shot SQL submission |
| postgres | postgres:15-alpine | 5432 | init-postgres.sql on startup |
| redis | redis:7-alpine | 6379 | healthcheck: redis-cli ping |
| minio | quay.io/minio/minio:latest | 9000, 9001 | healthcheck on /minio/health/live |
| minio-init | quay.io/minio/mc:latest | — | one-shot bucket creation |
| qdrant | qdrant/qdrant:latest | 6333 | persistent volume |
| marquez | marquezproject/marquez:0.47.0 | 5000 | backed by postgres/marquez db |
| marquez-web | marquezproject/marquez-web:0.47.0 | 3000 | |
| mlflow | custom Dockerfile.mlflow | 5001 | MinIO artifact store + postgres backend |
| streamlit | custom Dockerfile.streamlit | 8501 | dashboard volume-mounted at /app |

### Risk Mitigations Applied
- **[Risk 1]** `docker/Dockerfile.flink` created with 4 pinned JAR versions. All 3 Flink services use this custom image.
- **[Risk 2]** Noted in plan — lineage primitives will be built in Sub-Task 8 Phase A before Sub-Task 3.
- **[Risk 3]** Noted in plan — `make seed` preflight check for non-empty cleaned-tax bucket documented.

### Environment Correction Applied
- **Runtime:** Podman (not Docker). All `docker` references updated to `podman` throughout.
- Makefile now uses `podman compose` (Podman v4.7+ built-in) with fallback to `podman-compose`
- Makefile `topic-list` target uses `podman exec` (not `docker exec`)
- README updated: prerequisites now list Podman Desktop, `podman machine start` added to quickstart
- `tests/test_subtask1.log` updated with Podman-specific compatibility notes
- `scripts/submit_flink_jobs.py` (Sub-Task 3) noted to use `podman exec flink-sql-client`

### Manual Validation Steps Required
```bash
# 0. Start Podman machine (Podman Desktop or CLI)
podman machine start

# 1. Validate compose file
podman compose config

# 2. Build images and start full stack
podman compose up -d --build

# 3. Check all services healthy
podman compose ps

# 4. Verify Streamlit at http://localhost:8501
# 5. Verify Kafka UI   at http://localhost:8080
# 6. Verify MinIO      at http://localhost:9001
# 7. Verify Flink UI   at http://localhost:8081
# 8. Verify Marquez UI at http://localhost:3000
# 9. Verify MLflow UI  at http://localhost:5001
```

---

## Entry 002 — Sub-Task 2: Kafka Topic Setup and 4 Tax Domain Producers

**Date:** 2026-10-01
**Status:** ✅ COMPLETE

### Files Created
| File | Description |
|---|---|
| `producers/base_producer.py` | Abstract base: confluent-kafka, auto topic creation, rate control, graceful shutdown |
| `producers/producer_tax_applications.py` | Fields: application_id, customer_id, customer_name, email, country, province, taxable_income, employment_type, employment_status, submitted_date, tax_year, filing_status, is_fraud |
| `producers/producer_taxpayer_profiles.py` | Fields: customer_id, age, risk_score, historical_filings_count, avg_income_3yr, flagged_previously |
| `producers/producer_fraud_signals.py` | Fields: signal_id, application_id, customer_id, signal_type, severity, detected_at, description |
| `producers/producer_audit_events.py` | Fields: event_id, application_id, actor, action, timestamp, metadata |
| `producers/topic_init.py` | One-shot idempotent topic creation for all 4 topics |
| `producers/Containerfile` | python:3.11-slim, CMD overridden per service in compose |
| `producers/requirements.txt` | confluent-kafka==2.4.0, faker==25.2.0, python-dotenv==1.0.1 |
| `tests/test_subtask2.log` | Topics verified, message counts, issues + resolutions |

### Kafka Topics Confirmed
All 4 topics exist with 3 partitions, replication factor 1:
`tax-applications` · `taxpayer-profiles` · `fraud-signals` · `audit-events`

### Message Counts Verified (~90s runtime)
| Topic | Count |
|---|---|
| tax-applications | 82 |
| taxpayer-profiles | 84 |
| fraud-signals | 1775 |
| audit-events | 1772 |

### Issues Encountered and Resolved
1. **Faker en_ZA locale** — removed in Faker 25.x. Fixed: changed to `en_GB`
2. **Flink Containerfile DNS** — `ADD <url>` fails in Podman WSL at build time. Fixed: changed to `RUN wget`
3. **podman-compose profile rebuild timeout** — builds all services including Flink. Fixed: `podman build` once, then `podman run` directly

### Commits
- `cf430b8` feat(producers): add 4 tax-domain Kafka producers and topic init
- `be57673` feat(compose): add kafka-topic-init and 4 producer services
- `5093ef2` feat(makefile): add producer and topic management targets
- `fc04b05` fix(producers): replace en_ZA Faker locale with en_GB
- `7ec9c55` fix(build): replace ADD with RUN wget in Flink Containerfile

---

## Entry 003 -- Sub-Task 8 Phase A: OpenLineage Client and Schema Helpers

**Date:** 2026-10-01
**Status:** COMPLETE

### Files Created
| File | Description |
|---|---|
| `lineage/openlineage_client.py` | LineageClient: start/complete/fail, POSTs RunEvent to Marquez, fire-and-forget |
| `lineage/lineage_schemas.py` | Dataset builders: kafka, minio, postgres, mlflow, duckdb, redis, streamlit + pre-built field schemas |
| `lineage/requirements.txt` | requests, python-dotenv |
| `lineage/test_lineage_client.py` | Smoke test: 4 events emitted, Marquez API verified |
| `tests/test_subtask8a.log` | Full test results and issues resolved |

### Marquez Verification
Jobs registered after smoke test:
- flink: job_01_raw_to_minio, job_02_enrich, job_test_fail
- mlflow: train_fraud_model

### Issues Resolved
1. Marquez v0.47.0 healthcheck endpoint is /api/v1/namespaces not /api/v1/health
2. MARQUEZ_DB_USER/PASSWORD missing from .env -- hardcoded in compose
3. init-postgres.sql gexec pattern failed silently -- replaced with plain SQL
4. UnicodeEncodeError on Windows cp1252 console -- replaced unicode with ASCII

### Commits
- `42e7a30` feat(lineage): Sub-Task 8 Phase A -- OpenLineage client and schema helpers
- `083157e` fix(infra): fix Marquez DB credentials and healthcheck endpoint

---

## Entry 004 — Sub-Task 3: Flink SQL Jobs, Submission Wrapper, KPI Poller

**Date:** 2026-10-02
**Status:** ✅ COMPLETE

### Files Created / Modified

| File | Status | Description |
|---|---|---|
| `flink-jobs/job_01_raw_to_minio.sql` | committed | Kafka → MinIO raw-tax (archived in prior session) |
| `flink-jobs/job_02_enrich.sql` | committed | Temporal JOIN tax-apps + taxpayer-profiles → PostgreSQL enriched_tax_applications |
| `flink-jobs/job_03_kpi_windows.sql` | fixed + committed | Tumbling 5-min windows → PostgreSQL tax_kpi_windows (FIRST_VALUE → MAX fix) |
| `flink-jobs/job_04_fraud_pattern.sql` | NEW | High-frequency submitter + income spike → kafka://fraud-signals |
| `flink-jobs/job_05_cleaned.sql` | NEW | UPPER/TRIM standardise + filter → MinIO cleaned-tax (DuckDB source of truth) |
| `scripts/submit_flink_jobs.py` | NEW | OpenLineage emit + podman exec sql-client.sh -f <script> per job |
| `scripts/kpi_to_redis.py` | NEW | Polls PostgreSQL tax_kpi_windows → Redis HASHes (15 min TTL), tax:kpi:latest key |
| `docker-compose.yml` | UPDATED | flink-sql-client keep-alive entrypoint; kpi-poller service added |
| `Makefile` | UPDATED | flink-up, flink-logs, flink-ui, submit-job, kpi-poller-up, kpi-poller-logs targets |
| `tests/test_subtask3.log` | NEW | Full design decisions + manual start steps |

### Flink SQL Job Summary

| Job | Source | Sink | Pattern |
|---|---|---|---|
| job_01 | kafka://tax-applications | minio://raw-tax/ | Raw archival, JSON, partitioned by date |
| job_02 | kafka://tax-applications + taxpayer-profiles | postgres://enriched_tax_applications | Temporal LEFT JOIN on customer_id |
| job_03 | kafka://tax-applications | postgres://tax_kpi_windows | Tumbling 5-min aggregation: count, avg_income, fraud_rate |
| job_04 | kafka://tax-applications | kafka://fraud-signals | CEP: high-frequency (≥3 in 10 min) + income spike (>500k) |
| job_05 | kafka://tax-applications | minio://cleaned-tax/ | UPPER/TRIM standardise + NULL filter + processed_at |

### Design Decisions
- **job_03 fix**: `FIRST_VALUE` over `ORDER BY TUMBLE_START` is invalid in Flink SQL GROUP BY windows — replaced with `MAX(province)` (deterministic, same scope)
- **job_04 CEP**: `MATCH_RECOGNIZE` requires Flink CEP JAR not in our image — used tumbling window `HAVING COUNT(*) >= 3` equivalent
- **flink-sql-client**: Changed entrypoint from one-shot `sql-client.sh` to `tail -f /dev/null` keep-alive; removed `profiles: [tools]` so it starts with default stack
- **kpi-poller**: `python:3.11-slim` with inline `pip install`; writes both per-window hash keys + `tax:kpi:latest` summary key

### Makefile Targets Added
```
make flink-up          # start flink-jobmanager + taskmanager + sql-client
make flink-logs        # follow Flink container logs
make submit-sql        # submit all 5 jobs via submit_flink_jobs.py
make submit-job JOB=job_01  # submit single job by name substring
make kpi-poller-up     # start Redis KPI cache poller
make kpi-poller-logs   # follow poller logs
```

### Manual Steps to Activate Flink
```bash
podman build -t flink-custom:1.18 ./docker/build/flink/
make flink-up
# wait 30s, then:
make submit-sql
make kpi-poller-up
```

### Commits
- `feat(flink): add job_04 fraud pattern detection and job_05 cleaned output`
- `feat(scripts): add submit_flink_jobs.py and kpi_to_redis.py`
- `feat(compose): add kpi-poller service and fix flink-sql-client entrypoint`
- `fix(flink): replace FIRST_VALUE with MAX in job_03 KPI windows`
- `docs(audit): Sub-Task 3 complete`

---

## Entry 005 — Sub-Task 4: Storage Layer — PostgreSQL, Redis, MinIO, DuckDB

**Date:** 2026-10-02
**Status:** ✅ COMPLETE

### Files Created

| File | Description |
|---|---|
| `storage/postgres_schema.sql` | 4 tables + 4x indexes each + 2 views; applied to live taxdb |
| `storage/redis_schema.md` | Key naming conventions, TTL policies, field-level docs for all 4 key families |
| `storage/minio_client.py` | boto3 wrapper: list_files, list_partitions, bucket_has_files, download, upload |
| `storage/duckdb_queries.py` | 6 named query functions; httpfs MinIO config; graceful empty DataFrame fallback |

### PostgreSQL Applied Live
```
Get-Content storage/postgres_schema.sql | podman exec -i postgres psql -U taxuser -d taxdb
```
All CREATE TABLE / CREATE INDEX / CREATE VIEW commands executed successfully.

### DuckDB Query Functions

| Function | Output | Consumer |
|---|---|---|
| `get_tax_kpi_trend(n)` | time-series KPI windows | Dashboard Tab 1 trend chart |
| `get_fraud_summary()` | fraud by type/severity/province | Dashboard Tab 2 bar chart |
| `get_cleaned_data_for_ml(n)` | labelled records | MLflow train_fraud_model.py |
| `get_income_distribution(n_buckets)` | histogram buckets | Dashboard Tab 1 histogram |
| `get_top_flagged_customers(n)` | customer ranking | Dashboard Tab 2 table |
| `get_province_fraud_heatmap()` | province-level metrics | Dashboard Tab 2 heatmap |

### Commits
- `feat(storage): add postgres_schema.sql with 4 tables, indexes, and views`
- `feat(storage): add redis_schema.md key naming and TTL policy documentation`
- `feat(storage): add minio_client.py boto3 wrapper`
- `feat(storage): add duckdb_queries.py with 6 named OLAP query functions`
- `docs(audit): Sub-Task 4 complete`

---
