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
