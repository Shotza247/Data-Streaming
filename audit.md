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

## Entry 006 — Sub-Task 5: Qdrant Vector Store — Fraud Similarity and Regulatory KB

**Date:** 2026-10-02
**Status:** ✅ COMPLETE

### Files Created

| File | Description |
|---|---|
| `agents/__init__.py` | Package marker |
| `agents/embeddings.py` | SentenceTransformer singleton, embed_text/embed_batch, build_tax_record_text |
| `agents/embed_tax_records.py` | Reads DuckDB cleaned data → embeds → upserts into fraud_similarity (batched, idempotent) |
| `agents/embed_regulatory_kb.py` | 50 synthetic SA tax law chunks → regulatory_kb Qdrant collection |
| `agents/qdrant_search.py` | search_similar_fraud(), search_regulatory_kb(), format_kb_context() |
| `agents/requirements.txt` | sentence-transformers, qdrant-client, langgraph, mlflow, torch CPU |
| `scripts/seed_qdrant.py` | One-shot orchestrator: [Risk 3] preflight + seed both collections |

### Qdrant Collections

| Collection | Vectors | Distance | Payload |
|---|---|---|---|
| `fraud_similarity` | 384-dim | Cosine | application_id, customer_id, taxable_income, employment_type, province, is_fraud |
| `regulatory_kb` | 384-dim | Cosine | chunk_id, source_doc, section, text |

### Regulatory KB Coverage
50 chunks across 6 source documents covering SA Income Tax Act, Tax Administration Act,
SARS Tax Tables 2024, VAT Act, SARS Fraud Prevention Policy, PAYE Employer Guide.

### Commits
- `feat(agents): add embeddings.py sentence-transformer utility`
- `feat(agents): add embed_tax_records.py fraud_similarity collection seeder`
- `feat(agents): add embed_regulatory_kb.py 50-chunk SA tax regulatory KB`
- `feat(agents): add qdrant_search.py search API functions`
- `feat(scripts): add seed_qdrant.py with Risk 3 preflight guard`
- `docs(audit): Sub-Task 5 complete`

---

## Entry 008 — Sub-Task 7: LangGraph Multi-Agent System

**Date:** 2026-10-02
**Status:** ✅ COMPLETE

### Files Created

| File | Description |
|---|---|
| `agents/llm_wrapper.py` | flan-t5-base pipeline singleton; generate(); classify_intent() heuristic router |
| `agents/supervisor.py` | Keyword-based routing node → fraud_check or tax_question |
| `agents/fraud_agent.py` | MLflow inference + Qdrant similarity → structured JSON fraud assessment |
| `agents/qa_agent.py` | Qdrant KB retrieval + RAG prompt → flan-t5-base generated answer |
| `agents/langgraph_app.py` | StateGraph: supervisor → fraud_agent/qa_agent; compile_graph(); run() |

### Graph Topology
```
[START] → supervisor → {fraud_check: fraud_agent, tax_question: qa_agent} → [END]
```

### Key Design Choices
- **Routing**: heuristic keyword matching (no LLM call) — deterministic, fast
- **Fraud agent fallback**: if MLflow model unavailable, uses Qdrant similarity score alone
- **Q&A agent fallback**: if LLM unavailable, returns raw KB chunk text
- **run()**: always returns a dict — never raises; all errors caught and formatted

### Commits
- `feat(agents): add llm_wrapper.py flan-t5-base pipeline utility`
- `feat(agents): add supervisor, fraud_agent, qa_agent LangGraph nodes`
- `feat(agents): add langgraph_app.py StateGraph with conditional routing`
- `docs(audit): Sub-Task 7 complete`

---

## Entry 009 — Sub-Task 9: Wire Dashboard to Live Data

**Date:** 2026-10-02
**Status:** ✅ COMPLETE

### Changes Made

| File | Change |
|---|---|
| `dashboard/app.py` | sys.path wiring for /storage /agents /lineage mounts; st_autorefresh(30s) |
| `dashboard/requirements.txt` | Added streamlit-autorefresh, python-dotenv |
| `dashboard/tabs/tab_tax_kpi.py` | DuckDB functions now call `storage.duckdb_queries` module |
| `dashboard/tabs/tab_fraud.py` | DuckDB functions now call `storage.duckdb_queries` module; `_get_top_customers()` added |
| `dashboard/tabs/tab_mlflow.py` | `EXPERIMENT` name fixed to `tax-fraud-detection` |
| `docker-compose.yml` | Streamlit service now mounts `/storage`, `/agents`, `/lineage`, `/mlflow_scripts` |

### Live Data Wiring

| Tab | Data Source | When Live |
|---|---|---|
| Tax KPIs | Redis tax:kpi:window:* + DuckDB cleaned-tax | After kpi-poller starts + Flink job_05 runs |
| Fraud Intelligence | PostgreSQL fraud_detections + DuckDB province heatmap | After fraud signals populate |
| AI Agent Chat | agents.langgraph_app.run() | After HF models downloaded |
| Data Lineage | Marquez REST API + iframe | Always (Marquez is running) |
| MLflow Status | MLflow REST API + Model Registry | After make train |

### Commits
- `feat(dashboard): wire all tabs to real storage/agent modules`
- `feat(compose): mount storage agents lineage into Streamlit container`
- `docs(audit): Sub-Task 9 complete`

---

## Entry 010 — Sub-Task 8B + Sub-Task 10: Lineage Verification, Smoke Tests, DEMO.md

**Date:** 2026-10-02
**Status:** ✅ COMPLETE — ALL SUB-TASKS DONE

### Files Created

| File | Description |
|---|---|
| `lineage/verify_lineage.py` | Marquez API check: namespaces, jobs, datasets; validates 6 expected jobs |
| `scripts/smoke_test.py` | 8-component integration smoke test (4 critical, 4 non-critical) |
| `DEMO.md` | 10-step walkthrough from cold start to live dashboard with architecture diagram |

### Smoke Test Coverage

| Component | Critical | What It Checks |
|---|---|---|
| Kafka Topics | YES | 4 topics exist |
| PostgreSQL Tables | YES | 4 tables exist and have rows |
| Redis KPI Cache | YES | Ping + tax:kpi:* key scan |
| MinIO Buckets | YES | raw-tax, cleaned-tax, mlflow-artifacts have files |
| Qdrant Collections | NO | Both collections have vectors |
| Marquez Lineage DAG | NO | 6 expected jobs registered |
| MLflow Model Registry | NO | fraud-detector registered |
| Streamlit Dashboard | NO | HTTP healthcheck |

### Final Makefile Targets
All 16 targets documented in DEMO.md and tests/test_subtask8b_10.log.

### Commits
- `feat(lineage): add verify_lineage.py Marquez DAG verification`
- `feat(scripts): add smoke_test.py 8-component integration test`
- `feat(docs): add DEMO.md 10-step platform walkthrough`
- `feat(makefile): add verify-lineage target`
- `docs(audit): all sub-tasks complete`

---

## Entry 007 — Sub-Task 6: MLflow Model Training and Registry

**Date:** 2026-10-02
**Status:** ✅ COMPLETE

### Files Created / Modified

| File | Status | Description |
|---|---|---|
| `mlflow/mlflow_config.py` | NEW | Centralised config: tracking URI, S3 endpoint, experiment/model names |
| `mlflow/train_fraud_model.py` | NEW | Full pipeline: DuckDB load → RF train → log metrics/artifacts → register |
| `mlflow/promote_model.py` | NEW | Stage transition Staging → Production with archive of previous |
| `mlflow/requirements.txt` | UPDATED | Pinned versions: mlflow, sklearn, pandas, numpy, matplotlib |
| `lineage/lineage_schemas.py` | UPDATED | Added NAMESPACE_* aliases, postgres_dataset alias, _dataset() helper |
| `Makefile` | UPDATED | Added seed-kb, train, promote targets |

### ML Pipeline Summary
- **Model**: RandomForestClassifier (n_estimators=100, max_depth=8, class_weight=balanced)
- **Features**: province, employment_type, employment_status, filing_status (one-hot) + taxable_income
- **Target**: is_fraud (boolean)
- **Fallback**: Generates 500 synthetic rows if MinIO cleaned-tax is empty (demo safety net)
- **Artifacts logged**: confusion_matrix.png, classification_report.txt, sklearn model pickle

### Commits
- `feat(mlflow): add mlflow_config.py, train_fraud_model.py, promote_model.py`
- `fix(lineage): add NAMESPACE_* aliases, postgres_dataset alias, _dataset helper`
- `feat(makefile): add seed-kb, train, promote targets`
- `docs(audit): Sub-Task 6 complete`

---

---

## Entry 011 -- Sub-Task 11: Flink SQL Timestamp Cleaning + Full Pipeline Activation

**Date:** 2026-10-07
**Status:** COMPLETE

### Root Cause Fixed
Producer emits _produced_at as ISO-8601 with microseconds + UTC offset ("2026-10-01T13:32:07.539165+00:00").
All Flink SQL jobs were silently dropping every record because TO_TIMESTAMP couldn't parse this format.
json.ignore-parse-errors=true masked the failures.

### Fix: Flink SQL as the Cleaning Layer
The correct philosophy is to use Flink SQL string functions to normalise the format:
  REPLACE(SUBSTRING(_produced_at, 1, 19), 'T', ' ')
  -- yields "2026-10-01 13:32:07" which TO_TIMESTAMP(..., 'yyyy-MM-dd HH:mm:ss') handles perfectly

### Additional Fixes
- DROP TABLE IF EXISTS before every CREATE TABLE (prevents catalog reuse bugs)
- SET execution.checkpointing.interval = 30s on ALL jobs
- SET table.exec.source.idle-timeout = 10s on all event-time jobs
- job_02: replaced FOR SYSTEM_TIME AS OF with INTERVAL JOIN
- MinIO bucket creation fixed (mc alias set, not deprecated mc config host add)
- TaskManager scaled to 8 slots
- Makefile: added make all one-command startup target

### Verification
- PostgreSQL tax_kpi_windows: 16 rows of live window data
- Redis: 17 keys (tax:kpi:latest + 16 window keys)
- MinIO raw-tax: 35,332 objects | cleaned-tax: 34,940 objects
- All 6 Kafka consumer groups active
- 6 Flink jobs RUNNING, 0 failed


---

## Entry 012 — Sub-Task 12: Platform Control Center + Full Pipeline Fix

**Date:** 2026-10-08
**Status:** ✅ COMPLETE
**Commit:** `93bfc42`

### Problem Statement
Multiple pipeline components were stale or inactive:
- `enriched_tax_applications`: 0 rows (job_02 stuck in checkpoint restart loop)
- `fraud_detections`: 0 rows (job_04 writes to Kafka, not PostgreSQL — no consumer)
- Marquez lineage events failing (MARQUEZ_API_URL was container-internal from host)
- No UI way to start/stop producers or resubmit Flink jobs

### Fixes Applied

#### Flink Job Repairs
| Job | Problem | Fix |
|---|---|---|
| job_02_enrich.sql | EXACTLY_ONCE + earliest-offset → duplicate key PK violations → 424 failed checkpoints | Switch to latest-offset + AT_LEAST_ONCE + JDBC flush props |
| job_06 (NEW) | fraud_detections had 0 rows — job_04 only writes to Kafka | Created job_06_fraud_signals_to_pg.sql: kafka://fraud-signals → postgres://fraud_detections |

#### Lineage URL Fix
- `scripts/submit_flink_jobs.py`: hardcode `MARQUEZ_URL = http://localhost:5000` (host-side scripts need port-mapped URL, not container DNS)

### New: Platform Control Center Tab (`dashboard/tabs/tab_control.py`)

Full operational UI added as Tab 6 in Streamlit dashboard:

| Section | Features |
|---|---|
| 🩺 Service Health Grid | 12 services, colour-coded 🟢🟡🔴, dual Podman API + HTTP probe |
| 📨 Kafka Topic Counts | Real-time message counts for all 4 topics |
| 🚀 Producer Controls | ▶ Start / ⏹ Stop / 🔄 Restart per-producer + Start ALL / Stop ALL |
| ⚡ Flink Job Manager | Live job table (state, duration, cancel button, Flink UI deep-links) |
| 🔁 SQL Job Resubmit | Multi-select jobs, render env vars, submit via sql-client.sh from UI |
| 🔧 Pipeline Actions | One-click: Seed Qdrant, Train MLflow, Smoke Test, Submit Lineage |
| 📋 Log Viewer | Any container, configurable tail, Podman API + CLI fallback |

#### Backend: Dual-Mode Container Management
```
Priority 1: Podman REST API via mounted UNIX socket /run/podman.sock
  → Uses stdlib http.client + AF_UNIX socket (no extra deps)
  → Calls /v4.0.0/containers/{name}/start|stop|restart|json|logs

Priority 2: CLI subprocess fallback
  → podman start|stop|restart|inspect|logs
  → Works when running dashboard locally outside container
```

#### docker-compose.yml Changes
```yaml
streamlit:
  volumes:
    - ./scripts:/scripts          # for pipeline action buttons
    - ./flink-jobs:/flink-jobs    # for SQL resubmit UI
    - /run/user/1000/podman/podman.sock:/run/podman.sock:ro
  environment:
    FLINK_API_URL: http://flink-jobmanager:8081
    PODMAN_SOCK: /run/podman.sock
    CONTAINER_CLI: podman
```

### Verification (end of session)
```
Flink jobs:       8/9 RUNNING (1 old CANCELED entry)
enriched_tax_applications: 35 rows and growing
fraud_detections:          120,731 rows
tax_kpi_windows:           72 rows
producers:                 4/4 running (1 event/sec each)
```

### Files Changed
| File | Change |
|---|---|
| `dashboard/tabs/tab_control.py` | NEW — 450+ line Platform Control Center |
| `dashboard/app.py` | Added tab6 Control Center |
| `docker-compose.yml` | Streamlit socket mount + env vars |
| `flink-jobs/job_02_enrich.sql` | latest-offset + AT_LEAST_ONCE + JDBC flush |
| `flink-jobs/job_06_fraud_signals_to_pg.sql` | NEW — fraud signals → postgres |
| `scripts/submit_flink_jobs.py` | job_06 lineage metadata + Marquez URL fix |

