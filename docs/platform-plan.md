# Open-Source Real-Time Streaming Analytics Platform — Plan

## Top-Level Overview

Build a fully containerised, end-to-end real-time data streaming platform using exclusively open-source tools.
The platform is focused on a single cohesive domain:

1. **Tax Applications** — tax filings, taxpayer profiles, fraud signals, and audit events

### Goal
Replace the Confluent Cloud–based reference architecture with a fully local, Docker-composed stack that adds:
- Flink SQL stream processing via the **Flink SQL Client** — pure `.sql` scripts submitted to the cluster for clean separation of concerns and readable transformations
- Multi-tier storage (PostgreSQL, Redis, MinIO/S3, DuckDB OLAP, Qdrant vector)
- Full end-to-end data lineage via OpenLineage + Marquez
- A LangGraph multi-agent system (Supervisor → FraudDetection + TaxQA nodes) backed by HuggingFace open-source models
- MLflow model tracking for models trained on cleaned Flink-output data
- A single Streamlit multi-tab dashboard (Tax KPIs, Fraud Intelligence, AI Agent Chat, Data Lineage, MLflow Status)

### Non-Goals
- No cloud-managed services (no Confluent Cloud, no AWS, no Azure)
- No paid model APIs (no OpenAI, no Anthropic)
- No Kubernetes (Docker Compose only for now)
- No production hardening / security config (this is a demonstration platform)

---

## Architecture Summary

```
4 Kafka Producers (Tax Domain)
  └── Kafka Topics
        └── Flink SQL Client submits .sql scripts to Flink cluster
              ├── Enrichment streams → PostgreSQL
              ├── Windowed aggregations → Redis (hot cache)
              ├── Raw event archival → MinIO (S3-compatible)
              ├── Cleaned/standardised output → MinIO cleaned layer
              └── OpenLineage events emitted from each SQL job wrapper → Marquez
                    └── DuckDB queries MinIO (OLAP layer)
                          ├── Time-window KPIs → Streamlit
                          ├── Analytical reports → Streamlit
                          └── Cleaned OLAP data → MLflow training
                                └── MLflow Model Registry
                                      └── LangGraph Agents
                                            ├── Supervisor Node
                                            ├── Fraud Detection Node (Qdrant similarity)
                                            └── Tax Q&A Node (Qdrant knowledge base)
                                                  └── Streamlit Chat Tab
```

---

## Build Order and Cross-Task Dependencies

> **Read this before starting any sub-task.** Three structural dependencies run across the plan that are not obvious from the sub-task list alone. Ignoring them will cause build failures or empty data at runtime.

### ⚠️ Risk 1 — Flink Custom Docker Image (Sub-Task 1 prerequisite for Sub-Task 3)

The Flink SQL Client and JobManager both require connector JARs that are **not** included in the base `apache/flink:1.18` image. JAR versions must match the Flink 1.18 runtime exactly — a version mismatch causes silent `ClassNotFoundException` failures at job submission time, not at image build time.

**Mitigation baked into Sub-Task 1:** Sub-Task 1 must produce a working `docker/Dockerfile.flink` with **pinned download URLs** for all required JARs before any Flink SQL script is written. This Dockerfile is the single source of truth for the connector version matrix. Sub-Task 3 cannot begin until the custom image builds and the JobManager starts cleanly.

Required JARs with pinned versions:
- `flink-sql-connector-kafka-3.1.0-1.18.jar`
- `flink-connector-jdbc-3.1.2-1.18.jar`
- `flink-s3-fs-hadoop-1.18.1.jar`
- `postgresql-42.7.3.jar` (JDBC driver — not a Flink JAR, but must also be in `/opt/flink/lib/`)

---

### ⚠️ Risk 2 — Lineage Client Built Before It Is Called (Sub-Task 8 split into two phases)

The `lineage/openlineage_client.py` and `lineage/lineage_schemas.py` modules are **called from Sub-Tasks 3, 4, and 6** but are defined in Sub-Task 8. If Sub-Task 8 is left until last, Sub-Tasks 3/4/6 cannot emit lineage events and the Marquez DAG will be empty.

**Mitigation:** Sub-Task 8 is split into two phases:
- **Phase A (before Sub-Task 3):** Build only `lineage/openlineage_client.py` and `lineage/lineage_schemas.py` — the reusable emission primitives. Mark these two files as the Sub-Task 8 Phase A deliverable.
- **Phase B (after Sub-Task 9):** Instrument all call sites (`flink-jobs/` wrappers, `mlflow/train_fraud_model.py`, `storage/duckdb_queries.py`), run `lineage/verify_lineage.py`, and confirm the full DAG appears in Marquez UI.

The Sub-Task 8 entry below reflects this split. The **recommended build order** is:

```
Sub-Task 1 → Sub-Task 2 → Sub-Task 8 Phase A → Sub-Task 3 → Sub-Task 4
  → Sub-Task 5 → Sub-Task 6 → Sub-Task 7 → Sub-Task 9 → Sub-Task 8 Phase B → Sub-Task 10
```

---

### ⚠️ Risk 3 — Qdrant Seeding Requires Cleaned MinIO Data (Sub-Task 5 depends on Sub-Task 3)

`agents/embed_tax_records.py` (Sub-Task 5) reads from the `cleaned-tax` MinIO bucket to build the `fraud_similarity` Qdrant collection. If the Flink cleaning job (`job_05_cleaned.sql`) has not yet run and written records to that bucket, the collection will be seeded with zero vectors and the Fraud Detection Agent will return no similarity results.

**Mitigation:** The `make seed` command (Sub-Task 10) must enforce this ordering explicitly:
1. Assert `cleaned-tax` bucket is non-empty before running `scripts/seed_qdrant.py`
2. If empty, print a clear error: `"Run 'make submit-sql' and wait for cleaned-tax records before seeding Qdrant"`
3. Sub-Task 5 must document this pre-condition in `scripts/seed_qdrant.py` as a startup guard

---

## Sub-Tasks

---

### Sub-Task 1 — Project Scaffold and Docker Compose Infrastructure

**Status:** `[ ] pending`

**Intent:**
Establish the full Docker Compose stack with all services configured, networked, and health-checked.
This is the foundation every other sub-task builds on. All services must be reachable by their service name within the compose network.

**Expected Outcomes:**
- `docker-compose up` brings up all infrastructure services cleanly
- Kafka broker is reachable, topics can be created
- MinIO console is accessible at `localhost:9001`
- PostgreSQL is reachable on `localhost:5432`
- Redis is reachable on `localhost:6379`
- Flink Job Manager UI is accessible at `localhost:8081`
- Marquez API is accessible at `localhost:5000`, Marquez UI at `localhost:3000`
- Qdrant UI is accessible at `localhost:6333`
- MLflow tracking server is accessible at `localhost:5001`

**Todo List:**
- [ ] Create project root directory structure:
  ```
  /producers/
  /flink-jobs/
  /storage/
  /agents/
  /dashboard/
  /lineage/
  /mlflow/
  /scripts/
  /docker/
  docker-compose.yml
  .env
  Makefile
  README.md
  ```
- [ ] Write `docker-compose.yml` with the following services:
  - `zookeeper` (confluentinc/cp-zookeeper or bitnami/zookeeper)
  - `kafka` (confluentinc/cp-kafka or bitnami/kafka) — expose port 9092
  - `kafka-ui` (provectuslabs/kafka-ui) — expose port 8080
  - `flink-jobmanager` (apache/flink:1.18) — expose port 8081; mount `/flink-jobs` volume so `.sql` scripts are accessible inside the container
  - `flink-taskmanager` (apache/flink:1.18) — linked to jobmanager
  - `flink-sql-client` (apache/flink:1.18) — one-shot or interactive service used to submit `.sql` scripts via `./bin/sql-client.sh -f <script.sql>`; shares the same `/flink-jobs` volume
  - `postgres` (postgres:15) — expose port 5432, init scripts volume mounted
  - `redis` (redis:7-alpine) — expose port 6379
  - `minio` (minio/minio) — expose ports 9000 (API) and 9001 (console)
  - `minio-init` (one-shot mc container to create buckets on startup)
  - `qdrant` (qdrant/qdrant) — expose port 6333
  - `marquez` (marquezproject/marquez) — expose port 5000
  - `marquez-web` (marquezproject/marquez-web) — expose port 3000
  - `mlflow` (custom image or ghcr.io/mlflow/mlflow) — expose port 5001, backed by MinIO for artifact storage and PostgreSQL for run metadata
  - `streamlit` (custom Python image) — expose port 8501
- [ ] Write `.env` file with all configurable ports, credentials, and bucket names
- [ ] Write `docker/init-postgres.sql` to create schemas for the tax domain
- [ ] Write `docker/minio-init.sh` to create buckets: `raw-tax`, `cleaned-tax`, `mlflow-artifacts`
- [ ] Write a `Makefile` with targets: `up`, `down`, `restart`, `logs`, `topic-list`, `submit-sql` (submits all Flink SQL scripts via the sql-client container)

**Relevant Context:**
- Reference images show the original Confluent Cloud topology — we are replacing the cloud broker with a local Kafka container
- Marquez requires a PostgreSQL backend — configure it to use the same `postgres` container with a separate `marquez` database
- MLflow artifact store should point to MinIO via the S3-compatible endpoint
- **[Risk 1]** Sub-Task 1 must deliver `docker/Dockerfile.flink` with pinned connector JAR versions before Sub-Task 3 begins — see the Build Order section above for the required JAR list. Both `flink-jobmanager`, `flink-taskmanager`, and `flink-sql-client` services must use this custom image, not the base `apache/flink:1.18` image directly

---

### Sub-Task 2 — Kafka Topic Setup and 4 Tax Domain Producers

**Status:** `[ ] pending`

**Intent:**
Create all Kafka topics for the tax domain and implement 4 Python producer scripts that continuously generate realistic synthetic data. Producers run as long-lived Docker services and are the heartbeat of the platform.

**Expected Outcomes:**
- All 4 topics exist in Kafka and are visible in Kafka UI
- Each producer generates and publishes records at a configurable rate
- Messages are JSON-serialised with consistent schemas
- Occasional anomalies are injected to drive fraud detection downstream

**Todo List:**

**Tax Domain Topics + Producers:**
- [ ] Topic: `tax-applications` — fields: `application_id, customer_id, customer_name, email, country, province, taxable_income, employment_type, employment_status, submitted_date, tax_year, filing_status`
- [ ] Topic: `taxpayer-profiles` — fields: `customer_id, age, risk_score, historical_filings_count, avg_income_3yr, flagged_previously`
- [ ] Topic: `fraud-signals` — fields: `signal_id, application_id, customer_id, signal_type, severity, detected_at, description`
- [ ] Topic: `audit-events` — fields: `event_id, application_id, actor, action, timestamp, metadata`

**Implementation:**
- [ ] Create `producers/base_producer.py` with shared Kafka connection logic and configurable publish rate
- [ ] Create one producer file per topic (4 files), each inheriting from base: `producer_tax_applications.py`, `producer_taxpayer_profiles.py`, `producer_fraud_signals.py`, `producer_audit_events.py`
- [ ] Add producer Docker service definitions to `docker-compose.yml` (one service per producer, using compose profiles so they can be started independently)
- [ ] Create `producers/requirements.txt` (confluent-kafka, faker)

**Relevant Context:**
- The reference image shows `application_id, customer_id, customer_name, email, country, province, taxable_income, employment_type, employment_status, submitted_date` as the core tax application fields — use these exactly
- Use Python `Faker` library to generate realistic synthetic data
- Inject occasional anomalies (e.g. very high income, same `customer_id` filing multiple times quickly) to feed fraud pattern detection in Flink

---

### Sub-Task 3 — Flink SQL Jobs: Stream Processing, Enrichment, and Output Routing

**Status:** `[ ] pending`

**Intent:**
Write pure Flink SQL scripts (`.sql` files) that are submitted to the running Flink cluster via the Flink SQL Client (`./bin/sql-client.sh -f <script.sql>`). Each script defines its own `CREATE TABLE` DDL statements (Kafka source, connector sinks) and the `INSERT INTO` DML that drives the stream. A thin Python wrapper handles OpenLineage event emission before and after each submission. This keeps all transformation logic in readable SQL while Python handles only lineage side-effects.

**Expected Outcomes:**
- Flink Job Manager UI at `localhost:8081` shows all 5 jobs running with RUNNING status
- Raw events land in MinIO `raw-tax` bucket as JSON files partitioned by date
- Enriched, joined stream lands in PostgreSQL `enriched_tax_applications` table
- Windowed KPI aggregates (5-min tumbling windows) land in PostgreSQL `tax_kpi_windows` table (read into Redis by a lightweight Python consumer)
- Cleaned, standardised output lands in MinIO `cleaned-tax` bucket
- Flink-generated fraud patterns are published back to the `fraud-signals` Kafka topic
- OpenLineage `RunEvent` payloads are POSTed to Marquez for every job submission

**Todo List:**

**Flink SQL Scripts (stored in `/flink-jobs/`):**
- [ ] `job_01_raw_to_minio.sql`:
  - `CREATE TABLE kafka_tax_applications` (Kafka connector, JSON format)
  - `CREATE TABLE minio_raw_tax` (filesystem connector, S3/MinIO path, JSON format, partition by `DATE(submitted_date)`)
  - `INSERT INTO minio_raw_tax SELECT * FROM kafka_tax_applications`
- [ ] `job_02_enrich.sql`:
  - `CREATE TABLE kafka_tax_applications` (Kafka source)
  - `CREATE TABLE kafka_taxpayer_profiles` (Kafka source)
  - `CREATE TABLE pg_enriched_applications` (JDBC connector → PostgreSQL)
  - `INSERT INTO pg_enriched_applications SELECT a.*, p.age, p.risk_score, p.historical_filings_count, p.avg_income_3yr, p.flagged_previously FROM kafka_tax_applications a LEFT JOIN kafka_taxpayer_profiles FOR SYSTEM_TIME AS OF a.proc_time AS p ON a.customer_id = p.customer_id`
- [ ] `job_03_kpi_windows.sql`:
  - `CREATE TABLE kafka_tax_applications` (Kafka source)
  - `CREATE TABLE pg_tax_kpi_windows` (JDBC connector → PostgreSQL)
  - `INSERT INTO pg_tax_kpi_windows SELECT TUMBLE_START(..., INTERVAL '5' MINUTE) AS window_start, COUNT(*) AS total_count, AVG(taxable_income) AS avg_income, COUNT(DISTINCT customer_id) AS distinct_customers FROM kafka_tax_applications GROUP BY TUMBLE(..., INTERVAL '5' MINUTE)`
- [ ] `job_04_fraud_pattern.sql`:
  - `CREATE TABLE kafka_tax_applications` (Kafka source)
  - `CREATE TABLE kafka_fraud_signals_out` (Kafka sink → `fraud-signals` topic)
  - `INSERT INTO kafka_fraud_signals_out` — uses `MATCH_RECOGNIZE` or a subquery to detect: same `customer_id` appearing 3+ times within a 10-minute event-time window, or `taxable_income > avg_income_3yr * 3`
- [ ] `job_05_cleaned.sql`:
  - `CREATE TABLE kafka_tax_applications` (Kafka source)
  - `CREATE TABLE minio_cleaned_tax` (filesystem connector, S3/MinIO `cleaned-tax` path)
  - `INSERT INTO minio_cleaned_tax SELECT` — standardise `UPPER(country)`, `UPPER(province)`, cast types, filter `WHERE taxable_income IS NOT NULL AND customer_id IS NOT NULL`, add `CURRENT_TIMESTAMP AS processed_at`

**Job Submission Wrapper:**
- [ ] Write `scripts/submit_flink_jobs.py` — iterates over all `.sql` files in `/flink-jobs/`, emits OpenLineage START event, executes `docker exec flink-sql-client ./bin/sql-client.sh -f <script>`, emits OpenLineage COMPLETE or FAIL event
- [ ] Add `submit-sql` target to `Makefile` that calls `submit_flink_jobs.py`

**Redis KPI Cache:**
- [ ] Write `scripts/kpi_to_redis.py` — a lightweight polling consumer that reads the latest rows from PostgreSQL `tax_kpi_windows` and writes them to Redis as hash keys `tax:kpi:window:{window_start}` with 15-minute TTL; runs as a Docker service

**Relevant Context:**
- Flink SQL connector JARs required in the cluster image: `flink-sql-connector-kafka-*.jar`, `flink-connector-jdbc-*.jar`, `flink-connector-filesystem-*.jar`, `flink-s3-fs-hadoop-*.jar`
- The `flink-sql-client` container must share the same connector JARs and Flink lib path as the jobmanager — easiest approach is a custom Dockerfile extending `apache/flink:1.18` with all JARs pre-copied into `/opt/flink/lib/`
- JDBC sink for PostgreSQL requires the `postgresql-*.jar` driver also in `/opt/flink/lib/`
- For the fraud pattern job, `MATCH_RECOGNIZE` is the cleanest Flink SQL approach for CEP; alternatively a self-join with `INTERVAL` bounds works if CEP is too complex for initial pass
- MinIO S3 endpoint config in Flink SQL: set via `flink-conf.yaml` properties `s3.endpoint`, `s3.access-key`, `s3.secret-key`, `s3.path.style.access: true`

---

### Sub-Task 4 — Storage Layer: PostgreSQL, Redis, MinIO, DuckDB OLAP

**Status:** `[ ] pending`

**Intent:**
Implement all storage layer components. PostgreSQL holds enriched records and windowed KPI output from Flink. Redis serves as the hot KPI cache populated by the `kpi_to_redis.py` poller. MinIO holds all raw and cleaned event data as files. DuckDB queries MinIO directly via S3 API to serve deeper OLAP queries for the dashboard and MLflow training data preparation.

**Expected Outcomes:**
- PostgreSQL has the full tax domain schema with indexes on key fields
- Redis keys are being populated with the latest KPI windows and readable via `redis-cli`
- MinIO `raw-tax` and `cleaned-tax` buckets contain date-partitioned JSON files from Flink sinks
- DuckDB can query cleaned MinIO data and return results in under 5 seconds for demo-scale data
- A `storage/duckdb_queries.py` module exposes named query functions used by the dashboard and MLflow

**Todo List:**
- [ ] Write `storage/postgres_schema.sql` — tables for: `enriched_tax_applications`, `tax_kpi_windows`, `fraud_detections`, `audit_log`; add indexes on `customer_id`, `submitted_date`, `window_start`
- [ ] Write `storage/redis_schema.md` — document Redis key naming conventions (`tax:kpi:window:{window_start}`) and TTL policies (15 minutes)
- [ ] Write `storage/duckdb_queries.py` with functions:
  - `get_tax_kpi_trend(n_windows)` — reads `cleaned-tax` from MinIO, returns time-series windowed aggregates for the dashboard trend chart
  - `get_fraud_summary()` — aggregates fraud signals from `cleaned-tax` by signal type, severity, province, and time bucket
  - `get_cleaned_data_for_ml(n_rows)` — returns a clean DataFrame of tax application records with fraud label for MLflow training
  - `get_income_distribution()` — returns income bracket distribution across all cleaned applications for the dashboard histogram
- [ ] Write `storage/minio_client.py` — wrapper around `boto3` for file listing and download operations used by DuckDB and MLflow
- [ ] Configure DuckDB to use `httpfs` extension pointed at MinIO S3 endpoint via environment variables

**Relevant Context:**
- DuckDB reads from MinIO using the `httpfs` extension: `SET s3_endpoint='minio:9000'; SET s3_access_key_id=...; SET s3_secret_access_key=...; SET s3_use_ssl=false; SET s3_url_style='path'`
- The cleaned layer in MinIO is what DuckDB primarily queries — Flink cleans and standardises first, DuckDB analyses
- Redis TTL on KPI keys should be 15 minutes; the `kpi_to_redis.py` poller (Sub-Task 3) writes these keys

---

### Sub-Task 5 — Qdrant Vector Store: Embeddings for Fraud Similarity and Regulatory Q&A

**Status:** `[ ] pending`

**Intent:**
Populate two Qdrant collections using HuggingFace sentence-transformer models. One collection holds embedded tax records for fraud similarity search (find records similar to a known fraudulent case). The second holds a regulatory knowledge base (chunked tax law, filing rules) for the Q&A agent.

**Expected Outcomes:**
- Qdrant has two collections: `fraud_similarity` and `regulatory_kb`
- `fraud_similarity` contains embedded tax application records, updated periodically from the cleaned MinIO layer
- `regulatory_kb` contains chunked and embedded synthetic regulatory/tax documents
- Both collections are queryable via the Qdrant Python client
- A `agents/qdrant_client.py` module exposes `search_similar_fraud(record)` and `search_regulatory_kb(query)` functions

**Todo List:**
- [ ] Choose and document the HuggingFace embedding model: `sentence-transformers/all-MiniLM-L6-v2` (fast, 384-dim, open-source)
- [ ] Write `agents/embed_tax_records.py` — reads cleaned tax records from MinIO/DuckDB, generates embeddings, upserts into `fraud_similarity` Qdrant collection
- [ ] Write `agents/embed_regulatory_kb.py` — generates ~50 synthetic regulatory document chunks (tax rules, income thresholds, filing requirements), embeds and loads into `regulatory_kb` collection
- [ ] Write `agents/qdrant_search.py` — provides `search_similar_fraud(record_dict, top_k=5)` and `search_regulatory_kb(query_str, top_k=3)` functions
- [ ] Write `agents/embeddings.py` — shared embedding utility wrapping the HuggingFace sentence-transformers model
- [ ] Create a `scripts/seed_qdrant.py` one-shot script to populate both collections from existing data:
  - **[Risk 3]** Add a startup guard at the top of the script: check that the `cleaned-tax` MinIO bucket contains at least one file before proceeding; exit with a clear error message if empty

**Relevant Context:**
- `sentence-transformers/all-MiniLM-L6-v2` is ~80MB and runs CPU-only — no GPU needed for the demo
- The `fraud_similarity` collection payload should store: `application_id, customer_id, taxable_income, employment_type, risk_score`
- The `regulatory_kb` collection payload should store: `chunk_id, source_doc, text, section`
- **[Risk 3]** This sub-task depends on Sub-Task 3 having run `job_05_cleaned.sql` and written records to the `cleaned-tax` bucket — do not run `make seed` until `cleaned-tax` is non-empty

---

### Sub-Task 6 — MLflow: Model Training on Cleaned Flink Output Data

**Status:** `[ ] pending`

**Intent:**
Use the cleaned, standardised data from the MinIO cleaned layer (output of Flink cleaning jobs) to train a simple fraud detection classifier. Track all experiments, parameters, metrics, and model artifacts in MLflow. The resulting model is registered in the MLflow Model Registry for the LangGraph agent to load and call.

**Expected Outcomes:**
- MLflow UI shows at least one completed experiment with runs, metrics, and logged model
- A trained fraud detection model is registered in the MLflow Model Registry under the name `fraud-detector`
- The model can be loaded via `mlflow.sklearn.load_model()` or `mlflow.pyfunc.load_model()`
- MLflow artifacts are stored in MinIO (`mlflow-artifacts` bucket)
- OpenLineage events emitted for the training run (input dataset = cleaned MinIO data, output = MLflow model)

**Todo List:**
- [ ] Write `mlflow/train_fraud_model.py`:
  - Load cleaned tax data from DuckDB/MinIO using `get_cleaned_data_for_ml()`
  - Feature engineering: encode categorical fields, scale numerical fields
  - Train a `RandomForestClassifier` (scikit-learn) on a synthetic fraud label (injected by producer anomalies)
  - Log: parameters, accuracy, precision, recall, F1, confusion matrix artifact
  - Register model in MLflow Model Registry as `fraud-detector` with stage `Staging`
- [ ] Write `mlflow/requirements.txt` — mlflow, scikit-learn, pandas, boto3
- [ ] Write `mlflow/mlflow_config.py` — centralised config for tracking URI, S3 endpoint, experiment names
- [ ] Emit OpenLineage `RunEvent` from the training script (input: `minio://cleaned-tax/`, output: `mlflow://fraud-detector`)
- [ ] Write `mlflow/promote_model.py` — script to transition model stage from `Staging` to `Production`

**Relevant Context:**
- MLflow tracking server URI: `http://mlflow:5001`
- MLflow artifact root: `s3://mlflow-artifacts/` (MinIO-backed, using boto3 S3 config)
- The fraud label for training: records with fraud signals (`fraud_signals` topic) within 24 hours of application are labelled `1`, else `0`

---

### Sub-Task 7 — LangGraph Multi-Agent System

**Status:** `[ ] pending`

**Intent:**
Build a LangGraph multi-agent graph with a Supervisor node that routes incoming queries to one of two specialist agents:
- **Fraud Detection Agent** — uses the MLflow-registered model + Qdrant similarity search to assess whether a tax application is fraudulent
- **Tax Q&A Agent** — uses Qdrant regulatory knowledge base + a HuggingFace text generation model to answer natural-language tax questions

**Expected Outcomes:**
- A runnable LangGraph `StateGraph` defined in `agents/langgraph_app.py`
- Supervisor node correctly routes `fraud_check` intents to the fraud agent and `tax_question` intents to the Q&A agent
- Fraud agent returns a structured JSON response: `{application_id, fraud_probability, similar_cases, recommendation}`
- Q&A agent returns a natural-language answer with source regulatory references
- The graph is invokable as a Python function and from the Streamlit chat tab

**Todo List:**
- [ ] Choose HuggingFace text generation model: `google/flan-t5-base` (small, instruction-tuned, runs CPU-only, ~250MB)
- [ ] Write `agents/llm_wrapper.py` — loads `flan-t5-base` via `transformers` pipeline, exposes a `generate(prompt, max_tokens)` function
- [ ] Write `agents/fraud_agent.py` — LangGraph node:
  - Accepts `application_dict` in state
  - Loads `fraud-detector` model from MLflow Model Registry
  - Runs model inference → `fraud_probability`
  - Calls `search_similar_fraud()` for top-5 similar known cases from Qdrant
  - Returns structured response
- [ ] Write `agents/qa_agent.py` — LangGraph node:
  - Accepts `question` string in state
  - Calls `search_regulatory_kb()` to retrieve top-3 relevant document chunks
  - Builds a RAG prompt: context chunks + question
  - Calls `generate()` from `llm_wrapper.py`
  - Returns answer + source references
- [ ] Write `agents/supervisor.py` — LangGraph node:
  - Classifies intent: if input contains `application_id` → route to fraud agent; else → route to Q&A agent
  - Uses simple keyword/heuristic routing (no LLM needed for routing decision)
- [ ] Write `agents/langgraph_app.py` — assembles `StateGraph`, defines edges, compiles the graph, exposes `run(user_input)` entry point
- [ ] Write `agents/requirements.txt` — langgraph, langchain-core, transformers, torch (CPU), mlflow, qdrant-client

**Relevant Context:**
- `flan-t5-base` is small enough to run on CPU inside Docker — pin `torch==2.x` CPU-only wheel
- The Supervisor node uses `conditional_edges` in LangGraph to branch to fraud or Q&A node
- The state schema: `{input: str, application_dict: dict | None, question: str | None, response: str, sources: list}`

---

### Sub-Task 8 — Data Lineage: OpenLineage + Marquez Full Coverage

**Status:** `[ ] pending`

> **[Risk 2] This sub-task is split into two phases.** Phase A must be completed before Sub-Task 3. Phase B is completed after Sub-Task 9. See the Build Order section for the full rationale.

**Intent:**
Ensure every data movement in the platform emits OpenLineage events to Marquez, creating a complete, navigable lineage graph from raw Kafka topics all the way to MLflow model training runs. The Marquez UI should show the full DAG of data flows.

**Expected Outcomes:**
- Marquez UI shows a lineage DAG covering: Kafka topics → Flink jobs → PostgreSQL/MinIO/Redis → DuckDB → MLflow model
- Every Flink job has START and COMPLETE lineage events
- DuckDB query runs emit lineage events (input: MinIO files, output: dashboard/model)
- MLflow training run emits lineage event (input: cleaned MinIO, output: MLflow model artifact)
- Lineage events follow the OpenLineage `RunEvent` spec with proper `InputDataset` and `OutputDataset` fields

**Todo List:**

**Phase A — Build emission primitives (complete before Sub-Task 3):**
- [ ] Write `lineage/openlineage_client.py` — a reusable client that:
  - Builds `RunEvent` objects with `run_id` (UUID), `job.name`, `job.namespace`, `inputs`, `outputs`, `eventType` (START/COMPLETE/FAIL)
  - POSTs to Marquez API at `http://marquez:5000/api/v1/lineage`
- [ ] Write `lineage/lineage_schemas.py` — helper functions to build `InputDataset` and `OutputDataset` objects for each storage type (Kafka topic, MinIO path, PostgreSQL table, MLflow model)

**Phase B — Instrument call sites and verify (complete after Sub-Task 9):**
- [ ] Instrument `scripts/submit_flink_jobs.py` wrapper with `emit_start()` and `emit_complete()` calls per SQL job
- [ ] Instrument `mlflow/train_fraud_model.py` with lineage emit (input: `minio://cleaned-tax/`, output: `mlflow://fraud-detector`)
- [ ] Instrument `storage/duckdb_queries.py` with lineage emit on each query function
- [ ] Write `lineage/verify_lineage.py` — calls Marquez API and prints a summary of all registered jobs and datasets; used as part of `make smoke-test`
- [ ] Confirm full lineage DAG is visible in Marquez Web UI at `localhost:3000`

**Relevant Context:**
- Marquez API endpoint: `POST /api/v1/lineage` accepts OpenLineage `RunEvent` JSON
- Namespaces to use: `kafka`, `minio`, `postgres`, `duckdb`, `mlflow` — consistent naming is critical for Marquez to link nodes correctly
- The Marquez Web UI at port 3000 provides a visual graph — lineage is only visible if namespaces and dataset names are consistent across events
- **[Risk 2]** `lineage/openlineage_client.py` and `lineage/lineage_schemas.py` must exist before Sub-Tasks 3, 4, and 6 import them — Phase A is a hard prerequisite for those sub-tasks

---

### Sub-Task 9 — Streamlit Multi-Tab Dashboard

**Status:** `[ ] pending`

**Intent:**
Build a single Streamlit application with five tabs, each serving a different persona or analytical view. The e-commerce tab is removed; the remaining tabs cover the full tax analytics platform end-to-end. The dashboard reads from Redis (hot KPIs), DuckDB (OLAP queries), PostgreSQL (enriched records), Marquez API (lineage), MLflow API (model status), and the LangGraph agent (chat).

**Expected Outcomes:**
- Streamlit app runs at `localhost:8501` and auto-refreshes KPI tab every 30 seconds
- Tab 1 **Tax KPIs**: live windowed KPIs from Redis + time-series trend chart + income distribution histogram from DuckDB
- Tab 2 **Fraud Intelligence**: fraud signal counts by type/severity, top flagged customers table, province heatmap from DuckDB `get_fraud_summary()`
- Tab 3 **AI Agent Chat**: text input → LangGraph `run()` → response with sources displayed
- Tab 4 **Data Lineage**: lineage job/dataset table pulled from Marquez API + iframe embed of Marquez Web UI
- Tab 5 **MLflow Model Status**: experiment runs table, best model metrics, registered model stage badge

**Todo List:**
- [ ] Write `dashboard/app.py` — main Streamlit app with `st.tabs()` for all 5 tabs
- [ ] Write `dashboard/tabs/tab_tax_kpi.py` — reads Redis `tax:kpi:window:*` keys for live KPI cards; calls DuckDB `get_tax_kpi_trend()` for time-series line chart and `get_income_distribution()` for histogram
- [ ] Write `dashboard/tabs/tab_fraud.py` — reads PostgreSQL `fraud_detections` table + DuckDB `get_fraud_summary()`; renders: signal counts bar chart, top flagged customers table, province-level fraud heatmap
- [ ] Write `dashboard/tabs/tab_agent_chat.py` — renders a chat interface using `st.chat_input` / `st.chat_message`; calls `agents/langgraph_app.py:run()` on submit; shows spinner while agent runs; displays sources list under each response
- [ ] Write `dashboard/tabs/tab_lineage.py` — calls Marquez API `GET /api/v1/namespaces/{ns}/jobs` to render a lineage jobs/datasets table; embeds Marquez Web UI via `st.components.v1.iframe` at port 3000
- [ ] Write `dashboard/tabs/tab_mlflow.py` — calls MLflow tracking REST API to list experiment runs; renders runs table with accuracy/F1/precision columns; shows registered `fraud-detector` model stage as a coloured badge
- [ ] Write `dashboard/requirements.txt` — streamlit, redis, psycopg2, duckdb, boto3, requests, pandas, plotly
- [ ] Add `dashboard/.streamlit/config.toml` with theme settings and `server.runOnSave = true`

**Relevant Context:**
- Redis reads use `redis-py` with `r.scan_iter('tax:kpi:window:*')` to fetch the latest N window keys
- DuckDB in the Streamlit container connects to MinIO via environment variables from `.env`
- The LangGraph agent is CPU-bound and may take several seconds — wrap the call in `st.spinner()` and consider `asyncio` if blocking becomes an issue
- Marquez API base URL: `http://marquez:5000/api/v1`
- MLflow tracking API base URL: `http://mlflow:5001`

---

### Sub-Task 10 — Integration Testing, README, and Demo Script

**Status:** `[ ] pending`

**Intent:**
Validate that all components work end-to-end, write a comprehensive README, and create a demo script that walks through the full platform in sequence. This makes the project presentable and reproducible.

**Expected Outcomes:**
- `docker-compose up` followed by `make seed` fully initialises the platform
- Producers are publishing, Flink jobs are running, data is flowing into all storage layers
- Streamlit dashboard shows live data on all tabs
- A `DEMO.md` file walks through the platform with screenshots/instructions
- `README.md` explains the architecture, prerequisites, and quickstart

**Todo List:**
- [ ] Write `scripts/smoke_test.py` — checks: Kafka topics exist, MinIO buckets have files, PostgreSQL tables have rows, Redis keys exist, Qdrant collections have vectors, Marquez has registered jobs, MLflow has runs
- [ ] Write `Makefile` targets: `seed` (runs Qdrant seeding + model training), `smoke-test`, `reset` (wipe volumes and restart)
- [ ] Write `README.md` with: architecture diagram reference, prerequisites (Docker, Docker Compose, 16GB RAM recommended), quickstart steps, service port reference table
- [ ] Write `DEMO.md` — step-by-step walkthrough: start platform → watch producers in Kafka UI → view Flink jobs → check MinIO → run smoke test → open Streamlit dashboard → try AI agent chat → view lineage in Marquez → check MLflow

**Relevant Context:**
- Total RAM requirement is significant: Kafka + Flink + Postgres + Redis + MinIO + Qdrant + MLflow + Streamlit + 4 producers + kpi-poller — recommend 16GB minimum, document this clearly
- `flan-t5-base` and `all-MiniLM-L6-v2` downloads happen on first container start — add a pre-pull / model-cache step to the Makefile
- **[Risk 3]** The `make seed` target must assert that `cleaned-tax` is non-empty before calling `scripts/seed_qdrant.py` — add a preflight check that exits with a clear error if the bucket is empty
- **[Risk 2]** `make smoke-test` should call `lineage/verify_lineage.py` as one of its checks to confirm the full Marquez DAG is populated
- The smoke test should be runnable with `make smoke-test` after `make seed`

---

## Technology Reference

| Layer | Tool | Version / Image |
|---|---|---|
| Message broker | Apache Kafka | bitnami/kafka:3.6 |
| Kafka UI | kafka-ui | provectuslabs/kafka-ui:latest |
| Stream processing | Apache Flink SQL Client | apache/flink:1.18 (custom image with connector JARs) |
| Transactional DB | PostgreSQL | postgres:15 |
| Hot cache | Redis | redis:7-alpine |
| Object storage | MinIO | minio/minio:latest |
| OLAP engine | DuckDB | duckdb Python lib (in-process) |
| Vector store | Qdrant | qdrant/qdrant:latest |
| Data lineage | OpenLineage + Marquez | marquezproject/marquez:0.47 |
| Model tracking | MLflow | mlflow Docker or pip install |
| LLM / embeddings | HuggingFace Transformers | flan-t5-base + all-MiniLM-L6-v2 |
| Agent framework | LangGraph | langchain + langgraph pip |
| Dashboard | Streamlit | streamlit pip |
| Containerisation | Docker Compose | v2+ |

---

## Resolved Decisions

1. **Flink processing approach**: Pure Flink SQL scripts (`.sql` files) submitted via the Flink SQL Client (`./bin/sql-client.sh -f <script.sql>`). SQL handles all transformations; a thin Python wrapper handles OpenLineage side-effects only.
2. **Producer rate**: 1 event/second per producer (4/sec total across the tax domain) — configurable via `.env`.
3. **ML fraud label**: Synthetically injected by the producer (`is_fraud` flag set probabilistically, with higher probability on anomalous records) — keeps the training pipeline self-contained without requiring Flink feedback loop completion first.
4. **Streamlit hosting**: Runs as a Docker Compose service on port 8501 — fully contained in the stack.
