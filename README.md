# Open-Source Real-Time Streaming Analytics Platform

A fully containerised, end-to-end real-time data streaming and AI analytics platform built entirely with open-source technologies.

This platform simulates a **South African tax analytics environment** where events are generated, streamed through Kafka, processed with Flink SQL, stored across multiple data layers, enriched with machine learning, and exposed through AI-powered agents and a live dashboard.

---

## Project Goals

This platform was built to explore and demonstrate:

1. Real-time event streaming with Apache Kafka
2. Stream processing with Apache Flink SQL
3. Data lakehouse storage patterns
4. Feature engineering and ML model training with MLflow
5. Vector search and semantic retrieval with Qdrant
6. Agentic AI with LangGraph (Supervisor, Fraud Detection, Tax Q&A)
7. Data lineage and observability with OpenLineage and Marquez
8. Production-style containerised deployments with Podman

---

## Solution Architecture

```
  Tax Event Producers
  (tax_applications, taxpayer_profiles,
   fraud_signals, audit_events)
          │
          ▼
    Apache Kafka
    (Confluent 7.5)
          │
          ▼
  Apache Flink SQL
  ┌────────────────────────────────────────────────────┐
  │  job_01  raw → MinIO                               │
  │  job_02  enrich & join                             │
  │  job_03  KPI windowed aggregations                 │
  │  job_04  fraud pattern detection                   │
  │  job_05  cleaned records                           │
  │  job_06  fraud signals → PostgreSQL                │
  └────────────────────────────────────────────────────┘
          │
  ┌───────┼──────────┬──────────┬──────────────┐
  ▼       ▼          ▼          ▼              ▼
PostgreSQL  Redis   MinIO    Qdrant       OpenLineage
            │                 │               │
        (KPI cache)    (regulatory_kb,     Marquez
                        fraud_similarity)
          │                   │
          └──────────┬─────────┘
                     ▼
                  DuckDB
              (OLAP analytics)
          ┌──────────┴──────────┐
          ▼                     ▼
     Streamlit             MLflow
     Dashboard          (experiment tracking
     (6 tabs)            + model registry)
                              │
                              ▼
                      LangGraph Agents
                  ┌───────────┼───────────┐
                  ▼           ▼           ▼
             Supervisor   Fraud       Tax Q&A
               Agent    Detection     Agent
                          Agent       (RAG)
```

---

## Key Features

### Real-Time Event Streaming
- Four Kafka producers simulate realistic South African tax-domain events
- Topics: `tax_applications`, `taxpayer_profiles`, `fraud_signals`, `audit_events`
- Event-driven, schema-consistent payloads

### Stream Processing
- Six Apache Flink SQL jobs covering raw ingestion, enrichment, windowed KPIs, fraud pattern matching, data cleaning, and PostgreSQL sync
- Near real-time analytics with tumbling and sliding window aggregations

### Lakehouse Storage

| Layer | Technology | Purpose |
|---|---|---|
| Transactional | PostgreSQL | Structured records, fraud signals |
| Cache | Redis | Low-latency KPI reads for dashboard |
| Object Store | MinIO | Raw and processed Parquet/JSON |
| Analytics | DuckDB | OLAP queries over processed datasets |
| Vector Search | Qdrant | Embeddings for RAG and fraud similarity |

### AI & Machine Learning
- MLflow tracks fraud detection model experiments and hosts the model registry
- `fraud-detector` model trained on application features (province, taxable income, employment type/status, filing status) plus engineered features: income relative to the employment-type ceiling, log income and how often the customer has filed (see [Fraud model and tuning](#fraud-model-and-tuning))
- A decision threshold chosen on out-of-fold predictions is stored with the model, so predictions use the tuned operating point instead of a fixed 0.5
- HuggingFace `all-MiniLM-L6-v2` embeddings for semantic search
- Qdrant collections: `regulatory_kb` (50 SA tax law chunks) and `fraud_similarity` (case embeddings, kept in sync with the stream by the `qdrant-sync` service)

### Agentic AI (LangGraph)

| Agent | Role |
|---|---|
| Supervisor | Classifies user intent and routes to the correct specialist agent |
| Fraud Detection Agent | Runs ML inference + Qdrant similarity search to assess fraud risk |
| Tax Q&A Agent | RAG-powered retrieval over regulatory knowledge base with LLM answer generation |

### Data Lineage
- OpenLineage integration emits pipeline metadata at every stage
- Marquez UI provides end-to-end lineage visibility

### Dashboard (Streamlit)
Six tabs covering:
- **Control** — service health and platform controls
- **Tax KPIs** — real-time windowed metrics from Redis/DuckDB
- **Fraud** — fraud signal monitoring and risk scoring
- **Agent Chat** — conversational interface to the LangGraph agent system
- **MLflow** — experiment and model registry browser, live scoring, and a **hyperparameter tuning** panel (search space, objective, best config by precision / recall / F1)
- **Lineage** — pipeline lineage viewer

---

## Technology Stack

| Layer | Technology |
|---|---|
| Event Streaming | Apache Kafka (Confluent 7.5) |
| Stream Processing | Apache Flink SQL |
| Database | PostgreSQL |
| Cache | Redis |
| Object Storage | MinIO |
| Analytics Engine | DuckDB |
| Vector Database | Qdrant |
| Data Lineage | OpenLineage + Marquez |
| ML Platform | MLflow |
| Embeddings | HuggingFace (all-MiniLM-L6-v2) |
| AI Agent Framework | LangGraph |
| LLM | LiteLLM routing: HuggingFace Inference + OpenAI, with a token budget |
| Dashboard | Streamlit |
| Container Platform | Podman |

---

## Prerequisites

**Required**
- Podman Desktop v4.7+
- Python 3.11+
- GNU Make

**Recommended Hardware**

| Resource | Minimum |
|---|---|
| CPU | 8+ cores |
| RAM | 16 GB+ |
| Storage | 20 GB+ free |

---

## Quick Start

**1. Clone the repository**
```bash
git clone <repo-url>
cd data-streaming
```

**2. Start Podman**
```bash
podman machine start
```

**3. Launch the platform**
```bash
make start                         # or, without make: python scripts/launcher.py up
```
The [launcher](#launcher) starts every service, waits for them to become healthy, creates the Kafka topics, and submits the Flink SQL jobs that are not already running. It is safe to re-run at any time.

> Manual alternative: `make up` then `make submit-sql`.

**4. Start the data producers** (optional)
```bash
python scripts/launcher.py up --producers
```

**5. Seed the vector database**
```bash
make seed
```

**6. Validate the platform**
```bash
make smoke-test
```

**7. Open the dashboard**
```
http://localhost:8501
```

---

## Launcher

[`scripts/launcher.py`](scripts/launcher.py) is the host-side entry point for starting and checking the platform. It runs natively on Windows, WSL and Linux, with Podman or Docker (`CONTAINER_CLI` overrides auto-detection), so the same command works locally and on a Docker host.

```bash
python scripts/launcher.py up [--producers] [--no-wait]   # start, wait healthy, submit missing Flink jobs
python scripts/launcher.py down                           # stop all services
python scripts/launcher.py status                         # service health + Flink job state
python scripts/launcher.py jobs [filter]                  # submit only the Flink jobs that are not running
```

`make start`, `make stop` and `make status` are thin wrappers around the same commands.

- **Idempotent:** Flink jobs are matched by sink table against the running jobs in the Flink REST API. Jobs already running are skipped, so re-running never creates duplicates. A job file that is only partially running is reported and left alone.
- **Health-based waits:** `up` polls Kafka, Postgres, MinIO, Flink and Marquez instead of sleeping for a fixed time.
- **After a restart:** Flink job state is not persisted across a JobManager restart. Run `make start` (or `python scripts/launcher.py jobs`) to bring the jobs back. On a server this can be scheduled with cron or a systemd timer.
- **Not included:** the initial Qdrant seed and MLflow training are separate (`make seed`, `make train`, or the dashboard MLflow tab). `qdrant-sync` is started by `up` and keeps the similarity index current afterwards.

**Control Center and container control.** Start/Stop/Restart in the Control Center need either the Podman socket mounted into the Streamlit container or a container CLI inside it. The socket mount is unreliable on Windows/WSL. When neither is available, the Producer Controls are shown read-only with a pointer to the launcher; status, Flink job views and logs still work.

---

## Fraud model and tuning

**Injected fraud.** The tax-applications producer injects **2 to 5 highly fraudulent applications in every block of 50**, at random positions within the block (`producers/fraud_injection.py`). Each injected case carries at least two independent red flags (income inflation, UNEMPLOYED with a large income, or a repeat filer with inflated income), so it is clearly fraudulent to both the label and the model. Tune it with `FRAUD_BLOCK_SIZE`, `FRAUD_INJECT_MIN` and `FRAUD_INJECT_MAX`. The producer image copies its code at build time, so rebuild it after changing them: `podman compose --profile producers up -d --build producer-tax-applications`.

**Why the old model was weak.** It saw raw income and four categoricals only, but the labels depend on income relative to the employment-type ceiling and on how often a customer files. On the streamed data it scored about P 0.37 / R 0.23 / F1 0.29. The model now also uses `income_ratio`, `above_ceiling`, `log_income` and `customer_filings` (learned inside the pipeline, so callers only send `customer_id` / `application_id`). Cross-validated F1 on the same data rose to about 0.75 to 0.78.

**Threshold.** The decision threshold is chosen on out-of-fold predictions of the training split and stored with the model. Metrics are reported on a held-out test split. `/predict` returns `fraud_probability`, `predicted_fraud` and the `threshold`.

**Tuning panel.** The dashboard **MLflow** tab has a tuning section where you can:

- pick model families (Random Forest, Gradient Boosting, Logistic Regression) and edit each hyperparameter's search range
- choose the objective: **F1**, **recall** (with a minimum-precision floor) or **precision** (with a minimum-recall floor)
- set trials per family and CV folds, and watch live progress
- compare the best configuration of each family against the default, with CV and held-out test precision / recall / F1, the top trials and every trial's hyperparameters
- train and register a model with the winning configuration in one click

Every trial is logged to the `tax-fraud-tuning` MLflow experiment (a parent run plus one nested run per trial). The same search can be run from the command line: `python mlflow/tune_fraud_model.py --n-trials 12 --objective f1`. The model API exposes `GET /tune/space`, `POST /tune`, `GET /tune/status` and `GET /tune/result`. Training and tuning share one job slot, so a second request while one is running returns 409.

**Qdrant sync.** `make seed` is a one-off snapshot. The `qdrant-sync` service (`scripts/sync_qdrant.py`) runs continuously: every `QDRANT_SYNC_INTERVAL_SEC` (default 30) it looks at the newest `QDRANT_SYNC_LIMIT` (default 5000) cleaned records and embeds only those not yet in `fraud_similarity`. Point ids are `uuid5(application_id)`, so it is idempotent and safe to restart. Payloads now also carry `employment_status`, `filing_status` and `processed_at`. Run it once with `python scripts/sync_qdrant.py --once`.

**CPU note.** The ML containers see every host core but are capped at about one CPU. `OMP_NUM_THREADS` and `OPENBLAS_NUM_THREADS` are set to 2 for `ml-trainer` and `model-api`; without it, gradient boosting stalls on thread oversubscription.

---

## LLM Token Budget

Agent calls go through LiteLLM ([`agents/llm_wrapper.py`](agents/llm_wrapper.py)), governed by [`agents/litellm_config.yaml`](agents/litellm_config.yaml).

| Provider | Per stint | Total cap |
|---|---|---|
| HuggingFace | 20,000 tokens | 100,000 |
| OpenAI | 10,000 tokens | 50,000 |

- Rotation: HuggingFace (20k) → OpenAI (10k) → HuggingFace (20k) → … until each cap is reached.
- If a provider hits a quota, rate-limit or auth error it is put in cooldown (600s) and the call fails over to the other provider.
- Once both caps are reached the dashboard prompts the user to bring their own model/provider (session-only key, not counted against the budget).
- Dev override: set `LLM_FORCE_PROVIDER=openai` (or `huggingface`), or use the provider-mode radio in the Agent Chat tab. The OpenAI 50k cap still applies.
- Budget state is persisted to `LLM_BUDGET_STATE` (gitignored).

Environment variables (in `.env`): `HF_API_TOKEN`, `HF_MODEL_LLM` (must be a chat model on `router.huggingface.co`, e.g. `meta-llama/Llama-3.1-8B-Instruct`), `OPENAI_API_KEY`, `OPENAI_MODEL`.

---

## Project Structure

```
data-streaming/
│
├── producers/                  # Kafka event producers
│   ├── producer_tax_applications.py
│   ├── producer_taxpayer_profiles.py
│   ├── producer_fraud_signals.py
│   ├── producer_audit_events.py
│   ├── base_producer.py
│   └── topic_init.py
│
├── flink-jobs/                 # Flink SQL job definitions
│   ├── job_01_raw_to_minio.sql
│   ├── job_02_enrich.sql
│   ├── job_03_kpi_windows.sql
│   ├── job_04_fraud_pattern.sql
│   ├── job_05_cleaned.sql
│   └── job_06_fraud_signals_to_pg.sql
│
├── agents/                     # LangGraph agent system
│   ├── supervisor.py           # Intent classification and routing
│   ├── fraud_agent.py          # ML inference + Qdrant similarity
│   ├── qa_agent.py             # RAG-powered tax Q&A
│   ├── llm_wrapper.py          # LiteLLM wrapper (budget, failover, BYO model)
│   ├── token_budget.py         # Token budget: rotation, caps, cooldown
│   ├── litellm_config.yaml     # Models and token-budget config
│   ├── qdrant_search.py        # Vector search helpers
│   ├── embeddings.py           # Embedding utilities
│   ├── embed_regulatory_kb.py  # Seeds regulatory_kb collection
│   ├── embed_tax_records.py    # Seeds fraud_similarity; sync_new() embeds only unseen records
│   └── langgraph_app.py        # LangGraph graph definition
│
├── mlflow/                     # ML model training and registry
│   ├── fraud_model.py          # Features, model families, search space, threshold selection
│   ├── train_fraud_model.py
│   ├── tune_fraud_model.py     # Hyperparameter search (CV) logged to MLflow
│   ├── serve_model.py          # Model API: /predict, /train, /tune
│   ├── promote_model.py
│   └── mlflow_config.py
│
├── storage/                    # Storage layer helpers
│   ├── postgres_schema.sql
│   ├── duckdb_queries.py
│   ├── minio_client.py
│   └── redis_schema.md
│
├── lineage/                    # OpenLineage integration
│   ├── openlineage_client.py
│   ├── lineage_schemas.py
│   └── verify_lineage.py
│
├── dashboard/                  # Streamlit dashboard
│   ├── app.py
│   └── tabs/
│       ├── tab_control.py
│       ├── tab_tax_kpi.py
│       ├── tab_fraud.py
│       ├── tab_agent_chat.py
│       ├── tab_mlflow.py
│       └── tab_lineage.py
│
├── scripts/                    # Operational scripts
│   ├── launcher.py             # Host-side start/stop/status + idempotent Flink job submit
│   ├── seed_qdrant.py
│   ├── submit_flink_jobs.py
│   ├── kpi_to_redis.py
│   └── smoke_test.py
│
├── tests/                      # Test logs and validation outputs
├── docker/                     # Container configuration
├── docker-compose.yml
├── Makefile
└── README.md
```

---

## Service Endpoints

| Service | URL |
|---|---|
| Streamlit Dashboard | http://localhost:8501 |
| Kafka UI | http://localhost:8080 |
| Flink UI | http://localhost:8081 |
| MinIO Console | http://localhost:9001 |
| Qdrant | http://localhost:6333 |
| Marquez UI | http://localhost:3000 |
| MLflow | http://localhost:5001 |
| Model API (in-network) | http://model-api:8000 (`/health`, `/predict`, `/reload`, `/train`, `/train/status`) |

---

## Common Commands

**Infrastructure**
```bash
make start       # Launcher: start, wait healthy, submit missing Flink jobs
make stop        # Launcher: stop all services
make status      # Launcher: service health + Flink job state
make up          # Start all services (no health wait, no job submit)
make down        # Stop all services
make restart     # Restart all services
make logs        # Tail service logs
```

**Data Platform**
```bash
make submit-sql  # Deploy all Flink SQL jobs
make topic-list  # List Kafka topics
```

**AI Components**
```bash
make seed             # Embed and upsert all Qdrant collections (full reload)
make sync-qdrant      # Start the continuous fraud_similarity sync service
make sync-qdrant-logs # Follow the sync service
```

**Validation**
```bash
make smoke-test  # Run end-to-end platform validation
```

**Reset**
```bash
make reset       # Tear down and wipe all state
```

---

## Data Flow

1. Kafka producers generate synthetic tax domain events across four topics
2. Flink SQL jobs transform, enrich, and aggregate events in near real-time
3. Processed records are distributed to PostgreSQL, Redis, MinIO, and Qdrant
4. OpenLineage emits pipeline metadata to Marquez at each stage
5. DuckDB runs analytical queries over processed datasets from MinIO/PostgreSQL
6. MLflow trains and tracks the fraud detection model against historical records
7. LangGraph agents use the registered model and Qdrant collections to answer queries
8. Streamlit visualises operational metrics and surfaces agent responses

---

## Roadmap

- [ ] Domain-grounded knowledge base for fraud and tax agents (business rules, fraud typologies, FICA/POPIA compliance layer)
- [ ] `business_rules_kb` Qdrant collection for company-specific reasoning context
- [ ] Enhanced agent system prompts anchored to regulatory domain
- [ ] Apache Iceberg table format for the lakehouse layer
- [ ] Real-time feature store
- [ ] Grafana observability layer
- [ ] Kubernetes deployment manifests
- [ ] Apache Airflow orchestration
- [ ] CI/CD pipeline automation

---

## Author

**Jabulani Ndlovu**

Client Engineering Graduate | Data & AI Engineer | Building modern data platforms, AI agents, real-time analytics systems, and enterprise AI solutions.
