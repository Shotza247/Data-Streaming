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
- `fraud-detector` model trained on application features: province, taxable income, employment type/status, filing status
- HuggingFace `all-MiniLM-L6-v2` embeddings for semantic search
- Qdrant collections: `regulatory_kb` (50 SA tax law chunks) and `fraud_similarity` (historical case embeddings)

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
- **MLflow** — experiment and model registry browser
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
| LLM | Flan-T5-Base (local, via HuggingFace) |
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
make up
```

**4. Deploy Flink SQL jobs**
```bash
make submit-sql
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
│   ├── llm_wrapper.py          # HuggingFace LLM abstraction
│   ├── qdrant_search.py        # Vector search helpers
│   ├── embeddings.py           # Embedding utilities
│   ├── embed_regulatory_kb.py  # Seeds regulatory_kb collection
│   ├── embed_tax_records.py    # Seeds fraud_similarity collection
│   └── langgraph_app.py        # LangGraph graph definition
│
├── mlflow/                     # ML model training and registry
│   ├── train_fraud_model.py
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

---

## Common Commands

**Infrastructure**
```bash
make up          # Start all services
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
make seed        # Embed and upsert all Qdrant collections
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
