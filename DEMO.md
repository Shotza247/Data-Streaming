# DEMO.md — Real-Time Tax Analytics Platform Walkthrough

> **A full demo from cold start to live dashboard in ~15 minutes.**
> All services run locally in Podman containers — no cloud account required.

---

## Prerequisites

- Podman Desktop ≥ 1.9 (or Podman CLI ≥ 4.7) with Podman Machine running
- `podman-compose` 1.6+ (`pip install podman-compose`)
- Python 3.11 (host, for running scripts directly if needed)
- 16 GB RAM recommended (all containers combined ~8–10 GB)
- ~5 GB free disk (container images + model downloads)

---

## Step 1 — Start the Platform

```bash
# Clone the repository
git clone https://github.com/Shotza247/Data-Streaming.git
cd Data-Streaming

# Start the full stack (Kafka, Postgres, Redis, MinIO, Qdrant, Marquez, MLflow, Streamlit)
make up

# Wait ~60s for all services to become healthy, then check:
podman compose ps
```

**Expected:** All services show `running` or `healthy`. Key ports:

| Service | URL |
|---|---|
| Streamlit Dashboard | http://localhost:8501 |
| Kafka UI | http://localhost:8080 |
| Flink Job Manager | http://localhost:8081 |
| MarquezWeb Lineage | http://localhost:3000 |
| MLflow Tracking | http://localhost:5001 |
| MinIO Console | http://localhost:9001 |
| Qdrant REST | http://localhost:6333 |

---

## Step 2 — Create Kafka Topics and Start Producers

```bash
# Create all 4 Kafka topics (one-time)
make init-topics

# Start all 4 tax domain producers (1 event/second each)
make producers-up

# Watch messages flowing
make producers-logs
```

Open **Kafka UI** at http://localhost:8080 — you should see messages accumulating
in `tax-applications`, `taxpayer-profiles`, `fraud-signals`, `audit-events`.

---

## Step 3 — Build Flink Image and Start Flink Cluster

> The Flink image takes 3–5 minutes to build (downloads 4 connector JARs).

```bash
# Build the custom Flink 1.18 image with connector JARs
podman build -t flink-custom:1.18 ./docker/build/flink/

# Start Flink Job Manager, Task Manager, SQL Client
make flink-up

# Wait 30s for Flink to be ready, then verify:
# Open http://localhost:8081 — should show 0 running jobs, 2 task slots
```

---

## Step 4 — Submit All 5 Flink SQL Jobs

```bash
# Submit all jobs (emits OpenLineage events to Marquez)
make submit-sql
```

Watch in **Flink UI** at http://localhost:8081 — you should see 5 RUNNING jobs:
- `job_01_raw_to_minio` → MinIO raw-tax bucket
- `job_02_enrich` → PostgreSQL enriched_tax_applications
- `job_03_kpi_windows` → PostgreSQL tax_kpi_windows
- `job_04_fraud_pattern` → Kafka fraud-signals topic
- `job_05_cleaned` → MinIO cleaned-tax bucket

---

## Step 5 — Start the KPI Poller

```bash
# Poll PostgreSQL tax_kpi_windows → Redis every 30 seconds
make kpi-poller-up
make kpi-poller-logs

# Expected log: "Cached N KPI window(s) in Redis (TTL=900s)"
```

Verify Redis keys are populated:

```bash
podman exec redis redis-cli KEYS "tax:kpi:*"
podman exec redis redis-cli HGETALL "tax:kpi:latest"
```

---

## Step 6 — Seed Qdrant Vector Store

```bash
# Seed regulatory KB immediately (no MinIO data needed)
make seed-kb

# After job_05 has written ~5 minutes of cleaned data, seed tax records:
make seed
```

Verify Qdrant collections at http://localhost:6333/dashboard:
- `fraud_similarity` — should show embedded tax records
- `regulatory_kb` — should show 50 SA tax law chunks

---

## Step 7 — Train the MLflow Fraud Detection Model

```bash
# Train RandomForestClassifier on cleaned MinIO data
# (falls back to synthetic data if MinIO is still empty)
make train
```

Open **MLflow UI** at http://localhost:5001:
- Navigate to **Experiments → tax-fraud-detection**
- You should see 1 completed run with accuracy, F1, precision, recall
- The confusion matrix PNG and classification report are logged as artifacts
- Model is registered as `fraud-detector` in **Models** (stage: Staging)

Optionally promote to Production:
```bash
make promote
```

---

## Step 8 — Open the Streamlit Dashboard

Open http://localhost:8501 in your browser.

### Tab 1 — Tax KPIs
- **Live KPI cards**: Applications processed, avg taxable income, distinct customers
- **Trend chart**: 5-minute window volumes over the last hour from DuckDB/MinIO
- **Income histogram**: Distribution of taxable income across all cleaned records
- Auto-refreshes every 30 seconds

### Tab 2 — Fraud Intelligence
- **Signal type bar chart**: Which fraud patterns are most common
- **Province heatmap**: Fraud rate by South African province
- **Top flagged customers**: Customers ranked by number of fraud signals

### Tab 3 — AI Agent Chat
Try these example queries:
```
What is the penalty rate for late tax filing?
What income tax bracket applies to R 450,000 taxable income?
Check application APP-00001 for fraud
Is there fraud risk for a self-employed person earning R 750,000?
```
The **Supervisor** routes fraud queries to the ML + Qdrant agent,
and tax questions to the RAG + flan-t5 Q&A agent.

### Tab 4 — Data Lineage
- Live job/dataset table from the Marquez REST API
- Full visual lineage graph embedded via Marquez Web UI (http://localhost:3000)
- Shows the complete DAG: Kafka → Flink → PostgreSQL/MinIO → DuckDB → MLflow

### Tab 5 — MLflow Status
- Model stage badge (Staging / Production)
- All experiment runs with accuracy, F1, precision, recall columns
- Metrics bar chart for the best run

---

## Step 9 — Run the Smoke Test

```bash
make smoke-test
```

Expected output:
```
  [PASS]  Kafka Topics
  [PASS]  PostgreSQL Tables
  [PASS]  Redis KPI Cache
  [PASS]  MinIO Buckets
  [PASS]  Qdrant Collections
  [PASS]  Marquez Lineage DAG
  [PASS]  MLflow Model Registry
  [PASS]  Streamlit Dashboard

  All critical checks passed.
```

---

## Step 10 — Explore the Lineage Graph

Open **Marquez Web UI** at http://localhost:3000

Navigate to the `flink` namespace to see the full lineage DAG:
- Raw events from Kafka → MinIO (job_01)
- Enriched applications from Kafka JOIN → PostgreSQL (job_02)
- KPI windows → PostgreSQL → Redis (job_03 + kpi-poller)
- Fraud patterns → Kafka fraud-signals (job_04)
- Cleaned data → MinIO → DuckDB → MLflow training (job_05 + train_fraud_model)

---

## Reset Everything

```bash
# Stop all containers and wipe all volumes
make reset
```

---

## Architecture at a Glance

```
Producers (4x)
    │  tax-applications, taxpayer-profiles, fraud-signals, audit-events
    ▼
Kafka (bitnami/kafka:3.6)
    │
    ├── Flink Job 01 ──────────────────► MinIO raw-tax/
    ├── Flink Job 02 ──────────────────► PostgreSQL enriched_tax_applications
    ├── Flink Job 03 ──────────────────► PostgreSQL tax_kpi_windows ──► Redis
    ├── Flink Job 04 ──────────────────► Kafka fraud-signals (CEP patterns)
    └── Flink Job 05 ──────────────────► MinIO cleaned-tax/
                                              │
                              ┌───────────────┼──────────────────┐
                              ▼               ▼                  ▼
                        DuckDB OLAP      Qdrant vectors     MLflow training
                         (6 queries)   (fraud_similarity    (RandomForest
                              │          regulatory_kb)      fraud-detector)
                              └────────────────────────────────────┐
                                                                   ▼
                                                          Streamlit Dashboard
                                                          (5 tabs, live data)
                                                                   │
                                                         LangGraph Agents
                                                    (FraudAgent + TaxQAAgent)

Data Lineage: OpenLineage → Marquez (full DAG of all movements above)
```

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| Flink jobs not starting | Check `make flink-logs` — ensure jobmanager + taskmanager both running |
| MinIO cleaned-tax empty | Wait 5+ minutes after `make submit-sql` for first window to commit |
| Redis has no KPI keys | Check `make kpi-poller-logs` — table may be empty until first Flink window |
| MLflow 502 error | `podman compose restart mlflow` — postgres backend may have started after MLflow |
| LLM agent slow | flan-t5-base is ~250MB — first load takes 1–2 minutes; subsequent calls are fast |
| Qdrant seed fails | Ensure cleaned-tax has data first: `python storage/minio_client.py` to check |
