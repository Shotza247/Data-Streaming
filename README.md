# Open-Source Real-Time Streaming Analytics Platform

A fully containerised, end-to-end real-time data streaming platform built
exclusively with open-source tools. The platform ingests synthetic tax-domain
data through Kafka producers, processes it in real time with Flink SQL,
stores it across PostgreSQL, Redis, MinIO, and Qdrant, trains ML models
with MLflow, orchestrates AI agents with LangGraph, and visualises everything
through a Streamlit dashboard.

## Architecture

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
```

### Components

| Layer | Tool | Version / Image |
|-------|------|-----------------|
| Message broker | Apache Kafka | confluentinc/cp-kafka:7.5.0 |
| Kafka UI | kafka-ui | provectuslabs/kafka-ui:latest |
| Stream processing | Apache Flink SQL Client | apache/flink:1.18 (custom image with connector JARs) |
| Transactional DB | PostgreSQL | postgres:15-alpine |
| Hot cache | Redis | redis:7-alpine |
| Object storage | MinIO | minio/minio:latest |
| OLAP engine | DuckDB | duckdb Python lib (in-process) |
| Vector store | Qdrant | qdrant/qdrant:latest |
| Data lineage | OpenLineage + Marquez | marquezproject/marquez:0.47.0 |
| Model tracking | MLflow | python:3.11-slim |
| LLM / embeddings | HuggingFace Transformers | flan-t5-base + all-MiniLM-L6-v2 |
| Agent framework | LangGraph | langchain + langgraph |
| Dashboard | Streamlit | streamlit |

## Prerequisites

- **Podman Desktop** (v4.7+) — [podman.io](https://podman.io)
  - Podman v4.7+ includes `podman compose` built-in; earlier versions require `pip install podman-compose`
  - Ensure the Podman machine is started: `podman machine start`
- **Python 3.11+** (for `make submit-sql`, `make seed`, `make smoke-test`)
- **Make** — available via Git for Windows or WSL
- 16 GB RAM recommended (Kafka + Flink + Postgres + Redis + MinIO + Qdrant + MLflow + Streamlit + producers)

> ⚠️ **Podman note:** `podman compose` uses the `streaming-net` network in rootless mode. If you see
> hostname resolution errors between containers, ensure `podman machine` is running and that
> `--userns=keep-id` is **not** set globally in your `containers.conf`.

## Quickstart

```bash
# 0. Ensure Podman machine is running
podman machine start

# 1. Clone and enter the project directory
git clone <repo-url>
cd "data streaming"

# 2. Build images and start the infrastructure stack
make up

# 3. Wait for services to become healthy (~60s), then submit Flink SQL jobs
make submit-sql

# 4. Seed Qdrant vector store (only after cleaned-tax bucket has data)
make seed

# 5. Verify the full platform
make smoke-test

# 6. Open the Streamlit dashboard at http://localhost:8501
```

## Service Port Reference

| Service | Port(s) | URL |
|---------|---------|-----|
| Kafka | 9092 | `http://localhost:9092` |
| Kafka UI | 8080 | `http://localhost:8080` |
| Flink JobManager UI | 8081 | `http://localhost:8081` |
| PostgreSQL | 5432 | `localhost:5432` |
| Redis | 6379 | `localhost:6379` |
| MinIO API | 9000 | `http://localhost:9000` |
| MinIO Console | 9001 | `http://localhost:9001` |
| Qdrant | 6333 | `http://localhost:6333` |
| Marquez API | 5000 | `http://localhost:5000` |
| Marquez Web UI | 3000 | `http://localhost:3000` |
| MLflow Tracking | 5001 | `http://localhost:5001` |
| Streamlit Dashboard | 8501 | `http://localhost:8501` |

## Makefile Targets

| Target | Description |
|--------|-------------|
| `make up` | Start all infrastructure services in detached mode |
| `make down` | Stop and remove all containers |
| `make restart` | Stop then start all services |
| `make logs` | Follow live logs |
| `make topic-list` | List Kafka topics |
| `make submit-sql` | Submit all Flink SQL jobs via the Python wrapper |
| `make seed` | Seed Qdrant vector store (requires cleaned-tax data) |
| `make smoke-test` | Run integration smoke test |
| `make reset` | Full reset — wipe volumes and restart |
