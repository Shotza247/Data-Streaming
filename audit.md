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
