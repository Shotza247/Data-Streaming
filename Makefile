# Makefile — Open-Source Real-Time Streaming Analytics Platform
# Convenience targets for Podman Compose lifecycle management.
#
# This project runs on Podman. COMPOSE uses `podman compose` (Podman v4.7+
# built-in) and falls back to `podman-compose` (standalone pip package).
# CONTAINER_CLI is used for `exec` calls (e.g. submitting Flink SQL scripts).

ifeq ($(shell command -v podman 2>/dev/null),)
  $(error Podman not found. Install Podman Desktop from https://podman.io)
endif

# Prefer the built-in `podman compose` subcommand (Podman v4.7+);
# fall back to the standalone podman-compose if not available.
ifneq ($(shell podman compose version 2>/dev/null),)
  COMPOSE := podman compose
else
  COMPOSE := podman-compose
endif

CONTAINER_CLI := podman

.PHONY: up down restart logs \
        topic-list init-topics \
        producers-up producers-down producers-logs \
        flink-up flink-logs flink-ui \
        submit-sql submit-job \
        kpi-poller-up kpi-poller-logs \
        seed seed-kb train promote \
        smoke-test verify-lineage reset

# Sync build-context files into docker/ then start all services
up: _sync-docker-context
	$(COMPOSE) up -d

# Sync build-context subdirectories used by podman-compose.
# podman-compose 1.6 requires a Containerfile at the root of each build context.
# Each service gets docker/build/<service>/Containerfile + its build-time assets.
_sync-docker-context:
	cp dashboard/requirements.txt       docker/build/streamlit/requirements.streamlit.txt
	cp dashboard/.streamlit/config.toml docker/build/streamlit/streamlit_config.toml

# Stop and remove all containers, networks, and volumes
down:
	$(COMPOSE) down

# Restart all services
restart: down up

# Follow live logs
logs:
	$(COMPOSE) logs -f

# List Kafka topics (exec into kafka container)
topic-list:
	$(CONTAINER_CLI) exec kafka \
		kafka-topics --bootstrap-server localhost:9092 --list

# Initialise Kafka topics (run once after 'make up')
init-topics:
	$(COMPOSE) --profile setup up kafka-topic-init

# Start all 4 tax-domain producers
producers-up:
	$(COMPOSE) --profile producers up -d --build

# Stop all producers
producers-down:
	$(COMPOSE) --profile producers down

# Stream logs from all producers
producers-logs:
	$(COMPOSE) --profile producers logs -f

# ── Flink targets ─────────────────────────────────────────────────────────────

# Start Flink jobmanager, taskmanager, and sql-client (pre-build image first)
flink-up:
	$(COMPOSE) up -d flink-jobmanager flink-taskmanager flink-sql-client

# Follow Flink logs
flink-logs:
	$(COMPOSE) logs -f flink-jobmanager flink-taskmanager

# Open Flink UI (prints URL)
flink-ui:
	@echo "Flink Job Manager UI: http://localhost:8081"

# Submit all Flink SQL jobs (requires Flink containers running)
submit-sql:
	python scripts/submit_flink_jobs.py

# Submit a single job by substring name, e.g.: make submit-job JOB=job_01
submit-job:
	python scripts/submit_flink_jobs.py $(JOB)

# ── KPI Poller targets ────────────────────────────────────────────────────────

# Start the Redis KPI cache poller
kpi-poller-up:
	$(COMPOSE) up -d kpi-poller

# Stream KPI poller logs
kpi-poller-logs:
	$(COMPOSE) logs -f kpi-poller

# ── Seeding and testing ───────────────────────────────────────────────────────

# Seed Qdrant collections (regulatory KB always; fraud_similarity requires cleaned-tax data)
seed:
	python scripts/seed_qdrant.py

# Seed only the regulatory KB (safe to run immediately, no MinIO dependency)
seed-kb:
	python scripts/seed_qdrant.py --kb-only

# Train the MLflow fraud detection model
train:
	python mlflow/train_fraud_model.py

# Promote the latest Staging model to Production
promote:
	python mlflow/promote_model.py

# Run the integration smoke test (includes lineage verification)
smoke-test:
	python scripts/smoke_test.py

# Verify Marquez lineage DAG only
verify-lineage:
	python lineage/verify_lineage.py

# Full reset: wipe all volumes and restart from scratch
reset:
	$(COMPOSE) down -v
	$(COMPOSE) up -d
