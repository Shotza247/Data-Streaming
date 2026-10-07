# =============================================================================
# Makefile — Open-Source Real-Time Streaming Analytics Platform
# =============================================================================
#
# GOAL: One command to run the entire platform end-to-end with live data
#       flowing into Streamlit.
#
#   make all          — build image, start all services, seed, submit SQL jobs
#   make up           — start infrastructure (no Flink, no producers)
#   make flink-build  — build the custom Flink image with connector JARs
#   make flink-up     — start Flink cluster (requires image built)
#   make submit-sql   — submit all 5 Flink SQL jobs
#   make seed         — seed Qdrant + train MLflow model
#   make dashboard    — open dashboard URL
#
# Prerequisites: Podman Desktop running, enough RAM (~6GB free)
# =============================================================================

ifeq ($(shell command -v podman 2>/dev/null),)
  $(error Podman not found. Install Podman Desktop from https://podman.io)
endif

# Detect compose command
ifneq ($(shell podman compose version 2>/dev/null),)
  COMPOSE := podman compose
else
  COMPOSE := podman-compose
endif

CONTAINER_CLI  := podman
FLINK_IMAGE    := flink-custom:1.18
FLINK_CTX      := ./docker/build/flink
WAIT_HEALTHY   := 30
WAIT_WINDOW    := 310

.PHONY: all up down restart logs \
        flink-build flink-up flink-logs flink-ui \
        submit-sql submit-job flink-status \
        init-topics topic-list \
        producers-up producers-down producers-logs \
        kpi-poller-up kpi-poller-logs \
        seed seed-kb train promote \
        smoke-test verify-lineage \
        minio-buckets dashboard reset help

# =============================================================================
# DEFAULT TARGET — Full end-to-end startup
# =============================================================================
# Runs everything in the correct order:
#   1. Build Flink image (if not already built)
#   2. Start all infrastructure services
#   3. Create MinIO buckets
#   4. Start Flink cluster
#   5. Submit all Flink SQL jobs
#   6. Seed Qdrant + train MLflow model (background)
#   7. Print dashboard URL
# =============================================================================
all: _sync-docker-context
	@echo ""
	@echo "============================================================"
	@echo " Starting Open-Source Real-Time Streaming Analytics Platform"
	@echo "============================================================"
	@echo ""
	@echo "[1/6] Building custom Flink image with connector JARs..."
	$(MAKE) flink-build
	@echo ""
	@echo "[2/6] Starting all infrastructure services..."
	$(COMPOSE) up -d
	@echo "      Waiting $(WAIT_HEALTHY)s for services to become healthy..."
	sleep $(WAIT_HEALTHY)
	@echo ""
	@echo "[3/6] Creating MinIO buckets..."
	$(MAKE) minio-buckets
	@echo ""
	@echo "[4/6] Starting Flink cluster..."
	$(MAKE) flink-up
	@echo "      Waiting $(WAIT_HEALTHY)s for Flink to be healthy..."
	sleep $(WAIT_HEALTHY)
	@echo ""
	@echo "[5/6] Submitting all 5 Flink SQL jobs..."
	$(MAKE) submit-sql
	@echo ""
	@echo "[6/6] Seeding Qdrant KB + training MLflow model (background)..."
	python scripts/seed_qdrant.py --kb-only &
	python mlflow/train_fraud_model.py &
	@echo ""
	@echo "============================================================"
	@echo " Platform is UP and data is flowing!"
	@echo "============================================================"
	@echo ""
	@echo "  Streamlit Dashboard : http://localhost:8501"
	@echo "  Flink UI            : http://localhost:8081"
	@echo "  Kafka UI            : http://localhost:8080"
	@echo "  MinIO Console       : http://localhost:9001  (minioadmin/minioadmin)"
	@echo "  MLflow              : http://localhost:5001"
	@echo "  Marquez Lineage     : http://localhost:3000"
	@echo ""
	@echo "  KPI windows will appear in Streamlit after ~5 minutes."
	@echo ""

# =============================================================================
# INFRASTRUCTURE
# =============================================================================

_sync-docker-context:
	cp dashboard/requirements.txt       docker/build/streamlit/requirements.streamlit.txt
	cp dashboard/.streamlit/config.toml docker/build/streamlit/streamlit_config.toml

up: _sync-docker-context
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

restart: down up

logs:
	$(COMPOSE) logs -f

# =============================================================================
# MINIO BUCKETS
# =============================================================================
minio-buckets:
	$(CONTAINER_CLI) run --rm --network streaming-net --entrypoint /bin/sh \
	  quay.io/minio/mc:latest -c \
	  "mc alias set local http://minio:9000 minioadmin minioadmin 2>/dev/null && \
	   mc mb --ignore-existing local/raw-tax && \
	   mc mb --ignore-existing local/cleaned-tax && \
	   mc mb --ignore-existing local/mlflow-artifacts && \
	   mc anonymous set download local/raw-tax && \
	   mc anonymous set download local/cleaned-tax && \
	   mc anonymous set download local/mlflow-artifacts && \
	   mc ls local/"

# =============================================================================
# FLINK
# =============================================================================

flink-build:
	@echo "Building $(FLINK_IMAGE) from $(FLINK_CTX)..."
	$(CONTAINER_CLI) build -t $(FLINK_IMAGE) $(FLINK_CTX)/

flink-up:
	$(COMPOSE) up -d flink-jobmanager flink-taskmanager flink-sql-client

flink-down:
	$(CONTAINER_CLI) stop flink-sql-client flink-taskmanager flink-jobmanager 2>/dev/null || true
	$(CONTAINER_CLI) rm   flink-sql-client flink-taskmanager flink-jobmanager 2>/dev/null || true

flink-restart: flink-down flink-up

flink-logs:
	$(COMPOSE) logs -f flink-jobmanager flink-taskmanager

flink-ui:
	@echo "Flink Job Manager UI: http://localhost:8081"

flink-status:
	@echo "--- Flink Cluster Overview ---"
	$(CONTAINER_CLI) exec flink-jobmanager sh -c "curl -s http://localhost:8081/overview" 2>/dev/null
	@echo ""
	@echo "--- Running Jobs ---"
	$(CONTAINER_CLI) exec flink-jobmanager sh -c "curl -s http://localhost:8081/jobs/overview" 2>/dev/null
	@echo ""
	@echo "--- Kafka Consumer Groups ---"
	$(CONTAINER_CLI) exec kafka kafka-consumer-groups \
	  --bootstrap-server localhost:9092 --list 2>/dev/null

submit-sql:
	python scripts/submit_flink_jobs.py

submit-job:
	python scripts/submit_flink_jobs.py $(JOB)

# =============================================================================
# KAFKA
# =============================================================================

topic-list:
	$(CONTAINER_CLI) exec kafka \
	  kafka-topics --bootstrap-server localhost:9092 --list

init-topics:
	$(COMPOSE) --profile setup up kafka-topic-init

# =============================================================================
# PRODUCERS
# =============================================================================

producers-up:
	$(COMPOSE) --profile producers up -d --build

producers-down:
	$(COMPOSE) --profile producers down

producers-logs:
	$(COMPOSE) --profile producers logs -f

# =============================================================================
# KPI POLLER
# =============================================================================

kpi-poller-up:
	$(COMPOSE) up -d kpi-poller

kpi-poller-logs:
	$(COMPOSE) logs -f kpi-poller

# =============================================================================
# ML + QDRANT
# =============================================================================

seed:
	python scripts/seed_qdrant.py

seed-kb:
	python scripts/seed_qdrant.py --kb-only

train:
	python mlflow/train_fraud_model.py

promote:
	python mlflow/promote_model.py

# =============================================================================
# TESTING + LINEAGE
# =============================================================================

smoke-test:
	python scripts/smoke_test.py

verify-lineage:
	python lineage/verify_lineage.py

# =============================================================================
# DASHBOARD
# =============================================================================

dashboard:
	@echo ""
	@echo "  Streamlit Dashboard : http://localhost:8501"
	@echo "  Flink UI            : http://localhost:8081"
	@echo "  Kafka UI            : http://localhost:8080"
	@echo "  MinIO Console       : http://localhost:9001"
	@echo "  MLflow              : http://localhost:5001"
	@echo "  Marquez Lineage     : http://localhost:3000"
	@echo "  Qdrant              : http://localhost:6333/dashboard"
	@echo ""

# =============================================================================
# RESET
# =============================================================================

reset:
	@echo "WARNING: This will wipe all containers and volumes!"
	$(COMPOSE) down -v
	$(CONTAINER_CLI) rmi $(FLINK_IMAGE) 2>/dev/null || true

# =============================================================================
# HELP
# =============================================================================

help:
	@echo ""
	@echo "Open-Source Real-Time Streaming Analytics Platform — Makefile targets"
	@echo ""
	@echo "  make all          Full end-to-end startup (build + start + submit + seed)"
	@echo "  make up           Start infrastructure services only"
	@echo "  make down         Stop all services"
	@echo "  make restart      Stop then start all services"
	@echo ""
	@echo "  make flink-build  Build the custom Flink 1.18 image with connector JARs"
	@echo "  make flink-up     Start Flink cluster (jobmanager + taskmanager + sql-client)"
	@echo "  make flink-down   Stop and remove Flink containers"
	@echo "  make flink-status Show running Flink jobs + Kafka consumer groups"
	@echo "  make submit-sql   Submit all 5 Flink SQL jobs"
	@echo "  make submit-job JOB=job_03  Submit one specific job"
	@echo ""
	@echo "  make minio-buckets  Create raw-tax, cleaned-tax, mlflow-artifacts buckets"
	@echo "  make init-topics    Create Kafka topics"
	@echo ""
	@echo "  make seed         Seed Qdrant collections (KB + fraud vectors)"
	@echo "  make seed-kb      Seed Qdrant regulatory KB only (fast, no MinIO dep)"
	@echo "  make train        Train MLflow fraud detection model"
	@echo "  make promote      Promote latest model from Staging to Production"
	@echo ""
	@echo "  make smoke-test   Run 8-component integration test"
	@echo "  make dashboard    Print all service URLs"
	@echo "  make reset        Wipe everything and start fresh (DESTRUCTIVE)"
	@echo ""
