# Makefile — Open-Source Real-Time Streaming Analytics Platform
# Convenience targets for Docker/Podman Compose lifecycle management.
#
# Automatically detects whether `docker compose` or `podman-compose` is
# available and uses the first one found.

ifeq ($(shell command -v docker 2>/dev/null),)
  COMPOSE := podman-compose
else
  COMPOSE := docker compose
endif

.PHONY: up down restart logs \
        topic-list submit-sql seed smoke-test reset

# Start all infrastructure services in detached mode
up:
	$(COMPOSE) up -d

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
	$(COMPOSE) exec kafka \
		kafka-topics --bootstrap-server localhost:9092 --list

# Submit all Flink SQL jobs via the Python wrapper script
submit-sql:
	python scripts/submit_flink_jobs.py

# Preflight: ensure cleaned-tax bucket is non-empty, then seed Qdrant
seed:
	@if ! python scripts/preflight_bucket_check.py "$(MINIO_BUCKET_CLEANED)" 2>/dev/null; then \
		echo "Run 'make submit-sql' and wait for cleaned-tax records before seeding Qdrant"; \
		exit 1; \
	fi
	python scripts/seed_qdrant.py

# Run the integration smoke test
smoke-test:
	python scripts/smoke_test.py

# Full reset: wipe all volumes and restart from scratch
reset:
	$(COMPOSE) down -v
	$(COMPOSE) up -d
