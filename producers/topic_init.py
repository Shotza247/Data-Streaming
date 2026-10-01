"""
producers/topic_init.py

One-shot script that creates all 4 tax-domain Kafka topics.
Runs as the `kafka-topic-init` service in docker-compose (profile: setup).
Safe to run multiple times — idempotent.

Topics created:
    tax-applications    — 3 partitions, replication 1
    taxpayer-profiles   — 3 partitions, replication 1
    fraud-signals       — 3 partitions, replication 1
    audit-events        — 3 partitions, replication 1
"""

import logging
import os
import sys
import time

from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("topic-init")

TOPICS = [
    "tax-applications",
    "taxpayer-profiles",
    "fraud-signals",
    "audit-events",
]

PARTITIONS   = int(os.getenv("KAFKA_TOPIC_PARTITIONS",  "3"))
REPLICATION  = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))
RETRIES      = int(os.getenv("KAFKA_INIT_RETRIES",       "15"))
RETRY_DELAY  = 5   # seconds between retries


def create_topics(bootstrap: str) -> bool:
    """Attempt to create all topics. Returns True on success."""
    try:
        admin = AdminClient({"bootstrap.servers": bootstrap})

        # Check existing topics
        meta          = admin.list_topics(timeout=5)
        existing      = set(meta.topics.keys())
        topics_to_create = [
            NewTopic(t, num_partitions=PARTITIONS, replication_factor=REPLICATION)
            for t in TOPICS
            if t not in existing
        ]

        if not topics_to_create:
            log.info("All topics already exist — nothing to create.")
            return True

        futures = admin.create_topics(topics_to_create)
        all_ok  = True
        for topic, future in futures.items():
            try:
                future.result()
                log.info("✓ Created topic: %s  (partitions=%d)", topic, PARTITIONS)
            except Exception as e:
                # TOPIC_ALREADY_EXISTS is not an error
                if "TOPIC_ALREADY_EXISTS" in str(e) or "already exists" in str(e).lower():
                    log.info("✓ Topic already exists: %s", topic)
                else:
                    log.error("✗ Failed to create topic %s: %s", topic, e)
                    all_ok = False
        return all_ok

    except Exception as exc:
        log.warning("Kafka not ready: %s", exc)
        return False


def main():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    log.info("Connecting to Kafka at %s", bootstrap)

    for attempt in range(1, RETRIES + 1):
        log.info("Attempt %d/%d …", attempt, RETRIES)
        if create_topics(bootstrap):
            log.info("Topic initialisation complete.")
            sys.exit(0)
        time.sleep(RETRY_DELAY)

    log.error("Failed to initialise topics after %d attempts.", RETRIES)
    sys.exit(1)


if __name__ == "__main__":
    main()
