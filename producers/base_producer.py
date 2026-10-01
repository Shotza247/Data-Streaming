"""
producers/base_producer.py

Shared base class for all tax-domain Kafka producers.
Each producer subclasses BaseProducer, implements generate_message(),
and calls run() to start the publish loop.

Features:
- Confluent Kafka producer with delivery confirmation callback
- Configurable publish rate via PRODUCER_RATE_PER_SECOND env var
- JSON serialisation of all messages
- Structured logging with timestamp and topic
- Graceful shutdown on SIGINT / SIGTERM
"""

import json
import logging
import os
import signal
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)


class BaseProducer(ABC):
    """
    Abstract base for all tax-domain Kafka producers.

    Subclasses must implement:
        topic (class attribute)   — Kafka topic name
        generate_message()        — returns a dict to be JSON-serialised and published
    """

    topic: str = ""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self.rate  = float(os.getenv("PRODUCER_RATE_PER_SECOND", "1"))

        # Confluent Kafka producer config
        self._producer = Producer({
            "bootstrap.servers":            bootstrap,
            "client.id":                    self.__class__.__name__,
            "acks":                         "1",
            "retries":                      5,
            "retry.backoff.ms":             500,
            "socket.timeout.ms":            10_000,
            "message.timeout.ms":           10_000,
        })

        # Ensure topic exists before publishing
        self._ensure_topic(bootstrap, self.topic)

        # Graceful shutdown flag
        self._running = True
        signal.signal(signal.SIGINT,  self._stop)
        signal.signal(signal.SIGTERM, self._stop)

        self.logger.info("Producer initialised — topic=%s  rate=%.1f/s", self.topic, self.rate)

    # ── Topic auto-creation ───────────────────────────────────────────────────

    def _ensure_topic(self, bootstrap: str, topic: str, retries: int = 10) -> None:
        """Create the topic if it does not yet exist. Retries until Kafka is ready."""
        for attempt in range(1, retries + 1):
            try:
                admin = AdminClient({"bootstrap.servers": bootstrap})
                meta  = admin.list_topics(timeout=5)
                if topic not in meta.topics:
                    fs = admin.create_topics([
                        NewTopic(topic, num_partitions=3, replication_factor=1)
                    ])
                    for t, f in fs.items():
                        try:
                            f.result()
                            self.logger.info("Created topic: %s", t)
                        except Exception as e:
                            # Topic may already exist (race) — not fatal
                            self.logger.debug("Topic creation note for %s: %s", t, e)
                else:
                    self.logger.info("Topic already exists: %s", topic)
                return
            except Exception as exc:
                self.logger.warning(
                    "Kafka not ready (attempt %d/%d): %s", attempt, retries, exc
                )
                time.sleep(5)
        raise RuntimeError(f"Cannot connect to Kafka at {bootstrap} after {retries} attempts")

    # ── Delivery callback ─────────────────────────────────────────────────────

    def _on_delivery(self, err, msg):
        if err:
            self.logger.error("Delivery failed [%s]: %s", msg.topic(), err)
        else:
            self.logger.debug(
                "Delivered → topic=%s  partition=%d  offset=%d",
                msg.topic(), msg.partition(), msg.offset(),
            )

    # ── Abstract interface ────────────────────────────────────────────────────

    @abstractmethod
    def generate_message(self) -> dict:
        """Return a dict representing one Kafka message payload."""

    # ── Publish one message ───────────────────────────────────────────────────

    def publish(self) -> None:
        """Serialise and publish one generated message."""
        payload = self.generate_message()
        payload.setdefault("_produced_at", datetime.now(timezone.utc).isoformat())
        self._producer.produce(
            self.topic,
            key=str(payload.get("application_id") or payload.get("customer_id") or ""),
            value=json.dumps(payload, default=str),
            callback=self._on_delivery,
        )
        self._producer.poll(0)  # trigger delivery callbacks without blocking

    # ── Main loop ─────────────────────────────────────────────────────────────

    def run(self) -> None:
        """Publish messages at the configured rate until stopped."""
        self.logger.info("Starting publish loop — Ctrl+C to stop")
        interval = 1.0 / self.rate
        while self._running:
            start = time.monotonic()
            try:
                self.publish()
            except Exception as exc:
                self.logger.error("Publish error: %s", exc)
            elapsed = time.monotonic() - start
            sleep_for = max(0.0, interval - elapsed)
            time.sleep(sleep_for)

        # Flush remaining messages on shutdown
        self.logger.info("Flushing remaining messages…")
        self._producer.flush(timeout=10)
        self.logger.info("Producer stopped.")

    # ── Signal handler ────────────────────────────────────────────────────────

    def _stop(self, *_):
        self.logger.info("Shutdown signal received.")
        self._running = False
