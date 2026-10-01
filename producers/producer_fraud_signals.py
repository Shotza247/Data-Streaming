"""
producers/producer_fraud_signals.py

Produces synthetic fraud signal records to the `fraud-signals` topic.

In production this topic is also written to by Flink's fraud pattern job
(job_04_fraud_pattern.sql). This producer seeds the topic with initial
signals and simulates signals from external fraud detection systems.

Schema:
    signal_id       — UUID string
    application_id  — UUID string (references a tax-applications record)
    customer_id     — from shared CUSTOMER_ID_POOL
    signal_type     — category of fraud signal
    severity        — LOW | MEDIUM | HIGH | CRITICAL
    detected_at     — ISO datetime string
    description     — human-readable explanation
"""

import random
import uuid
from datetime import datetime, timedelta, timezone

from base_producer import BaseProducer

# Shared customer ID pool
CUSTOMER_ID_POOL = [f"C{str(i).zfill(5)}" for i in range(1, 5001)]

SIGNAL_TYPES = [
    "DUPLICATE_APPLICATION",
    "HIGH_INCOME_ANOMALY",
    "VELOCITY_BREACH",
    "PROFILE_MISMATCH",
    "KNOWN_FRAUD_PATTERN",
    "SUSPICIOUS_EMAIL_DOMAIN",
    "INCOME_INCONSISTENCY",
]

SEVERITIES = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]

# Severity weights: more LOW/MEDIUM than CRITICAL
SEVERITY_WEIGHTS = [0.35, 0.35, 0.20, 0.10]

DESCRIPTIONS = {
    "DUPLICATE_APPLICATION":    "Customer submitted multiple applications within a short window.",
    "HIGH_INCOME_ANOMALY":      "Declared income is significantly above historical average.",
    "VELOCITY_BREACH":          "Submission rate exceeds normal taxpayer behaviour thresholds.",
    "PROFILE_MISMATCH":         "Declared employment status conflicts with SARS records.",
    "KNOWN_FRAUD_PATTERN":      "Application matches a known fraudulent filing template.",
    "SUSPICIOUS_EMAIL_DOMAIN":  "Email domain associated with previous fraudulent accounts.",
    "INCOME_INCONSISTENCY":     "Income figures across multiple filings are inconsistent.",
}


class FraudSignalProducer(BaseProducer):
    topic = "fraud-signals"

    def generate_message(self) -> dict:
        signal_type = random.choice(SIGNAL_TYPES)
        severity    = random.choices(SEVERITIES, weights=SEVERITY_WEIGHTS, k=1)[0]
        customer_id = random.choice(CUSTOMER_ID_POOL)

        # detected_at: mostly recent, occasionally back-dated for realism
        minutes_ago = random.randint(0, 1440)   # up to 24 hours ago
        detected_at = (
            datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
        ).isoformat()

        return {
            "signal_id":      str(uuid.uuid4()),
            "application_id": str(uuid.uuid4()),   # references a tax application
            "customer_id":    customer_id,
            "signal_type":    signal_type,
            "severity":       severity,
            "detected_at":    detected_at,
            "description":    DESCRIPTIONS[signal_type],
        }


if __name__ == "__main__":
    FraudSignalProducer().run()
