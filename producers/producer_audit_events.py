"""
producers/producer_audit_events.py

Produces synthetic audit event records to the `audit-events` topic.

Audit events track every action taken on a tax application — submissions,
reviews, approvals, rejections, and system-level changes. These are used
for the historical audit trail in the persistence layer.

Schema:
    event_id        — UUID string
    application_id  — UUID string
    actor           — who performed the action (taxpayer ID, agent ID, or SYSTEM)
    action          — what was done
    timestamp       — ISO datetime string
    metadata        — JSON object with action-specific context
"""

import json
import random
import uuid
from datetime import datetime, timedelta, timezone

from base_producer import BaseProducer

# Shared customer ID pool
CUSTOMER_ID_POOL = [f"C{str(i).zfill(5)}" for i in range(1, 5001)]
AGENT_ID_POOL    = [f"AGT{str(i).zfill(3)}" for i in range(1, 51)]

ACTIONS = [
    "APPLICATION_SUBMITTED",
    "APPLICATION_RECEIVED",
    "DOCUMENT_UPLOADED",
    "REVIEW_STARTED",
    "REVIEW_COMPLETED",
    "FRAUD_CHECK_TRIGGERED",
    "FRAUD_CHECK_CLEARED",
    "MANUAL_REVIEW_ASSIGNED",
    "APPLICATION_APPROVED",
    "APPLICATION_REJECTED",
    "AMENDMENT_REQUESTED",
    "AMENDMENT_SUBMITTED",
    "REFUND_ISSUED",
    "SYSTEM_VALIDATION_PASSED",
    "SYSTEM_VALIDATION_FAILED",
]

# Actor type by action
ACTOR_MAP = {
    "APPLICATION_SUBMITTED":     "TAXPAYER",
    "APPLICATION_RECEIVED":      "SYSTEM",
    "DOCUMENT_UPLOADED":         "TAXPAYER",
    "REVIEW_STARTED":            "AGENT",
    "REVIEW_COMPLETED":          "AGENT",
    "FRAUD_CHECK_TRIGGERED":     "SYSTEM",
    "FRAUD_CHECK_CLEARED":       "SYSTEM",
    "MANUAL_REVIEW_ASSIGNED":    "SYSTEM",
    "APPLICATION_APPROVED":      "AGENT",
    "APPLICATION_REJECTED":      "AGENT",
    "AMENDMENT_REQUESTED":       "AGENT",
    "AMENDMENT_SUBMITTED":       "TAXPAYER",
    "REFUND_ISSUED":             "SYSTEM",
    "SYSTEM_VALIDATION_PASSED":  "SYSTEM",
    "SYSTEM_VALIDATION_FAILED":  "SYSTEM",
}


class AuditEventProducer(BaseProducer):
    topic = "audit-events"

    def generate_message(self) -> dict:
        action     = random.choice(ACTIONS)
        actor_type = ACTOR_MAP[action]
        customer_id = random.choice(CUSTOMER_ID_POOL)

        # Resolve actor ID based on actor type
        if actor_type == "TAXPAYER":
            actor = customer_id
        elif actor_type == "AGENT":
            actor = random.choice(AGENT_ID_POOL)
        else:
            actor = "SYSTEM"

        # timestamp: within last 48 hours
        minutes_ago = random.randint(0, 2880)
        timestamp   = (
            datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
        ).isoformat()

        # Metadata varies by action type
        metadata = self._build_metadata(action, customer_id)

        return {
            "event_id":       str(uuid.uuid4()),
            "application_id": str(uuid.uuid4()),
            "actor":          actor,
            "action":         action,
            "timestamp":      timestamp,
            "metadata":       metadata,
        }

    def _build_metadata(self, action: str, customer_id: str) -> dict:
        """Return action-specific metadata fields."""
        base = {"customer_id": customer_id}

        if action == "APPLICATION_SUBMITTED":
            base["channel"] = random.choice(["WEB", "MOBILE", "BRANCH"])
        elif action in ("REVIEW_COMPLETED", "APPLICATION_APPROVED", "APPLICATION_REJECTED"):
            base["reviewer_notes"] = random.choice([
                "All documents verified.",
                "Income declaration consistent with employer records.",
                "Minor discrepancy in submitted date — within tolerance.",
                "Flagged for secondary review.",
                "High-risk score — escalated.",
            ])
        elif action == "FRAUD_CHECK_TRIGGERED":
            base["trigger_reason"] = random.choice([
                "velocity_breach", "income_anomaly", "profile_mismatch"
            ])
        elif action == "REFUND_ISSUED":
            base["refund_amount"] = round(random.uniform(500, 25_000), 2)
            base["payment_method"] = random.choice(["EFT", "CHEQUE"])
        elif "VALIDATION" in action:
            base["validation_rule"] = random.choice([
                "income_range_check", "duplicate_id_check",
                "province_code_validation", "tax_year_check",
            ])

        return base


if __name__ == "__main__":
    AuditEventProducer().run()
