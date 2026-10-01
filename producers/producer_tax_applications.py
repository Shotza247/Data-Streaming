"""
producers/producer_tax_applications.py

Produces synthetic tax application records to the `tax-applications` topic.

Schema (matches reference architecture image exactly):
    application_id      — UUID string
    customer_id         — short alphanumeric ID  (shared with taxpayer-profiles)
    customer_name       — full name
    email               — email address
    country             — always "South Africa"
    province            — one of 9 SA provinces
    taxable_income      — float; occasionally anomalous (>3x avg) for fraud detection
    employment_type     — EMPLOYED | SELF_EMPLOYED | CONTRACTOR | UNEMPLOYED
    employment_status   — FULL_TIME | PART_TIME | SEASONAL | N/A
    submitted_date      — ISO date string
    tax_year            — e.g. "2024/25"
    filing_status       — FIRST_TIME | RETURNING | AMENDED
    is_fraud            — bool; set True ~5% of the time, more often on anomalous records
                          (used as synthetic training label for MLflow model)
"""

import os
import random
import uuid
from datetime import date, timedelta

from faker import Faker
from base_producer import BaseProducer

fake = Faker("en_ZA")   # South African locale for realistic names

# ── Constants ─────────────────────────────────────────────────────────────────

SA_PROVINCES = [
    "Gauteng", "Western Cape", "KwaZulu-Natal", "Eastern Cape",
    "Limpopo", "Mpumalanga", "North West", "Free State", "Northern Cape",
]
EMPLOYMENT_TYPES   = ["EMPLOYED", "SELF_EMPLOYED", "CONTRACTOR", "UNEMPLOYED"]
EMPLOYMENT_STATUSES = {
    "EMPLOYED":     ["FULL_TIME", "PART_TIME"],
    "SELF_EMPLOYED":["FULL_TIME", "SEASONAL"],
    "CONTRACTOR":   ["FULL_TIME", "PART_TIME", "SEASONAL"],
    "UNEMPLOYED":   ["N/A"],
}
TAX_YEARS      = ["2022/23", "2023/24", "2024/25"]
FILING_STATUSES = ["FIRST_TIME", "RETURNING", "AMENDED"]

# Approximate annual income ranges by employment type (ZAR)
INCOME_RANGES = {
    "EMPLOYED":      (80_000,  650_000),
    "SELF_EMPLOYED": (40_000,  900_000),
    "CONTRACTOR":    (60_000,  750_000),
    "UNEMPLOYED":    (0,        30_000),
}

# Shared pool of customer IDs — reused across producers so JOIN works in Flink
CUSTOMER_ID_POOL = [f"C{str(i).zfill(5)}" for i in range(1, 5001)]

# Anomaly injection: some IDs will file multiple times quickly
REPEAT_FILERS = random.sample(CUSTOMER_ID_POOL, k=50)


class TaxApplicationProducer(BaseProducer):
    topic = "tax-applications"

    def __init__(self):
        super().__init__()
        # Track how many times each repeat filer has appeared this session
        self._repeat_counts: dict[str, int] = {}

    def generate_message(self) -> dict:
        # ~8% of records come from the repeat filer pool (velocity anomaly)
        if random.random() < 0.08 and REPEAT_FILERS:
            customer_id = random.choice(REPEAT_FILERS)
        else:
            customer_id = random.choice(CUSTOMER_ID_POOL)

        emp_type   = random.choice(EMPLOYMENT_TYPES)
        emp_status = random.choice(EMPLOYMENT_STATUSES[emp_type])
        low, high  = INCOME_RANGES[emp_type]
        income     = round(random.uniform(low, high), 2)

        # Anomaly: ~4% of records have income inflated 3–8× (high-income anomaly)
        is_income_anomaly = random.random() < 0.04
        if is_income_anomaly:
            income = round(income * random.uniform(3.0, 8.0), 2)

        # Fraud label: True if income anomaly OR repeat filer; also ~1% random
        is_fraud = (
            is_income_anomaly
            or customer_id in REPEAT_FILERS
            or random.random() < 0.01
        )

        # Submitted date within last 365 days
        days_ago = random.randint(0, 365)
        submitted = (date.today() - timedelta(days=days_ago)).isoformat()

        return {
            "application_id":  str(uuid.uuid4()),
            "customer_id":     customer_id,
            "customer_name":   fake.name(),
            "email":           fake.email(),
            "country":         "South Africa",
            "province":        random.choice(SA_PROVINCES),
            "taxable_income":  income,
            "employment_type": emp_type,
            "employment_status": emp_status,
            "submitted_date":  submitted,
            "tax_year":        random.choice(TAX_YEARS),
            "filing_status":   random.choice(FILING_STATUSES),
            "is_fraud":        is_fraud,
        }


if __name__ == "__main__":
    TaxApplicationProducer().run()
