"""
producers/producer_taxpayer_profiles.py

Produces synthetic taxpayer profile records to the `taxpayer-profiles` topic.

The customer_id pool must overlap with tax-applications so Flink's
temporal JOIN (FOR SYSTEM_TIME AS OF) can match profiles to applications.

Schema:
    customer_id               — from shared CUSTOMER_ID_POOL
    age                       — 18–75
    risk_score                — 0.0–1.0 float; higher for flagged individuals
    historical_filings_count  — 0–15
    avg_income_3yr            — average taxable income over last 3 years (ZAR)
    flagged_previously        — bool; True ~8% of the time
"""

import random

from faker import Faker
from base_producer import BaseProducer

fake = Faker("en_ZA")

# Shared customer ID pool — must match producer_tax_applications.py
CUSTOMER_ID_POOL = [f"C{str(i).zfill(5)}" for i in range(1, 5001)]


class TaxpayerProfileProducer(BaseProducer):
    topic = "taxpayer-profiles"

    def generate_message(self) -> dict:
        customer_id = random.choice(CUSTOMER_ID_POOL)
        flagged     = random.random() < 0.08

        # Risk score: flagged individuals score higher
        if flagged:
            risk_score = round(random.uniform(0.55, 1.0), 4)
        else:
            risk_score = round(random.uniform(0.0, 0.45), 4)

        # Historical filing count inversely correlated with risk for simplicity
        filings = random.randint(0, 15)

        # Average income over 3 years — base range with some variance
        avg_income = round(random.uniform(25_000, 700_000), 2)

        return {
            "customer_id":              customer_id,
            "age":                      random.randint(18, 75),
            "risk_score":               risk_score,
            "historical_filings_count": filings,
            "avg_income_3yr":           avg_income,
            "flagged_previously":       flagged,
        }


if __name__ == "__main__":
    TaxpayerProfileProducer().run()
