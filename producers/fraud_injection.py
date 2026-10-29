"""
producers/fraud_injection.py
----------------------------
Block-based injection of highly fraudulent tax applications.

For every block of FRAUD_BLOCK_SIZE applications (default 50), between
FRAUD_INJECT_MIN and FRAUD_INJECT_MAX (default 2 to 5) randomly chosen
applications are replaced by a high-risk profile. Each profile carries at
least two independent red flags so it is unambiguous to both the label and
the model:

    income_inflation  income 2.5-6x the ceiling for the employment type,
                      filed as AMENDED
    phantom_income    UNEMPLOYED with a large income (150k-1.2M)
    serial_filer      a customer from the repeat-filer pool (high filing
                      velocity), income 1.5-3x the ceiling, filed as AMENDED

Pure Python (no Kafka dependency) so it can be unit tested.
"""

import os
import random

BLOCK_SIZE    = int(os.getenv("FRAUD_BLOCK_SIZE", "50"))
MIN_PER_BLOCK = int(os.getenv("FRAUD_INJECT_MIN", "2"))
MAX_PER_BLOCK = int(os.getenv("FRAUD_INJECT_MAX", "5"))

ARCHETYPES = ("income_inflation", "phantom_income", "serial_filer")


class BlockInjector:
    """
    Decides, message by message, whether the next application is injected.
    Exactly randint(min, max) positions per block are chosen up front, so a
    block always contains between `min_per_block` and `max_per_block` cases.
    """

    def __init__(self, block_size: int = BLOCK_SIZE, min_per_block: int = MIN_PER_BLOCK,
                 max_per_block: int = MAX_PER_BLOCK, rng: random.Random | None = None):
        if not 1 <= min_per_block <= max_per_block <= block_size:
            raise ValueError(
                f"Need 1 <= min ({min_per_block}) <= max ({max_per_block}) <= block size ({block_size})"
            )
        self.block_size = block_size
        self.min_per_block = min_per_block
        self.max_per_block = max_per_block
        self._rng = rng or random.Random()
        self._pos = 0
        self._slots: set[int] = set()

    def next(self) -> bool:
        """True if the next application should be a high-risk case."""
        if self._pos == 0:
            k = self._rng.randint(self.min_per_block, self.max_per_block)
            self._slots = set(self._rng.sample(range(self.block_size), k))
        hit = self._pos in self._slots
        self._pos = (self._pos + 1) % self.block_size
        return hit


def apply_high_risk(record: dict, rng, income_ranges: dict, employment_statuses: dict,
                    repeat_filers: list) -> tuple[dict, str]:
    """
    Turn `record` into a high-risk application. Returns (record, archetype).
    `rng` is a random.Random (or the random module).
    """
    archetype = rng.choice(ARCHETYPES)
    rec = dict(record)

    if archetype == "phantom_income":
        rec["employment_type"] = "UNEMPLOYED"
        rec["employment_status"] = rng.choice(employment_statuses["UNEMPLOYED"])
        rec["taxable_income"] = round(rng.uniform(150_000, 1_200_000), 2)
        rec["filing_status"] = rng.choice(["RETURNING", "AMENDED"])
    else:
        emp = rng.choice([t for t in income_ranges if t != "UNEMPLOYED"])
        ceiling = income_ranges[emp][1]
        rec["employment_type"] = emp
        rec["employment_status"] = rng.choice(employment_statuses[emp])
        rec["filing_status"] = "AMENDED"
        if archetype == "income_inflation":
            rec["taxable_income"] = round(ceiling * rng.uniform(2.5, 6.0), 2)
        else:  # serial_filer
            rec["taxable_income"] = round(ceiling * rng.uniform(1.5, 3.0), 2)
            if repeat_filers:
                rec["customer_id"] = rng.choice(repeat_filers)

    rec["is_fraud"] = True
    return rec, archetype
