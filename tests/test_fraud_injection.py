"""
tests/test_fraud_injection.py
-----------------------------
Offline tests for producers/fraud_injection.py (no Kafka, no network).

Run:  python -m pytest tests/test_fraud_injection.py
"""

import random
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "producers"))

from fraud_injection import ARCHETYPES, BlockInjector, apply_high_risk  # noqa: E402

INCOME_RANGES = {
    "EMPLOYED": (80_000, 650_000), "SELF_EMPLOYED": (40_000, 900_000),
    "CONTRACTOR": (60_000, 750_000), "UNEMPLOYED": (0, 30_000),
}
STATUSES = {
    "EMPLOYED": ["FULL_TIME", "PART_TIME"], "SELF_EMPLOYED": ["FULL_TIME", "SEASONAL"],
    "CONTRACTOR": ["FULL_TIME", "PART_TIME", "SEASONAL"], "UNEMPLOYED": ["N/A"],
}
REPEAT = [f"C{i:05d}" for i in range(1, 6)]
BASE = {"application_id": "a1", "customer_id": "C09999", "taxable_income": 100_000.0,
        "employment_type": "EMPLOYED", "employment_status": "FULL_TIME",
        "filing_status": "FIRST_TIME", "is_fraud": False}


def test_every_block_has_between_2_and_5_injections():
    inj = BlockInjector(block_size=50, min_per_block=2, max_per_block=5, rng=random.Random(1))
    counts = []
    for _ in range(400):                      # 400 blocks
        counts.append(sum(inj.next() for _ in range(50)))
    assert min(counts) >= 2 and max(counts) <= 5
    assert set(counts) == {2, 3, 4, 5}        # the full 2..5 range is exercised


def test_positions_are_random_across_blocks():
    inj = BlockInjector(rng=random.Random(2))
    positions = set()
    for _ in range(100):
        block = [inj.next() for _ in range(50)]
        positions.update(i for i, hit in enumerate(block) if hit)
    assert len(positions) > 40                # spread over (almost) the whole block


def test_default_block_matches_requirement():
    inj = BlockInjector(rng=random.Random(3))
    assert (inj.block_size, inj.min_per_block, inj.max_per_block) == (50, 2, 5)


@pytest.mark.parametrize("bad", [(50, 0, 5), (50, 6, 5), (50, 2, 51)])
def test_invalid_configuration_rejected(bad):
    with pytest.raises(ValueError):
        BlockInjector(*bad)


@pytest.mark.parametrize("seed", range(40))
def test_high_risk_records_carry_clear_red_flags(seed):
    rng = random.Random(seed)
    rec, kind = apply_high_risk(BASE, rng, INCOME_RANGES, STATUSES, REPEAT)
    assert kind in ARCHETYPES and rec["is_fraud"] is True
    assert rec["application_id"] == "a1" and BASE["is_fraud"] is False     # input not mutated
    emp = rec["employment_type"]
    assert rec["employment_status"] in STATUSES[emp]
    ceiling = INCOME_RANGES[emp][1]
    if kind == "phantom_income":
        assert emp == "UNEMPLOYED" and rec["taxable_income"] >= 150_000
    elif kind == "income_inflation":
        assert rec["taxable_income"] >= 2.5 * ceiling and rec["filing_status"] == "AMENDED"
    else:
        assert rec["taxable_income"] >= 1.5 * ceiling and rec["customer_id"] in REPEAT
    # in every case the income is clearly out of range for the employment type
    assert rec["taxable_income"] > ceiling
