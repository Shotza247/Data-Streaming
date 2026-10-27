"""
tests/test_token_budget.py
--------------------------
Offline tests for agents/token_budget.py (no network, no LLM).

Run:  python tests/test_token_budget.py
"""

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from agents.token_budget import TokenBudget, load_config  # noqa: E402


def fresh(tmp):
    return TokenBudget(load_config(), Path(tmp) / "state.json")


def simulate(b, call_tokens):
    """Serve calls of `call_tokens` until the budget refuses. Returns served sequence."""
    seq = []
    while True:
        plan = b.plan(est_prompt_tokens=call_tokens // 2)
        if not plan:
            return seq
        b.record(plan[0], call_tokens)
        seq.append(plan[0])


def test_rotation_and_caps(tmp):
    b = fresh(tmp)
    seq = simulate(b, 1000)
    s = b.status()
    assert s["providers"]["huggingface"]["used"] == 100_000, s
    assert s["providers"]["openai"]["used"] == 50_000, s
    assert s["byo_required"] and s["all_capped"], s
    # Strict alternation in stints: 20 HF calls, 10 OpenAI calls, x5
    expected = (["huggingface"] * 20 + ["openai"] * 10) * 5
    assert seq == expected, seq[:40]
    print("ok  rotation: HF 20k -> OpenAI 10k, 5 cycles, then BYO")


def test_caps_hold_with_uneven_calls(tmp):
    b = fresh(tmp)
    simulate(b, 1700)
    s = b.status()
    for n, p in s["providers"].items():
        assert p["used"] <= p["total"] + 1700, (n, p)   # overshoot < one call
    # max_completion clamps so the cap itself is not crossed by the completion
    b2 = fresh(tmp + "/x") if False else fresh(tempfile.mkdtemp())
    b2._state["providers"]["openai"]["used"] = 49_900
    assert b2.max_completion("openai", est_prompt_tokens=60, requested=512) == 40
    print("ok  caps hold; max_completion clamps to remaining budget")


def test_failover_and_cooldown(tmp):
    b = fresh(tmp)
    assert b.plan(100)[0] == "huggingface"
    b.mark_failed("huggingface", "429 quota exceeded")
    assert b.plan(100) == ["openai"], b.plan(100)
    assert b.status()["providers"]["huggingface"]["cooldown_s"] > 0
    b.clear_cooldown("huggingface")
    assert "huggingface" in b.plan(100)
    print("ok  failover to OpenAI on HF error; cooldown clears")


def test_force_provider_still_capped(tmp):
    b = fresh(tmp)
    b.set_force_provider("openai")
    seq = simulate(b, 1000)
    s = b.status()
    assert set(seq) == {"openai"} and len(seq) == 50, (set(seq), len(seq))
    assert s["providers"]["openai"]["used"] == 50_000
    assert s["providers"]["huggingface"]["used"] == 0
    assert s["byo_required"], s
    b.set_force_provider(None)
    assert b.plan(100) == ["huggingface"], "HF still available after forced mode ends"
    print("ok  forced OpenAI stops at 50k; HF budget untouched")


def test_persistence(tmp):
    b = fresh(tmp)
    b.record("huggingface", 20_000)             # completes a stint -> OpenAI next
    b2 = fresh(tmp)                              # same state file
    s = b2.status()
    assert s["providers"]["huggingface"]["used"] == 20_000
    assert s["active"] == "openai"
    print("ok  usage + active provider survive restart")


def test_single_provider_left(tmp):
    b = fresh(tmp)
    b._state["providers"]["openai"]["used"] = 50_000
    seq = simulate(b, 1000)
    assert set(seq) == {"huggingface"} and len(seq) == 100, (set(seq), len(seq))
    print("ok  when OpenAI is capped, HF continues alone to 100k")


if __name__ == "__main__":
    for fn in (test_rotation_and_caps, test_caps_hold_with_uneven_calls, test_failover_and_cooldown,
               test_force_provider_still_capped, test_persistence, test_single_provider_left):
        with tempfile.TemporaryDirectory() as d:
            fn(d)
    print("\nall token_budget tests passed")
