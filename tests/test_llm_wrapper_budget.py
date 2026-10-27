"""
tests/test_llm_wrapper_budget.py
--------------------------------
Offline tests for agents/llm_wrapper.generate() with litellm.completion stubbed.
No network, no tokens spent.

Run:  python tests/test_llm_wrapper_budget.py
"""

import os
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).parent.parent))

os.environ["HF_API_TOKEN"] = "hf_test"
os.environ["HF_MODEL_LLM"] = "mistralai/Mixtral-8x7B-Instruct-v0.1"
os.environ["OPENAI_API_KEY"] = "sk-test"
os.environ["OPENAI_MODEL"] = "gpt-test"
os.environ.pop("LLM_FORCE_PROVIDER", None)

import litellm  # noqa: E402
import agents.llm_wrapper as lw  # noqa: E402
from agents.token_budget import TokenBudget, load_config  # noqa: E402

CALLS = []
FAIL = {}          # provider prefix -> exception to raise


def fake_completion(model, messages, **kw):
    CALLS.append({"model": model, "max_tokens": kw.get("max_tokens"), "api_base": kw.get("api_base")})
    for prefix, exc in FAIL.items():
        if model.startswith(prefix):
            raise exc
    usage = SimpleNamespace(total_tokens=1000)
    msg = SimpleNamespace(content=f"answer from {model}")
    return SimpleNamespace(choices=[SimpleNamespace(message=msg)], usage=usage)


litellm.completion = fake_completion


def new_budget(tmp):
    lw._budget = TokenBudget(load_config(), Path(tmp) / "s.json")
    lw._config = load_config()
    CALLS.clear()
    FAIL.clear()


def provider_of(model):
    return "openai" if model.startswith("openai/") else "huggingface"


def test_rotation_through_generate(tmp):
    new_budget(tmp)
    served = []
    while True:
        try:
            lw.generate("hello " * 40, max_new_tokens=256)
        except lw.ByoRequired:
            break
        served.append(provider_of(CALLS[-1]["model"]))
    assert served == (["huggingface"] * 20 + ["openai"] * 10) * 5, served
    s = lw.get_budget().status()
    assert s["providers"]["huggingface"]["used"] == 100_000
    assert s["providers"]["openai"]["used"] == 50_000
    print("ok  generate(): HF 20k -> OpenAI 10k x5, then ByoRequired")


def test_failover(tmp):
    new_budget(tmp)
    FAIL["huggingface/"] = RuntimeError("429 quota exceeded")
    out = lw.generate("hi")
    assert out.startswith("answer from openai/"), out
    assert lw.get_budget().status()["providers"]["huggingface"]["cooldown_s"] > 0
    assert lw.get_budget().status()["providers"]["huggingface"]["used"] == 0
    print("ok  HF error -> same call answered by OpenAI; failed call not counted")


def test_both_down_is_not_byo(tmp):
    new_budget(tmp)
    FAIL["huggingface/"] = RuntimeError("down")
    FAIL["openai/"] = RuntimeError("down")
    out = lw.generate("hi")
    assert out.startswith("[LLM unavailable]"), out
    out2 = lw.generate("hi")                       # now both cooling down
    assert "cooling down" in out2, out2
    print("ok  outage != budget exhausted (no BYO prompt, message explains cooldown)")


def test_force_openai_dev_switch(tmp):
    new_budget(tmp)
    os.environ["LLM_FORCE_PROVIDER"] = "openai"
    try:
        for _ in range(50):
            lw.generate("hi")
        assert all(c["model"].startswith("openai/") for c in CALLS)
        try:
            lw.generate("hi")
            raise AssertionError("51st forced call should hit the 50k cap")
        except lw.ByoRequired:
            pass
    finally:
        os.environ.pop("LLM_FORCE_PROVIDER")
    assert lw.get_budget().status()["providers"]["openai"]["used"] == 50_000
    print("ok  forced OpenAI: 50 calls then ByoRequired at the 50k cap")


def test_byo_override_not_counted(tmp):
    new_budget(tmp)
    for p in ("huggingface", "openai"):
        lw.get_budget()._state["providers"][p]["used"] = lw.get_budget().providers[p]["total_tokens"]
    try:
        lw.generate("hi")
        raise AssertionError("expected ByoRequired")
    except lw.ByoRequired:
        pass
    out = lw.generate("hi", llm_override={"model": "openai/my-model", "api_key": "k", "api_base": "http://x/v1"})
    assert out == "answer from openai/my-model"
    assert CALLS[-1]["api_base"] == "http://x/v1"
    assert lw.get_budget().status()["providers"]["openai"]["used"] == 50_000   # unchanged
    print("ok  BYO override works after exhaustion and is not counted")


def test_max_tokens_clamped(tmp):
    new_budget(tmp)
    lw.get_budget()._state["providers"]["huggingface"]["used"] = 99_900
    lw.generate("hi", max_new_tokens=512)
    assert CALLS[0]["model"].startswith("huggingface/")
    assert CALLS[0]["max_tokens"] <= 100, CALLS[0]
    print("ok  max_tokens clamped to remaining budget (", CALLS[0]["max_tokens"], ")")


if __name__ == "__main__":
    for fn in (test_rotation_through_generate, test_failover, test_both_down_is_not_byo,
               test_force_openai_dev_switch, test_byo_override_not_counted, test_max_tokens_clamped):
        with tempfile.TemporaryDirectory() as d:
            fn(d)
    print("\nall llm_wrapper budget tests passed")
