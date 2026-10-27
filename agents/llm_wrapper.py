"""
agents/llm_wrapper.py
----------------------
LLM access for the agents via LiteLLM, governed by a token budget.

Routing and limits live in agents/litellm_config.yaml; the policy itself is in
agents/token_budget.py:

    HuggingFace 20,000 tokens -> OpenAI 10,000 tokens -> HuggingFace ... (loop)
    until HuggingFace has used 100,000 and OpenAI 50,000 in total.

When a provider errors (quota, rate limit, auth, outage) the call fails over to
the other provider and the failed one is skipped for a cooldown. When both
budgets are used up, generate() raises ByoRequired so the UI can ask the user to
supply their own model/provider (bring-your-own calls are not counted).

Environment variables:
    HF_API_TOKEN, HF_MODEL_LLM      HuggingFace provider
    OPENAI_API_KEY, OPENAI_MODEL    OpenAI provider
    LLM_FORCE_PROVIDER              dev switch: "openai" or "huggingface" sends every
                                    call to that provider (still counted against its cap)
    LITELLM_CONFIG, LLM_BUDGET_STATE  optional paths

Exposes:
    generate(prompt, max_new_tokens=512, llm_override=None)  -> str   (raises ByoRequired)
    classify_intent(user_input)                              -> "fraud_check" | "tax_question"
    get_model_info()                                         -> dict
    get_budget()                                             -> TokenBudget
"""

import logging

from agents.token_budget import TokenBudget, estimate_tokens, load_config, resolve_env

log = logging.getLogger(__name__)

BYO_MESSAGE = (
    "The built-in AI token allowance has been used up (HuggingFace 100,000 and "
    "OpenAI 50,000 tokens). To keep going, connect your own model/provider in the "
    "AI Agent Chat settings (model name, API key, and optional API base URL)."
)

_config = None
_budget = None
_last = {"model": "—", "backend": "unknown"}


class ByoRequired(Exception):
    """Raised when the built-in token budget is exhausted and no override is given."""


def get_budget() -> TokenBudget:
    global _config, _budget
    if _budget is None:
        _config = load_config()
        _budget = TokenBudget(_config)
    return _budget


def _provider_call_params(name: str) -> dict:
    """Resolve a provider's LiteLLM kwargs from litellm_config.yaml."""
    get_budget()
    model_name = get_budget().providers[name]["model_name"]
    entry = next(m for m in _config["model_list"] if m["model_name"] == model_name)
    params = dict(entry["litellm_params"])
    model = resolve_env(params.pop("model"))
    provider = params.pop("custom_llm_provider", name)
    api_key = resolve_env(params.pop("api_key", None))
    kwargs = {k: resolve_env(v) for k, v in params.items()}
    return {"model": f"{provider}/{model}" if model else None, "api_key": api_key, "extra": kwargs}


def _completion(model, api_key, api_base, prompt, max_tokens, extra):
    import litellm
    litellm.drop_params = True          # ignore params a given model does not support
    return litellm.completion(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        api_key=api_key,
        api_base=api_base or None,
        max_tokens=max_tokens,
        timeout=60,
        num_retries=0,                  # failover is handled here, not by LiteLLM
        **(extra or {}),
    )


def _text_and_tokens(resp, prompt: str):
    text = (resp.choices[0].message.content or "").strip()
    usage = getattr(resp, "usage", None)
    tokens = getattr(usage, "total_tokens", None) if usage else None
    if not tokens:
        tokens = estimate_tokens(prompt) + estimate_tokens(text)
    return text, int(tokens)


def get_model_info() -> dict:
    """Backend info + budget status for the dashboard."""
    import os
    info = {
        "model":   _last["model"],
        "backend": _last["backend"],     # "api" | "byo" | "unavailable" | "unknown"
        "token":   "configured" if (os.getenv("HF_API_TOKEN") or os.getenv("OPENAI_API_KEY")) else "missing",
    }
    try:
        info["budget"] = get_budget().status()
    except Exception as exc:
        info["budget_error"] = str(exc)
    return info


def generate(prompt: str, max_new_tokens: int = 512, llm_override: dict = None) -> str:
    """
    Generate a response to `prompt`.

    llm_override: {"model": "...", "api_key": "...", "api_base": "..."} -> bring-your-own
                  provider. Used as-is and NOT counted against the built-in budget.

    Raises ByoRequired when the built-in budget is exhausted and no override is given.
    Returns an "[LLM unavailable] ..." string if providers are configured but all failed.
    """
    # ── Bring-your-own provider ────────────────────────────────────────────────
    if llm_override and llm_override.get("model"):
        resp = _completion(
            llm_override["model"], llm_override.get("api_key"), llm_override.get("api_base"),
            prompt, max_new_tokens, None,
        )
        text, _ = _text_and_tokens(resp, prompt)
        _last.update(model=llm_override["model"], backend="byo")
        return text

    # ── Built-in providers under the token budget ──────────────────────────────
    budget = get_budget()
    est_prompt = estimate_tokens(prompt)
    plan = budget.plan(est_prompt)
    if not plan:
        _last.update(backend="unavailable")
        status = budget.status()
        if status["byo_required"]:
            raise ByoRequired(BYO_MESSAGE)
        waits = [f"{n} ({p['cooldown_s']}s)" for n, p in status["providers"].items() if p["cooldown_s"]]
        return (
            "[LLM unavailable] Providers are cooling down after errors"
            + (f": {', '.join(waits)}" if waits else "")
            + ". Token budget remains; try again shortly."
        )

    errors = []
    for name in plan:
        p = _provider_call_params(name)
        if not p["api_key"] or not p["model"]:
            budget.mark_failed(name, "missing API key or model in .env")
            errors.append(f"{name}: missing API key or model")
            continue
        max_tokens = budget.max_completion(name, est_prompt, max_new_tokens)
        try:
            resp = _completion(p["model"], p["api_key"], None, prompt, max_tokens, p["extra"])
            text, tokens = _text_and_tokens(resp, prompt)
        except Exception as exc:
            budget.mark_failed(name, f"{type(exc).__name__}: {exc}")
            errors.append(f"{name}: {type(exc).__name__}")
            log.warning(f"LLM [{name}] failed, trying next provider: {exc}")
            continue

        budget.record(name, tokens)          # count real usage even if text is empty
        if not text:
            errors.append(f"{name}: empty response")
            continue
        _last.update(model=p["model"], backend="api")
        log.info(f"LLM [{name}] {p['model']} used {tokens} tokens")
        return text

    # Every attempted provider failed. If that exhausted the budget, ask for BYO.
    _last.update(backend="unavailable")
    if budget.status()["byo_required"]:
        raise ByoRequired(BYO_MESSAGE)
    return "[LLM unavailable] " + "; ".join(errors) + ". Check keys/quotas in .env, or try again shortly."


# ── Intent classifier (no LLM needed — pure heuristic) ───────────────────────

def classify_intent(user_input: str) -> str:
    """
    Keyword-based intent classification. Fast and deterministic.
    Returns: 'fraud_check' | 'tax_question'
    """
    lower = user_input.lower()
    fraud_keywords = [
        "fraud", "suspicious", "risk", "investigate", "application_id",
        "app-", "application id", "check application", "is this fraud",
        "flag", "anomaly", "pattern", "detect",
    ]
    if any(kw in lower for kw in fraud_keywords):
        return "fraud_check"
    return "tax_question"
