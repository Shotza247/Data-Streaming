"""
agents/llm_wrapper.py
----------------------
LLM backend using the HuggingFace Inference API (free hosted models).
No local model download or GPU required — all inference is done via
the HuggingFace REST API using your free account token.

Default model: mistralai/Mixtral-8x7B-Instruct-v0.1
  - Free tier on HuggingFace Inference API
  - Excellent instruction-following, great for tax Q&A
  - Falls back to: HuggingFaceH4/zephyr-7b-beta (also free)
  - Final fallback: google/flan-t5-base (text2text, very lightweight)

Environment variables:
    HF_API_TOKEN    Your HuggingFace token (https://huggingface.co/settings/tokens)
                    Free account works — no payment required.
    HF_MODEL_LLM    Override the model (default: mistralai/Mixtral-8x7B-Instruct-v0.1)

Exposes:
    generate(prompt, max_new_tokens=512)   -> str
    classify_intent(user_input)            -> "fraud_check" | "tax_question"
    get_model_info()                       -> dict  (model name, backend, status)
"""

import os
import logging
import requests

log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
HF_API_TOKEN = os.getenv("HF_API_TOKEN", "")

# Primary: Mixtral (great instruction following, free tier)
# Set HF_MODEL_LLM env var to override
HF_MODEL_LLM = os.getenv(
    "HF_MODEL_LLM",
    "mistralai/Mixtral-8x7B-Instruct-v0.1",
)

# Fallback chain if primary is unavailable / rate-limited
_FALLBACK_MODELS = [
    "HuggingFaceH4/zephyr-7b-beta",
    "mistralai/Mistral-7B-Instruct-v0.2",
    "google/flan-t5-xxl",
]

HF_API_BASE = "https://api-inference.huggingface.co/models"

# ── Backend status (set at first call) ───────────────────────────────────────
_backend_status = "unknown"   # "api" | "local" | "unavailable"
_active_model   = HF_MODEL_LLM


def get_model_info() -> dict:
    """Return current backend info for display in the dashboard."""
    return {
        "model":   _active_model,
        "backend": _backend_status,
        "token":   "configured" if HF_API_TOKEN else "missing",
    }


# ── HuggingFace Inference API call ────────────────────────────────────────────

def _hf_api_generate(model: str, prompt: str, max_new_tokens: int = 512) -> str:
    """
    Call the HuggingFace Inference API for text generation.
    Returns generated text or raises on failure.
    """
    headers = {"Content-Type": "application/json"}
    if HF_API_TOKEN:
        headers["Authorization"] = f"Bearer {HF_API_TOKEN}"

    # Instruction-tuned models use a chat-style payload
    payload = {
        "inputs": prompt,
        "parameters": {
            "max_new_tokens":  max_new_tokens,
            "temperature":     0.3,
            "return_full_text": False,
            "do_sample":       True,
        },
        "options": {
            "wait_for_model": True,   # don't fail if model is cold-starting
            "use_cache":      True,
        },
    }

    url  = f"{HF_API_BASE}/{model}"
    resp = requests.post(url, headers=headers, json=payload, timeout=60)

    if resp.status_code == 503:
        raise RuntimeError(f"Model {model} is loading (503). Retry in ~20s.")
    if resp.status_code == 429:
        raise RuntimeError(f"Rate limited on {model} (429). Try again shortly.")
    if resp.status_code == 401:
        raise RuntimeError("HF_API_TOKEN is invalid or missing. Set it in .env")
    if resp.status_code != 200:
        raise RuntimeError(f"HF API error {resp.status_code}: {resp.text[:200]}")

    data = resp.json()

    # Different model families return different shapes
    if isinstance(data, list) and data:
        first = data[0]
        # text-generation models
        if "generated_text" in first:
            return first["generated_text"].strip()
        # text2text models
        if "translation_text" in first:
            return first["translation_text"].strip()
    if isinstance(data, dict) and "generated_text" in data:
        return data["generated_text"].strip()

    raise RuntimeError(f"Unexpected HF API response shape: {str(data)[:200]}")


# ── Local pipeline fallback (lightweight flan-t5-base) ────────────────────────

_local_pipeline = None

def _local_generate(prompt: str, max_new_tokens: int = 256) -> str:
    """
    Last-resort fallback: use a local HuggingFace transformers pipeline.
    Uses flan-t5-base (80MB) — only loaded if API is unavailable.
    """
    global _local_pipeline
    if _local_pipeline is None:
        try:
            from transformers import pipeline as hf_pipeline
            log.info("Loading local fallback model: google/flan-t5-base")
            _local_pipeline = hf_pipeline(
                task="text2text-generation",
                model="google/flan-t5-base",
                device=-1,
                max_new_tokens=max_new_tokens,
            )
            log.info("Local fallback model loaded.")
        except Exception as exc:
            raise RuntimeError(f"Local fallback also unavailable: {exc}") from exc

    outputs = _local_pipeline(prompt, max_new_tokens=max_new_tokens, do_sample=False)
    return outputs[0]["generated_text"].strip()


# ── Public generate() ─────────────────────────────────────────────────────────

def generate(prompt: str, max_new_tokens: int = 512) -> str:
    """
    Generate a response to `prompt`.

    Strategy:
      1. Try HF Inference API with primary model (HF_MODEL_LLM)
      2. On failure, try each fallback model in _FALLBACK_MODELS
      3. If all API calls fail, try local flan-t5-base (if transformers installed)
      4. Return an informative error string if everything fails

    Returns the generated text string.
    """
    global _backend_status, _active_model

    # ── 1. Try HF Inference API ────────────────────────────────────────────────
    if HF_API_TOKEN or True:   # attempt API even without token (rate-limited but works for light use)
        models_to_try = [HF_MODEL_LLM] + [m for m in _FALLBACK_MODELS if m != HF_MODEL_LLM]
        for model in models_to_try:
            try:
                result = _hf_api_generate(model, prompt, max_new_tokens)
                _backend_status = "api"
                _active_model   = model
                log.info(f"HF API response from {model} ({len(result)} chars)")
                return result
            except RuntimeError as exc:
                log.warning(f"HF API [{model}] failed: {exc}")
                continue
            except Exception as exc:
                log.warning(f"HF API [{model}] unexpected error: {exc}")
                continue

    # ── 2. Local fallback ──────────────────────────────────────────────────────
    try:
        log.info("All HF API models failed — attempting local flan-t5-base fallback")
        result = _local_generate(prompt, max_new_tokens=min(max_new_tokens, 256))
        _backend_status = "local"
        _active_model   = "google/flan-t5-base (local)"
        return result
    except Exception as exc:
        log.error(f"Local fallback failed: {exc}")

    # ── 3. Give up gracefully ──────────────────────────────────────────────────
    _backend_status = "unavailable"
    if not HF_API_TOKEN:
        return (
            "[LLM unavailable] No HF_API_TOKEN set. "
            "Get a free token at https://huggingface.co/settings/tokens "
            "and add it to your .env file as HF_API_TOKEN=hf_..."
        )
    return (
        "[LLM unavailable] All HuggingFace API models are currently rate-limited or loading. "
        "Please try again in 30 seconds."
    )


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
