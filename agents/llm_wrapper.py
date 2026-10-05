"""
agents/llm_wrapper.py
----------------------
Wraps the HuggingFace flan-t5-base text2text-generation pipeline.
Model: google/flan-t5-base  (~250MB, instruction-tuned T5, CPU-only, no GPU needed)

Exposes:
    get_pipeline()                              -> transformers.Pipeline (cached)
    generate(prompt, max_new_tokens=256)        -> str
    classify_intent(user_input)                 -> "fraud_check" | "tax_question"

Environment:
    HF_MODEL_LLM  (default: google/flan-t5-base)
"""

import os
import logging

log = logging.getLogger(__name__)

HF_MODEL_LLM = os.getenv("HF_MODEL_LLM", "google/flan-t5-base")

# ── Cached singleton ──────────────────────────────────────────────────────────
_pipeline = None


def get_pipeline():
    """
    Load and cache the flan-t5-base text2text pipeline.
    Downloads ~250MB on first call. CPU-only (torch no-GPU).
    """
    global _pipeline
    if _pipeline is None:
        try:
            from transformers import pipeline as hf_pipeline
        except ImportError as exc:
            raise ImportError(
                "transformers not installed: pip install transformers"
            ) from exc

        log.info(f"Loading LLM pipeline: {HF_MODEL_LLM}")
        _pipeline = hf_pipeline(
            task="text2text-generation",
            model=HF_MODEL_LLM,
            device=-1,          # CPU only (-1 = CPU, 0+ = GPU)
            max_new_tokens=256,
        )
        log.info("LLM pipeline loaded.")
    return _pipeline


def generate(prompt: str, max_new_tokens: int = 256) -> str:
    """
    Run text2text generation on `prompt`.
    Returns the generated text string.
    Falls back to a template response on error.
    """
    try:
        pipe    = get_pipeline()
        outputs = pipe(prompt, max_new_tokens=max_new_tokens, do_sample=False)
        return outputs[0]["generated_text"].strip()
    except Exception as exc:
        log.warning(f"LLM generate failed: {exc}")
        return f"[LLM unavailable — {exc}]"


def classify_intent(user_input: str) -> str:
    """
    Simple heuristic intent classification — no LLM needed for routing.
    Returns: 'fraud_check' | 'tax_question'

    Rules:
    - If input contains an application_id pattern (APP-XXXXX) → fraud_check
    - If input contains fraud / suspicious / risk keywords → fraud_check
    - Otherwise → tax_question
    """
    lower = user_input.lower()
    fraud_keywords = [
        "fraud", "suspicious", "risk", "investigate", "application_id",
        "app-", "application id", "check application", "is this fraud",
        "flag", "anomaly", "pattern",
    ]
    if any(kw in lower for kw in fraud_keywords):
        return "fraud_check"
    return "tax_question"
