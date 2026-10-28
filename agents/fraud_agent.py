"""
agents/fraud_agent.py
----------------------
LangGraph Fraud Detection Agent node.

Capabilities:
  1. Scores the application with the 'fraud-detector' model via the model-api
     service (mlflow/serve_model.py) -> fraud_probability
  3. Searches Qdrant fraud_similarity collection for top-5 similar known cases
  4. Assembles a structured JSON response

State reads:  state["application_dict"]
State writes: state["response"]   (str — formatted JSON summary)
              state["sources"]    (list — similar case IDs from Qdrant)
"""

import logging
import os
from typing import Any, Optional

import requests

log = logging.getLogger(__name__)

MODEL_API_URL = os.getenv("MODEL_API_URL", "http://model-api:8000")


def _score_application(app_dict: dict) -> tuple[Optional[float], str]:
    """
    Ask the model-api service for a fraud probability.
    Returns (probability or None, reason). The reason explains a None result.
    """
    record = {
        "province":          str(app_dict.get("province", "UNKNOWN")),
        "taxable_income":    float(app_dict.get("taxable_income", 0.0) or 0.0),
        "employment_type":   str(app_dict.get("employment_type", "UNKNOWN")),
        "employment_status": str(app_dict.get("employment_status", "UNKNOWN")),
        "filing_status":     str(app_dict.get("filing_status", "UNKNOWN")),
    }
    try:
        r = requests.post(f"{MODEL_API_URL}/predict", json={"records": [record]}, timeout=10)
    except requests.RequestException as exc:
        return None, f"model-api unreachable ({type(exc).__name__})"
    if r.status_code == 503:
        return None, "no trained model yet"
    if r.status_code != 200:
        return None, f"model-api error {r.status_code}: {r.text[:120]}"
    return r.json()["fraud_probability"][0], ""


def fraud_agent_node(state: dict) -> dict:
    """
    LangGraph node: Fraud Detection Agent.

    Reads:  state["application_dict"]   dict with application fields
    Writes: state["response"]           JSON-formatted fraud assessment
            state["sources"]            list of similar case IDs
    """
    import json
    import pandas as pd

    app_dict = state.get("application_dict") or {}

    # ── ML inference (model-api service) ───────────────────────────────────────
    fraud_probability, unavailable_reason = _score_application(app_dict)

    # ── Qdrant similarity search ───────────────────────────────────────────────
    similar_cases = []
    try:
        from agents.qdrant_search import search_similar_fraud
        results = search_similar_fraud(app_dict, top_k=5)
        for r in results:
            p = r.payload or {}
            similar_cases.append({
                "application_id":  p.get("application_id", "?"),
                "customer_id":     p.get("customer_id", "?"),
                "similarity":      round(r.score, 4),
                "is_fraud":        p.get("is_fraud", False),
                "taxable_income":  p.get("taxable_income", 0),
                "province":        p.get("province", "?"),
            })
    except Exception as exc:
        log.warning(f"Qdrant similarity search failed: {exc}")

    # ── Recommendation logic ───────────────────────────────────────────────────
    high_similarity_fraud = sum(1 for c in similar_cases if c.get("is_fraud") and c.get("similarity", 0) > 0.85)

    if fraud_probability is not None:
        if fraud_probability >= 0.75:
            recommendation = "HIGH RISK — Flag for manual review. Model confidence is high."
        elif fraud_probability >= 0.45:
            recommendation = "MEDIUM RISK — Monitor this application. Further investigation recommended."
        else:
            recommendation = "LOW RISK — Application appears legitimate."
    elif high_similarity_fraud >= 3:
        recommendation = "ELEVATED RISK — Highly similar to known fraud cases in the database."
    else:
        recommendation = f"ASSESSMENT UNAVAILABLE — {unavailable_reason}. Train the model from the MLflow tab."

    # ── Assemble response ──────────────────────────────────────────────────────
    response_data = {
        "application_id":    app_dict.get("application_id", "N/A"),
        "fraud_probability": fraud_probability,
        "recommendation":    recommendation,
        "similar_cases":     similar_cases[:3],   # top-3 for display
        "model_available":   fraud_probability is not None,
    }

    state["response"] = json.dumps(response_data, indent=2)
    state["sources"]  = [c["application_id"] for c in similar_cases]

    return state
