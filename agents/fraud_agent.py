"""
agents/fraud_agent.py
----------------------
LangGraph Fraud Detection Agent node.

Capabilities:
  1. Loads the 'fraud-detector' model from the MLflow Model Registry
  2. Runs ML inference on the application record → fraud_probability
  3. Searches Qdrant fraud_similarity collection for top-5 similar known cases
  4. Assembles a structured JSON response

State reads:  state["application_dict"]
State writes: state["response"]   (str — formatted JSON summary)
              state["sources"]    (list — similar case IDs from Qdrant)
"""

import logging
from typing import Any

log = logging.getLogger(__name__)

# ── MLflow model loader (cached per process) ──────────────────────────────────
_fraud_model = None

def _get_fraud_model():
    global _fraud_model
    if _fraud_model is None:
        try:
            import mlflow
            import sys
            from pathlib import Path
            sys.path.insert(0, str(Path(__file__).parent.parent))
            from mlflow.mlflow_config import setup_mlflow, MODEL_NAME, PRODUCTION_STAGE

            setup_mlflow()
            client = mlflow.tracking.MlflowClient()

            # Try Production stage first, fall back to Staging
            for stage in [PRODUCTION_STAGE, "Staging", "None"]:
                versions = [
                    v for v in client.search_model_versions(f"name='{MODEL_NAME}'")
                    if v.current_stage == stage
                ]
                if versions:
                    latest = max(versions, key=lambda v: int(v.version))
                    uri = f"models:/{MODEL_NAME}/{stage}"
                    log.info(f"Loading MLflow model: {uri}")
                    _fraud_model = mlflow.sklearn.load_model(uri)
                    break

            if _fraud_model is None:
                log.warning("No registered fraud-detector model found in MLflow.")
        except Exception as exc:
            log.warning(f"MLflow model load failed: {exc}")
    return _fraud_model


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

    # ── Attempt ML inference ───────────────────────────────────────────────────
    fraud_probability = None
    model = _get_fraud_model()

    if model is not None:
        try:
            features = pd.DataFrame([{
                "province":          str(app_dict.get("province", "UNKNOWN")),
                "taxable_income":    float(app_dict.get("taxable_income", 0.0)),
                "employment_type":   str(app_dict.get("employment_type", "UNKNOWN")),
                "employment_status": str(app_dict.get("employment_status", "UNKNOWN")),
                "filing_status":     str(app_dict.get("filing_status", "UNKNOWN")),
            }])
            proba = model.predict_proba(features)
            fraud_probability = round(float(proba[0][1]), 4)
        except Exception as exc:
            log.warning(f"Model inference failed: {exc}")

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
        recommendation = "ASSESSMENT UNAVAILABLE — MLflow model not loaded. Check MLflow service."

    # ── Assemble response ──────────────────────────────────────────────────────
    response_data = {
        "application_id":    app_dict.get("application_id", "N/A"),
        "fraud_probability": fraud_probability,
        "recommendation":    recommendation,
        "similar_cases":     similar_cases[:3],   # top-3 for display
        "model_available":   model is not None,
    }

    state["response"] = json.dumps(response_data, indent=2)
    state["sources"]  = [c["application_id"] for c in similar_cases]

    return state
