"""
dashboard/tabs/tab_mlflow.py
Tab 5 — MLflow Model Status
Calls the MLflow tracking REST API to list experiment runs and the registered
model. Real data only: when nothing is trained yet it says so. Training is
started from here (sample data for testing, or streamed data from MinIO) via
the model-api service.
"""

import os
import time
import streamlit as st
import requests
import pandas as pd
from datetime import datetime


# ── MLflow REST API helpers ───────────────────────────────────────────────────

# In-network URL (the container listens on 5000; 5001 is only the host mapping)
MLFLOW_URI   = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_UI_URL = "http://localhost:5001"
MODEL_NAME   = "fraud-detector"
EXPERIMENT   = "tax-fraud-detection"   # matches mlflow_config.py EXPERIMENT_NAME


def _mlflow_healthy():
    try:
        return requests.get(f"{MLFLOW_URI}/health", timeout=3).status_code == 200
    except Exception:
        return False


def _get_experiments():
    try:
        r = requests.get(f"{MLFLOW_URI}/api/2.0/mlflow/experiments/search",
                         params={"max_results": 100}, timeout=4)
        if r.status_code == 200:
            return r.json().get("experiments", [])
    except Exception:
        pass
    return []


def _get_runs(experiment_id: str, max_results: int = 20):
    try:
        payload = {"experiment_ids": [experiment_id], "max_results": max_results,
                   "order_by": ["attributes.start_time DESC"]}
        r = requests.post(f"{MLFLOW_URI}/api/2.0/mlflow/runs/search", json=payload, timeout=5)
        if r.status_code == 200:
            return r.json().get("runs", [])
    except Exception:
        pass
    return []


def _get_latest_version(name: str):
    """Return the highest-numbered model version, or None if not registered."""
    try:
        r = requests.get(f"{MLFLOW_URI}/api/2.0/mlflow/model-versions/search",
                         params={"filter": f"name='{name}'", "max_results": 50}, timeout=4)
        if r.status_code == 200:
            versions = r.json().get("model_versions", [])
            return max(versions, key=lambda v: int(v["version"])) if versions else None
    except Exception:
        pass
    return None


def _flatten_runs(runs: list) -> pd.DataFrame:
    rows = []
    for run in runs:
        info    = run.get("info", {})
        metrics = {m["key"]: round(m["value"], 4) for m in run.get("data", {}).get("metrics", [])}
        params  = {p["key"]: p["value"]           for p in run.get("data", {}).get("params",  [])}
        rows.append({
            "run_id":       info.get("run_id", "")[:8] + "…",
            "status":       info.get("status", ""),
            "start_time":   datetime.fromtimestamp(int(info["start_time"]) / 1000).strftime("%Y-%m-%d %H:%M") if info.get("start_time") else "—",
            "accuracy":     metrics.get("accuracy", "—"),
            "f1_score":     metrics.get("f1_score", "—"),
            "precision":    metrics.get("precision", "—"),
            "recall":       metrics.get("recall",  "—"),
            "n_estimators": params.get("n_estimators", "—"),
            "max_depth":    params.get("max_depth", "—"),
        })
    return pd.DataFrame(rows)


# ── Training service (model-api: POST /train, GET /train/status) ──────────────

MODEL_API_URL = os.getenv("MODEL_API_URL", "http://model-api:8000")


def _train_status():
    """Return the trainer status dict, or None if model-api is unreachable."""
    try:
        r = requests.get(f"{MODEL_API_URL}/train/status", timeout=3)
        return r.json() if r.status_code == 200 else None
    except Exception:
        return None


def _start_training(source: str, n_rows: int) -> tuple[bool, str]:
    try:
        r = requests.post(f"{MODEL_API_URL}/train",
                          json={"source": source, "n_rows": n_rows}, timeout=5)
        return r.status_code == 202, r.json().get("message", r.text[:120])
    except Exception as exc:
        return False, f"model-api unreachable ({type(exc).__name__})"



# ── Stage badge helper ────────────────────────────────────────────────────────

STAGE_COLOURS = {
    "Production": "#22c55e",
    "Staging":    "#f59e0b",
    "Archived":   "#94a3b8",
    "None":       "#64748b",
}

def _stage_badge(stage: str) -> str:
    colour = STAGE_COLOURS.get(stage, "#64748b")
    return f'<span style="background:{colour};color:white;padding:3px 10px;border-radius:12px;font-size:0.85rem;font-weight:600;">{stage}</span>'


# ── Main render ───────────────────────────────────────────────────────────────

def _render_training_panel(has_model: bool):
    """Train buttons (sample data / streamed data) with live status and logs."""
    st.markdown("#### Model training")
    status = _train_status()

    if status is None:
        st.warning("The training service (`model-api`) is not reachable. Start it with "
                   "`podman compose up -d model-api`, then press Refresh.", icon="⚠️")
        return

    running = status["state"] == "running"
    c1, c2, c3 = st.columns([1, 1, 2])
    with c1:
        if st.button("🧪 Train on sample data", disabled=running, key="train_sample",
                     help="Generated records, no streaming needed. Use to test the pipeline."):
            ok, msg = _start_training("sample", 2000)
            (st.success if ok else st.error)(msg)
            time.sleep(1)
            st.rerun()
    with c2:
        if st.button("📡 Train on streamed data", disabled=running, key="train_streamed",
                     type="primary",
                     help="Cleaned events from MinIO written by the Flink pipeline."):
            ok, msg = _start_training("streamed", 20000)
            (st.success if ok else st.error)(msg)
            time.sleep(1)
            st.rerun()
    with c3:
        icon = {"running": "⏳", "succeeded": "✅", "failed": "❌", "idle": "💤"}[status["state"]]
        st.markdown(f"**Trainer:** {icon} {status['state']}")
        if running:
            st.caption("Training in progress — press Refresh to update.")
        elif not has_model:
            st.caption("No model yet. Train on sample data to test, then on streamed data.")

    if status["log"]:
        with st.expander("Trainer logs", expanded=running or status["state"] == "failed"):
            st.code("\n".join(status["log"][-40:]), language="text")



def render():
    st.subheader("🧪 MLflow Model Status")
    st.markdown(
        f"Experiment tracking and model registry for the **`{MODEL_NAME}`** fraud detection classifier. "
        "Trained on cleaned Flink SQL output data from MinIO."
    )

    mlflow_up = _mlflow_healthy()

    col_s1, col_s2 = st.columns(2)
    with col_s1:
        if mlflow_up:
            st.markdown('<span class="status-ok">● MLflow connected</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="status-warn">● MLflow — not reachable</span>', unsafe_allow_html=True)
    with col_s2:
        st.markdown(f"MLflow UI: [localhost:5001]({MLFLOW_UI_URL})")

    if not mlflow_up:
        st.error("MLflow is not reachable. Start it from the Control Center tab "
                 "(or `podman start mlflow`), then press Refresh.", icon="🔌")
        if st.button("🔄 Refresh MLflow"):
            st.rerun()
        return

    st.divider()

    # ── Registered model card ─────────────────────────────────────────────────
    st.markdown("#### Registered Model — `fraud-detector`")

    latest  = _get_latest_version(MODEL_NAME)
    stage   = latest.get("current_stage", "None") if latest else "Not trained"
    version = latest.get("version", "—")          if latest else "—"
    run_id  = (latest.get("run_id", "—")[:8] + "…") if latest else "—"

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-value">{_stage_badge(stage)}</div>
            <div class="kpi-label">Current Stage</div>
        </div>""", unsafe_allow_html=True)
    with c2:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-value">{'v' + str(version) if latest else '—'}</div>
            <div class="kpi-label">Latest Version</div>
        </div>""", unsafe_allow_html=True)
    with c3:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-value" style="font-size:1.1rem;">{run_id}</div>
            <div class="kpi-label">Source Run ID</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    _render_training_panel(has_model=latest is not None)
    st.markdown("<br>", unsafe_allow_html=True)

    # ── Experiment runs table ─────────────────────────────────────────────────
    st.markdown(f"#### Experiment Runs — `{EXPERIMENT}`")

    experiments = _get_experiments()
    exp_id = next((e["experiment_id"] for e in experiments if e.get("name") == EXPERIMENT), None)
    runs = _get_runs(exp_id) if exp_id else []

    if not runs:
        st.info("No training runs yet. Press **Train model** above once the producers and Flink "
                "jobs have written cleaned data.", icon="ℹ️")
        if st.button("🔄 Refresh MLflow"):
            st.rerun()
        return

    runs_df = _flatten_runs(runs)
    st.dataframe(
        runs_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "run_id":       st.column_config.TextColumn("Run ID"),
            "status":       st.column_config.TextColumn("Status"),
            "start_time":   st.column_config.TextColumn("Started At"),
            "accuracy":     st.column_config.NumberColumn("Accuracy",  format="%.4f"),
            "f1_score":     st.column_config.NumberColumn("F1 Score",  format="%.4f"),
            "precision":    st.column_config.NumberColumn("Precision", format="%.4f"),
            "recall":       st.column_config.NumberColumn("Recall",    format="%.4f"),
            "n_estimators": st.column_config.TextColumn("n_estimators"),
            "max_depth":    st.column_config.TextColumn("max_depth"),
        }
    )

    # ── Latest run metrics bar chart ──────────────────────────────────────────
    st.markdown("#### Latest Run — Metric Comparison")

    best = runs_df.iloc[0]
    metrics = {}
    for col in ["accuracy", "f1_score", "precision", "recall"]:
        try:
            metrics[col] = float(best.get(col))
        except (TypeError, ValueError):
            pass

    if metrics:
        import plotly.graph_objects as go
        fig = go.Figure(go.Bar(
            x=list(metrics.keys()),
            y=list(metrics.values()),
            marker_color=["#2563eb", "#7c3aed", "#059669", "#d97706"],
            text=[f"{v:.3f}" for v in metrics.values()],
            textposition="outside",
        ))
        fig.update_layout(
            height=280,
            yaxis=dict(range=[0, 1.05]),
            margin=dict(l=0, r=0, t=10, b=0),
            plot_bgcolor="#f8fafc",
            paper_bgcolor="#f8fafc",
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

    _render_live_scoring()

    if st.button("🔄 Refresh MLflow"):
        st.rerun()


def _render_live_scoring():
    """Score the newest streamed events with the served model."""
    st.markdown("#### Live scoring — newest streamed events")
    st.caption("Latest cleaned events from the Flink pipeline, scored by the registered model. "
               "`is_fraud` is the label carried by the event, shown for comparison.")

    n = st.slider("Events to score", 10, 200, 50, step=10, key="live_n")
    threshold = st.slider("Flag as fraud at probability ≥", 0.1, 0.9, 0.5, step=0.05, key="live_thr")

    try:
        import sys
        if "/" not in sys.path:
            sys.path.insert(0, "/")
        from storage.duckdb_queries import get_recent_events
        events = get_recent_events(n_rows=n)
    except Exception as exc:
        st.error(f"Could not read streamed events: {exc}")
        return
    if events.empty:
        st.info("No streamed events yet. Start the producers and Flink jobs from the Control Center.",
                icon="ℹ️")
        return

    cols = ["province", "taxable_income", "employment_type", "employment_status", "filing_status"]
    try:
        r = requests.post(f"{MODEL_API_URL}/predict",
                          json={"records": events[cols].to_dict(orient="records")}, timeout=15)
    except Exception as exc:
        st.error(f"model-api unreachable: {type(exc).__name__}")
        return
    if r.status_code != 200:
        st.warning(r.json().get("error", r.text[:120]) + " — train a model above first.", icon="⚠️")
        return

    scored = events.copy()
    scored["fraud_probability"] = r.json()["fraud_probability"]
    scored["predicted_fraud"] = scored["fraud_probability"] >= threshold
    scored["is_fraud"] = scored["is_fraud"].astype(bool)

    tp = int((scored.predicted_fraud & scored.is_fraud).sum())
    fp = int((scored.predicted_fraud & ~scored.is_fraud).sum())
    fn = int((~scored.predicted_fraud & scored.is_fraud).sum())
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Scored", len(scored))
    m2.metric("Flagged", int(scored.predicted_fraud.sum()))
    m3.metric("Caught (true +)", tp)
    m4.metric("Missed / false alarms", f"{fn} / {fp}")
    st.caption(f"Model v{r.json()['version']} ({r.json()['stage']})")

    show = scored[["flink_processed_at", "application_id", "province", "taxable_income",
                   "employment_type", "fraud_probability", "predicted_fraud", "is_fraud"]]
    st.dataframe(
        show.sort_values("fraud_probability", ascending=False),
        use_container_width=True, hide_index=True,
        column_config={
            "fraud_probability": st.column_config.ProgressColumn(
                "Fraud probability", min_value=0.0, max_value=1.0, format="%.2f"),
            "taxable_income": st.column_config.NumberColumn("Taxable income", format="R %.0f"),
        },
    )
