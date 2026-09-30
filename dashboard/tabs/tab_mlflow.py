"""
dashboard/tabs/tab_mlflow.py
Tab 5 — MLflow Model Status
Calls the MLflow tracking REST API to list experiment runs.
Shows registered model stage badge for the fraud-detector model.
Falls back gracefully if MLflow is not yet reachable.
"""

import os
import streamlit as st
import requests
import pandas as pd
from datetime import datetime


# ── MLflow REST API helpers ───────────────────────────────────────────────────

MLFLOW_URI   = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5001")
MODEL_NAME   = "fraud-detector"
EXPERIMENT   = "fraud-detection"


def _mlflow_healthy():
    try:
        r = requests.get(f"{MLFLOW_URI}/api/2.0/mlflow/experiments/list", timeout=3)
        return r.status_code == 200
    except Exception:
        return False


def _get_experiments():
    try:
        r = requests.get(f"{MLFLOW_URI}/api/2.0/mlflow/experiments/list", timeout=4)
        if r.status_code == 200:
            return r.json().get("experiments", [])
    except Exception:
        pass
    return []


def _get_runs(experiment_id: str, max_results: int = 20):
    try:
        payload = {"experiment_ids": [experiment_id], "max_results": max_results}
        r = requests.post(f"{MLFLOW_URI}/api/2.0/mlflow/runs/search", json=payload, timeout=5)
        if r.status_code == 200:
            return r.json().get("runs", [])
    except Exception:
        pass
    return []


def _get_registered_model(name: str):
    try:
        r = requests.get(f"{MLFLOW_URI}/api/2.0/mlflow/registered-models/get",
                         params={"name": name}, timeout=4)
        if r.status_code == 200:
            return r.json().get("registered_model")
    except Exception:
        pass
    return None


def _get_latest_version(name: str):
    try:
        r = requests.get(f"{MLFLOW_URI}/api/2.0/mlflow/registered-models/get-latest-versions",
                         params={"name": name}, timeout=4)
        if r.status_code == 200:
            versions = r.json().get("model_versions", [])
            return versions[0] if versions else None
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
            "start_time":   datetime.fromtimestamp(info.get("start_time", 0) / 1000).strftime("%Y-%m-%d %H:%M") if info.get("start_time") else "—",
            "accuracy":     metrics.get("accuracy", "—"),
            "f1_score":     metrics.get("f1_score", "—"),
            "precision":    metrics.get("precision", "—"),
            "recall":       metrics.get("recall",  "—"),
            "n_estimators": params.get("n_estimators", "—"),
            "max_depth":    params.get("max_depth", "—"),
        })
    return pd.DataFrame(rows)


def _mock_runs_df():
    return pd.DataFrame([
        {"run_id": "a1b2c3d4…", "status": "FINISHED", "start_time": "2026-07-08 14:30",
         "accuracy": 0.9312, "f1_score": 0.8974, "precision": 0.9102, "recall": 0.8851,
         "n_estimators": "200", "max_depth": "15"},
        {"run_id": "e5f6a7b8…", "status": "FINISHED", "start_time": "2026-07-08 14:10",
         "accuracy": 0.9108, "f1_score": 0.8762, "precision": 0.8891, "recall": 0.8638,
         "n_estimators": "100", "max_depth": "10"},
        {"run_id": "c9d0e1f2…", "status": "FINISHED", "start_time": "2026-07-08 13:55",
         "accuracy": 0.8843, "f1_score": 0.8521, "precision": 0.8643, "recall": 0.8404,
         "n_estimators": "50",  "max_depth": "8"},
    ])


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
        mlflow_port = MLFLOW_URI.split(":")[-1] if ":" in MLFLOW_URI else "5001"
        st.markdown(f"MLflow UI: [localhost:{mlflow_port}](http://localhost:{mlflow_port})")

    st.divider()

    # ── Registered model card ─────────────────────────────────────────────────
    st.markdown("#### Registered Model — `fraud-detector`")

    if mlflow_up:
        model    = _get_registered_model(MODEL_NAME)
        latest   = _get_latest_version(MODEL_NAME)
        stage    = latest.get("current_stage", "None") if latest else "Not registered"
        version  = latest.get("version", "—")         if latest else "—"
        run_id   = (latest.get("run_id", "—")[:8] + "…") if latest else "—"
        is_mock  = False
    else:
        stage, version, run_id = "Staging", "1", "a1b2c3d4…"
        is_mock = True

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
            <div class="kpi-value">v{version}</div>
            <div class="kpi-label">Latest Version</div>
        </div>""", unsafe_allow_html=True)
    with c3:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-value" style="font-size:1.1rem;">{run_id}</div>
            <div class="kpi-label">Source Run ID</div>
        </div>""", unsafe_allow_html=True)

    if is_mock:
        st.info("⚠️ Demo data — MLflow not reachable or model not yet trained (Sub-Task 6). "
                "Run `make seed` after `make submit-sql` to train and register the model.", icon="ℹ️")

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Experiment runs table ─────────────────────────────────────────────────
    st.markdown(f"#### Experiment Runs — `{EXPERIMENT}`")

    if mlflow_up:
        experiments = _get_experiments()
        exp_id = next(
            (e["experiment_id"] for e in experiments if e.get("name") == EXPERIMENT),
            None
        )
        if exp_id:
            runs    = _get_runs(exp_id)
            runs_df = _flatten_runs(runs) if runs else _mock_runs_df()
        else:
            runs_df = _mock_runs_df()
            st.caption(f"Experiment `{EXPERIMENT}` not yet created — run `python mlflow/train_fraud_model.py`")
    else:
        runs_df = _mock_runs_df()

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

    # ── Best run metrics bar chart ────────────────────────────────────────────
    st.markdown("#### Best Run — Metric Comparison")

    if not runs_df.empty:
        best = runs_df.iloc[0]
        metrics = {}
        for col in ["accuracy", "f1_score", "precision", "recall"]:
            val = best.get(col)
            try:
                metrics[col] = float(val)
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

    if st.button("🔄 Refresh MLflow"):
        st.rerun()
