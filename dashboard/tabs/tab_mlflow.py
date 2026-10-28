"""
dashboard/tabs/tab_mlflow.py
Tab 5 — MLflow Model Status
Calls the MLflow tracking REST API to list experiment runs and the registered
model. Real data only: when nothing is trained yet it says so and offers a
Train button that starts the one-shot `ml-trainer` container.
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
TRAINER      = "ml-trainer"            # one-shot training container (docker-compose.yml)


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
        r = requests.post(f"{MLFLOW_URI}/api/2.0/mlflow/model-versions/search",
                          json={"filter": f"name='{name}'", "max_results": 50}, timeout=4)
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


# ── Trainer container helpers (Podman socket via the Control Center) ──────────

def _trainer_state() -> str:
    from tabs.tab_control import _container_state
    return _container_state(TRAINER)


def _start_trainer() -> tuple[bool, str]:
    from tabs.tab_control import _container_action
    return _container_action(TRAINER, "start")


def _trainer_logs(tail: int = 30) -> str:
    from tabs.tab_control import _container_logs
    return _container_logs(TRAINER, tail=tail)


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
    """Train / retrain button plus live trainer status and logs."""
    state = _trainer_state()
    st.markdown("#### Model training")

    if state == "missing":
        st.warning(
            f"The `{TRAINER}` container does not exist yet. Create it once with "
            f"`podman compose up -d {TRAINER}` (also done by `make up`).",
            icon="⚠️",
        )
        return

    running = state == "running"
    label = "🔁 Retrain model" if has_model else "▶️ Train model"
    c1, c2 = st.columns([1, 3])
    with c1:
        if st.button(label, disabled=running, key="mlflow_train", type="primary"):
            ok, msg = _start_trainer()
            (st.success if ok else st.error)(
                f"Training started — {msg}" if ok else f"Could not start trainer: {msg}"
            )
            time.sleep(1)
            st.rerun()
    with c2:
        if running:
            st.info("Training in progress (waits for MLflow and enough cleaned records in MinIO). "
                    "Press Refresh to see progress.", icon="⏳")
        else:
            st.caption(f"Trainer status: **{state}**. Uses real cleaned records from MinIO only — "
                       "no synthetic data.")

    with st.expander("Trainer logs", expanded=running or not has_model):
        st.code(_trainer_logs(), language="text")


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

    if st.button("🔄 Refresh MLflow"):
        st.rerun()
