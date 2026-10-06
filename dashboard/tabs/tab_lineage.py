"""
dashboard/tabs/tab_lineage.py
Tab 4 — Data Lineage
Calls the Marquez API to list jobs and datasets, renders as a table.
Embeds the Marquez Web UI via an iframe.
Falls back gracefully if Marquez is not yet reachable.
"""

import os
import streamlit as st
import streamlit.components.v1 as components
import requests
import pandas as pd
from datetime import datetime


# ── Marquez API helpers ───────────────────────────────────────────────────────

MARQUEZ_API = os.getenv("MARQUEZ_API_URL", "http://marquez:5000")
NAMESPACES  = ["kafka", "minio", "postgres", "duckdb", "mlflow"]


def _marquez_healthy():
    # NOTE: Marquez v0.47 does not expose /api/v1/health (returns 404).
    # Use /api/v1/namespaces which always returns 200 when the API is up.
    try:
        r = requests.get(f"{MARQUEZ_API}/api/v1/namespaces", timeout=3)
        return r.status_code == 200
    except Exception:
        return False


def _get_jobs(namespace: str):
    """Fetch jobs for a given namespace from Marquez."""
    try:
        r = requests.get(f"{MARQUEZ_API}/api/v1/namespaces/{namespace}/jobs", timeout=4)
        if r.status_code == 200:
            return r.json().get("jobs", [])
    except Exception:
        pass
    return []


def _get_datasets(namespace: str):
    """Fetch datasets for a given namespace from Marquez."""
    try:
        r = requests.get(f"{MARQUEZ_API}/api/v1/namespaces/{namespace}/datasets", timeout=4)
        if r.status_code == 200:
            return r.json().get("datasets", [])
    except Exception:
        pass
    return []


def _build_lineage_df():
    """Pull all jobs across all namespaces and flatten to a DataFrame."""
    rows = []
    for ns in NAMESPACES:
        for job in _get_jobs(ns):
            rows.append({
                "namespace":  ns,
                "job_name":   job.get("name", ""),
                "inputs":     ", ".join(d.get("name", "") for d in job.get("inputs", [])),
                "outputs":    ", ".join(d.get("name", "") for d in job.get("outputs", [])),
                "updated_at": job.get("updatedAt", ""),
            })
    return pd.DataFrame(rows) if rows else None


def _build_datasets_df():
    """Pull all datasets across all namespaces."""
    rows = []
    for ns in NAMESPACES:
        for ds in _get_datasets(ns):
            rows.append({
                "namespace":  ns,
                "dataset":    ds.get("name", ""),
                "type":       ds.get("type", ""),
                "updated_at": ds.get("updatedAt", ""),
            })
    return pd.DataFrame(rows) if rows else None


def _mock_lineage_df():
    return pd.DataFrame([
        {"namespace": "kafka",    "job_name": "producer_tax_applications",  "inputs": "—",                   "outputs": "kafka://tax-applications",       "updated_at": "—"},
        {"namespace": "flink",    "job_name": "job_01_raw_to_minio",        "inputs": "kafka://tax-applications", "outputs": "minio://raw-tax/",          "updated_at": "—"},
        {"namespace": "flink",    "job_name": "job_02_enrich",              "inputs": "kafka://tax-applications, kafka://taxpayer-profiles", "outputs": "postgres://enriched_tax_applications", "updated_at": "—"},
        {"namespace": "flink",    "job_name": "job_03_kpi_windows",         "inputs": "kafka://tax-applications", "outputs": "postgres://tax_kpi_windows", "updated_at": "—"},
        {"namespace": "flink",    "job_name": "job_04_fraud_pattern",       "inputs": "kafka://tax-applications", "outputs": "kafka://fraud-signals",      "updated_at": "—"},
        {"namespace": "flink",    "job_name": "job_05_cleaned",             "inputs": "kafka://tax-applications", "outputs": "minio://cleaned-tax/",       "updated_at": "—"},
        {"namespace": "duckdb",   "job_name": "get_tax_kpi_trend",          "inputs": "minio://cleaned-tax/",     "outputs": "streamlit://tax-kpi-tab",    "updated_at": "—"},
        {"namespace": "duckdb",   "job_name": "get_cleaned_data_for_ml",    "inputs": "minio://cleaned-tax/",     "outputs": "mlflow://fraud-detector",    "updated_at": "—"},
        {"namespace": "mlflow",   "job_name": "train_fraud_model",          "inputs": "minio://cleaned-tax/",     "outputs": "mlflow://fraud-detector",    "updated_at": "—"},
    ])


# ── Main render ───────────────────────────────────────────────────────────────

def render():
    st.subheader("🔗 Data Lineage — OpenLineage + Marquez")
    st.markdown(
        "Full end-to-end lineage: Kafka topics → Flink SQL jobs → "
        "PostgreSQL / MinIO → DuckDB → MLflow model registry."
    )

    marquez_up = _marquez_healthy()

    col_s1, col_s2 = st.columns(2)
    with col_s1:
        if marquez_up:
            st.markdown('<span class="status-ok">● Marquez API connected</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="status-warn">● Marquez API — not reachable</span>', unsafe_allow_html=True)
    with col_s2:
        marquez_web_port = os.getenv("MARQUEZ_WEB_PORT", "3000")
        st.markdown(f'Marquez UI: [localhost:{marquez_web_port}](http://localhost:{marquez_web_port})', unsafe_allow_html=False)

    st.divider()

    # ── Lineage jobs table ────────────────────────────────────────────────────
    st.markdown("#### Registered Jobs & Data Flows")

    if marquez_up:
        df = _build_lineage_df()
        is_mock = df is None
        if is_mock:
            df = _mock_lineage_df()
            st.info("Marquez is running but no lineage events have been emitted yet. "
                    "Run `make submit-sql` to start populating lineage. Showing expected lineage below.", icon="ℹ️")
    else:
        df = _mock_lineage_df()
        is_mock = True
        st.info("⚠️ Demo lineage — Marquez not reachable. This shows the expected lineage once all jobs are running.", icon="ℹ️")

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "namespace":  st.column_config.TextColumn("Namespace"),
            "job_name":   st.column_config.TextColumn("Job / Producer"),
            "inputs":     st.column_config.TextColumn("Inputs"),
            "outputs":    st.column_config.TextColumn("Outputs"),
            "updated_at": st.column_config.TextColumn("Last Updated"),
        }
    )

    # ── Datasets table ────────────────────────────────────────────────────────
    if marquez_up and not is_mock:
        st.markdown("#### Registered Datasets")
        ds_df = _build_datasets_df()
        if ds_df is not None:
            st.dataframe(ds_df, use_container_width=True, hide_index=True)

    # ── Embedded Marquez UI ───────────────────────────────────────────────────
    st.markdown("#### Marquez Lineage Graph (Live UI)")

    if marquez_up:
        marquez_web_url = f"http://localhost:{os.getenv('MARQUEZ_WEB_PORT', '3000')}"
        components.iframe(marquez_web_url, height=550, scrolling=True)
    else:
        st.markdown("""
        <div style="background:#f1f5f9;border-radius:10px;padding:2rem;text-align:center;color:#64748b;">
            <div style="font-size:3rem;">🔗</div>
            <div style="font-size:1.1rem;font-weight:600;margin-top:0.5rem;">Marquez UI will appear here</div>
            <div style="margin-top:0.5rem;">Start the stack with <code>make up</code> and run <code>make submit-sql</code></div>
            <div style="margin-top:0.3rem;">Then access directly at <a href="http://localhost:3000" target="_blank">localhost:3000</a></div>
        </div>
        """, unsafe_allow_html=True)

    if st.button("🔄 Refresh Lineage"):
        st.rerun()

