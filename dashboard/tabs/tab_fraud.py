"""
dashboard/tabs/tab_fraud.py
Tab 2 — Fraud Intelligence
Reads fraud signals from PostgreSQL and aggregated summaries from DuckDB.
Falls back gracefully if services are not yet available.
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import random


# ── Connection helpers ────────────────────────────────────────────────────────

def _get_postgres_fraud():
    """Fetch recent fraud detections from PostgreSQL. Returns DataFrame or None."""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            dbname=os.getenv("POSTGRES_DB", "taxdb"),
            user=os.getenv("POSTGRES_USER", "taxuser"),
            password=os.getenv("POSTGRES_PASSWORD", "taxpass"),
            connect_timeout=3,
        )
        df = pd.read_sql("""
            SELECT application_id, customer_id, signal_type, severity,
                   detected_at, description
            FROM fraud_detections
            ORDER BY detected_at DESC
            LIMIT 100
        """, conn)
        conn.close()
        return df if not df.empty else None
    except Exception:
        return None


def _get_duckdb_fraud_summary():
    """Aggregate fraud signals from MinIO via DuckDB. Returns DataFrame or None."""
    try:
        import duckdb
        con = duckdb.connect()
        endpoint   = os.getenv("MINIO_ENDPOINT", "minio:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        bucket     = os.getenv("MINIO_BUCKET_CLEANED", "cleaned-tax")

        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"SET s3_endpoint='{endpoint}';")
        con.execute(f"SET s3_access_key_id='{access_key}';")
        con.execute(f"SET s3_secret_access_key='{secret_key}';")
        con.execute("SET s3_use_ssl=false;")
        con.execute("SET s3_url_style='path';")

        df = con.execute(f"""
            SELECT
                province,
                COUNT(*) AS application_count,
                SUM(CASE WHEN is_fraud = true THEN 1 ELSE 0 END) AS fraud_count,
                ROUND(AVG(CAST(taxable_income AS DOUBLE)), 0) AS avg_income
            FROM read_json_auto('s3://{bucket}/**/*.json', ignore_errors=true)
            GROUP BY province
            ORDER BY fraud_count DESC
        """).df()
        con.close()
        return df if not df.empty else None
    except Exception:
        return None


def _mock_fraud_detections():
    signal_types = ["DUPLICATE_APPLICATION", "HIGH_INCOME_ANOMALY", "VELOCITY_BREACH", "PROFILE_MISMATCH"]
    severities   = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    provinces    = ["Gauteng", "Western Cape", "KZN", "Eastern Cape", "Limpopo"]
    rows = []
    base = datetime.now()
    for i in range(25):
        rows.append({
            "application_id": f"APP-{random.randint(10000,99999)}",
            "customer_id":    f"C-{random.randint(1000,9999)}",
            "signal_type":    random.choice(signal_types),
            "severity":       random.choice(severities),
            "detected_at":    (base - timedelta(minutes=random.randint(0, 120))).strftime("%Y-%m-%d %H:%M"),
            "province":       random.choice(provinces),
        })
    return pd.DataFrame(rows)


def _mock_province_data():
    provinces = ["Gauteng", "Western Cape", "KZN", "Eastern Cape", "Limpopo",
                 "Mpumalanga", "North West", "Free State", "Northern Cape"]
    return pd.DataFrame({
        "province":          provinces,
        "application_count": [random.randint(50, 300) for _ in provinces],
        "fraud_count":       [random.randint(2, 30) for _ in provinces],
        "avg_income":        [round(random.uniform(30000, 90000)) for _ in provinces],
    })


# ── Main render ───────────────────────────────────────────────────────────────

def render():
    st.subheader("🚨 Fraud Intelligence Dashboard")

    pg_data    = _get_postgres_fraud()
    duck_data  = _get_duckdb_fraud_summary()
    using_mock = pg_data is None and duck_data is None

    # Status bar
    col_s1, col_s2 = st.columns(2)
    with col_s1:
        if pg_data is not None:
            st.markdown('<span class="status-ok">● PostgreSQL connected</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="status-warn">● PostgreSQL — awaiting fraud detections table</span>', unsafe_allow_html=True)
    with col_s2:
        if duck_data is not None:
            st.markdown('<span class="status-ok">● DuckDB / MinIO connected</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="status-warn">● DuckDB — awaiting cleaned-tax data</span>', unsafe_allow_html=True)

    if using_mock:
        st.info("⚠️ No live data yet — showing demo data. Start producers and Flink jobs to populate.", icon="ℹ️")

    st.divider()

    # ── Summary KPI cards ─────────────────────────────────────────────────────
    fraud_df = pg_data if pg_data is not None else _mock_fraud_detections()
    prov_df  = duck_data if duck_data is not None else _mock_province_data()

    total_signals = len(fraud_df)
    critical      = len(fraud_df[fraud_df.get("severity", pd.Series(dtype=str)) == "CRITICAL"]) if "severity" in fraud_df.columns else "—"
    high          = len(fraud_df[fraud_df.get("severity", pd.Series(dtype=str)) == "HIGH"])      if "severity" in fraud_df.columns else "—"
    top_province  = prov_df.iloc[0]["province"] if not prov_df.empty else "—"

    c1, c2, c3, c4 = st.columns(4)
    cards = [
        (total_signals, "Total Fraud Signals"),
        (critical,      "Critical Severity"),
        (high,          "High Severity"),
        (top_province,  "Highest-Risk Province"),
    ]
    for col, (val, label) in zip([c1, c2, c3, c4], cards):
        col.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-value">{val}</div>
            <div class="kpi-label">{label}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Signal type bar chart + province heatmap ──────────────────────────────
    left_col, right_col = st.columns(2)

    with left_col:
        st.markdown("#### Signals by Type")
        if "signal_type" in fraud_df.columns:
            counts = fraud_df["signal_type"].value_counts().reset_index()
            counts.columns = ["signal_type", "count"]
        else:
            counts = pd.DataFrame({
                "signal_type": ["DUPLICATE_APPLICATION", "HIGH_INCOME_ANOMALY", "VELOCITY_BREACH", "PROFILE_MISMATCH"],
                "count":       [12, 8, 5, 3],
            })
        fig = px.bar(
            counts, x="count", y="signal_type", orientation="h",
            color="count", color_continuous_scale="Blues",
            labels={"count": "Count", "signal_type": "Signal Type"},
        )
        fig.update_layout(height=280, margin=dict(l=0, r=0, t=10, b=0),
                          plot_bgcolor="#f8fafc", paper_bgcolor="#f8fafc",
                          coloraxis_showscale=False, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with right_col:
        st.markdown("#### Fraud Count by Province")
        fig2 = px.bar(
            prov_df.sort_values("fraud_count", ascending=True).tail(9),
            x="fraud_count", y="province", orientation="h",
            color="fraud_count", color_continuous_scale="Reds",
            labels={"fraud_count": "Fraud Signals", "province": "Province"},
        )
        fig2.update_layout(height=280, margin=dict(l=0, r=0, t=10, b=0),
                           plot_bgcolor="#f8fafc", paper_bgcolor="#f8fafc",
                           coloraxis_showscale=False, showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)

    # ── Top flagged customers table ───────────────────────────────────────────
    st.markdown("#### Top Flagged Customers")

    if "customer_id" in fraud_df.columns:
        top_customers = (
            fraud_df.groupby("customer_id")
            .agg(signal_count=("signal_type", "count"),
                 latest_signal=("detected_at", "max"))
            .reset_index()
            .sort_values("signal_count", ascending=False)
            .head(10)
        )
    else:
        top_customers = pd.DataFrame({
            "customer_id":   [f"C-{random.randint(1000,9999)}" for _ in range(10)],
            "signal_count":  sorted([random.randint(1, 8) for _ in range(10)], reverse=True),
            "latest_signal": [(datetime.now() - timedelta(minutes=random.randint(0, 60))).strftime("%H:%M") for _ in range(10)],
        })

    st.dataframe(
        top_customers,
        use_container_width=True,
        hide_index=True,
        column_config={
            "customer_id":   st.column_config.TextColumn("Customer ID"),
            "signal_count":  st.column_config.NumberColumn("# Signals", format="%d"),
            "latest_signal": st.column_config.TextColumn("Latest Signal At"),
        }
    )

    if st.button("🔄 Refresh Fraud Data"):
        st.rerun()
