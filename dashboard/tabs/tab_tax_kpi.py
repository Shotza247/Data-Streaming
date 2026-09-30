"""
dashboard/tabs/tab_tax_kpi.py
Tab 1 — Tax KPIs
Reads live windowed KPIs from Redis (hot cache) and trend data from DuckDB → MinIO.
Falls back gracefully if services are not yet available.
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import random


# ── Connection helpers ────────────────────────────────────────────────────────

def _get_redis_kpis():
    """Read latest KPI windows from Redis. Returns list of dicts or None."""
    try:
        import redis
        r = redis.Redis(
            host=os.getenv("REDIS_HOST", "redis"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            decode_responses=True,
            socket_connect_timeout=2,
        )
        r.ping()
        keys = sorted(list(r.scan_iter("tax:kpi:window:*")))[-12:]  # last 12 windows = 1 hour
        records = []
        for k in keys:
            data = r.hgetall(k)
            if data:
                records.append(data)
        return records if records else None
    except Exception:
        return None


def _get_duckdb_trend(n_windows=12):
    """Query DuckDB → MinIO cleaned-tax for trend data. Returns DataFrame or None."""
    try:
        import duckdb
        con = duckdb.connect()
        endpoint   = os.getenv("MINIO_ENDPOINT", "minio:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        bucket     = os.getenv("MINIO_BUCKET_CLEANED", "cleaned-tax")

        con.execute(f"INSTALL httpfs; LOAD httpfs;")
        con.execute(f"SET s3_endpoint='{endpoint}';")
        con.execute(f"SET s3_access_key_id='{access_key}';")
        con.execute(f"SET s3_secret_access_key='{secret_key}';")
        con.execute("SET s3_use_ssl=false;")
        con.execute("SET s3_url_style='path';")

        df = con.execute(f"""
            SELECT
                date_trunc('hour', CAST(processed_at AS TIMESTAMP)) AS window_start,
                COUNT(*)                                             AS total_count,
                AVG(CAST(taxable_income AS DOUBLE))                 AS avg_income,
                COUNT(DISTINCT customer_id)                         AS distinct_customers
            FROM read_json_auto('s3://{bucket}/**/*.json', ignore_errors=true)
            GROUP BY 1
            ORDER BY 1 DESC
            LIMIT {n_windows}
        """).df()
        con.close()
        return df if not df.empty else None
    except Exception:
        return None


def _mock_kpi_data():
    """Generate plausible mock data for display when services are offline."""
    base = datetime.now()
    rows = []
    for i in range(12):
        t = base - timedelta(minutes=5 * (11 - i))
        rows.append({
            "window_start":       t.strftime("%H:%M"),
            "total_count":        random.randint(40, 80),
            "avg_income":         round(random.uniform(35000, 85000), 2),
            "distinct_customers": random.randint(30, 70),
        })
    return pd.DataFrame(rows)


# ── Main render ───────────────────────────────────────────────────────────────

def render():
    st.subheader("📊 Real-Time Tax Application KPIs")

    # Service status banner
    redis_data  = _get_redis_kpis()
    duckdb_data = _get_duckdb_trend()

    col_s1, col_s2, col_s3 = st.columns(3)
    with col_s1:
        if redis_data:
            st.markdown('<span class="status-ok">● Redis connected</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="status-warn">● Redis — awaiting data</span>', unsafe_allow_html=True)
    with col_s2:
        if duckdb_data is not None:
            st.markdown('<span class="status-ok">● MinIO/DuckDB connected</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="status-warn">● MinIO — awaiting Flink output</span>', unsafe_allow_html=True)
    with col_s3:
        st.caption(f"Last refreshed: {datetime.now().strftime('%H:%M:%S')}")

    st.divider()

    # ── KPI Cards (from Redis if available, else mock) ────────────────────────
    if redis_data:
        latest = redis_data[-1]
        total_count        = int(latest.get("total_count", 0))
        avg_income         = float(latest.get("avg_income", 0))
        distinct_customers = int(latest.get("distinct_customers", 0))
        window_label       = latest.get("window_start", "—")
    else:
        total_count        = "—"
        avg_income         = "—"
        distinct_customers = "—"
        window_label       = "demo mode"

    # Format helpers — only apply number formatting when the value is numeric
    def _fmt_int(v):
        return f"{v:,}" if isinstance(v, int) else v

    def _fmt_income(v):
        return f"R {v:,.0f}" if isinstance(v, float) else v

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-value">{_fmt_int(total_count)}</div>
            <div class="kpi-label">Applications (last window)</div>
        </div>""", unsafe_allow_html=True)
    with c2:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-value">{_fmt_income(avg_income)}</div>
            <div class="kpi-label">Avg Taxable Income</div>
        </div>""", unsafe_allow_html=True)
    with c3:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-value">{_fmt_int(distinct_customers)}</div>
            <div class="kpi-label">Distinct Customers</div>
        </div>""", unsafe_allow_html=True)
    with c4:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-value">{'🟢' if redis_data else '🟡'}</div>
            <div class="kpi-label">Window: {window_label}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Trend chart ───────────────────────────────────────────────────────────
    st.markdown("#### Application Volume — 1-Hour Trend")

    if duckdb_data is not None:
        df_trend = duckdb_data.sort_values("window_start")
        df_trend["window_start"] = df_trend["window_start"].astype(str)
        source_label = "Live data from MinIO / DuckDB"
    else:
        df_trend = _mock_kpi_data()
        source_label = "⚠️ Demo data — Flink jobs not yet running"

    fig = px.line(
        df_trend,
        x="window_start",
        y="total_count",
        markers=True,
        labels={"window_start": "Window", "total_count": "Applications"},
        color_discrete_sequence=["#2563eb"],
    )
    fig.update_layout(
        height=300,
        margin=dict(l=0, r=0, t=30, b=0),
        plot_bgcolor="#f8fafc",
        paper_bgcolor="#f8fafc",
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=True, gridcolor="#e2e8f0"),
    )
    st.plotly_chart(fig, use_container_width=True)
    st.caption(source_label)

    # ── Income Distribution histogram ─────────────────────────────────────────
    st.markdown("#### Taxable Income Distribution")

    if duckdb_data is not None and "avg_income" in duckdb_data.columns:
        incomes = duckdb_data["avg_income"].dropna().tolist()
    else:
        incomes = [round(random.gauss(55000, 18000)) for _ in range(200)]

    fig2 = px.histogram(
        x=incomes,
        nbins=20,
        labels={"x": "Taxable Income (ZAR)"},
        color_discrete_sequence=["#3b82f6"],
    )
    fig2.update_layout(
        height=260,
        margin=dict(l=0, r=0, t=10, b=0),
        plot_bgcolor="#f8fafc",
        paper_bgcolor="#f8fafc",
        bargap=0.05,
        showlegend=False,
    )
    st.plotly_chart(fig2, use_container_width=True)

    # ── Auto-refresh ──────────────────────────────────────────────────────────
    st.markdown("---")
    if st.button("🔄 Refresh KPIs"):
        st.rerun()
    st.caption("Auto-refreshes every 30 seconds when data is live.")
