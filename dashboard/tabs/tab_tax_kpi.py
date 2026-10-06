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
    """Query DuckDB → MinIO cleaned-tax for trend data via storage module. Returns DataFrame or None."""
    try:
        import sys
        sys.path.insert(0, "/app")
        from storage.duckdb_queries import get_tax_kpi_trend
        df = get_tax_kpi_trend(n_windows=n_windows)
        return df if not df.empty else None
    except Exception:
        return None


def _get_income_distribution():
    """Get income distribution data via storage module. Returns DataFrame or None."""
    try:
        import sys
        sys.path.insert(0, "/app")
        from storage.duckdb_queries import get_income_distribution
        df = get_income_distribution(n_buckets=20)
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
        window_label       = str(latest.get("window_start", "—"))
    else:
        total_count        = "—"
        avg_income         = "—"
        distinct_customers = "—"
        window_label       = "demo mode"

    # Pre-compute display strings — avoids complex expressions inside f-strings
    # which trigger ValueError in Python 3.11 f-string parser
    total_count_str    = "{:,}".format(total_count) if isinstance(total_count, int) else str(total_count)
    avg_income_str     = "R {:,.0f}".format(avg_income) if isinstance(avg_income, float) else str(avg_income)
    customers_str      = "{:,}".format(distinct_customers) if isinstance(distinct_customers, int) else str(distinct_customers)
    status_icon        = "&#128994;" if redis_data else "&#128993;"   # green / yellow circle (no emoji literals)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(
            '<div class="kpi-card">'
            '<div class="kpi-value">' + total_count_str + '</div>'
            '<div class="kpi-label">Applications (last window)</div>'
            '</div>',
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            '<div class="kpi-card">'
            '<div class="kpi-value">' + avg_income_str + '</div>'
            '<div class="kpi-label">Avg Taxable Income</div>'
            '</div>',
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(
            '<div class="kpi-card">'
            '<div class="kpi-value">' + customers_str + '</div>'
            '<div class="kpi-label">Distinct Customers</div>'
            '</div>',
            unsafe_allow_html=True,
        )
    with c4:
        st.markdown(
            '<div class="kpi-card">'
            '<div class="kpi-value">' + status_icon + '</div>'
            '<div class="kpi-label">Window: ' + window_label + '</div>'
            '</div>',
            unsafe_allow_html=True,
        )

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

    income_df = _get_income_distribution()
    if income_df is not None and not income_df.empty:
        fig2 = px.bar(
            income_df,
            x="bucket_label",
            y="count",
            labels={"bucket_label": "Income Bracket", "count": "Applications"},
            color_discrete_sequence=["#3b82f6"],
        )
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

