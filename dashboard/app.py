"""
dashboard/app.py
Main Streamlit application entry point.
5 tabs: Tax KPIs | Fraud Intelligence | AI Agent Chat | Data Lineage | MLflow Status
"""

import sys
import os

# ── sys.path setup ────────────────────────────────────────────────────────────
# In the container: dashboard → /app, modules mounted at /storage, /agents, /lineage
# Locally:         dashboard is a subdirectory; project root is one level up.
# We add both so imports work in either environment.

_here         = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.abspath(os.path.join(_here, ".."))

for _p in [_project_root, "/", "/app", "/storage/..", "/agents/.."]:
    try:
        _resolved = os.path.abspath(_p)
        if os.path.isdir(_resolved) and _resolved not in sys.path:
            sys.path.insert(0, _resolved)
    except Exception:
        pass

import streamlit as st
from streamlit_autorefresh import st_autorefresh  # noqa: F401 — triggers 30s refresh

st.set_page_config(
    page_title="Tax Analytics Platform",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .main-header {
        font-size: 2rem;
        font-weight: 700;
        color: #1a3a5c;
        margin-bottom: 0.2rem;
    }
    .sub-header {
        font-size: 1rem;
        color: #5a7a9a;
        margin-bottom: 1.5rem;
    }
    .status-ok   { color: #22c55e; font-weight: 600; }
    .status-warn { color: #f59e0b; font-weight: 600; }
    .status-err  { color: #ef4444; font-weight: 600; }
    .kpi-card {
        background: #f0f6ff;
        border-radius: 10px;
        padding: 1rem 1.5rem;
        border-left: 4px solid #2563eb;
    }
    .kpi-value { font-size: 2rem; font-weight: 700; color: #1e40af; }
    .kpi-label { font-size: 0.85rem; color: #64748b; margin-top: 0.2rem; }
    div[data-testid="stTabs"] button { font-size: 0.95rem; font-weight: 600; }
</style>
""", unsafe_allow_html=True)

# ── Auto-refresh every 30 seconds (fires for Tab 1 KPI live data) ─────────────
try:
    count = st_autorefresh(interval=30_000, key="kpi_autorefresh")
except Exception:
    pass  # streamlit-autorefresh not installed — graceful skip

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown('<div class="main-header">&#127963;&#65039; Real-Time Tax Analytics Platform</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Open-source streaming &middot; Kafka &middot; Flink SQL &middot; DuckDB &middot; LangGraph &middot; MLflow</div>', unsafe_allow_html=True)

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Tax KPIs",
    "🚨 Fraud Intelligence",
    "🤖 AI Agent Chat",
    "🔗 Data Lineage",
    "🧪 MLflow Status",
])

with tab1:
    from tabs.tab_tax_kpi import render
    render()

with tab2:
    from tabs.tab_fraud import render
    render()

with tab3:
    from tabs.tab_agent_chat import render
    render()

with tab4:
    from tabs.tab_lineage import render
    render()

with tab5:
    from tabs.tab_mlflow import render
    render()
