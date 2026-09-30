"""
dashboard/app.py
Main Streamlit application entry point.
5 tabs: Tax KPIs | Fraud Intelligence | AI Agent Chat | Data Lineage | MLflow Status
"""

import streamlit as st

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

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown('<div class="main-header">🏛️ Real-Time Tax Analytics Platform</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Open-source streaming · Kafka · Flink SQL · DuckDB · LangGraph · MLflow</div>', unsafe_allow_html=True)

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
