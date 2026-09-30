"""
dashboard/tabs/tab_agent_chat.py
Tab 3 — AI Agent Chat
Invokes the LangGraph multi-agent graph.
Falls back to a clear "not yet available" state until agents/langgraph_app.py exists.
"""

import streamlit as st
import os
import sys


# ── Agent availability check ──────────────────────────────────────────────────

def _agent_available():
    """Check whether the LangGraph app module is importable."""
    try:
        sys.path.insert(0, "/app")
        import importlib
        spec = importlib.util.find_spec("agents.langgraph_app")
        return spec is not None
    except Exception:
        return False


def _run_agent(user_input: str) -> dict:
    """Call the LangGraph agent. Returns {response, sources}."""
    try:
        sys.path.insert(0, "/app")
        from agents.langgraph_app import run
        result = run(user_input)
        return result
    except Exception as e:
        return {"response": f"Agent error: {e}", "sources": []}


# ── Canned demo responses (used when agent not yet available) ─────────────────

DEMO_RESPONSES = {
    "fraud": {
        "response": (
            "**[Demo Mode]** Application APP-47823 has been assessed. "
            "The fraud detection model returns a probability of **0.87** (HIGH RISK). "
            "Top 3 similar historical cases: APP-31204 (0.92 similarity), "
            "APP-28876 (0.89), APP-19043 (0.84). "
            "**Recommendation:** Flag for manual review and request supporting documentation."
        ),
        "sources": ["MLflow fraud-detector v1.0", "Qdrant: fraud_similarity collection"],
    },
    "tax": {
        "response": (
            "**[Demo Mode]** Based on the South African tax tables for the 2024/25 year, "
            "a taxpayer earning R 450,000 taxable income falls in the 31% marginal bracket. "
            "Estimated tax before rebates is approximately R 115,263. "
            "The primary rebate of R 17,235 reduces this to **R 98,028**."
        ),
        "sources": ["Qdrant: regulatory_kb — SARS Tax Tables 2024/25", "Qdrant: regulatory_kb — Income Tax Act s6"],
    },
    "default": {
        "response": (
            "**[Demo Mode]** I can help with two types of queries:\n\n"
            "1. **Fraud check** — paste an `application_id` (e.g. `APP-47823`) to get a fraud risk assessment\n"
            "2. **Tax question** — ask any tax-related question (e.g. 'What is the tax on R 450,000?')"
        ),
        "sources": [],
    },
}

def _demo_response(user_input: str) -> dict:
    lower = user_input.lower()
    if "app-" in lower or "application" in lower or "fraud" in lower:
        return DEMO_RESPONSES["fraud"]
    elif any(w in lower for w in ["tax", "income", "bracket", "rebate", "sars", "filing"]):
        return DEMO_RESPONSES["tax"]
    return DEMO_RESPONSES["default"]


# ── Main render ───────────────────────────────────────────────────────────────

def render():
    st.subheader("🤖 AI Agent Chat")
    st.markdown(
        "Ask a **tax question** or paste an **application_id** to run a fraud check. "
        "The Supervisor agent routes your query to the correct specialist node."
    )

    # Agent status banner
    agent_ready = _agent_available()
    if agent_ready:
        st.markdown('<span class="status-ok">● LangGraph agent ready</span>', unsafe_allow_html=True)
    else:
        st.markdown('<span class="status-warn">● LangGraph agent not yet built (Sub-Task 7) — running in demo mode</span>', unsafe_allow_html=True)
        st.info(
            "The AI agent layer (LangGraph + HuggingFace models) will be built in Sub-Task 7. "
            "In demo mode, canned responses illustrate what the agent will return.",
            icon="ℹ️"
        )

    st.divider()

    # ── Chat history stored in session state ──────────────────────────────────
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    # Render existing messages
    for msg in st.session_state.chat_history:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if msg.get("sources"):
                with st.expander("📎 Sources"):
                    for s in msg["sources"]:
                        st.markdown(f"- {s}")

    # ── Input ─────────────────────────────────────────────────────────────────
    examples = st.columns(3)
    with examples[0]:
        if st.button("💡 Fraud check example", use_container_width=True):
            st.session_state._prefill = "Check application APP-47823 for fraud"
    with examples[1]:
        if st.button("💡 Tax question example", use_container_width=True):
            st.session_state._prefill = "What is the income tax on R 450,000 for the 2024/25 tax year?"
    with examples[2]:
        if st.button("💡 What can you do?", use_container_width=True):
            st.session_state._prefill = "What can you help me with?"

    prefill = st.session_state.pop("_prefill", "")
    user_input = st.chat_input("Type your question or paste an application_id…", key="chat_input")

    # Handle button-prefilled input or typed input
    query = user_input or prefill
    if query:
        # Append user message
        st.session_state.chat_history.append({"role": "user", "content": query})
        with st.chat_message("user"):
            st.markdown(query)

        # Run agent or demo
        with st.chat_message("assistant"):
            with st.spinner("🔍 Agent thinking…"):
                if agent_ready:
                    result = _run_agent(query)
                else:
                    result = _demo_response(query)

            st.markdown(result["response"])
            if result.get("sources"):
                with st.expander("📎 Sources"):
                    for s in result["sources"]:
                        st.markdown(f"- {s}")

        st.session_state.chat_history.append({
            "role":    "assistant",
            "content": result["response"],
            "sources": result.get("sources", []),
        })

    # ── Clear chat ────────────────────────────────────────────────────────────
    if st.session_state.chat_history:
        if st.button("🗑️ Clear conversation"):
            st.session_state.chat_history = []
            st.rerun()
