"""
dashboard/tabs/tab_agent_chat.py
Tab 3 — AI Agent Chat
Invokes the LangGraph multi-agent graph backed by the HuggingFace Inference API.
Falls back to canned demo responses if the agent modules are not importable.
"""

import streamlit as st
import os
import sys


# ── Agent availability check ──────────────────────────────────────────────────

def _agent_available() -> bool:
    """Check whether the LangGraph app module is importable."""
    try:
        sys.path.insert(0, "/app")
        import importlib
        spec = importlib.util.find_spec("agents.langgraph_app")
        return spec is not None
    except Exception:
        return False


def _run_agent(user_input: str) -> dict:
    """Call the LangGraph agent. Returns {response, sources, intent}."""
    try:
        sys.path.insert(0, "/app")
        from agents.langgraph_app import run
        result = run(user_input)
        return result
    except Exception as e:
        return {"response": f"Agent error: {e}", "sources": [], "intent": "error"}


def _get_model_info() -> dict:
    """Get current HF model / backend status."""
    try:
        sys.path.insert(0, "/app")
        from agents.llm_wrapper import get_model_info
        return get_model_info()
    except Exception:
        return {"model": "unknown", "backend": "unknown", "token": "unknown"}


def _hf_token_configured() -> bool:
    """Check if HF_API_TOKEN is set in the environment."""
    return bool(os.getenv("HF_API_TOKEN", "").strip())


# ── Canned demo responses (used when agent not importable) ───────────────────

DEMO_RESPONSES = {
    "fraud": {
        "response": (
            "**[Demo Mode — agent not yet loaded]**\n\n"
            "Application APP-47823 has been assessed.\n"
            "- Fraud probability: **0.87** (HIGH RISK)\n"
            "- Similar cases: APP-31204 (similarity 0.92), APP-28876 (0.89), APP-19043 (0.84)\n\n"
            "**Recommendation:** Flag for manual review and request supporting documentation.\n\n"
            "_Once the agent modules are mounted, this will use the real MLflow model + Qdrant._"
        ),
        "sources": ["MLflow fraud-detector v1.0", "Qdrant: fraud_similarity collection"],
        "intent": "fraud_check",
    },
    "tax": {
        "response": (
            "**[Demo Mode — agent not yet loaded]**\n\n"
            "Based on SARS tax tables for 2024/25:\n"
            "- R 450,000 taxable income falls in the **31% marginal bracket**\n"
            "- Tax before rebates: approximately R 115,263\n"
            "- Less primary rebate (R 17,235): **Tax payable ≈ R 98,028**\n\n"
            "_Once the agent modules are mounted, this will use the HF Inference API + Qdrant KB._"
        ),
        "sources": ["Qdrant: regulatory_kb — SARS Tax Tables 2024/25", "Income Tax Act §6"],
        "intent": "tax_question",
    },
    "default": {
        "response": (
            "**[Demo Mode — agent not yet loaded]**\n\n"
            "I can help with two types of queries:\n\n"
            "1. **Fraud check** — paste an `application_id` (e.g. `APP-47823`) to assess fraud risk\n"
            "2. **Tax question** — ask any South African tax question\n\n"
            "_Once the agent modules are mounted, responses will come from the live AI agent._"
        ),
        "sources": [],
        "intent": "tax_question",
    },
}


def _demo_response(user_input: str) -> dict:
    lower = user_input.lower()
    if "app-" in lower or "application" in lower or "fraud" in lower:
        return DEMO_RESPONSES["fraud"]
    if any(w in lower for w in ["tax", "income", "bracket", "rebate", "sars", "filing", "penalty"]):
        return DEMO_RESPONSES["tax"]
    return DEMO_RESPONSES["default"]


# ── Main render ───────────────────────────────────────────────────────────────

def render():
    st.subheader("🤖 AI Agent Chat")
    st.markdown(
        "Ask a **tax question** or paste an **application_id** to run a fraud check. "
        "The Supervisor routes your query to the **Fraud Agent** or **Tax Q&A Agent**."
    )

    agent_ready  = _agent_available()
    token_ok     = _hf_token_configured()
    model_info   = _get_model_info()

    # ── Status banner ─────────────────────────────────────────────────────────
    col_a, col_b, col_c = st.columns(3)
    with col_a:
        if agent_ready:
            st.markdown('<span class="status-ok">&#9679; LangGraph agent ready</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="status-warn">&#9679; Agent modules not mounted</span>', unsafe_allow_html=True)
    with col_b:
        if token_ok:
            st.markdown('<span class="status-ok">&#9679; HF API token configured</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="status-warn">&#9679; HF_API_TOKEN not set</span>', unsafe_allow_html=True)
    with col_c:
        backend_colour = "status-ok" if model_info.get("backend") == "api" else "status-warn"
        model_short = model_info.get("model", "—").split("/")[-1]
        st.markdown(
            f'<span class="{backend_colour}">&#9679; {model_short}</span>',
            unsafe_allow_html=True,
        )

    # ── Setup hint if no token ────────────────────────────────────────────────
    if not token_ok and agent_ready:
        st.info(
            "**Set your HuggingFace API token** to enable the AI agent:\n\n"
            "1. Get a free token at https://huggingface.co/settings/tokens\n"
            "2. Add `HF_API_TOKEN=hf_your_token_here` to your `.env` file\n"
            "3. Restart the Streamlit container: `podman restart streamlit`\n\n"
            "The agent works without a token but may be rate-limited.",
            icon="🔑",
        )
    elif not agent_ready:
        st.info(
            "Agent modules are not yet mounted in this container. "
            "The chat is running in demo mode showing example responses.",
            icon="ℹ️",
        )

    # ── Model info expander ───────────────────────────────────────────────────
    if agent_ready:
        with st.expander("⚙️ Agent configuration", expanded=False):
            st.markdown(f"""
| Setting | Value |
|---|---|
| **LLM Backend** | HuggingFace Inference API |
| **Primary Model** | `{model_info.get('model', HF_MODEL_INFO_DEFAULT)}` |
| **Fallback Models** | `zephyr-7b-beta` → `Mistral-7B-Instruct-v0.2` → `flan-t5-base (local)` |
| **Token Status** | {'✅ Configured' if token_ok else '⚠️ Not set (set HF_API_TOKEN in .env)'} |
| **Routing** | Keyword heuristic — no LLM call needed for routing |
| **Fraud Agent** | MLflow `fraud-detector` + Qdrant `fraud_similarity` |
| **Q&A Agent** | Qdrant `regulatory_kb` + HF API RAG |
""")

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

    # ── Example prompt buttons ────────────────────────────────────────────────
    examples = st.columns(3)
    with examples[0]:
        if st.button("💡 Fraud check", use_container_width=True):
            st.session_state._prefill = "Check application APP-47823 for fraud"
    with examples[1]:
        if st.button("💡 Tax question", use_container_width=True):
            st.session_state._prefill = "What is the income tax on R 450,000 for the 2024/25 tax year?"
    with examples[2]:
        if st.button("💡 Filing penalty", use_container_width=True):
            st.session_state._prefill = "What is the penalty for late tax filing in South Africa?"

    prefill    = st.session_state.pop("_prefill", "")
    user_input = st.chat_input("Type your question or paste an application_id…", key="chat_input")
    query      = user_input or prefill

    if query:
        st.session_state.chat_history.append({"role": "user", "content": query})
        with st.chat_message("user"):
            st.markdown(query)

        with st.chat_message("assistant"):
            spinner_msg = "Calling HuggingFace API..." if (agent_ready and token_ok) else "Thinking..."
            with st.spinner(spinner_msg):
                if agent_ready:
                    result = _run_agent(query)
                else:
                    result = _demo_response(query)

            st.markdown(result["response"])
            intent = result.get("intent", "")
            if intent:
                st.caption(f"Routed to: **{intent.replace('_', ' ').title()}**")
            if result.get("sources"):
                with st.expander("📎 Sources"):
                    for s in result["sources"]:
                        st.markdown(f"- {s}")

        st.session_state.chat_history.append({
            "role":    "assistant",
            "content": result["response"],
            "sources": result.get("sources", []),
        })

    # ── Clear chat button ─────────────────────────────────────────────────────
    if st.session_state.chat_history:
        if st.button("🗑️ Clear conversation"):
            st.session_state.chat_history = []
            st.rerun()


# Constant used in the expander when model_info hasn't been called yet
HF_MODEL_INFO_DEFAULT = "mistralai/Mixtral-8x7B-Instruct-v0.1"
