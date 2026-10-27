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
        sys.path.insert(0, "/")
        import importlib
        spec = importlib.util.find_spec("agents.langgraph_app")
        return spec is not None
    except Exception:
        return False


def _run_agent(user_input: str, llm_override: dict = None) -> dict:
    """Call the LangGraph agent. Returns {response, sources, intent, byo_required}."""
    try:
        sys.path.insert(0, "/")
        from agents.langgraph_app import run
        result = run(user_input, llm_override=llm_override)
        return result
    except Exception as e:
        return {"response": f"Agent error: {e}", "sources": [], "intent": "error"}


def _get_model_info() -> dict:
    """Get current HF model / backend status."""
    try:
        sys.path.insert(0, "/")
        from agents.llm_wrapper import get_model_info
        return get_model_info()
    except Exception:
        return {"model": "unknown", "backend": "unknown", "token": "unknown"}


def _keys_configured() -> dict:
    """Which built-in providers have credentials in the environment."""
    return {
        "huggingface": bool(os.getenv("HF_API_TOKEN", "").strip()),
        "openai":      bool(os.getenv("OPENAI_API_KEY", "").strip()),
    }


def _set_provider_mode(mode: str) -> None:
    """Dev switch: 'auto' rotates HF/OpenAI; otherwise force one provider (still capped)."""
    try:
        sys.path.insert(0, "/")
        from agents.llm_wrapper import get_budget
        get_budget().set_force_provider(None if mode == "auto" else mode)
    except Exception:
        pass


def _fmt_tokens(n: int) -> str:
    return f"{int(n):,}"


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
    keys         = _keys_configured()
    model_info   = _get_model_info()
    budget       = model_info.get("budget")          # None if llm_wrapper not importable
    byo          = st.session_state.get("byo_llm")   # {"model","api_key","api_base"} or None

    # ── Status banner ─────────────────────────────────────────────────────────
    col_a, col_b, col_c = st.columns(3)
    with col_a:
        if agent_ready:
            st.markdown('<span class="status-ok">&#9679; LangGraph agent ready</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="status-warn">&#9679; Agent modules not mounted</span>', unsafe_allow_html=True)
    with col_b:
        if byo:
            st.markdown('<span class="status-ok">&#9679; Using your own model</span>', unsafe_allow_html=True)
        elif budget and budget["byo_required"]:
            st.markdown('<span class="status-warn">&#9679; Token allowance used up</span>', unsafe_allow_html=True)
        elif keys["huggingface"] or keys["openai"]:
            n = sum(keys.values())
            st.markdown(f'<span class="status-ok">&#9679; {n} provider key(s) configured</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="status-warn">&#9679; No API keys set</span>', unsafe_allow_html=True)
    with col_c:
        backend_colour = "status-ok" if model_info.get("backend") in ("api", "byo") else "status-warn"
        model_short = model_info.get("model", "—").split("/")[-1]
        st.markdown(f'<span class="{backend_colour}">&#9679; {model_short}</span>', unsafe_allow_html=True)

    if not agent_ready:
        st.info(
            "Agent modules are not yet mounted in this container. "
            "The chat is running in demo mode showing example responses.",
            icon="ℹ️",
        )
    elif not (keys["huggingface"] or keys["openai"]) and not byo:
        st.info(
            "Add `HF_API_TOKEN` and/or `OPENAI_API_KEY` (with `OPENAI_MODEL`) to your `.env`, "
            "then `podman compose up -d --force-recreate streamlit`.",
            icon="🔑",
        )

    # ── Token allowance + provider controls ───────────────────────────────────
    if agent_ready and budget:
        with st.expander("🔢 Token allowance & provider", expanded=bool(budget["byo_required"] and not byo)):
            for name, label in (("huggingface", "HuggingFace"), ("openai", "OpenAI")):
                p = budget["providers"][name]
                st.progress(
                    min(1.0, p["used"] / p["total"]),
                    text=(f"{label}: {_fmt_tokens(p['used'])} / {_fmt_tokens(p['total'])} tokens used"
                          f"  ·  current stint {_fmt_tokens(p['stint_used'])} / {_fmt_tokens(p['stint_tokens'])}"
                          + (f"  ·  cooling down {p['cooldown_s']}s" if p["cooldown_s"] else "")),
                )
                if p["last_error"]:
                    st.caption(f"{label} last error: {p['last_error']}")
            st.caption(
                "Rotation: HuggingFace 20,000 → OpenAI 10,000 → repeat, until HuggingFace has used "
                "100,000 and OpenAI 50,000. Then bring your own model."
            )

            modes   = ["auto", "huggingface", "openai"]
            current = budget["forced"] or "auto"
            choice  = st.radio(
                "Provider mode (dev)", modes, index=modes.index(current), horizontal=True,
                help="auto = rotate. Choosing a provider sends every call there for testing. "
                     "Its total cap still applies.",
            )
            if choice != current:
                _set_provider_mode(choice)
                st.rerun()

            st.markdown("**Bring your own model**")
            with st.form("byo_form", clear_on_submit=False):
                m = st.text_input("Model (LiteLLM format)", value=(byo or {}).get("model", ""),
                                  placeholder="openai/gpt-4o-mini  ·  anthropic/claude-sonnet-4  ·  ollama/llama3")
                k = st.text_input("API key", value=(byo or {}).get("api_key", ""), type="password")
                b = st.text_input("API base URL (optional)", value=(byo or {}).get("api_base", ""),
                                  placeholder="https://your-endpoint/v1")
                c1, c2 = st.columns(2)
                save  = c1.form_submit_button("Use my model")
                clear = c2.form_submit_button("Back to built-in")
            if save and m.strip():
                st.session_state.byo_llm = {"model": m.strip(), "api_key": k.strip(), "api_base": b.strip()}
                st.rerun()
            if clear:
                st.session_state.pop("byo_llm", None)
                st.rerun()
            st.caption("Your key is kept in this browser session only and is never written to disk. "
                       "Your own model's usage is not counted against the built-in allowance.")

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
            with st.spinner("Thinking..."):
                if agent_ready:
                    result = _run_agent(query, llm_override=byo)
                else:
                    result = _demo_response(query)

            st.markdown(result["response"])
            if result.get("byo_required"):
                st.warning(
                    "Open **🔢 Token allowance & provider** above and enter your own model, "
                    "API key and (optional) API base URL, then ask again.",
                    icon="🔑",
                )
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


