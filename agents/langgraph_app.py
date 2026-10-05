"""
agents/langgraph_app.py
-----------------------
Assembles the LangGraph StateGraph for the Tax Analytics multi-agent system.

Graph topology:
    [START] → supervisor → {fraud_check: fraud_agent, tax_question: qa_agent} → [END]

State schema:
    input             str         Raw user input
    intent            str         "fraud_check" | "tax_question"
    next              str         Target node name from supervisor
    application_dict  dict|None   Parsed application data (fraud path)
    question          str|None    Cleaned question text (Q&A path)
    response          str         Final response string
    sources           list[str]   Source references (KB chunks or similar case IDs)

Exposed:
    compile_graph()  -> CompiledGraph
    run(user_input)  -> dict with "response" and "sources"

Usage:
    from agents.langgraph_app import run
    result = run("What is the penalty for late filing?")
    print(result["response"])
"""

import logging
from typing import TypedDict, Optional

log = logging.getLogger(__name__)


# ── State schema ─────────────────────────────────────────────────────────────

class AgentState(TypedDict, total=False):
    input:            str
    intent:           str
    next:             str
    application_dict: Optional[dict]
    question:         Optional[str]
    response:         str
    sources:          list


# ── Graph compilation ─────────────────────────────────────────────────────────

_compiled_graph = None


def compile_graph():
    """
    Build and compile the LangGraph StateGraph.
    Returns a compiled graph (cached after first call).
    """
    global _compiled_graph
    if _compiled_graph is not None:
        return _compiled_graph

    try:
        from langgraph.graph import StateGraph, END
    except ImportError as exc:
        raise ImportError(
            "langgraph not installed: pip install langgraph langchain-core"
        ) from exc

    from agents.supervisor   import supervisor_node
    from agents.fraud_agent  import fraud_agent_node
    from agents.qa_agent     import qa_agent_node

    # ── Build graph ────────────────────────────────────────────────────────────
    builder = StateGraph(AgentState)

    builder.add_node("supervisor",  supervisor_node)
    builder.add_node("fraud_agent", fraud_agent_node)
    builder.add_node("qa_agent",    qa_agent_node)

    # Entry point
    builder.set_entry_point("supervisor")

    # Conditional routing from supervisor
    def route_from_supervisor(state: AgentState) -> str:
        return state.get("next", "qa_agent")

    builder.add_conditional_edges(
        "supervisor",
        route_from_supervisor,
        {
            "fraud_agent": "fraud_agent",
            "qa_agent":    "qa_agent",
        },
    )

    # Both specialist agents finish at END
    builder.add_edge("fraud_agent", END)
    builder.add_edge("qa_agent",    END)

    _compiled_graph = builder.compile()
    log.info("LangGraph compiled successfully.")
    return _compiled_graph


# ── Public run entry point ────────────────────────────────────────────────────

def run(user_input: str) -> dict:
    """
    Main entry point. Accepts a raw string from the user and returns
    a dict with "response" (str) and "sources" (list).

    Args:
        user_input: Natural-language question or fraud check request.

    Returns:
        {
            "response": "...",        # formatted answer string
            "sources":  [...],        # list of source references
            "intent":   "...",        # "fraud_check" | "tax_question"
        }

    On error: returns a graceful fallback response.
    """
    try:
        graph = compile_graph()
        initial_state: AgentState = {
            "input":            user_input,
            "intent":           "",
            "next":             "",
            "application_dict": None,
            "question":         None,
            "response":         "",
            "sources":          [],
        }
        final_state = graph.invoke(initial_state)
        return {
            "response": final_state.get("response", "No response generated."),
            "sources":  final_state.get("sources", []),
            "intent":   final_state.get("intent", "unknown"),
        }
    except Exception as exc:
        log.error(f"Agent graph failed: {exc}")
        return {
            "response": (
                f"The agent encountered an error: {exc}\n\n"
                "Please check that Qdrant, MLflow, and the HuggingFace models are available."
            ),
            "sources": [],
            "intent":  "error",
        }


# ── Quick smoke test ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("LangGraph agent smoke test\n")

    test_inputs = [
        "What is the penalty rate for late tax filing in South Africa?",
        "What income tax bracket applies for R450 000 taxable income?",
        "Check application APP-001 for fraud",
        "Is there fraud risk for a self-employed person earning R750 000?",
    ]

    for user_input in test_inputs:
        print(f"[Q] {user_input}")
        result = run(user_input)
        print(f"[intent] {result['intent']}")
        print(f"[A] {result['response'][:300]}...")
        if result["sources"]:
            print(f"[sources] {result['sources']}")
        print()
