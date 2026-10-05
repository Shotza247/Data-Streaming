"""
agents/supervisor.py
---------------------
LangGraph Supervisor node.

Classifies incoming user input and routes to the appropriate specialist agent:
  - fraud_check   → FraudAgent
  - tax_question  → QAAgent

Uses simple keyword/heuristic routing (no LLM call for routing — fast and deterministic).
"""

from typing import Any
from agents.llm_wrapper import classify_intent


def supervisor_node(state: dict) -> dict:
    """
    LangGraph node: Supervisor.

    Reads:   state["input"]          (str) — raw user input
    Writes:  state["intent"]         (str) — "fraud_check" | "tax_question"
             state["next"]           (str) — name of next node
             state["application_dict"] (dict | None) — parsed from input if fraud check
             state["question"]       (str | None) — cleaned question text
    """
    user_input = state.get("input", "")

    intent = classify_intent(user_input)
    state["intent"] = intent

    if intent == "fraud_check":
        # Try to extract an application_id from the input
        import re
        match = re.search(r"APP-[A-Z0-9\-]+", user_input, re.IGNORECASE)
        if match:
            state["application_dict"] = {"application_id": match.group(0).upper()}
        else:
            # No explicit ID — pass the raw text to the fraud agent for
            # similarity-based lookup
            state["application_dict"] = {"raw_query": user_input}
        state["next"] = "fraud_agent"
    else:
        state["question"] = user_input
        state["next"] = "qa_agent"

    return state
