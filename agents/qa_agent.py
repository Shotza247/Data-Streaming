"""
agents/qa_agent.py
-------------------
LangGraph Tax Q&A Agent node.

Capabilities:
  1. Searches Qdrant regulatory_kb collection for top-3 relevant document chunks (RAG)
  2. Builds a context-enriched prompt
  3. Generates a natural-language answer using flan-t5-base via llm_wrapper.py
  4. Returns the answer with source references

State reads:  state["question"]
State writes: state["response"]   (str — natural-language answer)
              state["sources"]    (list — source document references)
"""

import logging

log = logging.getLogger(__name__)


def qa_agent_node(state: dict) -> dict:
    """
    LangGraph node: Tax Q&A Agent.

    Reads:  state["question"]   str — natural language tax question
    Writes: state["response"]   str — RAG-generated answer with sources
            state["sources"]    list[str] — source document references
    """
    question = state.get("question", "").strip()

    if not question:
        state["response"] = "Please provide a question about tax regulations."
        state["sources"]  = []
        return state

    # ── Retrieve relevant regulatory chunks ────────────────────────────────────
    context_text = ""
    sources      = []

    try:
        from agents.qdrant_search import search_regulatory_kb, format_kb_context
        results  = search_regulatory_kb(question, top_k=3)
        context_text = format_kb_context(results)
        sources = [
            f"{r.payload.get('source_doc', '?')} §{r.payload.get('section', '?')}"
            for r in results if r.payload
        ]
    except Exception as exc:
        log.warning(f"Qdrant KB search failed: {exc}")
        context_text = "No regulatory context available."

    # ── Build RAG prompt ───────────────────────────────────────────────────────
    prompt = (
        "You are a South African tax assistant. "
        "Answer the question below using the regulatory context provided. "
        "Be concise and accurate.\n\n"
        f"Context:\n{context_text}\n\n"
        f"Question: {question}\n\n"
        "Answer:"
    )

    # ── Generate answer ────────────────────────────────────────────────────────
    try:
        from agents.llm_wrapper import generate
        answer = generate(prompt, max_new_tokens=256)
    except Exception as exc:
        log.warning(f"LLM generate failed: {exc}")
        # Graceful fallback: return the most relevant KB chunk directly
        if sources:
            answer = (
                f"Based on {sources[0]}: {context_text.split(chr(10))[0]}"
                if context_text else "LLM unavailable. Please check the MLflow/agent service."
            )
        else:
            answer = "I could not find relevant regulatory information for that question."

    # ── Format response ────────────────────────────────────────────────────────
    if sources:
        sources_str = "\n".join(f"  - {s}" for s in sources)
        full_response = f"{answer}\n\n**Sources:**\n{sources_str}"
    else:
        full_response = answer

    state["response"] = full_response
    state["sources"]  = sources

    return state
