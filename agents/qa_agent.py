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
        from agents.qdrant_search import (
            search_knowledge_base, search_regulatory_kb, format_kb_context,
        )
        # Curated knowledge base first; fall back to the older synthetic KB.
        results = search_knowledge_base(question, top_k=4) or search_regulatory_kb(question, top_k=3)
        context_text = format_kb_context(results)
        sources = []
        for r in results:
            p = r.payload or {}
            if "file" in p:
                label = f"{p['file']} - {p.get('section', '?')} [{p.get('status', 'n/a')}]"
            else:
                label = f"{p.get('source_doc', '?')} §{p.get('section', '?')}"
            sources.append(label)
    except Exception as exc:
        log.warning(f"Qdrant KB search failed: {exc}")
        context_text = "No regulatory context available."

    # ── Build RAG prompt ───────────────────────────────────────────────────────
    prompt = (
        "You are a South African tax, fraud and compliance assistant. "
        "Answer the question below using ONLY the context provided. "
        "Cite the source heading for each legal claim. "
        "If a source is marked 'unverified', say the point should be confirmed against its linked origin. "
        "Give the tax year with any rate, threshold or penalty. "
        "Never state that a named person committed fraud; use 'indicators consistent with'. "
        "If the context does not support an answer, say so. "
        "Be concise.\n\n"
        f"Context:\n{context_text}\n\n"
        f"Question: {question}\n\n"
        "Answer:"
    )

    # ── Generate answer ────────────────────────────────────────────────────────
    try:
        from agents.llm_wrapper import generate, ByoRequired
        try:
            answer = generate(prompt, max_new_tokens=512, llm_override=state.get("llm_override"))
        except ByoRequired as byo:
            # Built-in token budget used up: tell the user how to continue.
            state["response"] = str(byo)
            state["sources"]  = sources
            state["byo_required"] = True
            return state
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
