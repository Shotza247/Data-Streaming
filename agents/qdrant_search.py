"""
agents/qdrant_search.py
-----------------------
Public API for Qdrant vector search operations.
Used by the LangGraph fraud agent, Q&A agent, and Streamlit dashboard.

Collections:
    fraud_similarity   - embedded tax application records (dim=384)
    regulatory_kb      - embedded regulatory document chunks (dim=384)

Exposes:
    get_qdrant_client()
    search_similar_fraud(record_dict, top_k=5)  -> list[ScoredPoint]
    search_regulatory_kb(query_str, top_k=3)    -> list[ScoredPoint]

Environment:
    QDRANT_HOST  (default: localhost)
    QDRANT_PORT  (default: 6333)
"""

import os
import logging
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

log = logging.getLogger(__name__)

QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))

COLLECTION_FRAUD = "fraud_similarity"
COLLECTION_KB    = "regulatory_kb"
COLLECTION_KNOWLEDGE = "knowledge_base"

# ── Client factory ────────────────────────────────────────────────────────────

def get_qdrant_client():
    """Return a connected Qdrant Python client."""
    try:
        from qdrant_client import QdrantClient
    except ImportError as exc:
        raise ImportError(
            "qdrant-client not installed: pip install qdrant-client"
        ) from exc

    return QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)


# ── Search functions ──────────────────────────────────────────────────────────

def search_similar_fraud(record_dict: dict, top_k: int = 5) -> list:
    """
    Find the top_k most similar known fraud cases to `record_dict`.

    `record_dict` should contain tax application fields:
        province, taxable_income, employment_type, employment_status,
        filing_status, is_fraud (optional)

    Returns a list of qdrant_client.models.ScoredPoint objects.
    Each ScoredPoint has:
        .id          - Qdrant point ID
        .score       - cosine similarity (0..1)
        .payload     - dict with application_id, customer_id, taxable_income,
                       employment_type, risk_score, is_fraud

    Returns empty list on error.
    """
    try:
        from agents.embeddings import build_tax_record_text, embed_text
        query_text = build_tax_record_text(record_dict)
        query_vec  = embed_text(query_text)

        client  = get_qdrant_client()
        return client.query_points(
            collection_name=COLLECTION_FRAUD,
            query=query_vec,
            limit=top_k,
            with_payload=True,
        ).points
    except Exception as exc:
        log.warning(f"search_similar_fraud failed: {exc}")
        return []


def search_regulatory_kb(query_str: str, top_k: int = 3) -> list:
    """
    Retrieve the top_k most relevant regulatory document chunks for `query_str`.

    Returns a list of qdrant_client.models.ScoredPoint objects.
    Each ScoredPoint has:
        .id          - Qdrant point ID
        .score       - cosine similarity
        .payload     - dict with chunk_id, source_doc, section, text

    Returns empty list on error.
    """
    try:
        from agents.embeddings import embed_text
        query_vec = embed_text(query_str)

        client  = get_qdrant_client()
        return client.query_points(
            collection_name=COLLECTION_KB,
            query=query_vec,
            limit=top_k,
            with_payload=True,
        ).points
    except Exception as exc:
        log.warning(f"search_regulatory_kb failed: {exc}")
        return []


def search_knowledge_base(query_str: str, top_k: int = 4) -> list:
    """
    Search the curated `knowledge_base` collection (business context, fraud
    typologies, regulation) built from the markdown files in knowledge_base/.

    Each ScoredPoint payload has: chunk_id, file, section, source, status,
    authority, text.  Returns an empty list on error or if the collection is
    missing, so callers can fall back to `search_regulatory_kb`.
    """
    try:
        from agents.embeddings import embed_text
        client = get_qdrant_client()
        return client.query_points(
            collection_name=COLLECTION_KNOWLEDGE,
            query=embed_text(query_str),
            limit=top_k,
            with_payload=True,
        ).points
    except Exception as exc:
        log.warning(f"search_knowledge_base failed: {exc}")
        return []


def format_kb_context(results: list) -> str:
    """
    Format KB search results (either collection) into a context string
    suitable for inclusion in a RAG prompt. Curated chunks also carry their
    verification status so the model can warn about unverified content.
    """
    if not results:
        return "No relevant regulatory information found."

    parts = []
    for i, r in enumerate(results, 1):
        p = r.payload or {}
        if "file" in p:
            header = (
                f"[Source {i}: {p.get('file')} - {p.get('section', '?')} | "
                f"origin: {p.get('source', 'n/a')} | status: {p.get('status', 'n/a')}]"
            )
        else:
            header = (
                f"[Source {i}: {p.get('source_doc', 'unknown')} "
                f"§{p.get('section', '?')}]"
            )
        parts.append(f"{header}\n{p.get('text', '')}")
    return "\n\n".join(parts)
