"""
agents/embeddings.py
--------------------
Shared embedding utility wrapping the HuggingFace sentence-transformers model.
Model: sentence-transformers/all-MiniLM-L6-v2  (~80 MB, 384-dim, CPU-only)

Exposes:
    get_model()                         -> SentenceTransformer (cached singleton)
    embed_text(text)                    -> list[float]   (384-dim vector)
    embed_batch(texts, batch_size=64)   -> list[list[float]]

The model is downloaded on first call to get_model() and cached in memory for
the lifetime of the process. On container restart it is re-downloaded from
HuggingFace Hub unless a local cache volume is mounted at ~/.cache/huggingface.

Environment:
    HF_MODEL_EMBED   (default: sentence-transformers/all-MiniLM-L6-v2)
    TRANSFORMERS_CACHE (set in compose to persist model across container restarts)
"""

import os
import logging
from typing import Union

log = logging.getLogger(__name__)

HF_MODEL_EMBED = os.getenv(
    "HF_MODEL_EMBED",
    "sentence-transformers/all-MiniLM-L6-v2",
)

VECTOR_DIM = 384   # all-MiniLM-L6-v2 output dimension

# ── Cached singleton ──────────────────────────────────────────────────────────
_model = None


def get_model():
    """
    Load and cache the sentence-transformer model.
    Thread-safe for single-process usage (Streamlit, scripts).
    """
    global _model
    if _model is None:
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as exc:
            raise ImportError(
                "sentence-transformers not installed: "
                "pip install sentence-transformers"
            ) from exc

        log.info(f"Loading embedding model: {HF_MODEL_EMBED}")
        _model = SentenceTransformer(HF_MODEL_EMBED)
        log.info(f"Model loaded. Output dim: {_model.get_sentence_embedding_dimension()}")
    return _model


def embed_text(text: str) -> list[float]:
    """
    Embed a single string. Returns a list of 384 floats.
    """
    model = get_model()
    vector = model.encode(str(text), normalize_embeddings=True)
    return vector.tolist()


def embed_batch(
    texts: list[str],
    batch_size: int = 64,
    show_progress: bool = False,
) -> list[list[float]]:
    """
    Embed a list of strings. Returns a list of 384-dim float lists.
    Uses batched encoding for efficiency.
    """
    model = get_model()
    vectors = model.encode(
        [str(t) for t in texts],
        batch_size=batch_size,
        normalize_embeddings=True,
        show_progress_bar=show_progress,
    )
    return [v.tolist() for v in vectors]


def build_tax_record_text(record: dict) -> str:
    """
    Serialise a tax record dict into a natural-language string for embedding.
    This determines what the similarity search is sensitive to.
    """
    return (
        f"Province {record.get('province', 'unknown')} "
        f"income {record.get('taxable_income', 0)} "
        f"employment {record.get('employment_type', 'unknown')} "
        f"status {record.get('employment_status', 'unknown')} "
        f"filing {record.get('filing_status', 'unknown')} "
        f"fraud {record.get('is_fraud', False)}"
    )
