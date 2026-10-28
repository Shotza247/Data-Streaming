"""
agents/embed_knowledge_base.py
-------------------------------
Loads the markdown files in knowledge_base/ into the Qdrant `knowledge_base`
collection. Each `## heading` section is one chunk.

Payload per point:
    chunk_id, file, section, source, status, authority, text

`source` and `status` come from the "Source:" and "Status:" lines inside each
section, so the agent can cite the origin and warn when a chunk is unverified.

Usage:
    python agents/embed_knowledge_base.py

Called by: scripts/seed_qdrant.py
Idempotent: point ids are derived from file + section.
"""

import re
import sys
import uuid
import logging
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).parent.parent
load_dotenv(ROOT / ".env")
sys.path.insert(0, str(ROOT))

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [embed_knowledge_base] %(message)s",
    datefmt="%H:%M:%S",
)

COLLECTION_NAME = "knowledge_base"
VECTOR_DIM      = 384
KB_DIR          = ROOT / "knowledge_base"


def parse_markdown(path: Path) -> list[dict]:
    """Split one markdown file into chunks, one per `## ` section."""
    text = path.read_text(encoding="utf-8")

    authority = ""
    m = re.search(r"^Authority:\s*(.+)$", text, re.MULTILINE)
    if m:
        authority = m.group(1).strip()

    chunks = []
    for block in re.split(r"^## ", text, flags=re.MULTILINE)[1:]:
        title, _, body = block.partition("\n")
        body = body.strip()
        src = re.search(r"^Source:\s*(.+)$", body, re.MULTILINE)
        sta = re.search(r"^Status:\s*(.+)$", body, re.MULTILINE)
        chunks.append({
            "file":      path.name,
            "section":   title.strip(),
            "source":    src.group(1).strip() if src else "",
            "status":    sta.group(1).strip() if sta else "",
            "authority": authority,
            "text":      f"{title.strip()}\n{body}",
        })
    return chunks


def load_chunks() -> list[dict]:
    chunks = []
    for path in sorted(KB_DIR.glob("*.md")):
        if path.name.lower() == "readme.md":
            continue
        chunks.extend(parse_markdown(path))
    return chunks


def ensure_collection(client):
    from qdrant_client.models import Distance, VectorParams

    existing = [c.name for c in client.get_collections().collections]
    if COLLECTION_NAME not in existing:
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(size=VECTOR_DIM, distance=Distance.COSINE),
        )
        log.info(f"Created Qdrant collection: {COLLECTION_NAME}")


def run() -> int:
    from agents.embeddings import embed_batch
    from agents.qdrant_search import get_qdrant_client
    from qdrant_client.models import PointStruct

    chunks = load_chunks()
    if not chunks:
        raise RuntimeError(f"No knowledge base chunks found in {KB_DIR}")

    client = get_qdrant_client()
    ensure_collection(client)

    log.info(f"Embedding {len(chunks)} knowledge base chunks...")
    vectors = embed_batch([c["text"] for c in chunks], batch_size=32, show_progress=False)

    points = []
    for chunk, vector in zip(chunks, vectors):
        chunk_id = f"{chunk['file']}::{chunk['section']}"
        points.append(PointStruct(
            id=str(uuid.uuid5(uuid.NAMESPACE_DNS, chunk_id)),
            vector=vector,
            payload={"chunk_id": chunk_id, **chunk},
        ))

    client.upsert(collection_name=COLLECTION_NAME, points=points, wait=True)
    info = client.get_collection(COLLECTION_NAME)
    log.info(f"Done. '{COLLECTION_NAME}' has {info.points_count} points.")
    return len(points)


if __name__ == "__main__":
    print(f"Upserted {run()} chunks into Qdrant '{COLLECTION_NAME}'.")
