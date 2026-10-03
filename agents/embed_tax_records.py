"""
agents/embed_tax_records.py
----------------------------
Reads cleaned tax records from MinIO/DuckDB, generates sentence-transformer
embeddings, and upserts them into the Qdrant `fraud_similarity` collection.

Collection: fraud_similarity
Vectors:    384-dim (all-MiniLM-L6-v2), cosine distance
Payload fields per point:
    application_id, customer_id, taxable_income, employment_type,
    province, risk_score (0.0 if not available), is_fraud

Usage:
    python agents/embed_tax_records.py           # embed all available records
    python agents/embed_tax_records.py --limit 500  # embed first 500

Called by: scripts/seed_qdrant.py
"""

import os
import sys
import uuid
import logging
import argparse
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")
sys.path.insert(0, str(Path(__file__).parent.parent))

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [embed_tax_records] %(message)s",
    datefmt="%H:%M:%S",
)

COLLECTION_NAME = "fraud_similarity"
VECTOR_DIM      = 384
BATCH_SIZE      = 64


def ensure_collection(client):
    """Create the fraud_similarity collection if it does not exist."""
    from qdrant_client.models import Distance, VectorParams

    existing = [c.name for c in client.get_collections().collections]
    if COLLECTION_NAME not in existing:
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(size=VECTOR_DIM, distance=Distance.COSINE),
        )
        log.info(f"Created Qdrant collection: {COLLECTION_NAME}")
    else:
        log.info(f"Collection already exists: {COLLECTION_NAME}")


def upsert_batch(client, records: list[dict], vectors: list[list[float]]):
    """Upsert a batch of records into the Qdrant collection."""
    from qdrant_client.models import PointStruct

    points = []
    for record, vector in zip(records, vectors):
        # Use application_id as a stable UUID-like ID
        point_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, str(record.get("application_id", uuid.uuid4()))))
        points.append(
            PointStruct(
                id=point_id,
                vector=vector,
                payload={
                    "application_id":   str(record.get("application_id", "")),
                    "customer_id":      str(record.get("customer_id", "")),
                    "taxable_income":   float(record.get("taxable_income", 0.0)),
                    "employment_type":  str(record.get("employment_type", "UNKNOWN")),
                    "province":         str(record.get("province", "UNKNOWN")),
                    "risk_score":       float(record.get("risk_score", 0.0)),
                    "is_fraud":         bool(record.get("is_fraud", False)),
                },
            )
        )
    client.upsert(collection_name=COLLECTION_NAME, points=points, wait=True)


def run(limit: int = 10000):
    from storage.duckdb_queries import get_cleaned_data_for_ml
    from agents.embeddings import build_tax_record_text, embed_batch, get_model
    from agents.qdrant_search import get_qdrant_client

    # ── Load data ─────────────────────────────────────────────────────────────
    log.info(f"Loading up to {limit} cleaned records from MinIO/DuckDB...")
    df = get_cleaned_data_for_ml(n_rows=limit)

    if df.empty:
        log.warning("No records in cleaned-tax bucket yet. Run Flink job_05 first.")
        return 0

    records = df.to_dict(orient="records")
    log.info(f"Loaded {len(records)} records.")

    # ── Build text for embedding ───────────────────────────────────────────────
    texts = [build_tax_record_text(r) for r in records]

    # ── Ensure collection ──────────────────────────────────────────────────────
    get_model()   # warm up / verify model loads
    client = get_qdrant_client()
    ensure_collection(client)

    # ── Embed + upsert in batches ──────────────────────────────────────────────
    total_upserted = 0
    for i in range(0, len(texts), BATCH_SIZE):
        batch_texts   = texts[i: i + BATCH_SIZE]
        batch_records = records[i: i + BATCH_SIZE]

        vectors = embed_batch(batch_texts, batch_size=BATCH_SIZE, show_progress=False)
        upsert_batch(client, batch_records, vectors)

        total_upserted += len(batch_records)
        log.info(f"  Upserted {total_upserted}/{len(records)} records...")

    # ── Summary ───────────────────────────────────────────────────────────────
    info = client.get_collection(COLLECTION_NAME)
    log.info(
        f"Done. Collection '{COLLECTION_NAME}' now has "
        f"{info.points_count} points."
    )
    return total_upserted


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Embed tax records into Qdrant fraud_similarity")
    parser.add_argument("--limit", type=int, default=10000, help="Max records to embed")
    args = parser.parse_args()
    n = run(limit=args.limit)
    print(f"Upserted {n} records into Qdrant '{COLLECTION_NAME}'.")
