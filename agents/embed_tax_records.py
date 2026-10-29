"""
agents/embed_tax_records.py
----------------------------
Reads cleaned tax records from MinIO/DuckDB, generates sentence-transformer
embeddings, and upserts them into the Qdrant `fraud_similarity` collection.

Collection: fraud_similarity
Vectors:    384-dim (all-MiniLM-L6-v2), cosine distance
Payload fields per point:
    application_id, customer_id, taxable_income, employment_type,
    employment_status, filing_status, province, risk_score (0.0 if not
    available), is_fraud, processed_at (when Flink cleaned the record)

Point ids are uuid5(application_id), so every write is idempotent.

Usage:
    python agents/embed_tax_records.py           # embed all available records
    python agents/embed_tax_records.py --limit 500  # embed first 500
    python agents/embed_tax_records.py --sync    # embed only records not yet in Qdrant

Called by: scripts/seed_qdrant.py (full seed), scripts/sync_qdrant.py (continuous sync)
"""

import os
import sys
import uuid
import logging
import argparse
from datetime import timedelta
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


def ensure_collection(client, quiet: bool = False):
    """Create the fraud_similarity collection if it does not exist."""
    from qdrant_client.models import Distance, VectorParams

    existing = [c.name for c in client.get_collections().collections]
    if COLLECTION_NAME not in existing:
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(size=VECTOR_DIM, distance=Distance.COSINE),
        )
        log.info(f"Created Qdrant collection: {COLLECTION_NAME}")
    elif not quiet:
        log.info(f"Collection already exists: {COLLECTION_NAME}")


def point_id(application_id) -> str:
    """Stable Qdrant point id for an application (same id => upsert overwrites)."""
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, str(application_id)))


def build_payload(record: dict) -> dict:
    processed = record.get("flink_processed_at")
    return {
        "application_id":    str(record.get("application_id", "")),
        "customer_id":       str(record.get("customer_id", "")),
        "taxable_income":    float(record.get("taxable_income", 0.0)),
        "employment_type":   str(record.get("employment_type", "UNKNOWN")),
        "employment_status": str(record.get("employment_status", "UNKNOWN")),
        "filing_status":     str(record.get("filing_status", "UNKNOWN")),
        "province":          str(record.get("province", "UNKNOWN")),
        "risk_score":        float(record.get("risk_score", 0.0)),
        "is_fraud":          bool(record.get("is_fraud", False)),
        "processed_at":      str(processed) if processed is not None else None,
    }


def upsert_batch(client, records: list[dict], vectors: list[list[float]]):
    """Upsert a batch of records into the Qdrant collection."""
    from qdrant_client.models import PointStruct

    points = [
        PointStruct(id=point_id(r.get("application_id", uuid.uuid4())),
                    vector=vector, payload=build_payload(r))
        for r, vector in zip(records, vectors)
    ]
    client.upsert(collection_name=COLLECTION_NAME, points=points, wait=True)


def find_new(client, records: list[dict], chunk: int = 256) -> list[dict]:
    """Return the records whose application_id is not in the collection yet."""
    new = []
    for i in range(0, len(records), chunk):
        part = records[i: i + chunk]
        ids = [point_id(r["application_id"]) for r in part]
        present = {str(p.id) for p in client.retrieve(
            collection_name=COLLECTION_NAME, ids=ids, with_payload=False, with_vectors=False)}
        new += [r for r, pid in zip(part, ids) if pid not in present]
    return new


# Newest cleaned-file timestamp already examined. Later passes only read files
# written after it (minus an overlap), so a pass costs one bucket listing plus
# the handful of new files instead of re-reading `limit` files every time.
_watermark = {"since": None}
_OVERLAP = timedelta(minutes=2)   # re-read a little; find_new() makes that harmless


def sync_new(limit: int = 5000, client=None, quiet: bool = False) -> int:
    """
    Embed and upsert the cleaned records that Qdrant does not have yet.
    The first pass examines the newest `limit` files (by arrival time); later
    passes only the files written since. Cheap when nothing is new (no model
    call). Returns the number of records added.
    """
    from storage.duckdb_queries import get_recent_arrivals
    from agents.embeddings import build_tax_record_text, embed_batch
    from agents.qdrant_search import get_qdrant_client

    since = _watermark["since"]
    df, newest = get_recent_arrivals(since=since - _OVERLAP if since else None, max_files=limit)
    if df.empty:
        return 0
    records = df.drop_duplicates(subset="application_id").to_dict(orient="records")

    client = client or get_qdrant_client()
    ensure_collection(client, quiet=True)
    new = find_new(client, records)
    if not new:
        _watermark["since"] = newest
        return 0

    added = 0
    for i in range(0, len(new), BATCH_SIZE):
        batch = new[i: i + BATCH_SIZE]
        vectors = embed_batch([build_tax_record_text(r) for r in batch],
                              batch_size=BATCH_SIZE, show_progress=False)
        upsert_batch(client, batch, vectors)
        added += len(batch)
    _watermark["since"] = newest      # only advance once everything was written
    if not quiet:
        log.info(f"Synced {added} new record(s) into '{COLLECTION_NAME}'.")
    return added


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
    parser.add_argument("--sync", action="store_true",
                        help="Only embed records not already in Qdrant (newest first)")
    args = parser.parse_args()
    n = sync_new(limit=args.limit) if args.sync else run(limit=args.limit)
    print(f"Upserted {n} records into Qdrant '{COLLECTION_NAME}'.")
