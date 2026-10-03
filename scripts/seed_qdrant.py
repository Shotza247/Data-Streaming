#!/usr/bin/env python3
"""
scripts/seed_qdrant.py
-----------------------
One-shot orchestrator that:
  1. Checks the cleaned-tax MinIO bucket is non-empty (Risk 3 guard)
  2. Embeds all cleaned tax records into Qdrant fraud_similarity collection
  3. Embeds all regulatory KB chunks into Qdrant regulatory_kb collection

Usage:
    python scripts/seed_qdrant.py                  # full seed
    python scripts/seed_qdrant.py --kb-only        # regulatory KB only
    python scripts/seed_qdrant.py --records-only   # tax records only
    python scripts/seed_qdrant.py --limit 1000     # limit records to embed

Called by: make seed
"""

import sys
import argparse
import logging
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [seed_qdrant] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("seed_qdrant")


def preflight_check() -> bool:
    """
    [Risk 3] Assert that the cleaned-tax bucket is non-empty before seeding.
    Returns True if data is available, False otherwise.
    """
    try:
        from storage.minio_client import bucket_has_files, BUCKET_CLEANED
        has_data = bucket_has_files(BUCKET_CLEANED)
        if not has_data:
            log.error(
                f"Preflight FAILED: MinIO bucket '{BUCKET_CLEANED}' is empty. "
                "Flink job_05 (job_05_cleaned.sql) must run and produce records "
                "before seeding Qdrant. Start Flink first: make flink-up submit-sql"
            )
        return has_data
    except Exception as exc:
        log.error(f"Preflight check failed with exception: {exc}")
        return False


def seed_tax_records(limit: int) -> int:
    """Embed cleaned tax records → fraud_similarity collection."""
    from agents.embed_tax_records import run as embed_records
    log.info(f"Seeding fraud_similarity collection (limit={limit})...")
    n = embed_records(limit=limit)
    log.info(f"fraud_similarity: {n} records upserted.")
    return n


def seed_regulatory_kb() -> int:
    """Embed regulatory knowledge base → regulatory_kb collection."""
    from agents.embed_regulatory_kb import run as embed_kb
    log.info("Seeding regulatory_kb collection...")
    n = embed_kb()
    log.info(f"regulatory_kb: {n} chunks upserted.")
    return n


def main():
    parser = argparse.ArgumentParser(description="Seed Qdrant vector collections")
    parser.add_argument("--kb-only",      action="store_true", help="Only seed regulatory KB")
    parser.add_argument("--records-only", action="store_true", help="Only seed tax records")
    parser.add_argument("--limit",        type=int, default=10000, help="Max tax records to embed")
    parser.add_argument("--skip-preflight", action="store_true", help="Skip MinIO bucket check")
    args = parser.parse_args()

    # ── Preflight guard ────────────────────────────────────────────────────────
    if not args.kb_only and not args.skip_preflight:
        if not preflight_check():
            sys.exit(1)

    results = {}

    # ── Seed regulatory KB ─────────────────────────────────────────────────────
    if not args.records_only:
        try:
            n = seed_regulatory_kb()
            results["regulatory_kb"] = f"OK ({n} chunks)"
        except Exception as exc:
            log.error(f"regulatory_kb seeding failed: {exc}")
            results["regulatory_kb"] = f"FAILED: {exc}"

    # ── Seed tax records ───────────────────────────────────────────────────────
    if not args.kb_only:
        try:
            n = seed_tax_records(args.limit)
            results["fraud_similarity"] = f"OK ({n} records)"
        except Exception as exc:
            log.error(f"fraud_similarity seeding failed: {exc}")
            results["fraud_similarity"] = f"FAILED: {exc}"

    # ── Summary ────────────────────────────────────────────────────────────────
    print("\n[seed_qdrant] Summary:")
    for collection, status in results.items():
        icon = "[OK]  " if status.startswith("OK") else "[FAIL]"
        print(f"  {icon}  {collection}: {status}")

    if any(s.startswith("FAILED") for s in results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
