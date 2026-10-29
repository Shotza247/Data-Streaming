#!/usr/bin/env python3
"""
scripts/sync_qdrant.py
-----------------------
Keeps the Qdrant `fraud_similarity` collection in step with the streaming
pipeline. seed_qdrant.py loads a one-off snapshot; this loops forever and
embeds only the cleaned records Qdrant does not have yet, so similar-case
search covers new applications within one interval.

Usage:
    python scripts/sync_qdrant.py                 # loop, every QDRANT_SYNC_INTERVAL_SEC
    python scripts/sync_qdrant.py --once          # single pass, then exit
    python scripts/sync_qdrant.py --interval 10 --limit 2000

Runs as the `qdrant-sync` compose service (profile-free, restart: unless-stopped).
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [sync_qdrant] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("sync_qdrant")
# Model download / qdrant requests log every HTTP call at INFO; keep the service log readable.
for _noisy in ("httpx", "httpcore", "huggingface_hub", "qdrant_client"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

DEFAULT_INTERVAL = int(os.getenv("QDRANT_SYNC_INTERVAL_SEC", "30"))
DEFAULT_LIMIT    = int(os.getenv("QDRANT_SYNC_LIMIT", "5000"))
MAX_BACKOFF      = 300


def run_loop(interval: int, limit: int, once: bool, sync=None, sleep=time.sleep) -> int:
    """
    Run sync passes. A failed pass (MinIO/Qdrant not up yet) is logged and
    retried with a growing delay instead of killing the service.
    Returns the total records added (useful with --once).
    """
    if sync is None:
        from agents.embed_tax_records import sync_new as sync

    total, failures = 0, 0
    while True:
        try:
            added = sync(limit=limit, quiet=True)
            total += added
            failures = 0
            if added:
                log.info(f"Embedded {added} new record(s) (total this run: {total}).")
        except Exception as exc:
            failures += 1
            log.warning(f"Sync pass failed ({type(exc).__name__}: {exc}); retrying.")
            if once:
                raise
        if once:
            return total
        sleep(min(interval * (2 ** min(failures, 4)), MAX_BACKOFF) if failures else interval)


def main():
    ap = argparse.ArgumentParser(description="Continuously sync cleaned records into Qdrant")
    ap.add_argument("--once", action="store_true", help="Run a single pass and exit")
    ap.add_argument("--interval", type=int, default=DEFAULT_INTERVAL, help="Seconds between passes")
    ap.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Newest records examined per pass")
    args = ap.parse_args()

    log.info(f"Starting (interval={args.interval}s, limit={args.limit}, once={args.once})")
    n = run_loop(args.interval, args.limit, args.once)
    if args.once:
        print(f"Synced {n} new record(s) into Qdrant 'fraud_similarity'.")


if __name__ == "__main__":
    main()
