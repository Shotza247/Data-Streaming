#!/usr/bin/env python3
"""
scripts/kpi_to_redis.py
------------------------
Lightweight polling consumer that reads the latest rows from the PostgreSQL
tax_kpi_windows table (written by Flink job_03) and caches them in Redis as
hash keys with a 15-minute TTL.

Redis key schema:
    tax:kpi:window:<window_start_iso>   (HASH)
        window_start        YYYY-MM-DDTHH:MM:SS
        window_end          YYYY-MM-DDTHH:MM:SS
        total_count         integer
        avg_income          float (2 dp)
        distinct_customers  integer
        fraud_count         integer
        fraud_rate          float (4 dp)
        top_province        string

Usage:
    python scripts/kpi_to_redis.py          # runs forever, polls every POLL_INTERVAL_SEC
    python scripts/kpi_to_redis.py --once   # single poll and exit (useful for tests)

Environment (from .env):
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
    REDIS_HOST, REDIS_PORT
    KPI_POLL_INTERVAL_SEC   (default: 30)
    KPI_WINDOW_LOOKBACK     (default: 20  -- how many recent windows to cache)
    KPI_TTL_SECONDS         (default: 900 -- 15 minutes)
"""

import os
import sys
import time
import signal
import logging
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

# ── Lazy imports — give a clear error if deps missing ─────────────────────────
try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("[kpi_to_redis] ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)

try:
    import redis as redis_lib
except ImportError:
    print("[kpi_to_redis] ERROR: redis not installed. Run: pip install redis")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────
PG_HOST    = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT    = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB      = os.getenv("POSTGRES_DB", "taxdb")
PG_USER    = os.getenv("POSTGRES_USER", "taxuser")
PG_PASS    = os.getenv("POSTGRES_PASSWORD", "taxpass")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

POLL_INTERVAL = int(os.getenv("KPI_POLL_INTERVAL_SEC", "30"))
LOOKBACK      = int(os.getenv("KPI_WINDOW_LOOKBACK", "20"))
TTL_SECONDS   = int(os.getenv("KPI_TTL_SECONDS", "900"))   # 15 minutes

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [kpi_to_redis] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("kpi_to_redis")

# ── Graceful shutdown ─────────────────────────────────────────────────────────
_running = True

def _handle_sigterm(signum, frame):
    global _running
    log.info("SIGTERM received, shutting down gracefully...")
    _running = False

signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT,  _handle_sigterm)

# ── DB helpers ────────────────────────────────────────────────────────────────
def get_pg_connection():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS,
        connect_timeout=5,
    )


def fetch_latest_windows(conn, n: int) -> list[dict]:
    """Return the N most recent KPI windows from PostgreSQL."""
    sql = """
        SELECT
            window_start,
            window_end,
            total_count,
            avg_income,
            distinct_customers,
            fraud_count,
            fraud_rate,
            top_province
        FROM tax_kpi_windows
        ORDER BY window_start DESC
        LIMIT %s
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (n,))
        return [dict(row) for row in cur.fetchall()]


# ── Redis helpers ─────────────────────────────────────────────────────────────
def get_redis_client():
    return redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def write_to_redis(r: redis_lib.Redis, windows: list[dict]) -> int:
    """
    Write each KPI window as a Redis HASH.
    Returns count of keys written.
    """
    pipe = r.pipeline(transaction=False)
    count = 0
    for w in windows:
        # Serialise datetimes to ISO strings
        ws = w["window_start"]
        if isinstance(ws, datetime):
            ws = ws.isoformat()

        we = w["window_end"]
        if isinstance(we, datetime):
            we = we.isoformat()

        key = f"tax:kpi:window:{ws}"

        pipe.hset(key, mapping={
            "window_start":       str(ws),
            "window_end":         str(we),
            "total_count":        str(w.get("total_count", 0)),
            "avg_income":         f"{w.get('avg_income') or 0:.2f}",
            "distinct_customers": str(w.get("distinct_customers", 0)),
            "fraud_count":        str(w.get("fraud_count", 0)),
            "fraud_rate":         f"{w.get('fraud_rate') or 0:.4f}",
            "top_province":       str(w.get("top_province") or "UNKNOWN"),
        })
        pipe.expire(key, TTL_SECONDS)
        count += 1

    pipe.execute()
    return count


def also_write_summary(r: redis_lib.Redis, windows: list[dict]):
    """
    Write a compact summary key tax:kpi:latest — the single most recent window —
    for quick dashboard reads that don't need full history.
    """
    if not windows:
        return

    latest = windows[0]  # already sorted DESC
    ws = latest["window_start"]
    if isinstance(ws, datetime):
        ws = ws.isoformat()

    r.hset("tax:kpi:latest", mapping={
        "window_start":       str(ws),
        "total_count":        str(latest.get("total_count", 0)),
        "avg_income":         f"{latest.get('avg_income') or 0:.2f}",
        "fraud_rate":         f"{latest.get('fraud_rate') or 0:.4f}",
        "top_province":       str(latest.get("top_province") or "UNKNOWN"),
        "updated_at":         datetime.now(timezone.utc).isoformat(),
    })
    r.expire("tax:kpi:latest", TTL_SECONDS)


# ── Poll loop ─────────────────────────────────────────────────────────────────
def poll_once() -> bool:
    """
    One poll iteration.
    Returns True if data was found and written, False otherwise.
    """
    try:
        pg_conn = get_pg_connection()
    except Exception as exc:
        log.warning(f"PostgreSQL connection failed: {exc}")
        return False

    try:
        windows = fetch_latest_windows(pg_conn, LOOKBACK)
    except Exception as exc:
        log.warning(f"Query failed: {exc}")
        pg_conn.close()
        return False
    finally:
        pg_conn.close()

    if not windows:
        log.info("tax_kpi_windows table is empty — no data to cache yet.")
        return False

    try:
        r = get_redis_client()
        r.ping()
    except Exception as exc:
        log.warning(f"Redis connection failed: {exc}")
        return False

    written = write_to_redis(r, windows)
    also_write_summary(r, windows)
    log.info(f"Cached {written} KPI window(s) in Redis (TTL={TTL_SECONDS}s)")
    return True


def main():
    once_mode = "--once" in sys.argv

    log.info(f"Starting kpi_to_redis poller  poll={POLL_INTERVAL}s  lookback={LOOKBACK}  ttl={TTL_SECONDS}s")
    log.info(f"PostgreSQL: {PG_HOST}:{PG_PORT}/{PG_DB}")
    log.info(f"Redis:      {REDIS_HOST}:{REDIS_PORT}")

    if once_mode:
        log.info("--once mode: single poll then exit")
        ok = poll_once()
        sys.exit(0 if ok else 1)

    while _running:
        poll_once()
        # Sleep in short increments so SIGTERM is handled promptly
        for _ in range(POLL_INTERVAL):
            if not _running:
                break
            time.sleep(1)

    log.info("kpi_to_redis stopped.")


if __name__ == "__main__":
    main()
