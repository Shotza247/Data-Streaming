#!/usr/bin/env python3
"""
scripts/smoke_test.py
----------------------
Integration smoke test: verifies all platform components are reachable
and contain data. Designed to run after `make seed` to confirm the
full end-to-end pipeline is operational.

Exit code 0 = all checks passed or non-critical warnings only.
Exit code 1 = at least one critical check failed.

Usage:
    python scripts/smoke_test.py
    make smoke-test
"""

import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.WARNING,   # suppress library noise
    format="%(levelname)s %(name)s %(message)s",
)

# ── Colour/icon helpers ───────────────────────────────────────────────────────
def _ok(msg):   print(f"  [PASS]  {msg}")
def _fail(msg): print(f"  [FAIL]  {msg}")
def _warn(msg): print(f"  [WARN]  {msg}")
def _head(msg): print(f"\n{msg}")


# ── Individual checks ─────────────────────────────────────────────────────────

def check_kafka() -> bool:
    _head("Kafka")
    try:
        from confluent_kafka.admin import AdminClient
        cfg = {"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")}
        client = AdminClient(cfg)
        meta   = client.list_topics(timeout=5)
        topics = [t for t in meta.topics if not t.startswith("_")]
        expected = ["tax-applications", "taxpayer-profiles", "fraud-signals", "audit-events"]
        for t in expected:
            if t in topics:
                _ok(f"topic exists: {t}")
            else:
                _fail(f"topic missing: {t}")
                return False
        return True
    except Exception as exc:
        _fail(f"Kafka unreachable: {exc}")
        return False


def check_postgres() -> bool:
    _head("PostgreSQL")
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            dbname=os.getenv("POSTGRES_DB", "taxdb"),
            user=os.getenv("POSTGRES_USER", "taxuser"),
            password=os.getenv("POSTGRES_PASSWORD", "taxpass"),
            connect_timeout=5,
        )
        cur = conn.cursor()
        tables = ["enriched_tax_applications", "tax_kpi_windows", "fraud_detections", "audit_log"]
        all_ok = True
        for t in tables:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            count = cur.fetchone()[0]
            if count > 0:
                _ok(f"table {t}: {count} rows")
            else:
                _warn(f"table {t}: 0 rows (Flink jobs may not have run yet)")
        conn.close()
        return all_ok
    except Exception as exc:
        _fail(f"PostgreSQL unreachable: {exc}")
        return False


def check_redis() -> bool:
    _head("Redis")
    try:
        import redis
        r = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            decode_responses=True,
            socket_connect_timeout=3,
        )
        r.ping()
        _ok("Redis ping OK")

        keys = list(r.scan_iter("tax:kpi:*"))
        if keys:
            _ok(f"Redis has {len(keys)} tax:kpi:* key(s)")
        else:
            _warn("Redis has no tax:kpi:* keys yet (kpi-poller may not have run)")
        return True
    except Exception as exc:
        _fail(f"Redis unreachable: {exc}")
        return False


def check_minio() -> bool:
    _head("MinIO / S3")
    try:
        from storage.minio_client import (
            bucket_has_files, BUCKET_RAW, BUCKET_CLEANED, BUCKET_MLFLOW
        )
        for bucket in [BUCKET_RAW, BUCKET_CLEANED, BUCKET_MLFLOW]:
            has = bucket_has_files(bucket)
            if has:
                _ok(f"bucket {bucket}: has files")
            else:
                _warn(f"bucket {bucket}: empty (Flink may not have run yet)")
        return True
    except Exception as exc:
        _fail(f"MinIO unreachable: {exc}")
        return False


def check_qdrant() -> bool:
    _head("Qdrant")
    try:
        from agents.qdrant_search import get_qdrant_client, COLLECTION_FRAUD, COLLECTION_KB
        client = get_qdrant_client()
        collections = [c.name for c in client.get_collections().collections]
        all_ok = True
        for col in [COLLECTION_FRAUD, COLLECTION_KB]:
            if col in collections:
                info = client.get_collection(col)
                _ok(f"collection {col}: {info.points_count} vectors")
            else:
                _warn(f"collection {col}: not yet created (run 'make seed')")
                all_ok = False
        return all_ok
    except Exception as exc:
        _fail(f"Qdrant unreachable: {exc}")
        return False


def check_marquez() -> bool:
    _head("Marquez (Data Lineage)")
    try:
        import requests
        url  = os.getenv("MARQUEZ_API_URL", "http://localhost:5000")
        resp = requests.get(f"{url}/api/v1/namespaces", timeout=5)
        if resp.status_code == 200:
            namespaces = resp.json().get("namespaces", [])
            _ok(f"Marquez reachable. Namespaces: {[ns['name'] for ns in namespaces]}")
        else:
            _warn(f"Marquez returned HTTP {resp.status_code}")

        # Run full lineage verification
        from lineage.verify_lineage import verify
        ok = verify()
        return ok
    except Exception as exc:
        _fail(f"Marquez unreachable: {exc}")
        return False


def check_mlflow() -> bool:
    _head("MLflow")
    try:
        import requests
        url  = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001")
        resp = requests.get(f"{url}/api/2.0/mlflow/experiments/list", timeout=5)
        if resp.status_code != 200:
            _fail(f"MLflow returned HTTP {resp.status_code}")
            return False
        experiments = resp.json().get("experiments", [])
        _ok(f"MLflow reachable. Experiments: {[e['name'] for e in experiments]}")

        # Check for registered fraud-detector model
        resp2 = requests.get(
            f"{url}/api/2.0/mlflow/registered-models/list", timeout=5
        )
        if resp2.status_code == 200:
            models = resp2.json().get("registered_models", [])
            model_names = [m["name"] for m in models]
            if "fraud-detector" in model_names:
                _ok("Model 'fraud-detector' registered in MLflow Model Registry")
            else:
                _warn("Model 'fraud-detector' not yet registered (run 'make train')")
        return True
    except Exception as exc:
        _fail(f"MLflow unreachable: {exc}")
        return False


def check_streamlit() -> bool:
    _head("Streamlit Dashboard")
    try:
        import requests
        resp = requests.get("http://localhost:8501/healthz", timeout=5)
        if resp.status_code == 200:
            _ok("Streamlit dashboard healthy at localhost:8501")
            return True
        else:
            _warn(f"Streamlit returned HTTP {resp.status_code}")
            return True  # non-critical
    except Exception as exc:
        _warn(f"Streamlit not reachable at localhost:8501: {exc}")
        return True  # non-critical — may be behind container network


# ── Main ──────────────────────────────────────────────────────────────────────

CHECKS = [
    ("Kafka Topics",          check_kafka,      True),   # critical
    ("PostgreSQL Tables",     check_postgres,   True),
    ("Redis KPI Cache",       check_redis,      True),
    ("MinIO Buckets",         check_minio,      True),
    ("Qdrant Collections",    check_qdrant,     False),  # non-critical (needs seeding)
    ("Marquez Lineage DAG",   check_marquez,    False),
    ("MLflow Model Registry", check_mlflow,     False),
    ("Streamlit Dashboard",   check_streamlit,  False),
]


def main():
    print(f"\n{'=' * 60}")
    print(f"  Tax Analytics Platform — Smoke Test")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 60}")

    results = {}
    for name, fn, critical in CHECKS:
        try:
            ok = fn()
        except Exception as exc:
            _fail(f"Unexpected error in {name}: {exc}")
            ok = False
        results[name] = (ok, critical)

    # ── Summary ────────────────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print("  SUMMARY")
    print(f"{'=' * 60}")

    critical_failures = []
    for name, (ok, critical) in results.items():
        icon = "[PASS]" if ok else ("[FAIL]" if critical else "[WARN]")
        print(f"  {icon}  {name}")
        if not ok and critical:
            critical_failures.append(name)

    print(f"\n  Total checks: {len(results)}")
    print(f"  Passed: {sum(1 for ok, _ in results.values() if ok)}")
    print(f"  Failed: {sum(1 for ok, crit in results.values() if not ok and crit)}")
    print(f"  Warnings: {sum(1 for ok, crit in results.values() if not ok and not crit)}")

    if critical_failures:
        print(f"\n  CRITICAL FAILURES: {critical_failures}")
        print("  Fix the above issues before running make seed or make train.")
        sys.exit(1)
    else:
        print("\n  All critical checks passed.")
        sys.exit(0)


if __name__ == "__main__":
    main()
