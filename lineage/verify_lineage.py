#!/usr/bin/env python3
"""
lineage/verify_lineage.py
--------------------------
Calls the Marquez API and prints a summary of all registered jobs and datasets.
Used as part of `make smoke-test` to confirm the full lineage DAG is populated.

Usage:
    python lineage/verify_lineage.py
    python lineage/verify_lineage.py --namespace flink
"""

import sys
import argparse
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")
sys.path.insert(0, str(Path(__file__).parent.parent))

import requests

MARQUEZ_URL = os.getenv("MARQUEZ_API_URL", "http://localhost:5000")
NAMESPACES  = ["flink", "mlflow", "kafka", "minio", "postgres", "duckdb", "redis"]


def get_namespaces() -> list[str]:
    try:
        r = requests.get(f"{MARQUEZ_URL}/api/v1/namespaces", timeout=5)
        if r.status_code == 200:
            return [ns["name"] for ns in r.json().get("namespaces", [])]
    except Exception as exc:
        print(f"[verify_lineage] WARNING: Could not reach Marquez at {MARQUEZ_URL}: {exc}")
    return []


def get_jobs(namespace: str) -> list[dict]:
    try:
        r = requests.get(f"{MARQUEZ_URL}/api/v1/namespaces/{namespace}/jobs", timeout=5)
        if r.status_code == 200:
            return r.json().get("jobs", [])
    except Exception:
        pass
    return []


def get_datasets(namespace: str) -> list[dict]:
    try:
        r = requests.get(f"{MARQUEZ_URL}/api/v1/namespaces/{namespace}/datasets", timeout=5)
        if r.status_code == 200:
            return r.json().get("datasets", [])
    except Exception:
        pass
    return []


def verify(target_namespace: str | None = None) -> bool:
    print(f"\n[verify_lineage] Marquez API: {MARQUEZ_URL}")
    print("=" * 60)

    live_namespaces = get_namespaces()
    if not live_namespaces:
        print("[FAIL] Could not connect to Marquez or no namespaces registered.")
        return False

    print(f"[OK] Marquez reachable. Registered namespaces: {live_namespaces}")
    print()

    namespaces_to_check = [target_namespace] if target_namespace else NAMESPACES
    total_jobs     = 0
    total_datasets = 0
    all_ok         = True

    for ns in namespaces_to_check:
        jobs     = get_jobs(ns)
        datasets = get_datasets(ns)
        total_jobs     += len(jobs)
        total_datasets += len(datasets)

        if jobs or datasets:
            print(f"  namespace: {ns}")
            for j in jobs:
                latest_run = j.get("latestRun", {}) or {}
                state      = (latest_run.get("state") or "NO_RUN").upper()
                print(f"    [job]     {j['name']}  (latest_run={state})")
            for d in datasets:
                print(f"    [dataset] {d['name']}")
            print()

    print("─" * 60)
    print(f"[verify_lineage] Total jobs: {total_jobs}  |  Total datasets: {total_datasets}")

    # ── Minimum expectations ───────────────────────────────────────────────────
    expected_jobs = [
        ("flink", "job_01_raw_to_minio"),
        ("flink", "job_02_enrich"),
        ("flink", "job_03_kpi_windows"),
        ("flink", "job_04_fraud_pattern"),
        ("flink", "job_05_cleaned"),
        ("mlflow", "train_fraud_model"),
    ]

    missing = []
    for ns, job_name in expected_jobs:
        ns_jobs = get_jobs(ns)
        if not any(j["name"] == job_name for j in ns_jobs):
            missing.append(f"{ns}/{job_name}")

    if missing:
        print(f"\n[WARN] Expected jobs not yet registered: {missing}")
        print("       Run 'make submit-sql' and 'make train' to populate lineage.")
        all_ok = False
    else:
        print("\n[OK] All expected Flink and MLflow lineage jobs registered.")

    return all_ok


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verify Marquez lineage DAG")
    parser.add_argument("--namespace", type=str, default=None, help="Check a specific namespace only")
    args = parser.parse_args()
    ok = verify(target_namespace=args.namespace)
    sys.exit(0 if ok else 1)
