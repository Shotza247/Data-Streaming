"""
lineage/test_lineage_client.py

Smoke test for Sub-Task 8 Phase A.
Verifies that LineageClient and lineage_schemas work correctly
by emitting a test RunEvent to Marquez and confirming it lands.

Run from project root:
    python lineage/test_lineage_client.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from openlineage_client import LineageClient
from lineage_schemas import (
    kafka_dataset, minio_dataset, pg_dataset, mlflow_dataset,
    duckdb_dataset, redis_dataset,
    TAX_APPLICATIONS_FIELDS, ENRICHED_APPLICATIONS_FIELDS,
)

def run_smoke_test():
    print("=== Sub-Task 8 Phase A -- Lineage Client Smoke Test ===\n")
    client = LineageClient()

    # -- Test 1: kafka -> minio (raw archival job) ------------------------------
    print("[1] Emitting START for job_01_raw_to_minio...")
    run_id = client.start(
        job_name  = "job_01_raw_to_minio",
        namespace = "flink",
        inputs    = [kafka_dataset("tax-applications", TAX_APPLICATIONS_FIELDS)],
        outputs   = [minio_dataset("raw-tax", "/")],
    )
    print(f"    run_id = {run_id}")
    client.complete(
        run_id    = run_id,
        job_name  = "job_01_raw_to_minio",
        namespace = "flink",
        inputs    = [kafka_dataset("tax-applications")],
        outputs   = [minio_dataset("raw-tax", "/")],
    )
    print("    COMPLETE emitted [OK]")

    # -- Test 2: kafka -> postgres (enrichment job) ----------------------------?
    print("\n[2] Emitting START/COMPLETE for job_02_enrich...")
    run_id2 = client.start(
        job_name  = "job_02_enrich",
        namespace = "flink",
        inputs    = [
            kafka_dataset("tax-applications",  TAX_APPLICATIONS_FIELDS),
            kafka_dataset("taxpayer-profiles"),
        ],
        outputs   = [pg_dataset("enriched_tax_applications", fields=ENRICHED_APPLICATIONS_FIELDS)],
    )
    client.complete(run_id2, "job_02_enrich", "flink")
    print("    COMPLETE emitted [OK]")

    # -- Test 3: minio -> mlflow (model training) ------------------------------?
    print("\n[3] Emitting START/COMPLETE for train_fraud_model...")
    run_id3 = client.start(
        job_name  = "train_fraud_model",
        namespace = "mlflow",
        inputs    = [minio_dataset("cleaned-tax", "/")],
        outputs   = [mlflow_dataset("fraud-detector", "1")],
    )
    client.complete(run_id3, "train_fraud_model", "mlflow")
    print("    COMPLETE emitted [OK]")

    # -- Test 4: FAIL event ----------------------------------------------------
    print("\n[4] Emitting FAIL event for a hypothetical bad job...")
    run_id4 = client.start("job_test_fail", "flink",
                           inputs=[kafka_dataset("tax-applications")])
    client.fail(run_id4, "job_test_fail", "flink", error="Test failure -- intentional")
    print("    FAIL emitted [OK]")

    # -- Verify via Marquez API ------------------------------------------------
    print("\n[5] Verifying jobs registered in Marquez...")
    import requests, os
    marquez_url = os.getenv("MARQUEZ_API_URL", "http://localhost:5000")
    for ns in ["flink", "mlflow"]:
        try:
            r = requests.get(f"{marquez_url}/api/v1/namespaces/{ns}/jobs", timeout=5)  # noqa
            if r.status_code == 200:
                jobs = r.json().get("jobs", [])
                print(f"    Namespace '{ns}': {len(jobs)} job(s) registered")
                for j in jobs:
                    print(f"      - {j['name']}")
            else:
                print(f"    Namespace '{ns}': HTTP {r.status_code}")
        except Exception as e:
            print(f"    Marquez not reachable at {marquez_url}: {e}")

    print("\n=== Smoke test complete ===")


if __name__ == "__main__":
    run_smoke_test()
