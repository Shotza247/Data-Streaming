#!/usr/bin/env python3
"""
scripts/submit_flink_jobs.py
----------------------------
Iterates over all .sql files in /flink-jobs/, emits OpenLineage START events,
submits each job to the running Flink SQL Client via podman exec, then emits
COMPLETE or FAIL events.

Usage:
    python scripts/submit_flink_jobs.py              # submit all jobs
    python scripts/submit_flink_jobs.py job_01       # submit specific job (substring match)

Environment variables (from .env):
    KAFKA_BOOTSTRAP_SERVERS  - substituted into SQL before submission
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
    MINIO_BUCKET_RAW, MINIO_BUCKET_CLEANED
"""

import os
import sys
import subprocess
import time
import uuid
import re
from pathlib import Path
from dotenv import load_dotenv

# ── Bootstrap ─────────────────────────────────────────────────────────────────
load_dotenv(Path(__file__).parent.parent / ".env")

# Add lineage module to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from lineage.openlineage_client import LineageClient
from lineage.lineage_schemas import (
    kafka_dataset,
    minio_dataset,
    postgres_dataset,
    NAMESPACE_KAFKA,
    NAMESPACE_MINIO,
    NAMESPACE_POSTGRES,
    NAMESPACE_FLINK,
)

# ── Config ────────────────────────────────────────────────────────────────────
FLINK_JOBS_DIR  = Path(__file__).parent.parent / "flink-jobs"
CONTAINER_CLI   = os.getenv("CONTAINER_CLI", "podman")
SQL_CLIENT_CTR  = "flink-sql-client"
MARQUEZ_URL     = os.getenv("MARQUEZ_URL", "http://localhost:5000")

lineage = LineageClient(marquez_url=MARQUEZ_URL, namespace=NAMESPACE_FLINK)

# ── Per-job lineage metadata ──────────────────────────────────────────────────
JOB_LINEAGE = {
    "job_01_raw_to_minio": {
        "inputs":  [kafka_dataset("tax-applications")],
        "outputs": [minio_dataset("raw-tax/", "raw_tax_files")],
    },
    "job_02_enrich": {
        "inputs":  [kafka_dataset("tax-applications"), kafka_dataset("taxpayer-profiles")],
        "outputs": [postgres_dataset("enriched_tax_applications")],
    },
    "job_03_kpi_windows": {
        "inputs":  [kafka_dataset("tax-applications")],
        "outputs": [postgres_dataset("tax_kpi_windows")],
    },
    "job_04_fraud_pattern": {
        "inputs":  [kafka_dataset("tax-applications")],
        "outputs": [kafka_dataset("fraud-signals")],
    },
    "job_05_cleaned": {
        "inputs":  [kafka_dataset("tax-applications")],
        "outputs": [minio_dataset("cleaned-tax/", "cleaned_tax_files")],
    },
}

# ── Environment variable substitution ─────────────────────────────────────────
ENV_VARS = {
    "KAFKA_BOOTSTRAP_SERVERS": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    "POSTGRES_HOST":           os.getenv("POSTGRES_HOST", "postgres"),
    "POSTGRES_PORT":           os.getenv("POSTGRES_PORT", "5432"),
    "POSTGRES_DB":             os.getenv("POSTGRES_DB", "taxdb"),
    "POSTGRES_USER":           os.getenv("POSTGRES_USER", "taxuser"),
    "POSTGRES_PASSWORD":       os.getenv("POSTGRES_PASSWORD", "taxpass"),
    "MINIO_BUCKET_RAW":        os.getenv("MINIO_BUCKET_RAW", "raw-tax"),
    "MINIO_BUCKET_CLEANED":    os.getenv("MINIO_BUCKET_CLEANED", "cleaned-tax"),
}

def render_sql(sql_text: str) -> str:
    """Replace ${VAR} placeholders with .env values."""
    for key, val in ENV_VARS.items():
        sql_text = sql_text.replace(f"${{{key}}}", val)
    return sql_text


def submit_job(sql_file: Path) -> bool:
    """
    Copy the rendered SQL into the flink-sql-client container via stdin
    and execute with ./bin/sql-client.sh.
    Returns True on success, False on failure.
    """
    job_name = sql_file.stem
    run_id   = str(uuid.uuid4())
    meta     = JOB_LINEAGE.get(job_name, {"inputs": [], "outputs": []})

    print(f"\n[submit] {job_name}")
    print(f"  run_id : {run_id}")

    # ── Emit START lineage event ───────────────────────────────────────────────
    lineage.start(
        job_name=job_name,
        run_id=run_id,
        inputs=meta["inputs"],
        outputs=meta["outputs"],
    )

    # ── Render SQL ─────────────────────────────────────────────────────────────
    raw_sql   = sql_file.read_text(encoding="utf-8")
    final_sql = render_sql(raw_sql)

    # Write rendered SQL to a temp file inside the container via stdin
    # Strategy: pipe the SQL to sql-client.sh via stdin (-e flag not available;
    # use a here-doc passed through bash -c).
    # We write the file into /tmp/<job>.sql inside the container first, then
    # invoke sql-client.sh -f /tmp/<job>.sql.
    tmp_path = f"/tmp/{job_name}.sql"

    # Step 1: write rendered SQL into the container
    write_cmd = [
        CONTAINER_CLI, "exec", "-i", SQL_CLIENT_CTR,
        "bash", "-c", f"cat > {tmp_path}",
    ]
    try:
        result = subprocess.run(
            write_cmd,
            input=final_sql.encode("utf-8"),
            capture_output=True,
            timeout=30,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.decode())
    except Exception as exc:
        print(f"  [ERROR] Failed to write SQL to container: {exc}")
        lineage.fail(job_name=job_name, run_id=run_id,
                     inputs=meta["inputs"], outputs=meta["outputs"])
        return False

    # Step 2: execute via sql-client.sh
    exec_cmd = [
        CONTAINER_CLI, "exec", SQL_CLIENT_CTR,
        "/opt/flink/bin/sql-client.sh", "-f", tmp_path,
    ]
    print(f"  [exec] {' '.join(exec_cmd)}")
    t0 = time.time()
    try:
        result = subprocess.run(
            exec_cmd,
            capture_output=True,
            timeout=120,   # give Flink 2 min to accept the job
        )
        elapsed = round(time.time() - t0, 2)

        stdout = result.stdout.decode("utf-8", errors="replace")
        stderr = result.stderr.decode("utf-8", errors="replace")

        if result.returncode != 0:
            print(f"  [FAIL]  exit={result.returncode}  ({elapsed}s)")
            print(f"  stderr: {stderr[:500]}")
            lineage.fail(job_name=job_name, run_id=run_id,
                         inputs=meta["inputs"], outputs=meta["outputs"])
            return False

        print(f"  [OK]    exit=0  ({elapsed}s)")
        if "Job has been submitted with JobID" in stdout:
            jid = re.search(r"JobID ([a-f0-9]+)", stdout)
            if jid:
                print(f"  Flink JobID: {jid.group(1)}")

        lineage.complete(job_name=job_name, run_id=run_id,
                         inputs=meta["inputs"], outputs=meta["outputs"])
        return True

    except subprocess.TimeoutExpired:
        print(f"  [TIMEOUT] after 120s")
        lineage.fail(job_name=job_name, run_id=run_id,
                     inputs=meta["inputs"], outputs=meta["outputs"])
        return False
    except Exception as exc:
        print(f"  [ERROR] {exc}")
        lineage.fail(job_name=job_name, run_id=run_id,
                     inputs=meta["inputs"], outputs=meta["outputs"])
        return False


def main():
    filter_str = sys.argv[1] if len(sys.argv) > 1 else None

    sql_files = sorted(FLINK_JOBS_DIR.glob("job_*.sql"))
    if filter_str:
        sql_files = [f for f in sql_files if filter_str in f.name]

    if not sql_files:
        print("[submit_flink_jobs] No matching .sql files found.")
        sys.exit(1)

    print(f"[submit_flink_jobs] Submitting {len(sql_files)} job(s) to {SQL_CLIENT_CTR}")

    results = {}
    for sql_file in sql_files:
        ok = submit_job(sql_file)
        results[sql_file.name] = "OK" if ok else "FAIL"
        if ok:
            # Brief pause between jobs — let Flink register the job before next submit
            time.sleep(3)

    print("\n[submit_flink_jobs] Summary:")
    for name, status in results.items():
        icon = "[OK]  " if status == "OK" else "[FAIL]"
        print(f"  {icon}  {name}")

    failed = [n for n, s in results.items() if s == "FAIL"]
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
