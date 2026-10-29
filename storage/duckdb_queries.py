"""
storage/duckdb_queries.py
--------------------------
Named DuckDB query functions used by the Streamlit dashboard, MLflow training,
and Qdrant seeding scripts. DuckDB reads directly from the MinIO cleaned-tax
bucket via the httpfs extension pointed at the MinIO S3 endpoint.

All functions return pandas DataFrames.

Exposed functions:
    get_connection()                        -> duckdb.DuckDBPyConnection
    get_tax_kpi_trend(n_windows)            -> DataFrame (time-series KPI windows)
    get_fraud_summary()                     -> DataFrame (fraud by type/severity/province)
    get_cleaned_data_for_ml(n_rows)         -> DataFrame (labelled records for ML training)
    get_income_distribution()               -> DataFrame (income bracket histogram data)
    get_top_flagged_customers(top_n)        -> DataFrame (customers by fraud signal count)
    get_province_fraud_heatmap()            -> DataFrame (province-level fraud metrics)

Environment variables (from .env):
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
    MINIO_BUCKET_CLEANED    (default: cleaned-tax)
"""

import os
import logging
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_CLEANED   = os.getenv("MINIO_BUCKET_CLEANED", "cleaned-tax")

# Strip http:// / https:// prefix for DuckDB s3_endpoint setting
_endpoint_host = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
_use_ssl       = MINIO_ENDPOINT.startswith("https://")

# Path inside MinIO where Flink job_05 writes cleaned JSON files.
# The Flink filesystem sink names files "part-<uuid>-<n>-<n>" (no extension),
# partitioned as dt=YYYY-MM-DD/. Match "part-*" to skip the empty _SUCCESS markers.
CLEANED_GLOB = f"s3://{BUCKET_CLEANED}/dt=*/part-*"

# The Flink sink produces many small files (one per partition per checkpoint),
# and DuckDB opens one HTTP connection per file. Reading all of them exhausts
# ephemeral ports, so queries read only the newest N dt= partitions.
RECENT_PARTITIONS = int(os.getenv("CLEANED_RECENT_PARTITIONS", "7"))

# Flink writes newline-delimited JSON. Declaring the format avoids per-file
# schema merging: union_by_name over hundreds of tiny files runs DuckDB 2.x out
# of memory (12 GiB) even though the data is under 1 MB.
_JSON_OPTS = "format='newline_delimited'"


def _cleaned_source() -> str:
    """
    Return the SQL table expression for the cleaned layer, limited to the
    newest RECENT_PARTITIONS partitions. Falls back to the full glob if the
    partition listing fails.
    """
    try:
        import boto3
        endpoint = MINIO_ENDPOINT if "://" in MINIO_ENDPOINT else f"http://{MINIO_ENDPOINT}"
        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name="us-east-1",
        )
        prefixes = []
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=BUCKET_CLEANED, Delimiter="/"
        ):
            prefixes += [p["Prefix"] for p in page.get("CommonPrefixes", [])]
        newest = sorted(prefixes)[-RECENT_PARTITIONS:]
        if newest:
            globs = [f"s3://{BUCKET_CLEANED}/{p}part-*" for p in newest]
            return f"read_json_auto({globs!r}, {_JSON_OPTS})"
    except Exception as exc:
        log.warning(f"Partition listing failed, falling back to full glob: {exc}")
    return f"read_json_auto('{CLEANED_GLOB}', {_JSON_OPTS})"


# ── Arrival-ordered reads ─────────────────────────────────────────────────────
# `dt=` is the application's submitted_date, which the producer draws from the
# last 365 days, so the "newest dt partitions" hold only a few percent of the
# records that actually arrived recently. Training and Qdrant sync therefore
# select files by when they were written (S3 LastModified) instead.

_FILE_LIST_TTL = 20          # seconds; listing 100k+ objects takes ~15 s
_file_cache: dict = {"at": 0.0, "files": []}


def _list_part_files() -> list:
    """(key, last_modified) of every cleaned part file, newest first (briefly cached)."""
    import time
    import boto3
    if time.time() - _file_cache["at"] < _FILE_LIST_TTL and _file_cache["files"]:
        return _file_cache["files"]
    endpoint = MINIO_ENDPOINT if "://" in MINIO_ENDPOINT else f"http://{MINIO_ENDPOINT}"
    s3 = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=MINIO_ACCESS_KEY,
                      aws_secret_access_key=MINIO_SECRET_KEY, region_name="us-east-1")
    files = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET_CLEANED):
        files += [(o["Key"], o["LastModified"]) for o in page.get("Contents", [])
                  if "/part-" in o["Key"]]
    files.sort(key=lambda f: f[1], reverse=True)
    _file_cache.update(at=time.time(), files=files)
    return files


def _select_files(files: list, since=None, max_files: int = 5000) -> list:
    """Newest `max_files` of `files` (newest first), optionally only those written after `since`."""
    if since is not None:
        files = [f for f in files if f[1] > since]
    return files[:max_files]




# ── DuckDB connection factory ─────────────────────────────────────────────────

def get_connection():
    """
    Return a new DuckDB in-process connection configured for MinIO httpfs access.
    The caller is responsible for closing it.
    """
    try:
        import duckdb
    except ImportError as exc:
        raise ImportError("duckdb not installed: pip install duckdb") from exc

    con = duckdb.connect(database=":memory:")

    # Install and load httpfs (bundled with DuckDB >=0.8)
    con.execute("INSTALL httpfs; LOAD httpfs;")

    # Point DuckDB at MinIO
    con.execute(f"SET s3_endpoint='{_endpoint_host}';")
    con.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
    con.execute(f"SET s3_use_ssl={'true' if _use_ssl else 'false'};")
    con.execute("SET s3_url_style='path';")   # MinIO requires path-style

    return con


# ── Helper: safe query execution ──────────────────────────────────────────────

def _run_query(sql: str, empty_columns: list[str] | None = None):
    """
    Execute `sql` against a fresh DuckDB connection.
    Returns a pandas DataFrame. On failure returns an empty DataFrame with
    `empty_columns` column names if provided.
    """
    try:
        import pandas as pd
        con = get_connection()
        df = con.execute(sql).df()
        con.close()
        return df
    except Exception as exc:
        log.warning(f"DuckDB query failed: {exc}")
        try:
            import pandas as pd
            return pd.DataFrame(columns=empty_columns or [])
        except ImportError:
            return []


# ── Public query functions ────────────────────────────────────────────────────

def get_tax_kpi_trend(n_windows: int = 24):
    """
    Return a time-series DataFrame of windowed KPI aggregates from the
    cleaned MinIO layer.

    Columns: window_start, total_count, avg_income, fraud_count, fraud_rate

    Used by: dashboard Tab 1 trend line chart.
    Lineage: minio://cleaned-tax → dashboard:tax_kpi_trend
    """
    sql = f"""
        SELECT
            DATE_TRUNC('minute',
                CAST(flink_processed_at AS TIMESTAMPTZ)
                - (EXTRACT(MINUTE FROM CAST(flink_processed_at AS TIMESTAMPTZ))::INT % 5) * INTERVAL '1 minute'
            )                          AS window_start,
            COUNT(*)                   AS total_count,
            AVG(taxable_income)        AS avg_income,
            SUM(CASE WHEN is_fraud = TRUE THEN 1 ELSE 0 END) AS fraud_count,
            AVG(CASE WHEN is_fraud = TRUE THEN 1.0 ELSE 0.0 END) AS fraud_rate
        FROM {_cleaned_source()}
        WHERE flink_processed_at IS NOT NULL
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT {n_windows}
    """
    return _run_query(sql, ["window_start", "total_count", "avg_income", "fraud_count", "fraud_rate"])


def get_fraud_summary():
    """
    Aggregate fraud signals by signal_type, severity, and province.

    Columns: signal_type, severity, province, count

    Used by: dashboard Tab 2 Fraud Intelligence bar chart.
    Lineage: minio://cleaned-tax → dashboard:fraud_summary
    """
    sql = f"""
        SELECT
            employment_type                           AS signal_type,
            CASE
                WHEN is_fraud = TRUE AND taxable_income > 500000 THEN 'HIGH'
                WHEN is_fraud = TRUE                              THEN 'MEDIUM'
                ELSE                                                   'LOW'
            END                                        AS severity,
            province,
            COUNT(*)                                   AS count
        FROM {_cleaned_source()}
        WHERE is_fraud = TRUE
        GROUP BY 1, 2, 3
        ORDER BY count DESC
        LIMIT 200
    """
    return _run_query(sql, ["signal_type", "severity", "province", "count"])


def get_cleaned_data_for_ml(n_rows: int = 50000):
    """
    Return a clean DataFrame of labelled tax application records for ML training.
    Reads the most recently written files (by arrival), so training sees the
    stream as it is now rather than a few submitted_date partitions.

    Columns: application_id, customer_id, province, taxable_income,
             employment_type, employment_status, filing_status, is_fraud

    Used by: mlflow/train_fraud_model.py
    Lineage: minio://cleaned-tax → mlflow:training_dataset
    """
    try:
        # Each part file holds ~1.2 rows, so n_rows files covers n_rows records.
        paths = [f"s3://{BUCKET_CLEANED}/{k}" for k, _ in _select_files(_list_part_files(), None, int(n_rows))]
        source = f"read_json_auto({paths!r}, {_JSON_OPTS})" if paths else _cleaned_source()
    except Exception as exc:
        log.warning(f"Arrival-ordered file listing failed, using recent partitions: {exc}")
        source = _cleaned_source()
    sql = f"""
        SELECT
            application_id,
            customer_id,
            COALESCE(province, 'UNKNOWN')         AS province,
            COALESCE(taxable_income, 0.0)          AS taxable_income,
            COALESCE(employment_type, 'UNKNOWN')  AS employment_type,
            COALESCE(employment_status, 'UNKNOWN') AS employment_status,
            COALESCE(filing_status, 'UNKNOWN')     AS filing_status,
            COALESCE(is_fraud, false)              AS is_fraud
        FROM {source}
        WHERE application_id IS NOT NULL
          AND taxable_income >= 0
        LIMIT {int(n_rows)}
    """
    return _run_query(
        sql,
        ["application_id", "customer_id", "province", "taxable_income",
         "employment_type", "employment_status", "filing_status", "is_fraud"],
    )


def get_recent_events(n_rows: int = 50):
    """
    Newest cleaned events (most recent first) for live model scoring.

    Columns: application_id, customer_id, province, taxable_income,
             employment_type, employment_status, filing_status, is_fraud,
             flink_processed_at

    Used by: dashboard MLflow tab "Live scoring".
    """
    sql = f"""
        SELECT
            application_id,
            customer_id,
            COALESCE(province, 'UNKNOWN')          AS province,
            COALESCE(taxable_income, 0.0)           AS taxable_income,
            COALESCE(employment_type, 'UNKNOWN')   AS employment_type,
            COALESCE(employment_status, 'UNKNOWN') AS employment_status,
            COALESCE(filing_status, 'UNKNOWN')     AS filing_status,
            COALESCE(is_fraud, false)               AS is_fraud,
            flink_processed_at
        FROM {_cleaned_source()}
        WHERE application_id IS NOT NULL
        ORDER BY flink_processed_at DESC
        LIMIT {int(n_rows)}
    """
    return _run_query(
        sql,
        ["application_id", "customer_id", "province", "taxable_income", "employment_type",
         "employment_status", "filing_status", "is_fraud", "flink_processed_at"],
    )


def get_recent_arrivals(since=None, max_files: int = 5000):
    """
    Records from the most recently written cleaned files (by arrival time).

    Returns (DataFrame, newest_last_modified). Pass the previous
    `newest_last_modified` as `since` to read only what arrived after it.
    Columns as get_recent_events().

    Used by: agents/embed_tax_records.sync_new (continuous Qdrant sync).
    """
    columns = ["application_id", "customer_id", "province", "taxable_income", "employment_type",
               "employment_status", "filing_status", "is_fraud", "flink_processed_at"]
    files = _select_files(_list_part_files(), since, max_files)
    if not files:
        import pandas as pd
        return pd.DataFrame(columns=columns), since
    paths = [f"s3://{BUCKET_CLEANED}/{k}" for k, _ in files]
    sql = f"""
        SELECT
            application_id,
            customer_id,
            COALESCE(province, 'UNKNOWN')          AS province,
            COALESCE(taxable_income, 0.0)           AS taxable_income,
            COALESCE(employment_type, 'UNKNOWN')   AS employment_type,
            COALESCE(employment_status, 'UNKNOWN') AS employment_status,
            COALESCE(filing_status, 'UNKNOWN')     AS filing_status,
            COALESCE(is_fraud, false)               AS is_fraud,
            flink_processed_at
        FROM read_json_auto({paths!r}, {_JSON_OPTS})
        WHERE application_id IS NOT NULL
    """
    return _run_query(sql, columns), files[0][1]


def get_income_distribution(n_buckets: int = 20):
    """
    Return income bracket distribution for the dashboard histogram.

    Columns: bucket_label, count, min_income, max_income

    Used by: dashboard Tab 1 income histogram.
    Lineage: minio://cleaned-tax → dashboard:income_histogram
    """
    sql = f"""
        WITH bounds AS (
            SELECT
                MIN(taxable_income) AS mn,
                MAX(taxable_income) AS mx
            FROM {_cleaned_source()}
            WHERE taxable_income IS NOT NULL AND taxable_income >= 0
        ),
        bucketed AS (
            SELECT
                FLOOR((taxable_income - mn) / NULLIF((mx - mn), 0) * {n_buckets}) AS bucket_idx,
                taxable_income,
                mn,
                mx
            FROM {_cleaned_source()}
            CROSS JOIN bounds
            WHERE taxable_income IS NOT NULL AND taxable_income >= 0
        )
        SELECT
            CAST(ROUND(mn + bucket_idx * (mx - mn) / {n_buckets}) AS BIGINT)       AS min_income,
            CAST(ROUND(mn + (bucket_idx + 1) * (mx - mn) / {n_buckets}) AS BIGINT) AS max_income,
            CONCAT(
                CAST(ROUND(mn + bucket_idx * (mx - mn) / {n_buckets} / 1000) AS BIGINT),
                'k-',
                CAST(ROUND(mn + (bucket_idx + 1) * (mx - mn) / {n_buckets} / 1000) AS BIGINT),
                'k'
            )                                                                        AS bucket_label,
            COUNT(*)                                                                 AS count
        FROM bucketed
        GROUP BY bucket_idx, mn, mx
        ORDER BY bucket_idx
    """
    return _run_query(sql, ["bucket_label", "count", "min_income", "max_income"])


def get_top_flagged_customers(top_n: int = 10):
    """
    Return the top N customers by fraud signal count.

    Columns: customer_id, fraud_count, avg_income, provinces

    Used by: dashboard Tab 2 top flagged customers table.
    Lineage: minio://cleaned-tax → dashboard:top_flagged_customers
    """
    sql = f"""
        SELECT
            customer_id,
            COUNT(*)                                    AS fraud_count,
            ROUND(AVG(taxable_income), 2)               AS avg_income,
            STRING_AGG(DISTINCT province, ', ')         AS provinces
        FROM {_cleaned_source()}
        WHERE is_fraud = TRUE
        GROUP BY customer_id
        ORDER BY fraud_count DESC
        LIMIT {top_n}
    """
    return _run_query(sql, ["customer_id", "fraud_count", "avg_income", "provinces"])


def get_province_fraud_heatmap():
    """
    Return province-level fraud metrics for the heatmap visualisation.

    Columns: province, total_applications, fraud_count, fraud_rate, avg_income

    Used by: dashboard Tab 2 province fraud heatmap.
    Lineage: minio://cleaned-tax → dashboard:province_heatmap
    """
    sql = f"""
        SELECT
            COALESCE(province, 'UNKNOWN')                AS province,
            COUNT(*)                                      AS total_applications,
            SUM(CASE WHEN is_fraud = TRUE THEN 1 ELSE 0 END) AS fraud_count,
            ROUND(
                AVG(CASE WHEN is_fraud = TRUE THEN 1.0 ELSE 0.0 END),
                4
            )                                             AS fraud_rate,
            ROUND(AVG(taxable_income), 2)                 AS avg_income
        FROM {_cleaned_source()}
        GROUP BY province
        ORDER BY fraud_count DESC
    """
    return _run_query(sql, ["province", "total_applications", "fraud_count", "fraud_rate", "avg_income"])


# ── Smoke test ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("DuckDB storage layer smoke test")
    print(f"  MinIO endpoint : {MINIO_ENDPOINT}")
    print(f"  Cleaned bucket : {BUCKET_CLEANED}")
    print(f"  Glob pattern   : {CLEANED_GLOB}")
    print()

    tests = [
        ("get_tax_kpi_trend(5)",       lambda: get_tax_kpi_trend(5)),
        ("get_fraud_summary()",         get_fraud_summary),
        ("get_cleaned_data_for_ml(100)", lambda: get_cleaned_data_for_ml(100)),
        ("get_income_distribution(10)", lambda: get_income_distribution(10)),
        ("get_top_flagged_customers(5)", lambda: get_top_flagged_customers(5)),
        ("get_province_fraud_heatmap()", get_province_fraud_heatmap),
    ]

    for name, fn in tests:
        try:
            df = fn()
            print(f"  [OK]  {name} -> {len(df)} rows, cols={list(df.columns)}")
        except Exception as e:
            print(f"  [FAIL] {name}: {e}")
