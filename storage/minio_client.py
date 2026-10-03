"""
storage/minio_client.py
-----------------------
Thin wrapper around boto3 for MinIO S3-compatible object storage.
Used by duckdb_queries.py, mlflow training scripts, and Qdrant seeding.

Exposes:
    get_client()                         -> boto3.client
    list_files(bucket, prefix)           -> list[str]   (object keys)
    download_file(bucket, key, local)    -> Path
    download_to_bytes(bucket, key)       -> bytes
    file_exists(bucket, key)             -> bool
    bucket_has_files(bucket, prefix)     -> bool
    upload_file(local_path, bucket, key) -> None
    list_partitions(bucket, prefix)      -> list[str]   (partition folder names)

Environment variables (from .env):
    MINIO_ENDPOINT      e.g. http://minio:9000   (or http://localhost:9000 locally)
    MINIO_ACCESS_KEY
    MINIO_SECRET_KEY
"""

import os
import io
import logging
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

BUCKET_RAW     = os.getenv("MINIO_BUCKET_RAW",     "raw-tax")
BUCKET_CLEANED = os.getenv("MINIO_BUCKET_CLEANED", "cleaned-tax")
BUCKET_MLFLOW  = os.getenv("MINIO_BUCKET_MLFLOW",  "mlflow-artifacts")


def get_client():
    """
    Return a configured boto3 S3 client pointed at MinIO.
    Raises ImportError with a clear message if boto3 is missing.
    """
    try:
        import boto3
        from botocore.config import Config
    except ImportError as exc:
        raise ImportError(
            "boto3 is required: pip install boto3"
        ) from exc

    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",   # MinIO ignores this but boto3 requires it
    )


# ── File listing ──────────────────────────────────────────────────────────────

def list_files(bucket: str, prefix: str = "") -> list[str]:
    """
    Return all object keys in `bucket` under `prefix`.
    Uses paginator so it handles buckets with >1000 objects.
    """
    s3 = get_client()
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
    except Exception as exc:
        log.warning(f"list_files({bucket}, {prefix!r}) failed: {exc}")
    return keys


def list_partitions(bucket: str, prefix: str = "") -> list[str]:
    """
    Return the immediate sub-folder names (partition directories) under `prefix`.
    E.g. for cleaned-tax/dt=2024-01-15/... returns ['dt=2024-01-15', ...]
    """
    s3 = get_client()
    paginator = s3.get_paginator("list_objects_v2")
    partitions = set()
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
            for cp in page.get("CommonPrefixes", []):
                folder = cp.get("Prefix", "").rstrip("/").split("/")[-1]
                if folder:
                    partitions.add(folder)
    except Exception as exc:
        log.warning(f"list_partitions({bucket}, {prefix!r}) failed: {exc}")
    return sorted(partitions)


def bucket_has_files(bucket: str, prefix: str = "") -> bool:
    """Return True if the bucket/prefix contains at least one object."""
    s3 = get_client()
    try:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        return resp.get("KeyCount", 0) > 0
    except Exception as exc:
        log.warning(f"bucket_has_files({bucket}) failed: {exc}")
        return False


def file_exists(bucket: str, key: str) -> bool:
    """Return True if the exact object key exists."""
    s3 = get_client()
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


# ── Download ──────────────────────────────────────────────────────────────────

def download_file(bucket: str, key: str, local_path: Path) -> Path:
    """
    Download a single object to `local_path`.
    Creates parent directories as needed.
    Returns the local_path on success.
    """
    local_path = Path(local_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    s3 = get_client()
    s3.download_file(bucket, key, str(local_path))
    log.debug(f"Downloaded s3://{bucket}/{key} -> {local_path}")
    return local_path


def download_to_bytes(bucket: str, key: str) -> bytes:
    """Return the contents of an object as bytes (for in-memory processing)."""
    s3 = get_client()
    buf = io.BytesIO()
    s3.download_fileobj(bucket, key, buf)
    return buf.getvalue()


def download_prefix_to_dir(bucket: str, prefix: str, local_dir: Path) -> list[Path]:
    """
    Download all objects under `prefix` into `local_dir`.
    Preserves the key suffix as the local filename.
    Returns list of downloaded paths.
    """
    keys = list_files(bucket, prefix)
    paths = []
    for key in keys:
        # Strip the prefix, use the rest as the local filename
        rel = key[len(prefix):].lstrip("/")
        if not rel:
            continue
        local = Path(local_dir) / rel
        paths.append(download_file(bucket, key, local))
    return paths


# ── Upload ────────────────────────────────────────────────────────────────────

def upload_file(local_path: Path, bucket: str, key: str) -> None:
    """Upload a local file to `bucket` at `key`."""
    s3 = get_client()
    s3.upload_file(str(local_path), bucket, key)
    log.debug(f"Uploaded {local_path} -> s3://{bucket}/{key}")


def upload_bytes(data: bytes, bucket: str, key: str, content_type: str = "application/octet-stream") -> None:
    """Upload raw bytes to `bucket` at `key`."""
    s3 = get_client()
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
    log.debug(f"Uploaded {len(data)} bytes -> s3://{bucket}/{key}")


# ── Convenience ───────────────────────────────────────────────────────────────

def get_cleaned_file_uris(n_partitions: Optional[int] = None) -> list[str]:
    """
    Return s3a:// URIs for all JSON files in the cleaned-tax bucket,
    suitable for passing directly to DuckDB httpfs queries.
    Optionally limit to the N most recent date partitions.
    """
    partitions = list_partitions(BUCKET_CLEANED)
    if n_partitions:
        partitions = partitions[-n_partitions:]   # most recent N

    uris = []
    for part in partitions:
        for key in list_files(BUCKET_CLEANED, prefix=f"{part}/"):
            if key.endswith(".json") or key.endswith(".jsonl"):
                uris.append(f"s3a://{BUCKET_CLEANED}/{key}")
    return uris


if __name__ == "__main__":
    # Quick connectivity smoke test
    logging.basicConfig(level=logging.INFO)
    print(f"MinIO endpoint: {MINIO_ENDPOINT}")
    for bucket in [BUCKET_RAW, BUCKET_CLEANED, BUCKET_MLFLOW]:
        has_files = bucket_has_files(bucket)
        count = len(list_files(bucket))
        print(f"  s3://{bucket}: {count} objects  has_files={has_files}")
