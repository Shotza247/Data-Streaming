"""
lineage/lineage_schemas.py

Helper functions that build OpenLineage InputDataset and OutputDataset
objects for each storage type used in the platform.

Each function returns a dict conforming to the OpenLineage Dataset spec:
  {
      "namespace": "<storage-type>",
      "name":      "<dataset-identifier>",
      "facets":    { ... optional field metadata ... }
  }

Usage:
    from lineage.lineage_schemas import (
        kafka_dataset, minio_dataset, pg_dataset, mlflow_dataset
    )

    inputs  = [kafka_dataset("tax-applications")]
    outputs = [minio_dataset("raw-tax", partition="/date=2024-01-01/")]
"""

from __future__ import annotations


# ── Namespace constants ───────────────────────────────────────────────────────
# These must be consistent across ALL emitters — Marquez links nodes by
# matching namespace + name pairs across separate RunEvents.

NS_KAFKA   = "kafka"
NS_MINIO   = "minio"
NS_POSTGRES = "postgres"
NS_DUCKDB  = "duckdb"
NS_MLFLOW  = "mlflow"
NS_REDIS   = "redis"
NS_STREAMLIT = "streamlit"


# ── Dataset builder helpers ───────────────────────────────────────────────────

def kafka_dataset(topic: str, fields: list[dict] | None = None) -> dict:
    """
    OpenLineage dataset for a Kafka topic.

    Args:
        topic:  Kafka topic name (e.g. "tax-applications")
        fields: Optional list of field descriptors for schema facet
                [{"name": "application_id", "type": "STRING"}, ...]
    """
    dataset: dict = {
        "namespace": NS_KAFKA,
        "name":      topic,
        "facets":    {},
    }
    if fields:
        dataset["facets"]["schema"] = _schema_facet(fields)
    return dataset


def minio_dataset(bucket: str, path: str = "/", fields: list[dict] | None = None) -> dict:
    """
    OpenLineage dataset for a MinIO S3 path.

    Args:
        bucket: MinIO bucket name (e.g. "raw-tax", "cleaned-tax")
        path:   Sub-path within the bucket (e.g. "/date=2024-01-01/")
        fields: Optional schema field list
    """
    name = f"{bucket}{path}" if path != "/" else bucket
    dataset: dict = {
        "namespace": NS_MINIO,
        "name":      name,
        "facets": {
            "storage": {
                "_producer": "tax-analytics-platform",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/StorageDatasetFacet.json",
                "storageLayer": "s3",
                "fileFormat":   "JSON",
            }
        },
    }
    if fields:
        dataset["facets"]["schema"] = _schema_facet(fields)
    return dataset


def pg_dataset(table: str, schema: str = "public", fields: list[dict] | None = None) -> dict:
    """
    OpenLineage dataset for a PostgreSQL table.

    Args:
        table:  Table name (e.g. "enriched_tax_applications")
        schema: PG schema name (default: "public")
        fields: Optional schema field list
    """
    dataset: dict = {
        "namespace": NS_POSTGRES,
        "name":      f"{schema}.{table}",
        "facets":    {},
    }
    if fields:
        dataset["facets"]["schema"] = _schema_facet(fields)
    return dataset


def mlflow_dataset(model_name: str, version: str = "latest") -> dict:
    """
    OpenLineage dataset for an MLflow registered model.

    Args:
        model_name: Registered model name (e.g. "fraud-detector")
        version:    Model version string (e.g. "1", "latest")
    """
    return {
        "namespace": NS_MLFLOW,
        "name":      f"{model_name}@{version}",
        "facets":    {
            "storage": {
                "_producer": "tax-analytics-platform",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/StorageDatasetFacet.json",
                "storageLayer": "mlflow-model-registry",
                "fileFormat":   "sklearn",
            }
        },
    }


def duckdb_dataset(query_name: str) -> dict:
    """
    OpenLineage dataset for a DuckDB query result (virtual dataset).

    Args:
        query_name: Logical name of the DuckDB query function
                    (e.g. "get_tax_kpi_trend", "get_fraud_summary")
    """
    return {
        "namespace": NS_DUCKDB,
        "name":      query_name,
        "facets":    {},
    }


def redis_dataset(key_pattern: str) -> dict:
    """
    OpenLineage dataset for a Redis key namespace.

    Args:
        key_pattern: Redis key pattern (e.g. "tax:kpi:window:*")
    """
    return {
        "namespace": NS_REDIS,
        "name":      key_pattern,
        "facets":    {},
    }


def streamlit_dataset(tab_name: str) -> dict:
    """
    OpenLineage dataset representing a Streamlit dashboard tab (sink).

    Args:
        tab_name: Tab identifier (e.g. "tax-kpi-tab", "fraud-intelligence-tab")
    """
    return {
        "namespace": NS_STREAMLIT,
        "name":      tab_name,
        "facets":    {},
    }


# ── Internal helpers ──────────────────────────────────────────────────────────

def _schema_facet(fields: list[dict]) -> dict:
    """Build an OpenLineage SchemaDatasetFacet from a list of field descriptors."""
    return {
        "_producer": "tax-analytics-platform",
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
        "fields": fields,
    }


# ── Pre-built schema definitions for all platform datasets ───────────────────
# Importable by job wrappers for consistent schema metadata in Marquez.

TAX_APPLICATIONS_FIELDS = [
    {"name": "application_id",   "type": "STRING"},
    {"name": "customer_id",      "type": "STRING"},
    {"name": "customer_name",    "type": "STRING"},
    {"name": "email",            "type": "STRING"},
    {"name": "country",          "type": "STRING"},
    {"name": "province",         "type": "STRING"},
    {"name": "taxable_income",   "type": "DOUBLE"},
    {"name": "employment_type",  "type": "STRING"},
    {"name": "employment_status","type": "STRING"},
    {"name": "submitted_date",   "type": "DATE"},
    {"name": "tax_year",         "type": "STRING"},
    {"name": "filing_status",    "type": "STRING"},
    {"name": "is_fraud",         "type": "BOOLEAN"},
]

TAXPAYER_PROFILES_FIELDS = [
    {"name": "customer_id",               "type": "STRING"},
    {"name": "age",                       "type": "INTEGER"},
    {"name": "risk_score",                "type": "DOUBLE"},
    {"name": "historical_filings_count",  "type": "INTEGER"},
    {"name": "avg_income_3yr",            "type": "DOUBLE"},
    {"name": "flagged_previously",        "type": "BOOLEAN"},
]

FRAUD_SIGNALS_FIELDS = [
    {"name": "signal_id",      "type": "STRING"},
    {"name": "application_id", "type": "STRING"},
    {"name": "customer_id",    "type": "STRING"},
    {"name": "signal_type",    "type": "STRING"},
    {"name": "severity",       "type": "STRING"},
    {"name": "detected_at",    "type": "TIMESTAMP"},
    {"name": "description",    "type": "STRING"},
]

AUDIT_EVENTS_FIELDS = [
    {"name": "event_id",       "type": "STRING"},
    {"name": "application_id", "type": "STRING"},
    {"name": "actor",          "type": "STRING"},
    {"name": "action",         "type": "STRING"},
    {"name": "timestamp",      "type": "TIMESTAMP"},
    {"name": "metadata",       "type": "STRING"},
]

ENRICHED_APPLICATIONS_FIELDS = TAX_APPLICATIONS_FIELDS + [
    {"name": "age",                      "type": "INTEGER"},
    {"name": "risk_score",               "type": "DOUBLE"},
    {"name": "historical_filings_count", "type": "INTEGER"},
    {"name": "avg_income_3yr",           "type": "DOUBLE"},
    {"name": "flagged_previously",       "type": "BOOLEAN"},
    {"name": "processed_at",             "type": "TIMESTAMP"},
]

# ── Public namespace aliases (for callers that use NAMESPACE_* names) ─────────
NAMESPACE_KAFKA      = NS_KAFKA
NAMESPACE_MINIO      = NS_MINIO
NAMESPACE_POSTGRES   = NS_POSTGRES
NAMESPACE_DUCKDB     = NS_DUCKDB
NAMESPACE_MLFLOW     = NS_MLFLOW
NAMESPACE_REDIS      = NS_REDIS
NAMESPACE_STREAMLIT  = NS_STREAMLIT
NAMESPACE_FLINK      = "flink"

# ── Alias: postgres_dataset → pg_dataset ─────────────────────────────────────
postgres_dataset = pg_dataset


def _dataset(namespace: str, name: str, facets: dict | None = None) -> dict:
    """
    Generic dataset builder for any namespace/name combination.
    Used when no typed helper exists (e.g. mlflow model output in train scripts).
    """
    return {
        "namespace": namespace,
        "name":      name,
        "facets":    facets or {},
    }
