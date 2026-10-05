"""
mlflow/mlflow_config.py
------------------------
Centralised configuration for MLflow tracking, S3 artifact storage,
and experiment / model names used across all mlflow scripts.

Import this module in any script that uses MLflow:
    from mlflow.mlflow_config import setup_mlflow, EXPERIMENT_NAME, MODEL_NAME
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

# ── Tracking server ───────────────────────────────────────────────────────────
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001")

# ── S3 / MinIO artifact store ─────────────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
ARTIFACT_BUCKET  = os.getenv("MINIO_BUCKET_MLFLOW", "mlflow-artifacts")

# ── Experiment / model names ──────────────────────────────────────────────────
EXPERIMENT_NAME     = "tax-fraud-detection"
MODEL_NAME          = "fraud-detector"
TRAINING_STAGE      = "Staging"
PRODUCTION_STAGE    = "Production"

# ── Feature engineering config ────────────────────────────────────────────────
CATEGORICAL_FEATURES = ["province", "employment_type", "employment_status", "filing_status"]
NUMERIC_FEATURES     = ["taxable_income"]
TARGET_COLUMN        = "is_fraud"

# ── Model training defaults ────────────────────────────────────────────────────
RF_N_ESTIMATORS  = int(os.getenv("RF_N_ESTIMATORS",  "100"))
RF_MAX_DEPTH     = int(os.getenv("RF_MAX_DEPTH",     "8"))
RF_RANDOM_STATE  = int(os.getenv("RF_RANDOM_STATE",  "42"))
TEST_SIZE        = float(os.getenv("TEST_SIZE",       "0.2"))
MIN_TRAIN_ROWS   = int(os.getenv("MIN_TRAIN_ROWS",   "50"))   # skip training if fewer rows


def setup_mlflow():
    """
    Configure MLflow to use the tracking server and MinIO artifact store.
    Call this at the top of any script that logs runs.
    """
    import mlflow
    import os as _os

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    # boto3 / MLflow uses these env vars for S3-compatible artifact storage
    _os.environ["AWS_ACCESS_KEY_ID"]     = MINIO_ACCESS_KEY
    _os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_SECRET_KEY
    _os.environ["MLFLOW_S3_ENDPOINT_URL"] = MINIO_ENDPOINT

    # Create or get the experiment
    experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
    if experiment is None:
        mlflow.create_experiment(
            name=EXPERIMENT_NAME,
            artifact_location=f"s3://{ARTIFACT_BUCKET}/{EXPERIMENT_NAME}",
        )

    mlflow.set_experiment(EXPERIMENT_NAME)
    return mlflow


if __name__ == "__main__":
    mlf = setup_mlflow()
    print(f"MLflow tracking URI : {MLFLOW_TRACKING_URI}")
    print(f"Experiment          : {EXPERIMENT_NAME}")
    print(f"Artifact bucket     : s3://{ARTIFACT_BUCKET}")
    exp = mlf.get_experiment_by_name(EXPERIMENT_NAME)
    print(f"Experiment ID       : {exp.experiment_id if exp else 'not yet created'}")
