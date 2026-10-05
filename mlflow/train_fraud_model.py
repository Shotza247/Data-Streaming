#!/usr/bin/env python3
"""
mlflow/train_fraud_model.py
----------------------------
Train a RandomForestClassifier fraud detection model on cleaned tax data
from MinIO, log all artifacts to the MLflow tracking server, and register
the model in the MLflow Model Registry under the name 'fraud-detector'.

Steps:
  1. Load cleaned data from MinIO/DuckDB (get_cleaned_data_for_ml)
  2. Feature engineering: one-hot encode categorical fields, scale numeric
  3. Train RandomForestClassifier
  4. Evaluate: accuracy, precision, recall, F1, confusion matrix
  5. Log parameters, metrics, model, and confusion matrix to MLflow
  6. Register model in Model Registry with stage 'Staging'
  7. Emit OpenLineage RunEvent (input: minio cleaned-tax, output: MLflow model)

Usage:
    python mlflow/train_fraud_model.py
    python mlflow/train_fraud_model.py --n-rows 5000

Called by: make seed (after Qdrant seeding)
"""

import sys
import argparse
import logging
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [train_fraud_model] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("train_fraud_model")


def main(n_rows: int = 20000, run_name: str = "rf_baseline"):
    # ── Lazy imports ───────────────────────────────────────────────────────────
    try:
        import mlflow
        import mlflow.sklearn
        import pandas as pd
        import numpy as np
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.model_selection import train_test_split
        from sklearn.preprocessing import OneHotEncoder
        from sklearn.pipeline import Pipeline
        from sklearn.compose import ColumnTransformer
        from sklearn.metrics import (
            accuracy_score, precision_score, recall_score,
            f1_score, confusion_matrix, classification_report,
        )
        import matplotlib
        matplotlib.use("Agg")  # headless backend
        import matplotlib.pyplot as plt
    except ImportError as exc:
        log.error(f"Missing dependency: {exc}. Run: pip install -r mlflow/requirements.txt")
        sys.exit(1)

    from mlflow_config import (
        setup_mlflow, MODEL_NAME, TRAINING_STAGE,
        CATEGORICAL_FEATURES, NUMERIC_FEATURES, TARGET_COLUMN,
        RF_N_ESTIMATORS, RF_MAX_DEPTH, RF_RANDOM_STATE, TEST_SIZE, MIN_TRAIN_ROWS,
    )
    from storage.duckdb_queries import get_cleaned_data_for_ml

    # ── Setup MLflow ───────────────────────────────────────────────────────────
    setup_mlflow()
    log.info(f"MLflow experiment ready.")

    # ── Load data ──────────────────────────────────────────────────────────────
    log.info(f"Loading up to {n_rows} cleaned records from MinIO/DuckDB...")
    df = get_cleaned_data_for_ml(n_rows=n_rows)

    if df.empty or len(df) < MIN_TRAIN_ROWS:
        log.warning(
            f"Not enough training data ({len(df)} rows, minimum={MIN_TRAIN_ROWS}). "
            "Ensure Flink job_05 has written cleaned records to MinIO."
        )
        # Generate synthetic demo data so training can proceed
        log.info("Generating synthetic demo data for training demonstration...")
        df = _generate_synthetic_data(n_rows=500)

    log.info(f"Training dataset: {len(df)} rows, fraud rate={df[TARGET_COLUMN].mean():.2%}")

    # ── Feature engineering ────────────────────────────────────────────────────
    X = df[CATEGORICAL_FEATURES + NUMERIC_FEATURES]
    y = df[TARGET_COLUMN].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RF_RANDOM_STATE, stratify=y
    )
    log.info(f"Train: {len(X_train)}, Test: {len(X_test)}")

    # Pipeline: one-hot encode categoricals, pass numeric as-is
    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), CATEGORICAL_FEATURES),
            ("num", "passthrough", NUMERIC_FEATURES),
        ]
    )

    pipeline = Pipeline([
        ("preprocessor", preprocessor),
        ("classifier",   RandomForestClassifier(
            n_estimators=RF_N_ESTIMATORS,
            max_depth=RF_MAX_DEPTH,
            random_state=RF_RANDOM_STATE,
            n_jobs=-1,
            class_weight="balanced",   # handle fraud/non-fraud imbalance
        )),
    ])

    # ── Train and evaluate ─────────────────────────────────────────────────────
    log.info(f"Training RandomForest (n_estimators={RF_N_ESTIMATORS}, max_depth={RF_MAX_DEPTH})...")
    pipeline.fit(X_train, y_train)

    y_pred      = pipeline.predict(X_test)
    y_pred_prob = pipeline.predict_proba(X_test)[:, 1]

    accuracy  = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, zero_division=0)
    recall    = recall_score(y_test, y_pred, zero_division=0)
    f1        = f1_score(y_test, y_pred, zero_division=0)
    cm        = confusion_matrix(y_test, y_pred)

    log.info(f"Accuracy : {accuracy:.4f}")
    log.info(f"Precision: {precision:.4f}")
    log.info(f"Recall   : {recall:.4f}")
    log.info(f"F1 Score : {f1:.4f}")

    # ── Log to MLflow ──────────────────────────────────────────────────────────
    with mlflow.start_run(run_name=run_name) as run:
        run_id = run.info.run_id
        log.info(f"MLflow run_id: {run_id}")

        # Parameters
        mlflow.log_params({
            "n_estimators":       RF_N_ESTIMATORS,
            "max_depth":          RF_MAX_DEPTH,
            "random_state":       RF_RANDOM_STATE,
            "test_size":          TEST_SIZE,
            "n_train_rows":       len(X_train),
            "n_test_rows":        len(X_test),
            "categorical_features": str(CATEGORICAL_FEATURES),
            "numeric_features":   str(NUMERIC_FEATURES),
        })

        # Metrics
        mlflow.log_metrics({
            "accuracy":   accuracy,
            "precision":  precision,
            "recall":     recall,
            "f1_score":   f1,
            "fraud_rate": float(y.mean()),
        })

        # Confusion matrix plot
        fig, ax = plt.subplots(figsize=(5, 4))
        ax.imshow(cm, cmap="Blues")
        ax.set_xlabel("Predicted")
        ax.set_ylabel("Actual")
        ax.set_title("Confusion Matrix — Fraud Detector")
        for i in range(cm.shape[0]):
            for j in range(cm.shape[1]):
                ax.text(j, i, str(cm[i, j]), ha="center", va="center", fontsize=12)
        plt.tight_layout()
        mlflow.log_figure(fig, "confusion_matrix.png")
        plt.close(fig)

        # Classification report as text artifact
        report_text = classification_report(y_test, y_pred, target_names=["legit", "fraud"])
        mlflow.log_text(report_text, "classification_report.txt")

        # Log the sklearn pipeline model
        input_example = X_test.head(5)
        mlflow.sklearn.log_model(
            sk_model=pipeline,
            artifact_path="fraud-detector-model",
            registered_model_name=MODEL_NAME,
            input_example=input_example,
        )
        log.info(f"Model logged and registered as '{MODEL_NAME}'")

    # ── Transition to Staging ─────────────────────────────────────────────────
    client = mlflow.tracking.MlflowClient()
    versions = client.search_model_versions(f"name='{MODEL_NAME}'")
    if versions:
        latest_version = max(versions, key=lambda v: int(v.version))
        client.transition_model_version_stage(
            name=MODEL_NAME,
            version=latest_version.version,
            stage=TRAINING_STAGE,
            archive_existing_versions=False,
        )
        log.info(f"Model v{latest_version.version} transitioned to '{TRAINING_STAGE}'")

    # ── OpenLineage emit ───────────────────────────────────────────────────────
    _emit_lineage(run_id=run_id, accuracy=accuracy, f1=f1)

    log.info("Training complete.")
    return {
        "run_id":    run_id,
        "accuracy":  accuracy,
        "precision": precision,
        "recall":    recall,
        "f1_score":  f1,
    }


def _emit_lineage(run_id: str, accuracy: float, f1: float):
    """Emit OpenLineage RunEvent for the training run."""
    try:
        import uuid as _uuid
        from lineage.openlineage_client import LineageClient
        from lineage.lineage_schemas import minio_dataset, NAMESPACE_MINIO, NAMESPACE_MLFLOW

        client = LineageClient(namespace="mlflow")
        ol_run_id = str(_uuid.uuid4())

        # Custom output dataset for mlflow model
        from lineage.lineage_schemas import _dataset
        mlflow_output = _dataset(
            namespace=NAMESPACE_MLFLOW,
            name="fraud-detector",
            facets={},
        )

        client.start(
            job_name="train_fraud_model",
            run_id=ol_run_id,
            inputs=[minio_dataset("cleaned-tax/", "cleaned_tax_files")],
            outputs=[mlflow_output],
        )
        client.complete(
            job_name="train_fraud_model",
            run_id=ol_run_id,
            inputs=[minio_dataset("cleaned-tax/", "cleaned_tax_files")],
            outputs=[mlflow_output],
        )
        log.info(f"OpenLineage event emitted for training run.")
    except Exception as exc:
        log.warning(f"OpenLineage emit failed (non-fatal): {exc}")


def _generate_synthetic_data(n_rows: int = 500):
    """
    Generate synthetic labelled tax records for demo training when
    MinIO cleaned data is not yet available.
    """
    import pandas as pd
    import numpy as np

    rng = np.random.default_rng(42)
    provinces = ["GAUTENG", "WESTERN CAPE", "KWAZULU-NATAL", "EASTERN CAPE", "LIMPOPO"]
    emp_types = ["SALARIED", "SELF_EMPLOYED", "CONTRACT", "UNEMPLOYED"]
    emp_stats = ["ACTIVE", "INACTIVE", "RETIRED"]
    filings   = ["INDIVIDUAL", "COMPANY", "TRUST", "PARTNERSHIP"]

    incomes = rng.lognormal(mean=11.5, sigma=1.0, size=n_rows).clip(10000, 5000000)
    is_fraud = (
        (incomes > 800000) & (rng.random(n_rows) < 0.4)
    ) | (rng.random(n_rows) < 0.05)

    return pd.DataFrame({
        "application_id":   [f"SYNTH-{i:06d}" for i in range(n_rows)],
        "customer_id":      [f"CUST-{rng.integers(1, 5000):05d}" for _ in range(n_rows)],
        "province":         rng.choice(provinces, n_rows),
        "taxable_income":   incomes,
        "employment_type":  rng.choice(emp_types, n_rows),
        "employment_status":rng.choice(emp_stats, n_rows),
        "filing_status":    rng.choice(filings, n_rows),
        "is_fraud":         is_fraud,
    })


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train fraud detection model")
    parser.add_argument("--n-rows",   type=int, default=20000, help="Max training rows from MinIO")
    parser.add_argument("--run-name", type=str, default="rf_baseline", help="MLflow run name")
    args = parser.parse_args()

    metrics = main(n_rows=args.n_rows, run_name=args.run_name)
    print("\nTraining complete:")
    for k, v in metrics.items():
        print(f"  {k}: {v}")
