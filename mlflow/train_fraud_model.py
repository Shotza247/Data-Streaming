#!/usr/bin/env python3
"""
mlflow/train_fraud_model.py
----------------------------
Train the fraud-detector on cleaned tax data from MinIO, log everything to the
MLflow tracking server and register the model as 'fraud-detector'.

What it does
  1. Load cleaned data (MinIO/DuckDB), or generated sample data
  2. Hold out a stratified test split
  3. Pick the decision threshold from out-of-fold predictions on the TRAIN split
     only (objective: f1 | recall | precision | average_precision), so the test
     metrics stay an honest estimate
  4. Fit on the train split, report test metrics at that threshold
  5. Log params, metrics, plots and the model; register it in 'Staging'

The model is the feature-engineered pipeline from fraud_model.py (income vs the
employment-type ceiling, customer filing count, ...). The chosen threshold is
stored on the pipeline (`decision_threshold_`) and served by model-api.

Usage:
    python mlflow/train_fraud_model.py
    python mlflow/train_fraud_model.py --family gradient_boosting --objective recall
    python mlflow/train_fraud_model.py --params '{"n_estimators": 400, "max_depth": 10}'
    python mlflow/train_fraud_model.py --sample-data          # generated data
    python mlflow/train_fraud_model.py --wait 900             # wait for MLflow + data

Only real cleaned data is used by default: with fewer than MIN_TRAIN_ROWS rows the
script exits with code 2 instead of training on synthetic data.

Called by: the ml-trainer container, model-api POST /train, make train
"""

import sys
import time
import json
import argparse
import logging
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [train_fraud_model] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("train_fraud_model")


def _retry_until(fn, wait_seconds: int, what: str, interval: int = 15):
    """Call fn() until it succeeds or wait_seconds has elapsed (then re-raise)."""
    deadline = time.time() + wait_seconds
    while True:
        try:
            return fn()
        except Exception as exc:
            if time.time() >= deadline:
                raise
            log.info(f"Waiting for {what} ({type(exc).__name__}); retrying in {interval}s...")
            time.sleep(interval)


def _load_streamed(get_df, n_rows: int, min_rows: int, wait_seconds: int):
    """Load cleaned data, polling until min_rows is reached or wait_seconds elapses."""
    deadline = time.time() + wait_seconds
    while True:
        df = get_df(n_rows=n_rows)
        if len(df) >= min_rows or time.time() >= deadline:
            return df
        log.info(f"Only {len(df)}/{min_rows} cleaned rows so far; checking again in 30s...")
        time.sleep(30)


def load_data(n_rows: int = 20000, sample_data: bool = False,
              allow_synthetic: bool = False, wait_seconds: int = 0):
    """
    Return (DataFrame, data_source) where data_source is "streamed" or "sample".
    Exits with code 2 when there is too little real data (unless allow_synthetic).
    Shared with tune_fraud_model.py so tuning and training see the same data.
    """
    from mlflow_config import MIN_TRAIN_ROWS, TARGET_COLUMN
    from fraud_model import make_sample_data

    if sample_data:
        log.info("Using generated SAMPLE data; MinIO is not read.")
        df, source = make_sample_data(n_rows=min(max(n_rows, 500), 5000)), "sample"
    else:
        from storage.duckdb_queries import get_cleaned_data_for_ml
        log.info(f"Loading up to {n_rows} cleaned records from MinIO/DuckDB...")
        df = _load_streamed(get_cleaned_data_for_ml, n_rows, MIN_TRAIN_ROWS, wait_seconds)
        source = "streamed"
        if df.empty or len(df) < MIN_TRAIN_ROWS:
            msg = (f"Not enough training data ({len(df)} rows, minimum={MIN_TRAIN_ROWS}). "
                   "Ensure the producers and Flink job_05 have written cleaned records to MinIO.")
            if not allow_synthetic:
                log.error(msg)
                sys.exit(2)
            log.warning(msg)
            log.info("Generating sample data (--allow-synthetic)...")
            df, source = make_sample_data(n_rows=2000), "sample"

    # DuckDB returns UUID columns as uuid.UUID objects, which MLflow cannot
    # serialise into the model signature / input example.
    for col in ("application_id", "customer_id"):
        if col in df.columns:
            df[col] = df[col].astype(str)
    df = df.drop_duplicates(subset="application_id", keep="last").reset_index(drop=True)
    if df[TARGET_COLUMN].astype(int).nunique() < 2:
        log.error("Training data contains a single class; need both fraud and non-fraud rows.")
        sys.exit(2)
    log.info(f"Dataset: {len(df)} rows, fraud rate={df[TARGET_COLUMN].astype(int).mean():.2%} ({source})")
    return df, source


def split_data(df, test_size: float, random_state: int):
    """Stratified (X_train, X_test, y_train, y_test) on the model's raw input columns."""
    from sklearn.model_selection import train_test_split
    from fraud_model import INPUT_COLUMNS
    from mlflow_config import TARGET_COLUMN

    cols = [c for c in INPUT_COLUMNS if c in df.columns]
    X, y = df[cols], df[TARGET_COLUMN].astype(int)
    return train_test_split(X, y, test_size=test_size, random_state=random_state, stratify=y)


def main(n_rows: int = 20000, run_name: str = "rf_tuned",
         allow_synthetic: bool = False, wait_seconds: int = 0, sample_data: bool = False,
         family: str = "random_forest", params: dict | None = None,
         objective: str = "f1", min_precision: float = 0.5, min_recall: float = 0.5):
    # ── Lazy imports ───────────────────────────────────────────────────────────
    try:
        import mlflow
        import mlflow.sklearn
        import numpy as np
        from sklearn.metrics import classification_report, confusion_matrix
        from sklearn.model_selection import StratifiedKFold, cross_val_predict
        import matplotlib
        matplotlib.use("Agg")  # headless backend
        import matplotlib.pyplot as plt
    except ImportError as exc:
        log.error(f"Missing dependency: {exc}. Run: pip install -r mlflow/requirements.txt")
        sys.exit(1)

    import fraud_model
    from fraud_model import (
        DEFAULT_PARAMS, ENGINEERED_NUMERIC, FAMILIES, OBJECTIVES,
        build_pipeline, metrics_at, pick_threshold,
    )
    from mlflow_config import (
        setup_mlflow, MODEL_NAME, TRAINING_STAGE, CATEGORICAL_FEATURES,
        RF_RANDOM_STATE, TEST_SIZE,
    )
    import mlflow_config

    if family not in FAMILIES:
        log.error(f"Unknown model family {family!r}; choose one of {FAMILIES}")
        sys.exit(1)
    if objective not in OBJECTIVES:
        log.error(f"Unknown objective {objective!r}; choose one of {tuple(OBJECTIVES)}")
        sys.exit(1)

    # ── Setup MLflow ───────────────────────────────────────────────────────────
    _retry_until(setup_mlflow, wait_seconds, "MLflow tracking server")
    log.info("MLflow experiment ready.")

    # ── Data ───────────────────────────────────────────────────────────────────
    df, data_source = load_data(n_rows, sample_data, allow_synthetic, wait_seconds)
    X_train, X_test, y_train, y_test = split_data(df, TEST_SIZE, RF_RANDOM_STATE)
    log.info(f"Train: {len(X_train)} ({int(y_train.sum())} fraud), "
             f"Test: {len(X_test)} ({int(y_test.sum())} fraud)")

    merged = {**DEFAULT_PARAMS[family], **(params or {})}
    log.info(f"Model: {family}  params={merged}  objective={objective}")

    # ── Threshold from out-of-fold predictions on the train split ───────────────
    n_splits = int(min(5, y_train.sum(), (y_train == 0).sum()))
    if n_splits >= 2:
        log.info(f"Selecting decision threshold with {n_splits}-fold out-of-fold predictions...")
        oof = cross_val_predict(
            build_pipeline(family, merged), X_train, y_train,
            cv=StratifiedKFold(n_splits, shuffle=True, random_state=RF_RANDOM_STATE),
            method="predict_proba", n_jobs=1,
        )[:, 1]
        pick = pick_threshold(y_train, oof, objective, min_precision, min_recall)
        if not pick["feasible"]:
            log.warning(f"Objective '{objective}' constraint could not be met on the train folds; "
                        "using the F1-optimal threshold instead.")
    else:
        log.warning("Too few examples per class for cross-validation; using threshold 0.5.")
        pick = {"threshold": 0.5, "precision": 0.0, "recall": 0.0, "f1": 0.0, "feasible": False}
    threshold = float(pick["threshold"])
    log.info(f"Decision threshold: {threshold:.3f} "
             f"(train CV: precision={pick['precision']:.3f} recall={pick['recall']:.3f} f1={pick['f1']:.3f})")

    # ── Fit + evaluate on the held-out test split ──────────────────────────────
    pipeline = build_pipeline(family, merged)
    pipeline.fit(X_train, y_train)
    pipeline.decision_threshold_ = threshold          # travels with the pickled model
    probs = pipeline.predict_proba(X_test)[:, 1]

    final = metrics_at(y_test, probs, threshold)
    at_half = metrics_at(y_test, probs, 0.5)
    y_pred = (probs >= threshold).astype(int)
    cm = confusion_matrix(y_test, y_pred, labels=[0, 1])

    log.info(f"Accuracy : {final['accuracy']:.4f}")
    log.info(f"Precision: {final['precision']:.4f}")
    log.info(f"Recall   : {final['recall']:.4f}")
    log.info(f"F1 Score : {final['f1_score']:.4f}   (F1 at a fixed 0.5 threshold: {at_half['f1_score']:.4f})")

    # ── Log to MLflow ──────────────────────────────────────────────────────────
    with mlflow.start_run(run_name=f"{run_name}_{data_source}") as run:
        run_id = run.info.run_id
        log.info(f"MLflow run_id: {run_id}")
        mlflow.set_tag("data_source", data_source)   # "sample" or "streamed"
        mlflow.set_tag("model_family", family)
        mlflow.set_tag("objective", objective)

        mlflow.log_params({
            "model_family":         family,
            **{k: ("None" if v is None else v) for k, v in merged.items()},
            "objective":            objective,
            "threshold":            round(threshold, 4),
            "random_state":         RF_RANDOM_STATE,
            "test_size":            TEST_SIZE,
            "n_train_rows":         len(X_train),
            "n_test_rows":          len(X_test),
            "categorical_features": str(CATEGORICAL_FEATURES),
            "numeric_features":     str(ENGINEERED_NUMERIC),
        })
        mlflow.log_metrics({
            "accuracy":          final["accuracy"],
            "precision":         final["precision"],
            "recall":            final["recall"],
            "f1_score":          final["f1_score"],
            "average_precision": final["average_precision"],
            "threshold":         threshold,
            "cv_precision":      pick["precision"],
            "cv_recall":         pick["recall"],
            "cv_f1":             pick["f1"],
            "f1_at_default":     at_half["f1_score"],
            "recall_at_default": at_half["recall"],
            "fraud_rate":        float(y_train.mean()),
        })

        fig, ax = plt.subplots(figsize=(5, 4))
        ax.imshow(cm, cmap="Blues")
        ax.set_xticks([0, 1], ["legit", "fraud"])
        ax.set_yticks([0, 1], ["legit", "fraud"])
        ax.set_xlabel("Predicted")
        ax.set_ylabel("Actual")
        ax.set_title(f"Confusion Matrix — threshold {threshold:.2f}")
        for i in range(cm.shape[0]):
            for j in range(cm.shape[1]):
                ax.text(j, i, str(cm[i, j]), ha="center", va="center", fontsize=12)
        plt.tight_layout()
        mlflow.log_figure(fig, "confusion_matrix.png")
        plt.close(fig)

        mlflow.log_text(
            classification_report(y_test, y_pred, target_names=["legit", "fraud"], zero_division=0),
            "classification_report.txt",
        )
        mlflow.log_text(json.dumps({"family": family, "params": merged, "objective": objective,
                                    "threshold": threshold}, indent=2, default=str), "config.json")

        mlflow.sklearn.log_model(
            sk_model=pipeline,
            artifact_path="fraud-detector-model",
            registered_model_name=MODEL_NAME,
            input_example=X_test.head(5),
            code_paths=[fraud_model.__file__, mlflow_config.__file__],
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

    _emit_lineage(run_id=run_id, accuracy=final["accuracy"], f1=final["f1_score"])

    log.info("Training complete.")
    return {
        "run_id":    run_id,
        "accuracy":  final["accuracy"],
        "precision": final["precision"],
        "recall":    final["recall"],
        "f1_score":  final["f1_score"],
        "threshold": threshold,
    }


def _emit_lineage(run_id: str, accuracy: float, f1: float):
    """Emit OpenLineage RunEvent for the training run."""
    try:
        import uuid as _uuid
        from lineage.openlineage_client import LineageClient
        from lineage.lineage_schemas import minio_dataset, NAMESPACE_MLFLOW

        client = LineageClient(namespace="mlflow")
        ol_run_id = str(_uuid.uuid4())

        from lineage.lineage_schemas import _dataset
        mlflow_output = _dataset(namespace=NAMESPACE_MLFLOW, name="fraud-detector", facets={})

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
        log.info("OpenLineage event emitted for training run.")
    except Exception as exc:
        log.warning(f"OpenLineage emit failed (non-fatal): {exc}")


if __name__ == "__main__":
    from fraud_model import FAMILIES, OBJECTIVES

    parser = argparse.ArgumentParser(description="Train fraud detection model")
    parser.add_argument("--n-rows",   type=int, default=20000, help="Max training rows from MinIO")
    parser.add_argument("--run-name", type=str, default="rf_tuned", help="MLflow run name")
    parser.add_argument("--allow-synthetic", action="store_true",
                        help="Train on sample data if real data is insufficient (demo only)")
    parser.add_argument("--wait", type=int, default=0, metavar="SECONDS",
                        help="Wait up to this long for MLflow and enough cleaned data")
    parser.add_argument("--sample-data", action="store_true",
                        help="Train on generated sample data without reading MinIO (for testing)")
    parser.add_argument("--family", choices=FAMILIES, default="random_forest", help="Model family")
    parser.add_argument("--params", type=str, default="{}",
                        help='Hyperparameters as JSON, e.g. \'{"n_estimators": 400}\'')
    parser.add_argument("--objective", choices=tuple(OBJECTIVES), default="f1",
                        help="What the decision threshold optimises")
    parser.add_argument("--min-precision", type=float, default=0.5,
                        help="Precision floor for objective=recall")
    parser.add_argument("--min-recall", type=float, default=0.5,
                        help="Recall floor for objective=precision")
    args = parser.parse_args()

    try:
        extra_params = json.loads(args.params)
    except json.JSONDecodeError as exc:
        parser.error(f"--params is not valid JSON: {exc}")

    metrics = main(n_rows=args.n_rows, run_name=args.run_name,
                   allow_synthetic=args.allow_synthetic, wait_seconds=args.wait,
                   sample_data=args.sample_data, family=args.family, params=extra_params,
                   objective=args.objective, min_precision=args.min_precision,
                   min_recall=args.min_recall)
    print("\nTraining complete:")
    for k, v in metrics.items():
        print(f"  {k}: {v}")
