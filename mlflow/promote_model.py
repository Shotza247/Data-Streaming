#!/usr/bin/env python3
"""
mlflow/promote_model.py
------------------------
Promote the latest version of the 'fraud-detector' model in the MLflow
Model Registry from 'Staging' to 'Production'. Archives the previous
Production version.

Usage:
    python mlflow/promote_model.py
    python mlflow/promote_model.py --version 3   # promote a specific version
    python mlflow/promote_model.py --dry-run      # print what would happen without doing it
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
    format="%(asctime)s [promote_model] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("promote_model")


def promote(version: int | None = None, dry_run: bool = False):
    try:
        import mlflow
    except ImportError as exc:
        log.error(f"mlflow not installed: {exc}")
        sys.exit(1)

    from mlflow_config import setup_mlflow, MODEL_NAME, TRAINING_STAGE, PRODUCTION_STAGE

    setup_mlflow()
    client = mlflow.tracking.MlflowClient()

    # ── Find the version to promote ────────────────────────────────────────────
    if version is not None:
        target_version = str(version)
    else:
        # Find the latest Staging version
        staging_versions = [
            v for v in client.search_model_versions(f"name='{MODEL_NAME}'")
            if v.current_stage == TRAINING_STAGE
        ]
        if not staging_versions:
            log.error(
                f"No model versions in '{TRAINING_STAGE}' stage for '{MODEL_NAME}'. "
                "Run train_fraud_model.py first."
            )
            sys.exit(1)
        latest_staging = max(staging_versions, key=lambda v: int(v.version))
        target_version = latest_staging.version

    log.info(f"Model: {MODEL_NAME}  |  Version: {target_version}")
    log.info(f"Promoting from '{TRAINING_STAGE}' to '{PRODUCTION_STAGE}'...")

    if dry_run:
        log.info(f"[DRY RUN] Would promote v{target_version} to {PRODUCTION_STAGE}")
        return

    # ── Archive existing Production versions ───────────────────────────────────
    production_versions = [
        v for v in client.search_model_versions(f"name='{MODEL_NAME}'")
        if v.current_stage == PRODUCTION_STAGE
    ]
    for prod_v in production_versions:
        if prod_v.version != target_version:
            client.transition_model_version_stage(
                name=MODEL_NAME,
                version=prod_v.version,
                stage="Archived",
            )
            log.info(f"Archived previous Production version: v{prod_v.version}")

    # ── Promote ────────────────────────────────────────────────────────────────
    client.transition_model_version_stage(
        name=MODEL_NAME,
        version=target_version,
        stage=PRODUCTION_STAGE,
        archive_existing_versions=True,
    )
    log.info(f"Model '{MODEL_NAME}' v{target_version} is now in '{PRODUCTION_STAGE}'.")

    # ── Print current state ────────────────────────────────────────────────────
    print(f"\n[promote_model] Current model versions for '{MODEL_NAME}':")
    for v in sorted(
        client.search_model_versions(f"name='{MODEL_NAME}'"),
        key=lambda x: int(x.version)
    ):
        print(f"  v{v.version}  stage={v.current_stage}  run_id={v.run_id[:8]}...")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Promote fraud-detector to Production")
    parser.add_argument("--version", type=int, default=None, help="Version number to promote")
    parser.add_argument("--dry-run", action="store_true", help="Print what would happen without doing it")
    args = parser.parse_args()
    promote(version=args.version, dry_run=args.dry_run)
