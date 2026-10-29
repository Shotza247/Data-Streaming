#!/usr/bin/env python3
"""
mlflow/tune_fraud_model.py
--------------------------
Hyperparameter search for the fraud-detector.

Random search over one or more model families (random forest, gradient boosting,
logistic regression). For every trial:

  * the configuration is scored with stratified k-fold CV on the TRAIN split only
  * the decision threshold is chosen from the out-of-fold predictions
    (objective: f1 | recall | precision | average_precision)
  * precision / recall / F1 / average precision at that threshold are recorded

Trial 0 of every family is the default configuration, so you can see what tuning
gains over the baseline. The best trial is finally evaluated ONCE on the held-out
test split, so the reported test numbers are not tuned on.

Everything is logged to MLflow (experiment 'tax-fraud-tuning', one parent run with
a nested run per trial) and written to a JSON result file the dashboard reads.

Usage:
    python mlflow/tune_fraud_model.py --sample-data --n-trials 12
    python mlflow/tune_fraud_model.py --families random_forest,gradient_boosting \
        --objective recall --min-precision 0.6 --n-trials 30
    python mlflow/tune_fraud_model.py --space '{"random_forest": {...}}' --out result.json

Called by: model-api POST /tune
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [tune_fraud_model] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("tune_fraud_model")


def _clean(v):
    """MLflow-friendly param value."""
    return "None" if v is None else v


def run_trial(family, params, X_train, y_train, objective, min_precision, min_recall,
              n_splits, seed):
    """Score one configuration with out-of-fold CV on the train split."""
    from sklearn.metrics import average_precision_score
    from sklearn.model_selection import StratifiedKFold, cross_val_predict
    from fraud_model import build_pipeline, objective_score, pick_threshold

    t0 = time.time()
    oof = cross_val_predict(
        build_pipeline(family, params, random_state=seed), X_train, y_train,
        cv=StratifiedKFold(n_splits, shuffle=True, random_state=seed),
        method="predict_proba", n_jobs=1,
    )[:, 1]
    pick = pick_threshold(y_train, oof, objective, min_precision, min_recall)
    ap = float(average_precision_score(y_train, oof))
    return {
        "family": family, "params": params,
        "cv_precision": pick["precision"], "cv_recall": pick["recall"], "cv_f1": pick["f1"],
        "cv_average_precision": ap, "threshold": pick["threshold"], "feasible": pick["feasible"],
        "score": objective_score(pick, ap, objective),
        "seconds": round(time.time() - t0, 2),
    }


def tune(families, space, n_trials, objective, min_precision, min_recall, cv_folds,
         n_rows, sample_data, seed, out_path=None, allow_synthetic=False, wait_seconds=0):
    import mlflow
    import numpy as np
    from fraud_model import (
        DEFAULT_PARAMS, FAMILIES, OBJECTIVES, build_pipeline, metrics_at, sample_params,
        validate_space,
    )
    from mlflow_config import TUNING_EXPERIMENT_NAME, RF_RANDOM_STATE, TEST_SIZE, setup_mlflow
    from train_fraud_model import load_data, split_data

    if objective not in OBJECTIVES:
        raise ValueError(f"Unknown objective {objective!r}; choose one of {tuple(OBJECTIVES)}")
    unknown = [f for f in families if f not in FAMILIES]
    if unknown:
        raise ValueError(f"Unknown model families: {unknown}")
    if not families:
        raise ValueError("Select at least one model family to tune")
    if n_trials < 1:
        raise ValueError("n_trials must be >= 1")
    validate_space({f: space[f] for f in families if f in space})

    setup_mlflow(TUNING_EXPERIMENT_NAME)

    df, data_source = load_data(n_rows, sample_data, allow_synthetic, wait_seconds)
    X_train, X_test, y_train, y_test = split_data(df, TEST_SIZE, RF_RANDOM_STATE)
    n_splits = int(min(cv_folds, y_train.sum(), (y_train == 0).sum()))
    if n_splits < 2:
        raise ValueError("Not enough fraud / non-fraud examples for cross-validation")
    total = len(families) * (n_trials + 1)       # +1: default config per family
    log.info(f"Tuning {families} | {n_trials} random trials + 1 default each = {total} | "
             f"{n_splits}-fold CV | objective={objective} | train={len(X_train)} test={len(X_test)}")

    rng = np.random.default_rng(seed)
    trials = []
    started = time.time()

    with mlflow.start_run(run_name=f"tuning_{objective}_{data_source}") as parent:
        mlflow.set_tags({"data_source": data_source, "objective": objective,
                         "families": ",".join(families)})
        mlflow.log_params({"n_trials": n_trials, "cv_folds": n_splits, "objective": objective,
                           "min_precision": min_precision, "min_recall": min_recall,
                           "n_train_rows": len(X_train), "n_test_rows": len(X_test), "seed": seed})
        mlflow.log_text(json.dumps({f: space.get(f, {}) for f in families}, indent=2, default=str),
                        "search_space.json")

        done = 0
        for family in families:
            candidates = [("default", dict(DEFAULT_PARAMS[family]))]
            candidates += [(f"trial {i + 1}", {**DEFAULT_PARAMS[family],
                                               **sample_params(space.get(family) or {}, rng)})
                           for i in range(n_trials)]
            for label, params in candidates:
                done += 1
                try:
                    res = run_trial(family, params, X_train, y_train, objective,
                                    min_precision, min_recall, n_splits, seed)
                except Exception as exc:                       # one bad config must not abort the search
                    log.warning(f"[{done}/{total}] {family} {label} failed: {type(exc).__name__}: {exc}")
                    res = {"family": family, "params": params, "error": f"{type(exc).__name__}: {exc}",
                           "score": float("-inf")}
                res["label"] = label
                res["is_default"] = label == "default"
                trials.append(res)

                if "error" not in res:
                    log.info(f"[{done}/{total}] {family:<19} {label:<8} score={res['score']:.3f} "
                             f"P={res['cv_precision']:.3f} R={res['cv_recall']:.3f} "
                             f"F1={res['cv_f1']:.3f} thr={res['threshold']:.2f} ({res['seconds']}s)")
                    with mlflow.start_run(run_name=f"{family}_{label.replace(' ', '_')}", nested=True):
                        mlflow.set_tag("model_family", family)
                        mlflow.log_param("model_family", family)
                        mlflow.log_params({k: _clean(v) for k, v in params.items()})
                        mlflow.log_metrics({
                            "cv_precision": res["cv_precision"], "cv_recall": res["cv_recall"],
                            "cv_f1": res["cv_f1"], "cv_average_precision": res["cv_average_precision"],
                            "threshold": res["threshold"], "score": res["score"],
                        })
                print(f"PROGRESS {done}/{total}", flush=True)

        ok = [t for t in trials if "error" not in t]
        if not ok:
            raise RuntimeError("Every trial failed; see the log above")
        ranked = sorted(ok, key=lambda t: (t["score"], t["cv_f1"]), reverse=True)
        best = ranked[0]
        best_per_family = {}
        for t in ranked:
            best_per_family.setdefault(t["family"], t)
        defaults = {t["family"]: t for t in ok if t["is_default"]}

        # Final, untouched test-split evaluation of the winner
        pipe = build_pipeline(best["family"], best["params"], random_state=seed).fit(X_train, y_train)
        test = metrics_at(y_test, pipe.predict_proba(X_test)[:, 1], best["threshold"])
        best_default_test = None
        if best["family"] in defaults:
            d = defaults[best["family"]]
            dpipe = build_pipeline(d["family"], d["params"], random_state=seed).fit(X_train, y_train)
            best_default_test = metrics_at(y_test, dpipe.predict_proba(X_test)[:, 1], d["threshold"])

        mlflow.log_metrics({"best_score": best["score"], "best_cv_precision": best["cv_precision"],
                            "best_cv_recall": best["cv_recall"], "best_cv_f1": best["cv_f1"],
                            "test_precision": test["precision"], "test_recall": test["recall"],
                            "test_f1": test["f1_score"]})
        mlflow.set_tag("best_family", best["family"])

        result = {
            "objective": objective, "min_precision": min_precision, "min_recall": min_recall,
            "data_source": data_source, "n_rows": int(len(df)), "n_train": int(len(X_train)),
            "n_test": int(len(X_test)), "cv_folds": n_splits, "n_trials": n_trials,
            "families": families, "seed": seed,
            "seconds": round(time.time() - started, 1),
            "tuning_run_id": parent.info.run_id,
            "best": best, "best_test": test, "best_default_test": best_default_test,
            "best_per_family": best_per_family,
            "trials": ranked + [t for t in trials if "error" in t],
        }
        mlflow.log_text(json.dumps(result, indent=2, default=str), "tuning_result.json")

    if out_path:
        Path(out_path).write_text(json.dumps(result, default=str))
    log.info(f"Best: {best['family']} score={best['score']:.3f} "
             f"(CV P={best['cv_precision']:.3f} R={best['cv_recall']:.3f} F1={best['cv_f1']:.3f}); "
             f"held-out test P={test['precision']:.3f} R={test['recall']:.3f} F1={test['f1_score']:.3f}")
    log.info(f"Best params: {json.dumps(best['params'], default=str)}")
    return result


def main():
    from fraud_model import DEFAULT_SPACE, FAMILIES, OBJECTIVES

    ap = argparse.ArgumentParser(description="Hyperparameter search for the fraud detector")
    ap.add_argument("--families", default=",".join(FAMILIES),
                    help=f"Comma-separated model families ({', '.join(FAMILIES)})")
    ap.add_argument("--n-trials", type=int, default=12, help="Random trials per family (+1 default)")
    ap.add_argument("--objective", choices=tuple(OBJECTIVES), default="f1")
    ap.add_argument("--min-precision", type=float, default=0.5, help="Precision floor for objective=recall")
    ap.add_argument("--min-recall", type=float, default=0.5, help="Recall floor for objective=precision")
    ap.add_argument("--cv-folds", type=int, default=5)
    ap.add_argument("--n-rows", type=int, default=20000)
    ap.add_argument("--sample-data", action="store_true", help="Tune on generated data (no MinIO)")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--space", default=None,
                    help="Search space as JSON (same shape as GET /tune/space); default space if omitted")
    ap.add_argument("--out", default=None, help="Write the full result JSON here")
    ap.add_argument("--wait", type=int, default=0, metavar="SECONDS")
    args = ap.parse_args()

    space = json.loads(args.space) if args.space else DEFAULT_SPACE
    try:
        tune([f.strip() for f in args.families.split(",") if f.strip()], space, args.n_trials,
             args.objective, args.min_precision, args.min_recall, args.cv_folds, args.n_rows,
             args.sample_data, args.seed, args.out, wait_seconds=args.wait)
    except ValueError as exc:
        log.error(str(exc))
        sys.exit(1)


if __name__ == "__main__":
    main()
