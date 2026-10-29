#!/usr/bin/env python3
"""
mlflow/serve_model.py
---------------------
Tiny HTTP inference service for the registered 'fraud-detector' model.

Runs in the ml-trainer image (the same library versions the model was trained
with), so the dashboard/agents never need mlflow or scikit-learn installed.

    GET  /health   -> {"status": "ok"|"no_model", "version": ..., "stage": ..., "threshold": 0.57}
    POST /predict  -> body {"records": [{province, taxable_income, employment_type,
                                         employment_status, filing_status,
                                         customer_id?, application_id?}, ...]}
                      reply {"fraud_probability": [0.12, ...], "predicted_fraud": [false, ...],
                             "threshold": 0.57, "version": "1", "stage": "Staging"}
                      `customer_id` lets the model use the customer's filing history.
    POST /reload   -> force-reload the newest Production/Staging model
    POST /train    -> start training in the background (one job at a time)
                      body {"n_rows": 20000, "source": "streamed" | "sample",
                            "family": "random_forest" | "gradient_boosting" | "logistic_regression",
                            "params": {...hyperparameters}, "objective": "f1" | "recall" | ...,
                            "min_precision": 0.5, "min_recall": 0.5}
    GET  /train/status -> {"state": idle|running|succeeded|failed, "log": [...], ...}

    GET  /tune/space  -> default search space, default params, families, objectives
    POST /tune        -> start a hyperparameter search in the background
                         body {"families": [...], "n_trials": 12, "objective": "f1",
                               "min_precision": 0.5, "min_recall": 0.5, "cv_folds": 5,
                               "source": "streamed" | "sample", "n_rows": 20000, "space": {...}}
    GET  /tune/status -> {"state": ..., "done": 7, "total": 39, "log": [...]}
    GET  /tune/result -> full result of the last finished search (404 if none)

Training and tuning share the CPU, so only one of them runs at a time (409 otherwise).
The newest registered version is also picked up automatically every 30 seconds,
and right after a training run succeeds.
"""

import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [model-api] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("model-api")

PORT = int(os.getenv("MODEL_API_PORT", "8000"))
RELOAD_CHECK_SECONDS = 30
MAX_TRIALS_PER_FAMILY = 100

_lock = threading.Lock()
_state = {"model": None, "version": None, "stage": None, "threshold": 0.5}

# ── Background jobs (training + tuning share one slot) ────────────────────────
MAX_LOG_LINES = 200
_train_lock = threading.Lock()
_train = {"state": "idle", "started_at": None, "finished_at": None, "exit_code": None, "log": []}
_tune = {"state": "idle", "started_at": None, "finished_at": None, "exit_code": None, "log": [],
         "done": 0, "total": 0}
_TUNE_RESULT = Path(tempfile.gettempdir()) / "fraud_tuning_result.json"
_PROGRESS = re.compile(r"^PROGRESS (\d+)/(\d+)")


def _job_running() -> bool:
    return _train["state"] == "running" or _tune["state"] == "running"


def _run_job(cmd: list, job: dict, on_success=None):
    code = -1
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            cwd=str(Path(__file__).parent), env={**os.environ, "PYTHONPATH": "/"},
        )
        for line in proc.stdout:
            line = line.rstrip()
            with _train_lock:
                m = _PROGRESS.match(line)
                if m:
                    job["done"], job["total"] = int(m.group(1)), int(m.group(2))
                else:
                    job["log"].append(line)
                    del job["log"][:-MAX_LOG_LINES]
        code = proc.wait()
    except Exception as exc:
        with _train_lock:
            job["log"].append(f"Could not run job: {type(exc).__name__}: {exc}")
    if code == 0 and on_success:
        on_success()
    with _train_lock:
        job.update(state="succeeded" if code == 0 else "failed",
                   exit_code=code, finished_at=time.time())
    log.info(f"Job finished with exit code {code}")


def _start_job(cmd: list, job: dict, on_success=None) -> bool:
    """Start `cmd` as the single background job. Returns False if one is already running."""
    with _train_lock:
        if _job_running():
            return False
        job.update(state="running", started_at=time.time(), finished_at=None,
                   exit_code=None, log=[], done=0, total=0)
    threading.Thread(target=_run_job, args=(cmd, job, on_success), daemon=True).start()
    return True


def start_training(n_rows: int, sample_data: bool, family: str = "random_forest",
                   params: dict | None = None, objective: str = "f1",
                   min_precision: float = 0.5, min_recall: float = 0.5) -> bool:
    """Start a training run. Returns False if a training/tuning job is already running."""
    cmd = [sys.executable, str(Path(__file__).parent / "train_fraud_model.py"),
           "--n-rows", str(n_rows), "--family", family, "--objective", objective,
           "--min-precision", str(min_precision), "--min-recall", str(min_recall)]
    if params:
        cmd += ["--params", json.dumps(params)]
    if sample_data:
        cmd.append("--sample-data")
    return _start_job(cmd, _train, on_success=lambda: load_model(force=True))


def start_tuning(cfg: dict) -> bool:
    """Start a hyperparameter search. Returns False if a training/tuning job is already running."""
    _TUNE_RESULT.unlink(missing_ok=True)
    cmd = [sys.executable, str(Path(__file__).parent / "tune_fraud_model.py"),
           "--families", ",".join(cfg["families"]), "--n-trials", str(cfg["n_trials"]),
           "--objective", cfg["objective"], "--min-precision", str(cfg["min_precision"]),
           "--min-recall", str(cfg["min_recall"]), "--cv-folds", str(cfg["cv_folds"]),
           "--n-rows", str(cfg["n_rows"]), "--space", json.dumps(cfg["space"]),
           "--out", str(_TUNE_RESULT)]
    if cfg["sample_data"]:
        cmd.append("--sample-data")
    return _start_job(cmd, _tune)


def _parse_tune_request(body: dict) -> dict:
    """Validate a /tune body. Raises ValueError with a readable message."""
    from fraud_model import DEFAULT_SPACE, FAMILIES, OBJECTIVES, validate_space

    families = body.get("families") or list(FAMILIES)
    bad = [f for f in families if f not in FAMILIES]
    if bad:
        raise ValueError(f"Unknown model families: {bad}")
    objective = body.get("objective") or "f1"
    if objective not in OBJECTIVES:
        raise ValueError(f"Unknown objective {objective!r}")
    space = body.get("space") or DEFAULT_SPACE
    validate_space({f: space[f] for f in families if f in space})
    n_trials = int(body.get("n_trials") or 12)
    if not 1 <= n_trials <= MAX_TRIALS_PER_FAMILY:
        raise ValueError(f"n_trials must be between 1 and {MAX_TRIALS_PER_FAMILY}")
    cv_folds = int(body.get("cv_folds") or 5)
    if not 2 <= cv_folds <= 10:
        raise ValueError("cv_folds must be between 2 and 10")
    return {
        "families": families, "objective": objective, "space": space, "n_trials": n_trials,
        "cv_folds": cv_folds, "min_precision": float(body.get("min_precision", 0.5)),
        "min_recall": float(body.get("min_recall", 0.5)),
        "n_rows": int(body.get("n_rows") or 20000), "sample_data": body.get("source") == "sample",
    }


def _best_version():
    """Newest registered version, preferring Production > Staging > None."""
    import mlflow
    from mlflow_config import MODEL_NAME, PRODUCTION_STAGE, TRAINING_STAGE

    versions = mlflow.tracking.MlflowClient().search_model_versions(f"name='{MODEL_NAME}'")
    for stage in (PRODUCTION_STAGE, TRAINING_STAGE, "None"):
        in_stage = [v for v in versions if v.current_stage == stage]
        if in_stage:
            return max(in_stage, key=lambda v: int(v.version))
    return None


def load_model(force: bool = False) -> bool:
    """Load the best model if it differs from the one in memory. True if a model is loaded."""
    import mlflow.sklearn
    from mlflow_config import MODEL_NAME, setup_mlflow

    try:
        setup_mlflow()
        v = _best_version()
        if v is None:
            return _state["model"] is not None
        if not force and _state["version"] == v.version and _state["stage"] == v.current_stage:
            return True
        model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/{v.version}")
        threshold = float(getattr(model, "decision_threshold_", 0.5))   # older versions: 0.5
        with _lock:
            _state.update(model=model, version=v.version, stage=v.current_stage, threshold=threshold)
        log.info(f"Loaded {MODEL_NAME} v{v.version} ({v.current_stage}), threshold={threshold:.3f}")
        return True
    except Exception as exc:
        log.warning(f"Model load failed: {type(exc).__name__}: {exc}")
        return _state["model"] is not None


def _reload_loop():
    while True:
        time.sleep(RELOAD_CHECK_SECONDS)
        load_model()


def predict(records: list) -> list:
    import pandas as pd
    from fraud_model import INPUT_COLUMNS
    from mlflow_config import CATEGORICAL_FEATURES, NUMERIC_FEATURES

    df = pd.DataFrame(records)
    for c in CATEGORICAL_FEATURES:
        df[c] = df[c].fillna("UNKNOWN").astype(str) if c in df else "UNKNOWN"
    for c in NUMERIC_FEATURES:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0) if c in df else 0.0
    for c in INPUT_COLUMNS:                     # customer_id / application_id are optional
        if c not in df:
            df[c] = ""
    with _lock:
        model = _state["model"]
    probs = model.predict_proba(df[INPUT_COLUMNS])[:, 1]
    return [round(float(p), 4) for p in probs]


class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, body: dict):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/health":
            loaded = _state["model"] is not None
            self._send(200 if loaded else 503, {
                "status": "ok" if loaded else "no_model",
                "version": _state["version"], "stage": _state["stage"],
                "threshold": _state["threshold"],
            })
        elif self.path == "/train/status":
            with _train_lock:
                self._send(200, {**_train, "log": list(_train["log"])})
        elif self.path == "/tune/status":
            with _train_lock:
                self._send(200, {**_tune, "log": list(_tune["log"]),
                                 "busy": _job_running() and _tune["state"] != "running"})
        elif self.path == "/tune/space":
            from fraud_model import DEFAULT_PARAMS, DEFAULT_SPACE, FAMILY_LABELS, OBJECTIVES
            self._send(200, {"space": DEFAULT_SPACE, "defaults": DEFAULT_PARAMS,
                             "families": FAMILY_LABELS, "objectives": OBJECTIVES})
        elif self.path == "/tune/result":
            if not _TUNE_RESULT.exists():
                return self._send(404, {"error": "no tuning result yet"})
            self._send(200, json.loads(_TUNE_RESULT.read_text()))
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError:
            return self._send(400, {"error": "invalid JSON"})

        if self.path == "/train":
            from fraud_model import FAMILIES, OBJECTIVES
            family = body.get("family") or "random_forest"
            objective = body.get("objective") or "f1"
            if family not in FAMILIES:
                return self._send(400, {"error": f"unknown family {family!r}", "message": f"unknown family {family!r}"})
            if objective not in OBJECTIVES:
                return self._send(400, {"error": f"unknown objective {objective!r}",
                                        "message": f"unknown objective {objective!r}"})
            started = start_training(
                int(body.get("n_rows") or 20000), body.get("source") == "sample", family,
                body.get("params") or None, objective,
                float(body.get("min_precision", 0.5)), float(body.get("min_recall", 0.5)))
            return self._send(202 if started else 409, {
                "started": started,
                "message": "started" if started else "a training or tuning job is already running"})

        if self.path == "/tune":
            try:
                cfg = _parse_tune_request(body)
            except (ValueError, TypeError, KeyError) as exc:
                return self._send(400, {"error": str(exc), "message": str(exc)})
            started = start_tuning(cfg)
            return self._send(202 if started else 409, {
                "started": started,
                "message": "started" if started else "a training or tuning job is already running"})

        if self.path == "/reload":
            ok = load_model(force=True)
            return self._send(200 if ok else 503, {"loaded": ok, "version": _state["version"]})

        if self.path == "/predict":
            if _state["model"] is None and not load_model():
                return self._send(503, {"error": "no registered fraud-detector model yet"})
            records = body.get("records") or []
            if not records:
                return self._send(400, {"error": "records is required"})
            try:
                probs = predict(records)
                threshold = _state["threshold"]
                return self._send(200, {
                    "fraud_probability": probs,
                    "predicted_fraud": [p >= threshold for p in probs],
                    "threshold": threshold,
                    "version": _state["version"], "stage": _state["stage"],
                })
            except Exception as exc:
                log.warning(f"predict failed: {exc}")
                return self._send(500, {"error": f"{type(exc).__name__}: {exc}"})

        self._send(404, {"error": "not found"})

    def log_message(self, *args):   # keep container logs quiet
        pass


if __name__ == "__main__":
    load_model()
    threading.Thread(target=_reload_loop, daemon=True).start()
    log.info(f"Serving on :{PORT}")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
