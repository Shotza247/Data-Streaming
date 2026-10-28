#!/usr/bin/env python3
"""
mlflow/serve_model.py
---------------------
Tiny HTTP inference service for the registered 'fraud-detector' model.

Runs in the ml-trainer image (the same library versions the model was trained
with), so the dashboard/agents never need mlflow or scikit-learn installed.

    GET  /health   -> {"status": "ok"|"no_model", "version": ..., "stage": ...}
    POST /predict  -> body {"records": [{province, taxable_income, employment_type,
                                         employment_status, filing_status}, ...]}
                      reply {"fraud_probability": [0.12, ...], "version": "1", "stage": "Staging"}
    POST /reload   -> force-reload the newest Production/Staging model
    POST /train    -> start training in the background (one at a time)
                      body {"n_rows": 20000, "source": "streamed" | "sample"}
    GET  /train/status -> {"state": idle|running|succeeded|failed, "log": [...], ...}

The newest registered version is also picked up automatically every 30 seconds,
and right after a training run succeeds.
"""

import json
import logging
import os
import subprocess
import sys
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

_lock = threading.Lock()
_state = {"model": None, "version": None, "stage": None}

# ── Background training job ───────────────────────────────────────────────────
MAX_LOG_LINES = 200
_train_lock = threading.Lock()
_train = {"state": "idle", "started_at": None, "finished_at": None, "exit_code": None, "log": []}


def _run_training(cmd: list):
    code = -1
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            cwd=str(Path(__file__).parent), env={**os.environ, "PYTHONPATH": "/"},
        )
        for line in proc.stdout:
            with _train_lock:
                _train["log"].append(line.rstrip())
                del _train["log"][:-MAX_LOG_LINES]
        code = proc.wait()
    except Exception as exc:
        with _train_lock:
            _train["log"].append(f"Could not run trainer: {type(exc).__name__}: {exc}")
    if code == 0:
        load_model(force=True)          # serve the new model immediately
    with _train_lock:
        _train.update(state="succeeded" if code == 0 else "failed",
                      exit_code=code, finished_at=time.time())
    log.info(f"Training finished with exit code {code}")


def start_training(n_rows: int, sample_data: bool) -> bool:
    """Start a training run. Returns False if one is already running."""
    with _train_lock:
        if _train["state"] == "running":
            return False
        _train.update(state="running", started_at=time.time(), finished_at=None,
                      exit_code=None, log=[])
    cmd = [sys.executable, str(Path(__file__).parent / "train_fraud_model.py"),
           "--n-rows", str(n_rows)]
    if sample_data:
        cmd.append("--sample-data")
    threading.Thread(target=_run_training, args=(cmd,), daemon=True).start()
    return True


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
        with _lock:
            _state.update(model=model, version=v.version, stage=v.current_stage)
        log.info(f"Loaded {MODEL_NAME} v{v.version} ({v.current_stage})")
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
    from mlflow_config import CATEGORICAL_FEATURES, NUMERIC_FEATURES

    df = pd.DataFrame(records)
    for c in CATEGORICAL_FEATURES:
        df[c] = df[c].fillna("UNKNOWN").astype(str) if c in df else "UNKNOWN"
    for c in NUMERIC_FEATURES:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0) if c in df else 0.0
    with _lock:
        model = _state["model"]
    probs = model.predict_proba(df[CATEGORICAL_FEATURES + NUMERIC_FEATURES])[:, 1]
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
            })
        elif self.path == "/train/status":
            with _train_lock:
                self._send(200, {**_train, "log": list(_train["log"])})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError:
            return self._send(400, {"error": "invalid JSON"})

        if self.path == "/train":
            n_rows = int(body.get("n_rows") or 20000)
            started = start_training(n_rows, body.get("source") == "sample")
            return self._send(202 if started else 409,
                              {"started": started, "message": "started" if started else "already running"})

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
                return self._send(200, {
                    "fraud_probability": predict(records),
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
