"""
dashboard/tabs/tab_control.py
─────────────────────────────────────────────────────────────────────────────
Platform Control Center tab.

Provides:
  • Live health grid for all 12 platform services
  • Producer controls  — start / stop / restart each of the 4 producers
  • Flink job controls — view, cancel, and resubmit SQL jobs
  • One-click pipeline actions (Seed Qdrant, Train MLflow, Run Smoke Test)
  • Live log viewer for any container

Backend strategy — dual-mode container management:
  1. Podman REST API  via mounted UNIX socket /run/podman.sock  (preferred)
     Uses requests + unix socket adapter (requests-unixsocket2 or urllib)
  2. Podman/Docker CLI subprocess fallback — works on the host machine when
     running the dashboard outside of a container.
  Both modes are tried transparently; all errors produce inline messages
  rather than exceptions.
"""

import os
import subprocess
import time
import json
import requests
import streamlit as st
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
CONTAINER_CLI   = os.getenv("CONTAINER_CLI", "podman")
FLINK_API       = os.getenv("FLINK_API_URL",  "http://flink-jobmanager:8081")
FLINK_API_HOST  = "http://localhost:8081"          # always try host-side too
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
PODMAN_SOCK     = os.getenv("PODMAN_SOCK", "/run/podman.sock")

# Map of display name → container name
PRODUCERS = {
    "Tax Applications":    "producer-tax-applications",
    "Taxpayer Profiles":   "producer-taxpayer-profiles",
    "Fraud Signals":       "producer-fraud-signals",
    "Audit Events":        "producer-audit-events",
}

# All platform services with health check strategy
SERVICES = {
    "Kafka":           {"container": "kafka",            "port": 9094,  "proto": "tcp"},
    "Zookeeper":       {"container": "zookeeper",        "port": 2181,  "proto": "tcp"},
    "PostgreSQL":      {"container": "postgres",         "port": 5432,  "proto": "tcp"},
    "Redis":           {"container": "redis",            "port": 6379,  "proto": "tcp"},
    "MinIO":           {"container": "minio",            "port": 9001,  "proto": "http", "path": "/minio/health/live"},
    "Qdrant":          {"container": "qdrant",           "port": 6333,  "proto": "http", "path": "/healthz"},
    "Flink JM":        {"container": "flink-jobmanager", "port": 8081,  "proto": "http", "path": "/overview"},
    "Marquez":         {"container": "marquez",          "port": 5000,  "proto": "http", "path": "/api/v1/namespaces"},
    "MLflow":          {"container": "mlflow",           "port": 5001,  "proto": "http", "path": "/api/2.0/mlflow/experiments/list"},
    "Kafka UI":        {"container": "kafka-ui",         "port": 8080,  "proto": "http", "path": "/"},
    "Streamlit":       {"container": "streamlit",        "port": 8501,  "proto": "http", "path": "/healthz"},
    "KPI Poller":      {"container": "kpi-poller",       "port": None,  "proto": "container"},
}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers — Podman REST API (socket) + CLI subprocess fallback
# ─────────────────────────────────────────────────────────────────────────────

# ── Podman REST API via UNIX socket ──────────────────────────────────────────
def _podman_api(method: str, path: str, body: dict = None, timeout: int = 8):
    """
    Call the Podman REST API via the mounted UNIX socket.
    Returns (ok: bool, response_json or error_str).
    Uses requests with a custom UnixSocketHTTPAdapter when available,
    otherwise falls back to urllib directly.
    """
    sock = PODMAN_SOCK
    if not os.path.exists(sock):
        return False, f"Podman socket not found at {sock}"
    try:
        import urllib.request, urllib.error
        import http.client, socket

        class UnixHTTPConnection(http.client.HTTPConnection):
            def __init__(self, sock_path):
                super().__init__("localhost")
                self.sock_path = sock_path
            def connect(self):
                s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                s.connect(self.sock_path)
                self.sock = s

        conn = UnixHTTPConnection(sock)
        body_bytes = json.dumps(body).encode() if body else None
        headers = {"Content-Type": "application/json"} if body_bytes else {}
        conn.request(method, f"/v4.0.0{path}", body=body_bytes, headers=headers)
        resp = conn.getresponse()
        raw  = resp.read().decode("utf-8", errors="replace")
        try:
            return True, json.loads(raw) if raw else {}
        except Exception:
            return True, raw
    except Exception as exc:
        return False, str(exc)


def _container_action(container: str, action: str) -> tuple[bool, str]:
    """
    Perform start | stop | restart | kill on a named container.
    Tries Podman REST API first, falls back to CLI subprocess.
    action: "start" | "stop" | "restart" | "kill"
    """
    # Map action → Podman API path
    api_paths = {
        "start":   f"/containers/{container}/start",
        "stop":    f"/containers/{container}/stop",
        "restart": f"/containers/{container}/restart",
        "kill":    f"/containers/{container}/kill",
    }
    path = api_paths.get(action)
    if path:
        ok, result = _podman_api("POST", path)
        if ok:
            return True, f"{action} sent via Podman API"
        # API failed — fall through to CLI

    # CLI fallback
    code, out, err = _run([CONTAINER_CLI, action, container])
    if code == 0:
        return True, f"{action} sent via CLI"
    return False, err[:200] or out[:200]


def _container_logs(container: str, tail: int = 50) -> str:
    """Fetch container logs via Podman REST API or CLI."""
    ok, result = _podman_api("GET", f"/containers/{container}/logs?stderr=true&stdout=true&tail={tail}")
    if ok and isinstance(result, str) and result:
        return result
    # CLI fallback
    _, out, err = _run([CONTAINER_CLI, "logs", "--tail", str(tail), container], timeout=10)
    return (out or "") + (err or "") or "(no output)"


def _run(cmd: list, timeout: int = 15) -> tuple[int, str, str]:
    """Run a CLI command, return (returncode, stdout, stderr)."""
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=timeout)
        return r.returncode, r.stdout.decode("utf-8", errors="replace"), r.stderr.decode("utf-8", errors="replace")
    except FileNotFoundError:
        return -1, "", f"{cmd[0]} not found"
    except subprocess.TimeoutExpired:
        return -2, "", "timed out"
    except Exception as exc:
        return -3, "", str(exc)


def _container_state(name: str) -> str:
    """Return container state string via Podman API then CLI fallback."""
    # Try Podman REST API first (works inside containers with socket mount)
    ok, result = _podman_api("GET", f"/containers/{name}/json")
    if ok and isinstance(result, dict):
        return result.get("State", {}).get("Status", "unknown").lower()
    # CLI fallback
    code, out, _ = _run([CONTAINER_CLI, "inspect", "--format", "{{.State.Status}}", name])
    if code != 0:
        return "missing"
    return out.strip().lower() or "unknown"


def _http_ok(url: str, timeout: int = 3) -> bool:
    try:
        r = requests.get(url, timeout=timeout)
        return r.status_code < 500
    except Exception:
        return False


def _service_health(name: str, cfg: dict) -> tuple[str, str]:
    """Return (status_emoji, detail_string)."""
    cstate = _container_state(cfg["container"])

    if cstate == "missing":
        return "🔴", "not found"
    if cstate in ("exited", "dead", "stopped"):
        return "🔴", f"container {cstate}"
    if cstate == "paused":
        return "🟡", "paused"

    proto = cfg.get("proto", "tcp")
    port  = cfg.get("port")

    if proto == "container":
        return ("🟢", "running") if cstate == "running" else ("🟡", cstate)

    if proto == "http" and port:
        url = f"http://localhost:{port}{cfg.get('path','/')}"
        ok  = _http_ok(url)
        return ("🟢", f"HTTP {port} ✓") if ok else ("🟡", f"container up, HTTP {port} ✗")

    # tcp / fallback — container state is enough
    return ("🟢", "running") if cstate == "running" else ("🟡", cstate)


def _producer_status(container: str) -> dict:
    """Return rich status dict for a producer container."""
    code, out, _ = _run([
        CONTAINER_CLI, "inspect",
        "--format", "{{.State.Status}}|{{.State.StartedAt}}|{{.RestartCount}}",
        container,
    ])
    if code != 0:
        return {"state": "missing", "started": "-", "restarts": 0}
    parts = out.strip().split("|")
    return {
        "state":    parts[0] if parts else "unknown",
        "started":  parts[1][:19].replace("T", " ") if len(parts) > 1 else "-",
        "restarts": int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0,
    }


def _flink_jobs() -> list[dict]:
    """Fetch job list from Flink REST API (try host first, then container)."""
    for base in [FLINK_API_HOST, FLINK_API]:
        try:
            r = requests.get(f"{base}/jobs/overview", timeout=5)
            if r.status_code == 200:
                return r.json().get("jobs", [])
        except Exception:
            pass
    return []


def _kafka_topic_counts() -> dict[str, int]:
    """Return approximate message count per topic via kafka-log-dirs."""
    counts = {}
    topics = ["tax-applications", "taxpayer-profiles", "fraud-signals", "audit-events"]
    for t in topics:
        code, out, _ = _run([
            CONTAINER_CLI, "exec", "kafka",
            "kafka-run-class", "kafka.tools.GetOffsetShell",
            "--broker-list", "localhost:9092",
            "--topic", t, "--time", "-1",
        ], timeout=8)
        if code == 0:
            total = sum(
                int(line.split(":")[-1])
                for line in out.strip().splitlines()
                if line.count(":") >= 2 and line.split(":")[-1].isdigit()
            )
            counts[t] = total
        else:
            counts[t] = -1
    return counts


# ─────────────────────────────────────────────────────────────────────────────
# UI sections
# ─────────────────────────────────────────────────────────────────────────────

def _render_health_grid():
    st.subheader("🩺 Service Health", divider="blue")
    cols = st.columns(4)
    items = list(SERVICES.items())
    for idx, (svc_name, cfg) in enumerate(items):
        emoji, detail = _service_health(svc_name, cfg)
        with cols[idx % 4]:
            st.markdown(
                f"""<div style="background:#f8fafc;border-radius:8px;padding:10px 14px;
                     margin-bottom:8px;border-left:4px solid {'#22c55e' if emoji=='🟢' else '#ef4444' if emoji=='🔴' else '#f59e0b'}">
                <div style="font-size:1.1rem;font-weight:700">{emoji} {svc_name}</div>
                <div style="font-size:0.78rem;color:#64748b;margin-top:3px">{detail}</div>
                </div>""",
                unsafe_allow_html=True,
            )


def _render_producer_controls():
    st.subheader("🚀 Producer Controls", divider="orange")
    st.caption("Start, stop, or restart each Kafka data producer. Each producer emits ~1 event/second.")

    for label, cname in PRODUCERS.items():
        info = _producer_status(cname)
        state = info["state"]
        is_running = state == "running"

        col_name, col_state, col_start, col_stop, col_restart, col_logs = st.columns(
            [2.2, 1.2, 1, 1, 1, 1]
        )
        with col_name:
            st.markdown(f"**{label}**  \n`{cname}`")
        with col_state:
            badge_col = "#22c55e" if is_running else "#ef4444" if state == "missing" else "#f59e0b"
            st.markdown(
                f'<span style="background:{badge_col};color:white;padding:3px 10px;'
                f'border-radius:12px;font-size:0.8rem;font-weight:700">{state.upper()}</span>',
                unsafe_allow_html=True,
            )
        with col_start:
            if st.button("▶ Start", key=f"start_{cname}", disabled=is_running,
                         help="Start this producer container"):
                with st.spinner(f"Starting {cname}…"):
                    ok, msg = _container_action(cname, "start")
                    if ok:
                        st.success(f"Started ✓  ({msg})")
                    else:
                        st.error(f"Failed: {msg}")
                time.sleep(1)
                st.rerun()
        with col_stop:
            if st.button("⏹ Stop", key=f"stop_{cname}", disabled=not is_running,
                         help="Stop this producer container"):
                with st.spinner(f"Stopping {cname}…"):
                    ok, msg = _container_action(cname, "stop")
                    if ok:
                        st.warning(f"Stopped  ({msg})")
                    else:
                        st.error(f"Failed: {msg}")
                time.sleep(1)
                st.rerun()
        with col_restart:
            if st.button("🔄 Restart", key=f"restart_{cname}",
                         help="Restart this producer container"):
                with st.spinner(f"Restarting {cname}…"):
                    ok, msg = _container_action(cname, "restart")
                    if ok:
                        st.info(f"Restarted ✓  ({msg})")
                    else:
                        st.error(f"Failed: {msg}")
                time.sleep(2)
                st.rerun()
        with col_logs:
            if st.button("📋 Logs", key=f"logs_{cname}",
                         help="Show last 30 log lines"):
                _, log_out, _ = _run([CONTAINER_CLI, "logs", "--tail", "30", cname])
                st.session_state[f"show_logs_{cname}"] = log_out

        # Log expander (persists until next click)
        log_key = f"show_logs_{cname}"
        if log_key in st.session_state and st.session_state[log_key]:
            with st.expander(f"📋 {cname} — last 30 lines", expanded=True):
                st.code(st.session_state[log_key], language="text")

        st.divider()

    # ── Start All / Stop All ──────────────────────────────────────────────────
    c1, c2, c3 = st.columns([1, 1, 4])
    with c1:
        if st.button("▶▶ Start ALL Producers", type="primary"):
            results = []
            for cname in PRODUCERS.values():
                ok, _ = _container_action(cname, "start")
                results.append(f"{'✓' if ok else '✗'} {cname}")
            st.success("  |  ".join(results))
            time.sleep(1)
            st.rerun()
    with c2:
        if st.button("⏹⏹ Stop ALL Producers", type="secondary"):
            results = []
            for cname in PRODUCERS.values():
                ok, _ = _container_action(cname, "stop")
                results.append(f"{'✓' if ok else '✗'} {cname}")
            st.warning("  |  ".join(results))
            time.sleep(1)
            st.rerun()


def _render_flink_controls():
    st.subheader("⚡ Flink Job Manager", divider="violet")

    jobs = _flink_jobs()
    if not jobs:
        st.warning("⚠️ Flink REST API not reachable. Is flink-jobmanager running?")
        c1, _ = st.columns([1, 3])
        with c1:
            if st.button("🔄 Refresh Flink Status"):
                st.rerun()
        return

    # ── Job table ─────────────────────────────────────────────────────────────
    running  = [j for j in jobs if j["state"] == "RUNNING"]
    finished = [j for j in jobs if j["state"] not in ("RUNNING", "RESTARTING")]

    st.markdown(f"**{len(running)} RUNNING** · **{len(jobs)-len(running)} other** · "
                f"Flink cluster at `localhost:8081`")

    col_headers = st.columns([3, 1.2, 1.5, 1, 1])
    col_headers[0].markdown("**Job Name**")
    col_headers[1].markdown("**State**")
    col_headers[2].markdown("**Duration**")
    col_headers[3].markdown("**Cancel**")
    col_headers[4].markdown("**Metrics**")

    for job in sorted(jobs, key=lambda j: j["state"] == "RUNNING", reverse=True):
        jid   = job["jid"]
        name  = job["name"].replace("insert-into_default_catalog.default_database.", "→ ")
        state = job["state"]
        dur_s = int((job.get("duration", 0)) / 1000)
        dur_h = f"{dur_s//3600}h {(dur_s%3600)//60}m" if dur_s >= 3600 else f"{dur_s//60}m {dur_s%60}s"
        state_colour = {"RUNNING": "#22c55e", "RESTARTING": "#f59e0b",
                        "CANCELED": "#94a3b8", "FAILED": "#ef4444"}.get(state, "#94a3b8")

        c0, c1, c2, c3, c4 = st.columns([3, 1.2, 1.5, 1, 1])
        with c0:
            st.markdown(f"<small><code>{name[:70]}</code></small>", unsafe_allow_html=True)
        with c1:
            st.markdown(
                f'<span style="background:{state_colour};color:white;padding:2px 8px;'
                f'border-radius:10px;font-size:0.75rem;font-weight:700">{state}</span>',
                unsafe_allow_html=True,
            )
        with c2:
            st.markdown(f"<small>{dur_h}</small>", unsafe_allow_html=True)
        with c3:
            if state == "RUNNING" and st.button("✖ Cancel", key=f"cancel_{jid}",
                                                help=f"Cancel job {jid[:8]}"):
                for base in [FLINK_API_HOST, FLINK_API]:
                    try:
                        r = requests.patch(f"{base}/jobs/{jid}?mode=cancel", timeout=10)
                        if r.status_code in (200, 202):
                            st.success("Cancellation requested")
                            time.sleep(2)
                            st.rerun()
                            break
                    except Exception:
                        pass
                else:
                    st.error("Cancel failed — check Flink UI")
        with c4:
            if st.button("📈", key=f"metrics_{jid}", help="View Flink UI for this job"):
                st.markdown(
                    f"[Open in Flink UI ↗](http://localhost:8081/#/job/{jid})",
                    unsafe_allow_html=True,
                )

    # ── Resubmit controls ─────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### 🔁 Submit / Resubmit SQL Jobs")
    st.caption("Renders env-variable placeholders and submits via `sql-client.sh -f`. "
               "Existing RUNNING jobs with the same name are unaffected.")

    FLINK_JOBS_DIR = Path(__file__).parent.parent.parent / "flink-jobs"
    sql_files = sorted(FLINK_JOBS_DIR.glob("job_*.sql"))

    if not sql_files:
        st.warning(f"No SQL files found in `{FLINK_JOBS_DIR}`")
        return

    submit_col1, submit_col2 = st.columns([2, 2])
    with submit_col1:
        chosen = st.multiselect(
            "Select jobs to submit",
            options=[f.name for f in sql_files],
            default=[],
            help="Choose one or more .sql files to submit to Flink",
        )
    with submit_col2:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🚀 Submit Selected Jobs", disabled=not chosen, type="primary"):
            _submit_flink_jobs(chosen, FLINK_JOBS_DIR)

    if st.button("🚀 Submit ALL Jobs (full pipeline restart)", type="secondary"):
        _submit_flink_jobs([f.name for f in sql_files], FLINK_JOBS_DIR)


def _submit_flink_jobs(filenames: list[str], jobs_dir: Path):
    """Render + submit chosen SQL files to Flink SQL Client."""
    ENV_VARS = {
        "KAFKA_BOOTSTRAP_SERVERS": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        "POSTGRES_HOST":           os.getenv("POSTGRES_HOST", "postgres"),
        "POSTGRES_PORT":           os.getenv("POSTGRES_PORT", "5432"),
        "POSTGRES_DB":             os.getenv("POSTGRES_DB", "taxdb"),
        "POSTGRES_USER":           os.getenv("POSTGRES_USER", "taxuser"),
        "POSTGRES_PASSWORD":       os.getenv("POSTGRES_PASSWORD", "taxpass"),
        "MINIO_BUCKET_RAW":        os.getenv("MINIO_BUCKET_RAW", "raw-tax"),
        "MINIO_BUCKET_CLEANED":    os.getenv("MINIO_BUCKET_CLEANED", "cleaned-tax"),
    }

    progress = st.progress(0, text="Submitting jobs…")
    log_area = st.empty()
    log_lines = []

    for i, fname in enumerate(filenames):
        sql_path = jobs_dir / fname
        if not sql_path.exists():
            log_lines.append(f"❌ {fname}: file not found")
            continue

        job_name  = sql_path.stem
        raw_sql   = sql_path.read_text(encoding="utf-8")
        final_sql = raw_sql
        for k, v in ENV_VARS.items():
            final_sql = final_sql.replace(f"${{{k}}}", v)

        tmp_path  = f"/tmp/{job_name}.sql"
        log_lines.append(f"⏳ {fname}: writing SQL to container…")
        log_area.code("\n".join(log_lines), language="text")

        # Step 1: write SQL into container
        code, _, err = _run(
            [CONTAINER_CLI, "exec", "-i", "flink-sql-client", "bash", "-c", f"cat > {tmp_path}"],
            timeout=20,
        )
        # pipe SQL via stdin requires a different approach:
        try:
            result = subprocess.run(
                [CONTAINER_CLI, "exec", "-i", "flink-sql-client", "bash", "-c", f"cat > {tmp_path}"],
                input=final_sql.encode("utf-8"),
                capture_output=True,
                timeout=20,
            )
            if result.returncode != 0:
                log_lines.append(f"❌ {fname}: write failed — {result.stderr.decode()[:100]}")
                log_area.code("\n".join(log_lines), language="text")
                continue
        except Exception as exc:
            log_lines.append(f"❌ {fname}: {exc}")
            log_area.code("\n".join(log_lines), language="text")
            continue

        # Step 2: execute sql-client.sh -f
        log_lines.append(f"⏳ {fname}: executing sql-client.sh…")
        log_area.code("\n".join(log_lines), language="text")
        try:
            result = subprocess.run(
                [CONTAINER_CLI, "exec", "flink-sql-client",
                 "/opt/flink/bin/sql-client.sh", "-f", tmp_path],
                capture_output=True,
                timeout=120,
            )
            stdout = result.stdout.decode("utf-8", errors="replace")
            if result.returncode == 0:
                log_lines.append(f"✅ {fname}: submitted OK")
            else:
                log_lines.append(f"❌ {fname}: exit {result.returncode}")
                log_lines.append(f"   {result.stderr.decode()[:200]}")
        except subprocess.TimeoutExpired:
            log_lines.append(f"⏰ {fname}: timed out after 120s")
        except Exception as exc:
            log_lines.append(f"❌ {fname}: {exc}")

        log_area.code("\n".join(log_lines), language="text")
        progress.progress((i + 1) / len(filenames), text=f"Done {i+1}/{len(filenames)}")
        time.sleep(2)

    progress.progress(1.0, text="All done.")
    st.success("Job submission complete. Check Flink UI at http://localhost:8081")


def _render_pipeline_actions():
    st.subheader("🔧 One-Click Pipeline Actions", divider="green")
    st.caption("Run these to seed data, train models, or validate the full platform end-to-end.")

    PROJECT_ROOT = Path(__file__).parent.parent.parent

    a1, a2, a3, a4 = st.columns(4)

    with a1:
        st.markdown("**🌱 Seed Qdrant**")
        st.caption("Embed SA tax law KB + cleaned tax records into Qdrant vector store")
        if st.button("Run: seed_qdrant.py", key="seed_qdrant"):
            _run_script(PROJECT_ROOT, ["python", "scripts/seed_qdrant.py", "--kb-only"])

    with a2:
        st.markdown("**🤖 Train MLflow Model**")
        st.caption("Train RandomForest fraud detector on cleaned-tax MinIO data")
        if st.button("Run: train_fraud_model.py", key="train_mlflow"):
            _run_script(PROJECT_ROOT, ["python", "mlflow/train_fraud_model.py"])

    with a3:
        st.markdown("**🧪 Smoke Test**")
        st.caption("Run 8-component integration test across all services")
        if st.button("Run: smoke_test.py", key="smoke_test"):
            _run_script(PROJECT_ROOT, ["python", "scripts/smoke_test.py"])

    with a4:
        st.markdown("**📬 Submit Lineage**")
        st.caption("Emit OpenLineage events to Marquez for all 6 Flink jobs")
        if st.button("Run: submit_flink_jobs.py (lineage)", key="submit_lineage"):
            _run_script(PROJECT_ROOT, ["python", "scripts/submit_flink_jobs.py"])


def _run_script(cwd: Path, cmd: list):
    """Run a Python script and show output inline."""
    with st.spinner(f"Running `{' '.join(cmd)}`…"):
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                cwd=str(cwd),
                timeout=180,
                env={**os.environ, "PYTHONPATH": str(cwd)},
            )
            output = result.stdout.decode("utf-8", errors="replace")
            errors = result.stderr.decode("utf-8", errors="replace")
            with st.expander("📋 Script Output", expanded=True):
                if output:
                    st.code(output, language="text")
                if errors:
                    st.code(errors, language="bash")
                st.caption(f"Exit code: {result.returncode}")
        except subprocess.TimeoutExpired:
            st.error("Script timed out after 180s")
        except Exception as exc:
            st.error(f"Error: {exc}")


def _render_kafka_stats():
    st.subheader("📨 Kafka Topic Message Counts", divider="gray")
    st.caption("Approximate total messages (log end offsets summed across partitions)")

    with st.spinner("Querying Kafka…"):
        counts = _kafka_topic_counts()

    cols = st.columns(len(counts))
    colours = ["#3b82f6", "#8b5cf6", "#ef4444", "#f59e0b"]
    for i, (topic, count) in enumerate(counts.items()):
        with cols[i]:
            display = f"{count:,}" if count >= 0 else "N/A"
            st.markdown(
                f"""<div style="background:#f0f6ff;border-radius:8px;padding:12px 16px;
                     border-left:4px solid {colours[i]}">
                <div style="font-size:1.6rem;font-weight:700;color:{colours[i]}">{display}</div>
                <div style="font-size:0.8rem;color:#64748b;margin-top:4px">{topic}</div>
                </div>""",
                unsafe_allow_html=True,
            )


def _render_log_viewer():
    st.subheader("📋 Container Log Viewer", divider="gray")
    all_containers = list(PRODUCERS.values()) + [
        "kafka", "zookeeper", "postgres", "redis", "minio",
        "qdrant", "flink-jobmanager", "flink-taskmanager",
        "flink-sql-client", "marquez", "marquez-web",
        "mlflow", "streamlit", "kpi-poller",
    ]
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        selected = st.selectbox("Select container", all_containers, key="log_container_sel")
    with col2:
        n_lines = st.number_input("Lines", min_value=10, max_value=500, value=50, step=10, key="log_lines")
    with col3:
        st.markdown("<br>", unsafe_allow_html=True)
        fetch = st.button("📋 Fetch Logs", key="fetch_logs_btn")

    if fetch and selected:
        with st.spinner(f"Fetching logs from {selected}…"):
            log_output = _container_logs(selected, tail=int(n_lines))
        if log_output and log_output != "(no output)":
            st.code(log_output, language="text")
        else:
            st.info("No log output (container may be brand new or not started)")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def render():
    st.markdown("## ⚙️ Platform Control Center")
    st.markdown(
        "Full operational control over every service in the streaming platform. "
        "Start/stop producers, manage Flink jobs, seed data, and view live logs — "
        "all from a single pane of glass."
    )

    # ── Auto-refresh toggle ───────────────────────────────────────────────────
    col_r, col_i, _ = st.columns([1.5, 1.5, 5])
    with col_r:
        auto_refresh = st.checkbox("🔄 Auto-refresh health grid (10s)", value=False, key="ctrl_autorefresh")
    with col_i:
        if st.button("🔃 Refresh Now"):
            st.rerun()

    if auto_refresh:
        try:
            from streamlit_autorefresh import st_autorefresh
            st_autorefresh(interval=10_000, key="ctrl_refresh_ticker")
        except ImportError:
            st.caption("streamlit-autorefresh not installed — manual refresh only")

    st.markdown("---")

    # ── Sections ─────────────────────────────────────────────────────────────
    _render_health_grid()
    st.markdown("---")
    _render_kafka_stats()
    st.markdown("---")
    _render_producer_controls()
    st.markdown("---")
    _render_flink_controls()
    st.markdown("---")
    _render_pipeline_actions()
    st.markdown("---")
    _render_log_viewer()
