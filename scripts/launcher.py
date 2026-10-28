#!/usr/bin/env python3
"""
scripts/launcher.py
-------------------
Host-side launcher for the streaming platform. Runs natively on Windows,
WSL and Linux, with Podman or Docker.

    python scripts/launcher.py up [--producers] [--no-wait]
    python scripts/launcher.py down
    python scripts/launcher.py status
    python scripts/launcher.py jobs [filter]     # submit only the missing Flink jobs

`up` is idempotent: it can be re-run any time (after a reboot, after the Flink
JobManager restarts, on a fresh server). Flink SQL jobs are only submitted when
they are not already running, so re-running never creates duplicates.

Environment:
    CONTAINER_CLI   podman | docker   (auto-detected if unset)
"""

import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT          = Path(__file__).resolve().parent.parent
FLINK_JOBS    = ROOT / "flink-jobs"
FLINK_API     = "http://localhost:8081"
FLINK_IMAGE   = "flink-custom:1.18"
FLINK_CTX     = ROOT / "docker" / "build" / "flink"
LIVE_STATES   = {"RUNNING", "CREATED", "RESTARTING", "INITIALIZING", "RECONCILING"}

# (label, kind, target)  kind: http -> URL must answer <500, health -> container healthcheck
HEALTH_CHECKS = [
    ("kafka",    "health", "kafka"),
    ("postgres", "health", "postgres"),
    ("minio",    "http",   "http://localhost:9000/minio/health/live"),
    ("flink",    "http",   f"{FLINK_API}/overview"),
    ("marquez",  "http",   "http://localhost:5000/api/v1/namespaces"),
]


# ── Tooling detection ────────────────────────────────────────────────────────
def _ok(cmd: list[str]) -> bool:
    try:
        return subprocess.run(cmd, capture_output=True, timeout=30).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def detect_tools() -> tuple[str, list[str]]:
    """Return (container_cli, compose_cmd)."""
    cli = os.getenv("CONTAINER_CLI") or next(
        (c for c in ("podman", "docker") if shutil.which(c)), None
    )
    if not cli:
        sys.exit("[launcher] Neither podman nor docker found on PATH.")
    if _ok([cli, "compose", "version"]):
        return cli, [cli, "compose"]
    legacy = f"{cli}-compose"
    if shutil.which(legacy):
        return cli, [legacy]
    sys.exit(f"[launcher] No compose provider found for '{cli}'.")


CLI, COMPOSE = detect_tools()
os.environ["CONTAINER_CLI"] = CLI   # submit_flink_jobs.py reads this


def run(cmd: list[str], check: bool = True, timeout: int | None = None) -> subprocess.CompletedProcess:
    print(f"  $ {' '.join(cmd)}")
    r = subprocess.run(cmd, cwd=ROOT, timeout=timeout)
    if check and r.returncode != 0:
        sys.exit(f"[launcher] Command failed (exit {r.returncode}): {' '.join(cmd)}")
    return r


# ── Health ───────────────────────────────────────────────────────────────────
def http_json(url: str, timeout: int = 5):
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8") or "null")


def http_ok(url: str) -> bool:
    try:
        urllib.request.urlopen(url, timeout=3).close()
        return True
    except urllib.error.HTTPError as exc:
        return exc.code < 500
    except Exception:
        return False


def container_health(name: str) -> str:
    r = subprocess.run(
        [CLI, "inspect", "--format", "{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{end}}", name],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        return "missing"
    state, _, health = r.stdout.strip().partition("|")
    return health or state


def check(kind: str, target: str) -> bool:
    if kind == "http":
        return http_ok(target)
    return container_health(target) in ("healthy", "running")


def wait_healthy(timeout: int = 300) -> None:
    print(f"\n[launcher] Waiting for services (up to {timeout}s)...")
    deadline = time.time() + timeout
    pending = list(HEALTH_CHECKS)
    while pending and time.time() < deadline:
        pending = [c for c in pending if not check(c[1], c[2])]
        if pending:
            time.sleep(3)
    if pending:
        sys.exit("[launcher] Timed out waiting for: " + ", ".join(c[0] for c in pending))
    print("  all healthy: " + ", ".join(c[0] for c in HEALTH_CHECKS))


def wait_taskmanager(timeout: int = 120) -> None:
    print("[launcher] Waiting for a Flink TaskManager with free slots...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if http_json(f"{FLINK_API}/overview").get("slots-available", 0) > 0:
                return
        except Exception:
            pass
        time.sleep(3)
    sys.exit("[launcher] No Flink TaskManager slots became available.")


# ── Flink jobs ───────────────────────────────────────────────────────────────
def expected_sinks(sql_file: Path) -> dict[str, int]:
    """{sink_table: number of INSERT statements} for a job file (comments stripped)."""
    text = re.sub(r"--[^\n]*", "", sql_file.read_text(encoding="utf-8"))
    sinks: dict[str, int] = {}
    for table in re.findall(r"INSERT\s+INTO\s+`?([\w.]+)`?", text, flags=re.I):
        sinks[table] = sinks.get(table, 0) + 1
    return sinks


def live_job_counts() -> dict[str, int]:
    """{sink_table: live job count}, derived from Flink's auto-generated job names."""
    counts: dict[str, int] = {}
    for job in http_json(f"{FLINK_API}/jobs/overview")["jobs"]:
        if job["state"] in LIVE_STATES:
            table = job["name"].rsplit(".", 1)[-1]
            counts[table] = counts.get(table, 0) + 1
    return counts


def job_status() -> list[tuple[Path, int, int]]:
    """[(sql_file, live_jobs, expected_jobs)] for every job file."""
    live = live_job_counts()
    result = []
    for f in sorted(FLINK_JOBS.glob("job_*.sql")):
        sinks = expected_sinks(f)
        have = sum(min(live.get(t, 0), n) for t, n in sinks.items())
        result.append((f, have, sum(sinks.values())))
    return result


def submit_missing_jobs(name_filter: str | None = None) -> bool:
    """Submit job files that have no live jobs. Returns True if all are running afterwards."""
    sys.path.insert(0, str(ROOT / "scripts"))
    import submit_flink_jobs as sfj   # imported lazily: needs python-dotenv + lineage module

    ok = True
    for f, have, want in job_status():
        if name_filter and name_filter not in f.name:
            continue
        if have >= want:
            print(f"  [skip] {f.stem}: already running ({have}/{want})")
        elif have > 0:
            print(f"  [WARN] {f.stem}: partially running ({have}/{want}); not resubmitting "
                  "to avoid duplicates. Cancel the remaining job(s) in the Flink UI and re-run.")
            ok = False
        else:
            ok &= sfj.submit_job(f)
            time.sleep(3)
    return ok


# ── Commands ─────────────────────────────────────────────────────────────────
def cmd_up(producers: bool, wait: bool) -> None:
    print("[launcher] Syncing Streamlit build context...")
    shutil.copy(ROOT / "dashboard" / "requirements.txt",
                ROOT / "docker" / "build" / "streamlit" / "requirements.streamlit.txt")
    shutil.copy(ROOT / "dashboard" / ".streamlit" / "config.toml",
                ROOT / "docker" / "build" / "streamlit" / "streamlit_config.toml")

    if not _ok([CLI, "image", "inspect", FLINK_IMAGE]):
        print(f"[launcher] Building {FLINK_IMAGE}...")
        run([CLI, "build", "-t", FLINK_IMAGE, str(FLINK_CTX)])

    print("\n[launcher] Starting services...")
    run(COMPOSE + ["up", "-d"])
    if producers:
        run(COMPOSE + ["--profile", "producers", "up", "-d", "--build"])

    if wait:
        wait_healthy()
        print("\n[launcher] Ensuring Kafka topics exist...")
        run(COMPOSE + ["--profile", "setup", "up", "kafka-topic-init"], check=False)
        wait_taskmanager()
        print("\n[launcher] Submitting missing Flink jobs...")
        if not submit_missing_jobs():
            sys.exit("[launcher] Some Flink jobs are not running. See messages above.")

    print("\n[launcher] Platform is up.")
    print("  Streamlit    http://localhost:8501\n  Flink UI     http://localhost:8081\n"
          "  Kafka UI     http://localhost:8080\n  MinIO        http://localhost:9001\n"
          "  MLflow       http://localhost:5001\n  Marquez      http://localhost:3000")


def cmd_down() -> None:
    run(COMPOSE + ["--profile", "producers", "--profile", "setup", "down"])


def cmd_status() -> None:
    print("[launcher] Health")
    for label, kind, target in HEALTH_CHECKS:
        print(f"  {'OK  ' if check(kind, target) else 'DOWN'}  {label}")
    print("\n[launcher] Flink jobs")
    try:
        for f, have, want in job_status():
            print(f"  {'OK  ' if have >= want else 'DOWN'}  {f.stem}  ({have}/{want})")
    except Exception as exc:
        print(f"  Flink REST API unreachable: {exc}")


def main() -> None:
    args = sys.argv[1:]
    cmd = args[0] if args else "help"
    if cmd == "up":
        cmd_up(producers="--producers" in args, wait="--no-wait" not in args)
    elif cmd == "down":
        cmd_down()
    elif cmd == "status":
        cmd_status()
    elif cmd == "jobs":
        sys.exit(0 if submit_missing_jobs(args[1] if len(args) > 1 else None) else 1)
    else:
        print(__doc__)
        sys.exit(0 if cmd in ("help", "-h", "--help") else 2)


if __name__ == "__main__":
    main()
