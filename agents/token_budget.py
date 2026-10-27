"""
agents/token_budget.py
----------------------
Token budget + provider rotation for the agents. No LLM or network code here
(see agents/llm_wrapper.py) so the policy can be unit-tested offline.

Policy comes from agents/litellm_config.yaml -> token_budget:

    huggingface  stint 20,000 tokens, total 100,000
    openai       stint 10,000 tokens, total  50,000

Rotation: serve from the active provider until its stint is used up, then move
to the next provider, and so on in a loop until both providers hit their total.
A provider that errors (quota, rate limit, auth, outage) is skipped for
`cooldown_seconds` and the call fails over to the other one.

Dev override: set LLM_FORCE_PROVIDER=openai (or call set_force_provider) to send
every call to one provider. Forced calls still count toward that provider's
total, so the cap still holds.

Accuracy: usage is what the provider reports after a call. Before a call the
prompt size is only estimated (chars / 4), and max_tokens is clamped to the
remaining budget, so a total can be overshot by the estimation error only.

State is persisted to JSON (LLM_BUDGET_STATE, default agents/.llm_budget_state.json)
so usage survives restarts. Delete the file or run `--reset` to start over.

CLI:
    python -m agents.token_budget --status
    python -m agents.token_budget --reset
"""

import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

CONFIG_PATH = Path(os.getenv("LITELLM_CONFIG", Path(__file__).parent / "litellm_config.yaml"))
STATE_PATH = Path(os.getenv("LLM_BUDGET_STATE", Path(__file__).parent / ".llm_budget_state.json"))


def load_config(path: Path = CONFIG_PATH) -> dict:
    import yaml
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def resolve_env(value):
    """Resolve 'os.environ/NAME' strings; returns None if NAME is unset/empty."""
    if isinstance(value, str) and value.startswith("os.environ/"):
        return os.getenv(value.split("/", 1)[1]) or None
    return value


def estimate_tokens(text: str) -> int:
    """Rough token estimate used only before a call (providers report real usage)."""
    return len(text) // 4 + 1


class TokenBudget:
    def __init__(self, config: Optional[dict] = None, state_path: Path = STATE_PATH):
        cfg = (config or load_config())["token_budget"]
        self.cooldown_seconds = int(cfg.get("cooldown_seconds", 600))
        self.providers = {p["name"]: p for p in cfg["providers"]}
        self.order = [p["name"] for p in cfg["providers"]]
        self.state_path = Path(state_path)
        self._lock = threading.RLock()
        self._state = self._load()

    # ── persistence ──────────────────────────────────────────────────────────
    def _fresh(self) -> dict:
        return {
            "active": self.order[0],
            "force_provider": None,
            "providers": {
                n: {"used": 0, "stint_used": 0, "cooldown_until": 0.0, "last_error": ""}
                for n in self.order
            },
        }

    def _load(self) -> dict:
        state = self._fresh()
        try:
            saved = json.loads(self.state_path.read_text(encoding="utf-8"))
            state["active"] = saved.get("active", state["active"])
            state["force_provider"] = saved.get("force_provider")
            for n in self.order:
                state["providers"][n].update(saved.get("providers", {}).get(n, {}))
        except (OSError, ValueError):
            pass
        if state["active"] not in self.providers:
            state["active"] = self.order[0]
        return state

    def _save(self) -> None:
        """
        Persist state. Atomic replace is tried first (with retries: on Windows and
        bind mounts another process can briefly lock the file), then a plain write.
        A persistence failure never breaks an LLM call; in-memory state stays valid.
        """
        data = json.dumps(self._state, indent=2)
        tmp = self.state_path.with_suffix(".tmp")
        for attempt in range(3):
            try:
                tmp.write_text(data, encoding="utf-8")
                os.replace(tmp, self.state_path)
                return
            except OSError:
                time.sleep(0.05 * (attempt + 1))
        try:
            self.state_path.write_text(data, encoding="utf-8")
        except OSError as exc:
            log.warning(f"Could not persist LLM budget state to {self.state_path}: {exc}")

    def reset(self) -> None:
        with self._lock:
            self._state = self._fresh()
            self._save()

    # ── helpers ──────────────────────────────────────────────────────────────
    def forced(self) -> Optional[str]:
        name = os.getenv("LLM_FORCE_PROVIDER", "").strip().lower() or self._state["force_provider"]
        return name if name in self.providers else None

    def set_force_provider(self, name: Optional[str]) -> None:
        with self._lock:
            self._state["force_provider"] = name if name in self.providers else None
            self._save()

    def remaining(self, name: str) -> int:
        return max(0, int(self.providers[name]["total_tokens"]) - self._state["providers"][name]["used"])

    def _cooling(self, name: str) -> float:
        return max(0.0, self._state["providers"][name]["cooldown_until"] - time.time())

    def _usable(self, name: str, need: int) -> bool:
        return self.remaining(name) >= need and self._cooling(name) == 0

    def _next_after(self, name: str) -> str:
        """Next provider in rotation that still has budget (falls back to `name`)."""
        i = self.order.index(name)
        for step in range(1, len(self.order) + 1):
            cand = self.order[(i + step) % len(self.order)]
            if self.remaining(cand) > 0:
                return cand
        return name

    # ── per-call API ─────────────────────────────────────────────────────────
    def plan(self, est_prompt_tokens: int, min_completion: int = 16) -> list:
        """Providers to try for the next call, best first. Empty -> BYO required."""
        need = est_prompt_tokens + min_completion
        with self._lock:
            forced = self.forced()
            if forced:
                return [forced] if self._usable(forced, need) else []
            start = self.order.index(self._state["active"])
            rotated = self.order[start:] + self.order[:start]
            return [n for n in rotated if self._usable(n, need)]

    def max_completion(self, name: str, est_prompt_tokens: int, requested: int) -> int:
        return max(1, min(requested, self.remaining(name) - est_prompt_tokens))

    def record(self, name: str, tokens: int) -> None:
        with self._lock:
            p = self._state["providers"][name]
            p["used"] += max(0, int(tokens))
            p["last_error"] = ""
            if self.forced():
                self._save()          # forced calls count toward the cap, no rotation
                return
            p["stint_used"] += max(0, int(tokens))
            if p["stint_used"] >= int(self.providers[name]["stint_tokens"]):
                p["stint_used"] = 0
                self._state["active"] = self._next_after(name)
            else:
                self._state["active"] = name
            self._save()

    def mark_failed(self, name: str, reason: str) -> None:
        with self._lock:
            p = self._state["providers"][name]
            p["cooldown_until"] = time.time() + self.cooldown_seconds
            p["last_error"] = reason[:300]
            if self._state["active"] == name and not self.forced():
                self._state["active"] = self._next_after(name)
            self._save()

    def clear_cooldown(self, name: Optional[str] = None) -> None:
        with self._lock:
            for n in ([name] if name else self.order):
                self._state["providers"][n]["cooldown_until"] = 0.0
            self._save()

    def status(self) -> dict:
        with self._lock:
            forced = self.forced()
            provs = {}
            for n in self.order:
                p, cfg = self._state["providers"][n], self.providers[n]
                provs[n] = {
                    "used": p["used"],
                    "total": int(cfg["total_tokens"]),
                    "remaining": self.remaining(n),
                    "stint_used": p["stint_used"],
                    "stint_tokens": int(cfg["stint_tokens"]),
                    "cooldown_s": round(self._cooling(n)),
                    "last_error": p["last_error"],
                }
            considered = [forced] if forced else self.order
            return {
                "active": self._state["active"],
                "forced": forced,
                "providers": provs,
                "all_capped": all(self.remaining(n) == 0 for n in considered),
                # Budget only: a provider merely in cooldown does NOT mean "bring your own".
                "byo_required": not any(self.remaining(n) >= 17 for n in considered),
            }


if __name__ == "__main__":
    import sys
    budget = TokenBudget()
    if "--reset" in sys.argv:
        budget.reset()
        print("Budget reset.")
    print(json.dumps(budget.status(), indent=2))
