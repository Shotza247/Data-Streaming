"""
dashboard/tabs/mlflow_tuning.py
-------------------------------
Hyperparameter tuning panel for the MLflow tab.

Talks to the model-api service:
    GET  /tune/space   default search space, defaults, families, objectives
    POST /tune         start a random search (cross-validated, runs in the background)
    GET  /tune/status  progress
    GET  /tune/result  ranked trials + the winner's held-out test metrics
    POST /train        retrain the registered model with the winning configuration

The search space is editable: widen or narrow every numeric range and choose which
categorical options are tried. The result table is ranked by the chosen objective so
you can see which settings give the best precision, recall and F1.
"""

import os
import time

import pandas as pd
import requests
import streamlit as st

MODEL_API_URL = os.getenv("MODEL_API_URL", "http://model-api:8000")


# ── API helpers ───────────────────────────────────────────────────────────────

def _get(path: str, timeout: int = 5):
    try:
        r = requests.get(f"{MODEL_API_URL}{path}", timeout=timeout)
        return r.status_code, (r.json() if r.content else {})
    except Exception:
        return None, {}


def _post(path: str, body: dict, timeout: int = 10) -> tuple[bool, str]:
    try:
        r = requests.post(f"{MODEL_API_URL}{path}", json=body, timeout=timeout)
        msg = r.json().get("message") or r.json().get("error") or r.text[:200]
        return r.status_code == 202, msg
    except Exception as exc:
        return False, f"model-api unreachable ({type(exc).__name__})"


@st.cache_data(ttl=300, show_spinner=False)
def _space_info():
    code, data = _get("/tune/space")
    return data if code == 200 else None


# ── Search-space editor ───────────────────────────────────────────────────────

def _fmt(x) -> str:
    return str(x)


def _param_editor(family: str, name: str, spec: dict) -> dict:
    """One parameter row. Returns the edited spec."""
    key = f"tune_{family}_{name}"
    t = spec["type"]
    out = dict(spec)
    if t == "choice":
        options = spec.get("options") or spec["values"]
        out["values"] = st.multiselect(
            name, options, default=[v for v in spec["values"] if v in options],
            format_func=_fmt, key=key, help="Values to try. Pick at least one.",
        )
        return out

    c0, c1, c2 = st.columns([1.3, 1, 1])
    is_int = t == "int"
    lo_lim, hi_lim = spec.get("min", spec["low"]), spec.get("max", spec["high"])
    cast = int if is_int else float
    step = 1 if is_int else (0.001 if spec["high"] <= 1 else 0.1)
    fmt = "%d" if is_int else "%.4f"
    with c0:
        st.markdown(f"**{name}**" + ("  \n<small>log scale</small>" if spec.get("log") else ""),
                    unsafe_allow_html=True)
    with c1:
        out["low"] = st.number_input("min", min_value=cast(lo_lim), max_value=cast(hi_lim),
                                     value=cast(spec["low"]), step=cast(step), format=fmt,
                                     key=f"{key}_lo", label_visibility="collapsed")
    with c2:
        out["high"] = st.number_input("max", min_value=cast(lo_lim), max_value=cast(hi_lim),
                                      value=cast(spec["high"]), step=cast(step), format=fmt,
                                      key=f"{key}_hi", label_visibility="collapsed")
    return out


def _space_editor(info: dict, families: list[str]) -> tuple[dict, list[str]]:
    """Render the editor for the selected families. Returns (space, validation errors)."""
    space, errors = {}, []
    for family in families:
        label = info["families"].get(family, family)
        with st.expander(f"{label} — search space", expanded=False):
            fam_space = {}
            for name, spec in info["space"][family].items():
                fam_space[name] = _param_editor(family, name, spec)
                s = fam_space[name]
                if s["type"] in ("int", "float") and s["low"] > s["high"]:
                    errors.append(f"{label}: {name} min is above max")
                if s["type"] == "choice" and not s["values"]:
                    errors.append(f"{label}: select at least one value for {name}")
            space[family] = fam_space
    return space, errors


# ── Progress / results ────────────────────────────────────────────────────────

def _render_status(running: bool):
    """Live progress; polls while a search runs and refreshes the page when it ends."""

    @st.fragment(run_every=3 if running else None)
    def _inner():
        code, status = _get("/tune/status")
        if code != 200:
            return
        state = status["state"]
        if state == "running":
            total = max(status.get("total") or 0, 1)
            done = status.get("done") or 0
            st.progress(min(done / total, 1.0), text=f"Evaluating configurations: {done}/{status.get('total') or '?'}")
        elif running:                       # was running when the page rendered, has now finished
            st.rerun()
        elif status.get("busy"):
            st.info("A model training job is running; tuning can start when it finishes.", icon="⏳")
        elif state == "failed":
            st.error("The last tuning run failed. See the log below.", icon="❌")
        elif state == "succeeded":
            st.success("Tuning finished.", icon="✅")
        if status.get("log"):
            with st.expander("Tuning log", expanded=state in ("running", "failed")):
                st.code("\n".join(status["log"][-40:]), language="text")

    _inner()


def _metric_cols(title: str, m: dict, base: dict | None = None, prefix: str = ""):
    st.caption(title)
    c1, c2, c3 = st.columns(3)
    for col, key, label in ((c1, "precision", "Precision"), (c2, "recall", "Recall"),
                            (c3, "f1", "F1")):
        value = m[prefix + key]
        delta = None
        if base:
            delta = f"{value - base[prefix + key]:+.3f} vs default"
        col.metric(label, f"{value:.3f}", delta)


def _trials_frame(result: dict) -> pd.DataFrame:
    rows = []
    for rank, t in enumerate(result["trials"], start=1):
        if "error" in t:
            continue
        rows.append({
            "rank": rank, "family": t["family"], "trial": t["label"],
            "score": t["score"], "precision": t["cv_precision"], "recall": t["cv_recall"],
            "f1": t["cv_f1"], "avg_precision": t["cv_average_precision"],
            "threshold": t["threshold"],
            **{f"p_{k}": ("None" if v is None else v) for k, v in t["params"].items()},
        })
    return pd.DataFrame(rows)


def _render_results(result: dict, info: dict):
    best = result["best"]
    label = info["families"].get(best["family"], best["family"])
    obj = info["objectives"].get(result["objective"], result["objective"])

    st.markdown("##### Best configuration")
    st.markdown(f"**{label}** · objective: *{obj}* · {result['cv_folds']}-fold CV on "
                f"{result['n_train']} training rows · {result['seconds']}s")

    d = result.get("best_default_test")
    left, right = st.columns(2)
    with left:
        _metric_cols("Cross-validated (train split)", best, prefix="cv_")
    with right:
        _metric_cols(f"Held-out test split ({result['n_test']} rows, not used for tuning)",
                     {"precision": result["best_test"]["precision"],
                      "recall": result["best_test"]["recall"], "f1": result["best_test"]["f1_score"]},
                     {"precision": d["precision"], "recall": d["recall"], "f1": d["f1_score"]} if d else None)
    st.caption(f"Decision threshold {best['threshold']:.3f} · MLflow run `{result['tuning_run_id'][:8]}` "
               "in experiment `tax-fraud-tuning`")
    if not best.get("feasible", True):
        st.warning("The precision/recall floor could not be met by any configuration; "
                   "ranked by F1 instead. Relax the floor or widen the search space.", icon="⚠️")

    st.code("\n".join(f"{k} = {v!r}" for k, v in best["params"].items()), language="python")

    c1, c2 = st.columns([1, 2])
    with c1:
        if st.button("🏁 Train registered model with this config", type="primary", key="train_best",
                     help="Retrains 'fraud-detector' on the same data using these settings and "
                          "decision threshold objective; the new version is served automatically."):
            ok, msg = _post("/train", {
                "source": result["data_source"], "n_rows": result["n_rows"], "family": best["family"],
                "params": best["params"], "objective": result["objective"],
                "min_precision": result["min_precision"], "min_recall": result["min_recall"],
            })
            (st.success if ok else st.error)(msg if not ok else "Training started — see Trainer status above.")
            time.sleep(1)
            st.rerun()

    frame = _trials_frame(result)
    if frame.empty:
        return

    st.markdown("##### Best per model family")
    per = pd.DataFrame([
        {"family": info["families"].get(f, f), "score": t["score"], "precision": t["cv_precision"],
         "recall": t["cv_recall"], "f1": t["cv_f1"], "threshold": t["threshold"]}
        for f, t in result["best_per_family"].items()
    ])
    st.dataframe(per, hide_index=True, use_container_width=True, column_config={
        c: st.column_config.NumberColumn(c.title(), format="%.3f")
        for c in ("score", "precision", "recall", "f1", "threshold")})

    import plotly.graph_objects as go
    top = frame.head(12)
    names = [f"{r.family[:4]}·{r.trial}" for r in top.itertuples()]
    fig = go.Figure()
    for col, colour in (("precision", "#2563eb"), ("recall", "#059669"), ("f1", "#7c3aed")):
        fig.add_bar(name=col.title(), x=names, y=top[col], marker_color=colour)
    fig.update_layout(barmode="group", height=320, yaxis=dict(range=[0, 1.05]),
                      margin=dict(l=0, r=0, t=10, b=0), plot_bgcolor="#f8fafc", paper_bgcolor="#f8fafc",
                      legend=dict(orientation="h", y=1.12))
    st.markdown("##### Top 12 trials")
    st.plotly_chart(fig, use_container_width=True)

    with st.expander(f"All {len(frame)} trials with their hyperparameters"):
        st.dataframe(frame, hide_index=True, use_container_width=True, column_config={
            c: st.column_config.NumberColumn(c, format="%.3f")
            for c in ("score", "precision", "recall", "f1", "avg_precision", "threshold")})
    failed = [t for t in result["trials"] if "error" in t]
    if failed:
        st.caption(f"{len(failed)} configuration(s) failed and were skipped: "
                   + "; ".join(sorted({t['error'] for t in failed}))[:300])


# ── Panel ─────────────────────────────────────────────────────────────────────

def render_tuning_panel():
    st.markdown("#### Hyperparameter tuning")
    st.caption("Random search with cross-validation. The decision threshold is tuned for every "
               "configuration, so precision, recall and F1 are compared at their best operating point. "
               "The winner is checked once on a held-out test split.")

    info = _space_info()
    if info is None:
        st.warning("The tuning service (`model-api`) is not reachable. Start it with "
                   "`podman compose up -d model-api`, then press Refresh.", icon="⚠️")
        return

    _, status = _get("/tune/status")
    status = status or {}
    running = status.get("state") == "running"
    blocked = running or bool(status.get("busy"))

    with st.expander("Configure search", expanded=not running):
        c1, c2, c3 = st.columns(3)
        with c1:
            source = st.radio("Data", ["streamed", "sample"], horizontal=True, key="tune_source",
                              help="Streamed = cleaned events from MinIO. Sample = generated data.")
            families = st.multiselect("Model families", list(info["families"]),
                                      default=list(info["families"]), key="tune_families",
                                      format_func=lambda f: info["families"][f])
        with c2:
            objective = st.selectbox("Optimise for", list(info["objectives"]), key="tune_objective",
                                     format_func=lambda o: info["objectives"][o])
            min_precision, min_recall = 0.5, 0.5
            if objective == "recall":
                min_precision = st.slider("Minimum precision", 0.1, 0.95, 0.5, 0.05, key="tune_minp",
                                          help="Recall is maximised while precision stays at or above this.")
            elif objective == "precision":
                min_recall = st.slider("Minimum recall", 0.1, 0.95, 0.5, 0.05, key="tune_minr",
                                       help="Precision is maximised while recall stays at or above this.")
        with c3:
            n_trials = st.slider("Random trials per family", 3, 60, 12, key="tune_trials",
                                 help="A default-settings run is always added per family as a baseline.")
            cv_folds = st.slider("Cross-validation folds", 3, 10, 5, key="tune_folds")

        st.markdown("**Search space** — ranges are sampled uniformly (log scale where marked).")
        space, errors = _space_editor(info, families)
        total = len(families) * (n_trials + 1)
        st.caption(f"{total} configurations × {cv_folds} folds will be evaluated.")

    if not families:
        errors = ["Select at least one model family"]
    for e in errors:
        st.error(e)

    b1, b2 = st.columns([1, 3])
    with b1:
        if st.button("🔎 Start tuning", type="primary", disabled=blocked or bool(errors), key="tune_go"):
            ok, msg = _post("/tune", {
                "families": families, "n_trials": n_trials, "objective": objective,
                "min_precision": min_precision, "min_recall": min_recall, "cv_folds": cv_folds,
                "source": source, "n_rows": 20000, "space": space,
            })
            (st.success if ok else st.error)("Tuning started." if ok else msg)
            time.sleep(1)
            st.rerun()
    with b2:
        st.caption("Tuning and training share the CPU; only one runs at a time.")

    _render_status(running)

    code, result = _get("/tune/result", timeout=10)
    if code == 200 and result:
        _render_results(result, info)
    elif not running:
        st.info("No tuning results yet. Configure the search above and press **Start tuning**.", icon="ℹ️")
