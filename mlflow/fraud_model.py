"""
mlflow/fraud_model.py
---------------------
Single source of truth for the fraud-detector model, shared by training
(train_fraud_model.py), hyperparameter tuning (tune_fraud_model.py) and
serving (serve_model.py).

Why a shared module
    The classifier used to see only raw income and four categoricals, so it
    could not tell "high income" from "high income FOR THIS employment type",
    and had no notion of how often a customer files. Those two things are what
    the fraud labels are built from, so they are now features:

        income_ratio      taxable_income / ceiling for the employment type
        above_ceiling     1 if income_ratio > 1
        log_income        log1p(taxable_income)
        customer_filings  how many applications this customer has filed

    `customer_filings` is learned inside the pipeline (FraudFeatureBuilder), so
    the registered model is self-contained: at serving time the caller only
    sends `customer_id` (and `application_id`) and the model looks up the
    history it was trained on.

Also here: model families + default params, the default tuning search space,
decision-threshold selection (F1 / recall / precision objectives) and a sample
data generator that mirrors the producer.
"""

import os

# Containers expose every host core but are CPU-capped; unbounded OpenMP/BLAS
# thread pools oversubscribe and stall. Must be set before numpy/sklearn load.
for _var in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS"):
    os.environ.setdefault(_var, "2")

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score, average_precision_score, f1_score,
    precision_recall_curve, precision_score, recall_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from mlflow_config import (
    CATEGORICAL_FEATURES, RF_MAX_DEPTH, RF_MIN_SAMPLES_LEAF, RF_N_ESTIMATORS, RF_RANDOM_STATE,
)

# ── Raw model inputs (what callers send) ──────────────────────────────────────
ID_COLUMN        = "customer_id"
APP_ID_COLUMN    = "application_id"
RAW_NUMERIC      = "taxable_income"
INPUT_COLUMNS    = CATEGORICAL_FEATURES + [RAW_NUMERIC, ID_COLUMN, APP_ID_COLUMN]

# ── Domain knowledge: expected annual income ceiling per employment type (ZAR) ─
# Mirrors INCOME_RANGES in producers/producer_tax_applications.py and the
# "Expected income ranges" in knowledge_base/01_business_context.md.
INCOME_CEILING   = {
    "EMPLOYED": 650_000.0, "SELF_EMPLOYED": 900_000.0,
    "CONTRACTOR": 750_000.0, "UNEMPLOYED": 30_000.0,
}
DEFAULT_CEILING  = 650_000.0

ENGINEERED_NUMERIC = ["taxable_income", "log_income", "income_ratio", "above_ceiling", "customer_filings"]


def _column(X: pd.DataFrame, name: str, default) -> pd.Series:
    if name in X.columns:
        return X[name]
    return pd.Series(default, index=X.index)


class FraudFeatureBuilder(BaseEstimator, TransformerMixin):
    """
    Raw application columns -> model features.

    Learns each customer's filing count (and the set of application ids) from
    the data it is fitted on. On the fitted rows the count includes the row
    itself. For an application not seen at fit time the count is the known
    history plus one (the application being scored); for one that was seen,
    it is the known count, so re-scoring a training row does not double count.
    """

    def fit(self, X, y=None):
        self.customer_counts_ = self._counts(X)
        self.known_ids_ = self._app_ids(X)
        return self

    def fit_transform(self, X, y=None, **fit_params):
        self.fit(X, y)
        filings = self._customer_ids(X).map(self.customer_counts_).fillna(1).astype(float)
        return self._build(X, filings)

    def transform(self, X):
        history = self._customer_ids(X).map(self.customer_counts_).fillna(0).astype(float)
        seen = _column(X, APP_ID_COLUMN, "").astype(str).isin(self.known_ids_)
        filings = history + (~seen).astype(float)
        return self._build(X, filings)

    # -- helpers --
    @staticmethod
    def _customer_ids(X) -> pd.Series:
        s = _column(X, ID_COLUMN, "").fillna("").astype(str).str.strip()
        return s.where(s != "", other=pd.NA)

    def _counts(self, X) -> dict:
        return self._customer_ids(X).dropna().value_counts().to_dict()

    @staticmethod
    def _app_ids(X) -> frozenset:
        ids = _column(X, APP_ID_COLUMN, "").fillna("").astype(str)
        return frozenset(ids[ids != ""])

    @staticmethod
    def _build(X, filings: pd.Series) -> pd.DataFrame:
        out = pd.DataFrame(index=X.index)
        for c in CATEGORICAL_FEATURES:
            out[c] = _column(X, c, "UNKNOWN").fillna("UNKNOWN").astype(str).str.upper()
        income = pd.to_numeric(_column(X, RAW_NUMERIC, 0.0), errors="coerce").fillna(0.0).clip(lower=0.0)
        ceiling = out["employment_type"].map(INCOME_CEILING).fillna(DEFAULT_CEILING)
        ratio = income / ceiling
        out["taxable_income"]   = income
        out["log_income"]       = np.log1p(income)
        out["income_ratio"]     = ratio
        out["above_ceiling"]    = (ratio > 1.0).astype(float)
        out["customer_filings"] = filings.fillna(1.0).astype(float)
        return out


# ── Model families ────────────────────────────────────────────────────────────
FAMILIES = ("random_forest", "gradient_boosting", "logistic_regression")

FAMILY_LABELS = {
    "random_forest":       "Random Forest",
    "gradient_boosting":   "Gradient Boosting (HistGB)",
    "logistic_regression": "Logistic Regression",
}

DEFAULT_PARAMS = {
    "random_forest": {
        "n_estimators": RF_N_ESTIMATORS, "max_depth": RF_MAX_DEPTH,
        "min_samples_leaf": RF_MIN_SAMPLES_LEAF, "max_features": "sqrt",
        "class_weight": "balanced_subsample",
    },
    "gradient_boosting": {
        "learning_rate": 0.05, "max_depth": 4, "max_iter": 200,
        "min_samples_leaf": 20, "l2_regularization": 0.0, "class_weight": "balanced",
    },
    "logistic_regression": {"C": 1.0, "class_weight": "balanced"},
}

# Default search space shown (and editable) in the dashboard.
#   int/float: sampled in [low, high] (float may be log-uniform); min/max are the
#              hard limits the UI lets you widen the range to
#   choice   : sampled from `values`, a subset of `options`
DEFAULT_SPACE = {
    "random_forest": {
        "n_estimators":     {"type": "int",   "low": 100, "high": 500, "min": 50, "max": 1000},
        "max_depth":        {"type": "int",   "low": 4,   "high": 20,  "min": 2,  "max": 40},
        "min_samples_leaf": {"type": "int",   "low": 1,   "high": 10,  "min": 1,  "max": 50},
        "max_features":     {"type": "choice", "values": ["sqrt", "log2", 0.5],
                             "options": ["sqrt", "log2", 0.3, 0.5, 0.8, 1.0]},
        "class_weight":     {"type": "choice", "values": ["balanced", "balanced_subsample"],
                             "options": ["balanced", "balanced_subsample"]},
    },
    "gradient_boosting": {
        "learning_rate":     {"type": "float", "low": 0.01, "high": 0.3, "log": True,  "min": 0.001, "max": 1.0},
        "max_depth":         {"type": "int",   "low": 2,    "high": 8,                "min": 1,     "max": 20},
        "max_iter":          {"type": "int",   "low": 100,  "high": 400,               "min": 20,    "max": 1000},
        "min_samples_leaf":  {"type": "int",   "low": 5,    "high": 40,                "min": 1,     "max": 100},
        "l2_regularization": {"type": "float", "low": 0.0,  "high": 5.0, "log": False, "min": 0.0,   "max": 50.0},
        "class_weight":      {"type": "choice", "values": ["balanced"], "options": ["balanced"]},
    },
    "logistic_regression": {
        "C":            {"type": "float", "low": 0.01, "high": 100.0, "log": True, "min": 0.0001, "max": 1000.0},
        "class_weight": {"type": "choice", "values": ["balanced"], "options": ["balanced"]},
    },
}

OBJECTIVES = {
    "f1":                "Best F1 (balance of precision and recall)",
    "recall":            "Highest recall, keeping precision above a floor",
    "precision":         "Highest precision, keeping recall above a floor",
    "average_precision": "Best average precision (ranking quality)",
}


def _make_classifier(family: str, p: dict, random_state: int):
    if family == "random_forest":
        return RandomForestClassifier(
            n_estimators=int(p["n_estimators"]),
            max_depth=None if p.get("max_depth") is None else int(p["max_depth"]),
            min_samples_leaf=int(p["min_samples_leaf"]),
            max_features=p["max_features"], class_weight=p.get("class_weight"),
            random_state=random_state, n_jobs=-1,
        )
    if family == "gradient_boosting":
        return HistGradientBoostingClassifier(
            learning_rate=float(p["learning_rate"]), max_depth=int(p["max_depth"]),
            max_iter=int(p["max_iter"]), min_samples_leaf=int(p["min_samples_leaf"]),
            l2_regularization=float(p["l2_regularization"]), class_weight=p.get("class_weight"),
            random_state=random_state,
        )
    if family == "logistic_regression":
        return LogisticRegression(
            C=float(p["C"]), class_weight=p.get("class_weight"), max_iter=3000,
            random_state=random_state,
        )
    raise ValueError(f"Unknown model family: {family!r} (expected one of {FAMILIES})")


def build_pipeline(family: str = "random_forest", params: dict | None = None,
                   random_state: int = RF_RANDOM_STATE) -> Pipeline:
    """Feature builder -> one-hot/scale -> classifier. `params` override the family defaults."""
    if family not in FAMILIES:
        raise ValueError(f"Unknown model family: {family!r} (expected one of {FAMILIES})")
    merged = {**DEFAULT_PARAMS[family], **(params or {})}
    numeric = StandardScaler() if family == "logistic_regression" else "passthrough"
    preprocessor = ColumnTransformer([
        ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), CATEGORICAL_FEATURES),
        ("num", numeric, ENGINEERED_NUMERIC),
    ])
    return Pipeline([
        ("features", FraudFeatureBuilder()),
        ("preprocessor", preprocessor),
        ("classifier", _make_classifier(family, merged, random_state)),
    ])


# ── Search space sampling ─────────────────────────────────────────────────────
def validate_space(space: dict) -> None:
    """Raise ValueError with a readable message if a search space is malformed."""
    for family, params in space.items():
        if family not in FAMILIES:
            raise ValueError(f"Unknown model family in space: {family!r}")
        for name, spec in params.items():
            t = spec.get("type")
            if t in ("int", "float"):
                if spec["low"] > spec["high"]:
                    raise ValueError(f"{family}.{name}: low ({spec['low']}) > high ({spec['high']})")
                if t == "float" and spec.get("log") and spec["low"] <= 0:
                    raise ValueError(f"{family}.{name}: log-scale needs low > 0")
            elif t == "choice":
                if not spec.get("values"):
                    raise ValueError(f"{family}.{name}: choose at least one value")
            else:
                raise ValueError(f"{family}.{name}: unknown type {t!r}")


def sample_params(family_space: dict, rng: np.random.Generator) -> dict:
    """Draw one random configuration from a family's search space."""
    out = {}
    for name, spec in family_space.items():
        t = spec["type"]
        if t == "int":
            out[name] = int(rng.integers(int(spec["low"]), int(spec["high"]) + 1))
        elif t == "float":
            lo, hi = float(spec["low"]), float(spec["high"])
            out[name] = float(np.exp(rng.uniform(np.log(lo), np.log(hi)))) if spec.get("log") \
                else float(rng.uniform(lo, hi))
        else:
            values = spec["values"]
            out[name] = values[int(rng.integers(len(values)))]
    return out


# ── Thresholds and metrics ────────────────────────────────────────────────────
def pick_threshold(y, p, objective: str = "f1", min_precision: float = 0.5,
                   min_recall: float = 0.5) -> dict:
    """
    Choose the decision threshold (predict fraud when p >= threshold) on scored data.

        f1 / average_precision : maximise F1
        recall                 : maximise recall subject to precision >= min_precision
        precision              : maximise precision subject to recall >= min_recall

    `feasible` is False when the constraint cannot be met; the F1-optimal threshold
    is returned instead so callers still get a usable value.
    """
    y = np.asarray(y).astype(int)
    p = np.asarray(p, dtype=float)
    prec, rec, thr = precision_recall_curve(y, p)
    prec, rec = prec[:-1], rec[:-1]            # align with thr
    if len(thr) == 0:
        return {"threshold": 0.5, "precision": 0.0, "recall": 0.0, "f1": 0.0, "feasible": False}
    f1 = 2 * prec * rec / np.maximum(prec + rec, 1e-12)

    all_ok = np.ones_like(f1, dtype=bool)
    if objective == "recall":
        mask, primary = prec >= min_precision, rec
    elif objective == "precision":
        mask, primary = rec >= min_recall, prec
    else:
        mask, primary = all_ok, f1
    feasible = bool(mask.any())
    if not feasible:
        mask, primary = all_ok, f1
    key = np.where(mask, primary + 1e-6 * f1, -np.inf)   # tie-break on F1
    i = int(np.argmax(key))
    return {"threshold": float(thr[i]), "precision": float(prec[i]), "recall": float(rec[i]),
            "f1": float(f1[i]), "feasible": feasible}


def objective_score(pick: dict, ap: float, objective: str) -> float:
    """Single number used to rank tuning trials for the chosen objective."""
    if objective == "average_precision":
        return float(ap)
    if objective in ("recall", "precision"):
        return float(pick[objective]) if pick["feasible"] else 0.0
    return float(pick["f1"])


def metrics_at(y, p, threshold: float) -> dict:
    """Classification metrics for scores `p` at a fixed threshold."""
    y = np.asarray(y).astype(int)
    pred = (np.asarray(p, dtype=float) >= threshold).astype(int)
    return {
        "accuracy":  float(accuracy_score(y, pred)),
        "precision": float(precision_score(y, pred, zero_division=0)),
        "recall":    float(recall_score(y, pred, zero_division=0)),
        "f1_score":  float(f1_score(y, pred, zero_division=0)),
        "average_precision": float(average_precision_score(y, p)),
        "threshold": float(threshold),
    }


# ── Sample data (mirrors producers/producer_tax_applications.py) ──────────────
_PROVINCES = ["GAUTENG", "WESTERN CAPE", "KWAZULU-NATAL", "EASTERN CAPE", "LIMPOPO",
              "MPUMALANGA", "NORTH WEST", "FREE STATE", "NORTHERN CAPE"]
_INCOME_RANGES = {"EMPLOYED": (80_000, 650_000), "SELF_EMPLOYED": (40_000, 900_000),
                  "CONTRACTOR": (60_000, 750_000), "UNEMPLOYED": (0, 30_000)}
_STATUSES = {"EMPLOYED": ["FULL_TIME", "PART_TIME"], "SELF_EMPLOYED": ["FULL_TIME", "SEASONAL"],
             "CONTRACTOR": ["FULL_TIME", "PART_TIME", "SEASONAL"], "UNEMPLOYED": ["N/A"]}
_FILINGS = ["FIRST_TIME", "RETURNING", "AMENDED"]


def make_sample_data(n_rows: int = 2000, seed: int = 42) -> pd.DataFrame:
    """
    Generated labelled applications with the same structure as the producer's
    stream: a repeat-filer pool, ~4% inflated incomes, ~1% random labels, and
    2-5 injected high-risk applications in every block of 50.
    """
    rng = np.random.default_rng(seed)
    pool = [f"C{i:05d}" for i in range(1, 5001)]
    repeaters = list(rng.choice(pool, size=50, replace=False))
    emp_types = list(_INCOME_RANGES)

    inject = set()
    for start in range(0, n_rows, 50):
        k = int(rng.integers(2, 6))
        inject.update(start + int(i) for i in rng.choice(50, size=min(k, 50), replace=False))

    rows = []
    for i in range(n_rows):
        cust = str(rng.choice(repeaters)) if rng.random() < 0.08 else str(rng.choice(pool))
        emp = str(rng.choice(emp_types))
        lo, hi = _INCOME_RANGES[emp]
        income = float(rng.uniform(lo, hi))
        inflated = rng.random() < 0.04
        if inflated:
            income *= float(rng.uniform(3.0, 8.0))
        fraud = bool(inflated or cust in repeaters or rng.random() < 0.01)
        status = str(rng.choice(_STATUSES[emp]))
        filing = str(rng.choice(_FILINGS))

        if i in inject:
            kind = str(rng.choice(["income_inflation", "phantom_income", "serial_filer"]))
            if kind == "phantom_income":
                emp, status = "UNEMPLOYED", "N/A"
                income, filing = float(rng.uniform(150_000, 1_200_000)), str(rng.choice(["RETURNING", "AMENDED"]))
            else:
                emp = str(rng.choice([t for t in emp_types if t != "UNEMPLOYED"]))
                status, filing = str(rng.choice(_STATUSES[emp])), "AMENDED"
                ceiling = _INCOME_RANGES[emp][1]
                if kind == "income_inflation":
                    income = ceiling * float(rng.uniform(2.5, 6.0))
                else:
                    income = ceiling * float(rng.uniform(1.5, 3.0))
                    cust = str(rng.choice(repeaters))
            fraud = True

        rows.append({
            "application_id": f"SAMPLE-{i:06d}", "customer_id": cust,
            "province": str(rng.choice(_PROVINCES)), "taxable_income": round(income, 2),
            "employment_type": emp, "employment_status": status,
            "filing_status": filing, "is_fraud": fraud,
        })
    return pd.DataFrame(rows)
