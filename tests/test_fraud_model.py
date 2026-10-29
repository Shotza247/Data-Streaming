"""
tests/test_fraud_model.py
-------------------------
Offline tests for mlflow/fraud_model.py: feature builder, thresholds, search
space sampling and a learning check on generated data (no MLflow, no MinIO).

Run:  python -m pytest tests/test_fraud_model.py
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "mlflow"))

import fraud_model as fm  # noqa: E402


def _row(app="a1", cust="C1", income=100_000.0, emp="EMPLOYED"):
    return {"application_id": app, "customer_id": cust, "province": "GAUTENG",
            "taxable_income": income, "employment_type": emp,
            "employment_status": "FULL_TIME", "filing_status": "RETURNING"}


# ── Feature builder ───────────────────────────────────────────────────────────
def test_income_features_relative_to_employment_ceiling():
    X = pd.DataFrame([_row("a", "C1", 325_000.0, "EMPLOYED"),
                      _row("b", "C2", 60_000.0, "UNEMPLOYED")])
    out = fm.FraudFeatureBuilder().fit(X).transform(X)
    assert out.loc[0, "income_ratio"] == pytest.approx(0.5) and out.loc[0, "above_ceiling"] == 0
    assert out.loc[1, "income_ratio"] == pytest.approx(2.0) and out.loc[1, "above_ceiling"] == 1
    assert out.loc[1, "log_income"] == pytest.approx(np.log1p(60_000.0))


def test_customer_filings_counts_training_history():
    X = pd.DataFrame([_row("a", "C1"), _row("b", "C1"), _row("c", "C1"), _row("d", "C2")])
    fb = fm.FraudFeatureBuilder()
    train = fb.fit_transform(X)
    assert train["customer_filings"].tolist() == [3, 3, 3, 1]


def test_new_application_counts_itself_on_top_of_history():
    X = pd.DataFrame([_row("a", "C1"), _row("b", "C1")])
    fb = fm.FraudFeatureBuilder().fit(X)
    new = pd.DataFrame([_row("zzz", "C1"), _row("yyy", "C-new")])
    assert fb.transform(new)["customer_filings"].tolist() == [3, 1]       # 2 known + itself ; unseen -> 1
    # re-scoring a training application must not double count it
    assert fb.transform(X.iloc[[0]])["customer_filings"].tolist() == [2]


def test_builder_tolerates_missing_and_dirty_columns():
    X = pd.DataFrame([{"province": None, "taxable_income": "abc", "employment_type": "freelancer"}])
    out = fm.FraudFeatureBuilder().fit(X).transform(X)
    assert out.loc[0, "taxable_income"] == 0.0 and out.loc[0, "customer_filings"] == 1.0
    assert out.loc[0, "province"] == "UNKNOWN" and out.loc[0, "employment_type"] == "FREELANCER"
    assert out.loc[0, "income_ratio"] == 0.0


# ── Thresholds ────────────────────────────────────────────────────────────────
Y = np.array([0, 0, 0, 0, 0, 0, 1, 1, 1, 1])
P = np.array([.05, .1, .15, .2, .3, .55, .5, .6, .8, .9])      # one legit (0.55) scores above a fraud (0.5)


def test_f1_threshold_beats_default_cut():
    pick = fm.pick_threshold(Y, P, "f1")
    assert pick["feasible"] and pick["f1"] >= fm.metrics_at(Y, P, 0.5)["f1_score"]


def test_recall_objective_respects_precision_floor():
    pick = fm.pick_threshold(Y, P, "recall", min_precision=0.8)
    assert pick["feasible"] and pick["precision"] >= 0.8
    loose = fm.pick_threshold(Y, P, "recall", min_precision=0.1)
    assert loose["recall"] >= pick["recall"]


def test_precision_objective_respects_recall_floor():
    pick = fm.pick_threshold(Y, P, "precision", min_recall=0.5)
    assert pick["feasible"] and pick["recall"] >= 0.5 and pick["precision"] == pytest.approx(1.0)


def test_infeasible_constraint_falls_back_to_f1():
    pick = fm.pick_threshold(Y, P, "recall", min_precision=1.01)
    assert pick["feasible"] is False
    assert pick["f1"] == pytest.approx(fm.pick_threshold(Y, P, "f1")["f1"])
    assert fm.objective_score(pick, 0.9, "recall") == 0.0


def test_metrics_at_threshold():
    m = fm.metrics_at(Y, P, 0.5)
    assert m["recall"] == pytest.approx(1.0) and m["precision"] == pytest.approx(4 / 5)


# ── Search space ──────────────────────────────────────────────────────────────
def test_sampling_stays_inside_the_space():
    rng = np.random.default_rng(0)
    space = fm.DEFAULT_SPACE["random_forest"]
    for _ in range(200):
        p = fm.sample_params(space, rng)
        assert 100 <= p["n_estimators"] <= 500 and 4 <= p["max_depth"] <= 20
        assert 1 <= p["min_samples_leaf"] <= 10
        assert p["max_features"] in space["max_features"]["values"]
        assert p["class_weight"] in space["class_weight"]["values"]
    lr = fm.sample_params(fm.DEFAULT_SPACE["logistic_regression"], rng)
    assert 0.01 <= lr["C"] <= 100.0


def test_validate_space_rejects_bad_input():
    fm.validate_space(fm.DEFAULT_SPACE)
    with pytest.raises(ValueError, match="low"):
        fm.validate_space({"random_forest": {"max_depth": {"type": "int", "low": 9, "high": 3}}})
    with pytest.raises(ValueError, match="log-scale"):
        fm.validate_space({"gradient_boosting": {"learning_rate": {"type": "float", "low": 0, "high": 1, "log": True}}})
    with pytest.raises(ValueError, match="at least one"):
        fm.validate_space({"random_forest": {"max_features": {"type": "choice", "values": []}}})
    with pytest.raises(ValueError, match="Unknown model family"):
        fm.validate_space({"svm": {}})


def test_unknown_family_rejected():
    with pytest.raises(ValueError):
        fm.build_pipeline("svm")


# ── Learning check ────────────────────────────────────────────────────────────
@pytest.mark.parametrize("family", fm.FAMILIES)
def test_each_family_trains_and_scores_valid_probabilities(family):
    df = fm.make_sample_data(600, seed=1)
    X, y = df[fm.INPUT_COLUMNS], df["is_fraud"].astype(int)
    pipe = fm.build_pipeline(family, {"n_estimators": 40} if family == "random_forest" else None).fit(X, y)
    p = pipe.predict_proba(X.head(20))[:, 1]
    assert p.shape == (20,) and ((p >= 0) & (p <= 1)).all()


def test_feature_engineering_improves_over_raw_income_baseline():
    """The reason for the shared pipeline: income vs ceiling + filing velocity lift F1 a lot."""
    from sklearn.compose import ColumnTransformer
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import StratifiedKFold, cross_val_predict
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder

    df = fm.make_sample_data(2000, seed=7)
    X, y = df[fm.INPUT_COLUMNS], df["is_fraud"].astype(int)
    cv = StratifiedKFold(5, shuffle=True, random_state=0)

    baseline = Pipeline([
        ("p", ColumnTransformer([("c", OneHotEncoder(handle_unknown="ignore"), fm.CATEGORICAL_FEATURES),
                                 ("n", "passthrough", ["taxable_income"])])),
        ("m", RandomForestClassifier(n_estimators=100, max_depth=8, class_weight="balanced",
                                     random_state=0)),
    ])
    base_p = cross_val_predict(baseline, X, y, cv=cv, method="predict_proba")[:, 1]
    new_p = cross_val_predict(fm.build_pipeline("random_forest", {"n_estimators": 150}), X, y, cv=cv,
                              method="predict_proba")[:, 1]
    base_f1 = fm.pick_threshold(y, base_p, "f1")["f1"]
    new_f1 = fm.pick_threshold(y, new_p, "f1")["f1"]
    assert new_f1 > base_f1 + 0.15, f"baseline F1 {base_f1:.2f} vs engineered F1 {new_f1:.2f}"
    assert new_f1 > 0.7


def test_sample_data_has_injected_cases_in_every_block():
    df = fm.make_sample_data(1000, seed=3)
    assert df["application_id"].is_unique
    assert df["is_fraud"].astype(int).sum() >= 20 * 2                # >= 2 per block of 50 (20 blocks)
    unemployed_rich = df[(df.employment_type == "UNEMPLOYED") & (df.taxable_income >= 150_000)]
    assert len(unemployed_rich) > 0 and unemployed_rich["is_fraud"].all()
