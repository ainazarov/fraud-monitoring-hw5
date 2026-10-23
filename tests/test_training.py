"""Integration-level invariants for the gate, temporal state and online reload."""

from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import pytest
from sklearn.dummy import DummyClassifier

from pipeline.data import advance_month, read_mart, seed_mart, synthetic_month
from pipeline.features import RAW_FEATURES, TransactionFeatures
from pipeline.registry import (
    HotReloadingModel, ModelRegistry, ModelUnavailable, atomic_joblib, atomic_json,
    read_json, sha256_file,
)
from pipeline import registry as registry_module
from pipeline import training


def run_pipeline(storage, run_id):
    prepared = training.prepare_window(run_id, storage)
    trained = training.train_candidate(prepared)
    evaluated = training.evaluate_candidate(trained)
    return training.publish_candidate(evaluated)


@pytest.fixture
def bootstrapped(tmp_path):
    seed_mart(tmp_path, rows_per_month=200)
    outcome = run_pipeline(tmp_path, "bootstrap")
    assert outcome["published"] is True
    return tmp_path


def next_evaluated(storage, run_id="next-model"):
    advance_month("supply-for-next", storage_dir=storage, batch_size=200)
    reference = training.prepare_window(run_id, storage)
    training.train_candidate(reference)
    training.evaluate_candidate(reference)
    return reference


def test_custom_features_haversine_cyclic_and_no_pii():
    frame = synthetic_month("2024-01", rows=50)
    frame.loc[0, ["lat", "lon", "merchant_lat", "merchant_lon"]] = 0
    frame.loc[0, "transaction_time"] = "2024-01-01 00:00:00"
    original = TransactionFeatures().fit_transform(frame)
    assert original.loc[0, "customer_merchant_km"] == pytest.approx(0)
    assert original.loc[0, "hour_sin"] == pytest.approx(0)
    assert original.loc[0, "hour_cos"] == pytest.approx(1)
    assert original.loc[0, "log_amount"] == pytest.approx(np.log1p(frame.loc[0, "amount"]))
    frame["target"] = 1 - frame["target"]
    frame["transaction_id"] = "changed"
    frame["name_1"] = "Any name"
    frame["street"] = "Any street"
    frame["jobs"] = "Any job"
    pd.testing.assert_frame_equal(original, TransactionFeatures().transform(frame))


def test_snapshot_is_strict_temporal_and_pinned_across_mart_changes(tmp_path):
    seed_mart(tmp_path, rows_per_month=200)
    reference = training.prepare_window("pinned", tmp_path)
    state = read_json(Path(reference["run_dir"]) / "prepared.json")
    data = joblib.load(state["snapshot_path"])
    assert state["train_months"] == ["2024-01", "2024-02"]
    assert state["test_month"] == "2024-03"
    assert pd.to_datetime(data["train"].transaction_time).max() < pd.to_datetime(data["test"].transaction_time).min()
    assert set(data["train"].columns) == set(RAW_FEATURES + ["target"])
    training.train_candidate(reference)
    training.evaluate_candidate(reference)
    first = training.publish_candidate(reference)
    advance_month("supply-after-pin", storage_dir=tmp_path, batch_size=200)
    repeated = training.prepare_window("pinned", tmp_path)
    assert repeated == reference
    assert read_json(Path(reference["run_dir"]) / "prepared.json") == state
    assert training.publish_candidate(repeated) == first
    assert len(list((tmp_path / "models").glob("*.joblib"))) == 1


def test_bad_candidate_is_successful_rejection_without_pointer_advance(bootstrapped):
    storage = bootstrapped
    before = (storage / "models" / "current.json").read_bytes()
    advance_month("supply-for-bad", storage_dir=storage, batch_size=200)
    reference = training.prepare_window("bad-model", storage)
    training.train_candidate(reference)
    directory = Path(reference["run_dir"])
    train_meta = read_json(directory / "training.json")
    prepared = read_json(directory / "prepared.json")
    train = joblib.load(prepared["snapshot_path"])["train"]
    bad_model = DummyClassifier(strategy="prior").fit(train[RAW_FEATURES], train["target"])
    atomic_joblib(train_meta["candidate_path"], {"version": train_meta["version"], "pipeline": bad_model})
    train_meta["sha256"] = sha256_file(train_meta["candidate_path"])
    atomic_json(directory / "training.json", train_meta)
    training.evaluate_candidate(reference)
    evaluation = read_json(directory / "evaluation.json")
    assert evaluation["candidate_metrics"]["roc_auc"] == pytest.approx(0.5)
    assert evaluation["incumbent_metrics"]["roc_auc"] > 0.9
    decision = training.publish_candidate(reference)
    assert decision["status"] == "rejected"
    assert decision["published"] is False
    assert (storage / "models" / "current.json").read_bytes() == before
    assert len(list((storage / "models").glob("*.joblib"))) == 1
    # A new run evaluates the same pending month; it cannot silently skip it.
    next_reference = training.prepare_window("retry-after-rejection", storage)
    assert read_json(Path(next_reference["run_dir"]) / "prepared.json")["window_end"] == "2024-04"
    assert advance_month("supply-blocked", storage_dir=storage, batch_size=200)["status"] == "waiting_for_model"


@pytest.mark.parametrize("candidate,expected", [
    ({"pr_auc": 0.8, "roc_auc": 0.9}, True),
    ({"pr_auc": 0.9, "roc_auc": 0.89}, False),
    ({"pr_auc": 0.79, "roc_auc": 0.95}, False),
    ({"pr_auc": 0.8 - 5e-13, "roc_auc": 0.9}, True),
])
def test_gate_requires_both_metrics_and_accepts_numerical_ties(candidate, expected):
    assert training.compare_metrics(candidate, {"pr_auc": 0.8, "roc_auc": 0.9}) is expected


def test_failed_pointer_write_keeps_month_then_retry_reuses_same_artifact(bootstrapped, monkeypatch):
    storage = bootstrapped
    before = (storage / "models" / "current.json").read_bytes()
    reference = next_evaluated(storage)
    original_atomic_json = registry_module.atomic_json

    def fail_manifest(path, value):
        if Path(path).name == "current.json":
            raise OSError("Injected disk failure before pointer replacement")
        return original_atomic_json(path, value)

    monkeypatch.setattr(registry_module, "atomic_json", fail_manifest)
    with pytest.raises(OSError, match="Injected disk failure"):
        training.publish_candidate(reference)
    assert (storage / "models" / "current.json").read_bytes() == before
    models_after_failure = sorted((storage / "models").glob("*.joblib"))
    assert len(models_after_failure) == 2  # Safe orphan; the current pointer remains old.
    monkeypatch.setattr(registry_module, "atomic_json", original_atomic_json)
    result = training.publish_candidate(reference)
    assert result["published"] is True
    assert result["window_end"] == "2024-04"
    assert training.publish_candidate(reference) == result
    assert sorted((storage / "models").glob("*.joblib")) == models_after_failure


def test_retry_after_commit_before_decision_never_advances_twice(bootstrapped, monkeypatch):
    reference = next_evaluated(bootstrapped)
    original_record = training._record_decision

    def fail_receipt(*args, **kwargs):
        raise OSError("Injected receipt failure after commit")

    monkeypatch.setattr(training, "_record_decision", fail_receipt)
    with pytest.raises(OSError, match="receipt failure"):
        training.publish_candidate(reference)
    committed_bytes = (bootstrapped / "models" / "current.json").read_bytes()
    monkeypatch.setattr(training, "_record_decision", original_record)
    outcome = training.publish_candidate(reference)
    assert outcome["published"] is True
    assert (bootstrapped / "models" / "current.json").read_bytes() == committed_bytes
    assert len(list((bootstrapped / "models").glob("*.joblib"))) == 2


def test_stale_run_cannot_overwrite_a_newer_publication(bootstrapped):
    storage = bootstrapped
    first = next_evaluated(storage, "race-first")
    second = training.prepare_window("race-second", storage)
    training.train_candidate(second)
    training.evaluate_candidate(second)
    assert training.publish_candidate(first)["published"]
    committed = (storage / "models" / "current.json").read_bytes()
    stale = training.publish_candidate(second)
    assert stale["status"] == "stale_baseline"
    assert stale["published"] is False
    assert (storage / "models" / "current.json").read_bytes() == committed
    assert len(list((storage / "models").glob("*.joblib"))) == 2


def test_candidate_changed_after_evaluation_is_never_published(bootstrapped):
    reference = next_evaluated(bootstrapped)
    evaluation = read_json(Path(reference["run_dir"]) / "evaluation.json")
    before = (bootstrapped / "models" / "current.json").read_bytes()
    with Path(evaluation["candidate_path"]).open("ab") as output:
        output.write(b"corrupted after evaluation")
    with pytest.raises(ValueError, match="changed after evaluation"):
        training.publish_candidate(reference)
    assert (bootstrapped / "models" / "current.json").read_bytes() == before


def test_hot_reload_new_version_and_fallback_for_invalid_artifact(bootstrapped):
    storage = bootstrapped
    loader = HotReloadingModel(storage)
    sample = read_mart(storage).head(8)
    _, old_version = loader.predict(sample)
    next_ref = next_evaluated(storage)
    new = training.publish_candidate(next_ref)
    valid_scores, new_version = loader.predict(sample)
    assert new_version == new["version"] != old_version
    registry = ModelRegistry(storage)
    broken = {**registry.current(), "version": "broken-version", "artifact": "model-broken.joblib", "sha256": "0" * 64}
    (storage / "models" / broken["artifact"]).write_bytes(b"broken model bytes")
    atomic_json(storage / "models" / "current.json", broken)
    fallback_scores, fallback_version = loader.predict(sample)
    # Parallel forest reductions can differ by one floating-point ULP.
    np.testing.assert_allclose(fallback_scores, valid_scores, rtol=1e-14, atol=1e-15)
    assert fallback_version == new_version
    assert "SHA256 mismatch" in loader.last_error
    (storage / "models" / "current.json").write_text("{partial-json", encoding="utf-8")
    assert loader.predict(sample)[1] == new_version


def test_no_bootstrap_model_is_explicitly_unavailable(tmp_path):
    with pytest.raises(ModelUnavailable, match="No published model"):
        HotReloadingModel(tmp_path).predict(synthetic_month("2024-01", 50))


def test_unknown_category_does_not_break_loaded_model(bootstrapped):
    frame = read_mart(bootstrapped).head(5)
    frame["cat_id"] = "never_seen_category"
    frame["gender"] = "unknown"
    scores, version = HotReloadingModel(bootstrapped).predict(frame)
    assert np.isfinite(scores).all()
    assert ((scores >= 0) & (scores <= 1)).all()
    assert version == ModelRegistry(bootstrapped).current()["version"]


def test_missing_next_month_records_wait_and_keeps_pointer(bootstrapped):
    before = (bootstrapped / "models" / "current.json").read_bytes()
    result = run_pipeline(bootstrapped, "waiting-run")
    assert result["status"] == "waiting_for_data"
    assert result["missing_months"] == ["2024-04"]
    assert (bootstrapped / "models" / "current.json").read_bytes() == before


def test_failed_preparation_pins_input_and_baseline(tmp_path, monkeypatch):
    seed_mart(tmp_path, rows_per_month=200)
    import pipeline.quality as quality
    original = quality.assert_quality
    monkeypatch.setattr(quality, "assert_quality", lambda report: (_ for _ in ()).throw(ValueError("Injected DQ failure")))
    with pytest.raises(ValueError, match="Injected DQ failure"):
        training.prepare_window("failed-prepare", tmp_path)
    selection_path = next((tmp_path / "work").glob("*/selection.json"))
    selection = read_json(selection_path)
    monkeypatch.setattr(quality, "assert_quality", original)
    run_pipeline(tmp_path, "other-bootstrap")
    reference = training.prepare_window("failed-prepare", tmp_path)
    state = read_json(Path(reference["run_dir"]) / "prepared.json")
    assert state["baseline"] is None
    assert state["window_end"] == selection["window_end"] == "2024-03"
    assert state["input_sha256"] == selection["input_sha256"]
