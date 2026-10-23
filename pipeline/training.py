"""Explicit, retry-safe Airflow task functions for temporal retraining.

Every run pins both its input rows and its incumbent before fitting. The newest
month is validation data used to select the model, not an unbiased final test.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score

from pipeline.features import FEATURE_SCHEMA_VERSION, RAW_FEATURES, build_pipeline
from pipeline.registry import (
    ModelRegistry, StaleBaseline, atomic_joblib, atomic_json, file_lock,
    fraud_probabilities, read_json, sha256_file, storage_root,
)


WINDOW_MONTHS = 3
METRIC_TOLERANCE = 1e-12


def _now():
    return datetime.now(timezone.utc).isoformat()


def _reference(directory, storage, status):
    return {"run_dir": str(directory), "storage_dir": str(storage), "status": status}


def _task_input(explicit, context, task_id):
    if explicit is not None:
        return explicit
    return context["ti"].xcom_pull(task_ids=task_id)


def _run_state(reference):
    directory = Path(reference["run_dir"])
    return directory, read_json(directory / "prepared.json")


def _snapshot(prepared):
    if sha256_file(prepared["snapshot_path"]) != prepared["snapshot_sha256"]:
        raise ValueError("Pinned training snapshot has changed")
    return joblib.load(prepared["snapshot_path"])


def prepare_window(run_id=None, storage_dir=None, **context):
    """Read the labeled mart/current pointer; persist a pinned temporal snapshot.

    Writes prepared.json and snapshot.joblib under work/<hash of run_id> after
    validating the selected three months. An existing prepared.json is reused,
    so clearing/retrying a run cannot skip a month or change its incumbent. No
    deployment/month pointer is modified by preparation (including on failure).
    """
    from pipeline.data import read_mart
    from pipeline.quality import assert_quality, validate_frame

    run_id = str(run_id or context.get("run_id") or "manual-bootstrap")
    storage = storage_root(storage_dir)
    run_key = hashlib.sha256(run_id.encode()).hexdigest()[:20]
    directory = storage / "work" / run_key
    directory.mkdir(parents=True, exist_ok=True)
    with file_lock(directory / ".run.lock"):
        prepared_path = directory / "prepared.json"
        if prepared_path.exists():
            prepared = read_json(prepared_path)
            if prepared["run_id"] != run_id:
                raise ValueError("Run identifier hash collision")
            return _reference(directory, storage, prepared["status"])
        selection_path = directory / "selection.json"
        if selection_path.exists():
            common = read_json(selection_path)
            if sha256_file(common["input_path"]) != common["input_sha256"]:
                raise ValueError("Pinned input rows were modified")
            selected = joblib.load(common["input_path"])
            months = common["window_months"]
        else:
            baseline = ModelRegistry(storage).current()
            # Reading once supplies one SQLite snapshot, even when the
            # independent monthly-supply DAG commits concurrently.
            frame = read_mart(storage_dir=storage)
            timestamp = pd.to_datetime(frame["transaction_time"], errors="raise", utc=True)
            month = timestamp.dt.strftime("%Y-%m")
            available = sorted(month.dropna().unique().tolist())
            if not available:
                raise ValueError("The labeled mart is empty; initialize data first")
            if baseline:
                end = pd.Period(baseline["window_end"], freq="M") + 1
            else:
                end = pd.Period(available[0], freq="M") + WINDOW_MONTHS - 1
            months = [str(end - offset) for offset in reversed(range(WINDOW_MONTHS))]
            common = {
                "run_id": run_id, "run_key": run_key, "storage_dir": str(storage),
                "run_dir": str(directory), "baseline": baseline, "window_months": months,
                "window_start": months[0], "window_end": months[-1],
                "train_months": months[:-1], "test_month": months[-1],
                "created_at": _now(),
            }
            missing = sorted(set(months) - set(available))
            if missing:
                atomic_json(prepared_path, {
                    **common, "status": "waiting_for_data", "missing_months": missing,
                    "reason": "Next contiguous window is not yet available; pointer unchanged",
                })
                return _reference(directory, storage, "waiting_for_data")
            selected = frame.loc[month.isin(months)].copy()
            input_path = directory / "input.joblib"
            atomic_joblib(input_path, selected)
            common.update({"input_path": str(input_path), "input_sha256": sha256_file(input_path)})
            # Pin before the quality gate so even a failed preparation retry
            # cannot silently switch to different rows, month, or incumbent.
            atomic_json(selection_path, common)
        # Quality checks run against the exact snapshot that will be fitted,
        # independent of the scheduled HTML-reporting DQ DAG.
        report = validate_frame(selected, end_month=months[-1], window_months=WINDOW_MONTHS)
        atomic_json(directory / "quality.json", report)
        assert_quality(report)
        selected_month = pd.to_datetime(selected["transaction_time"], utc=True).dt.strftime("%Y-%m")
        columns = RAW_FEATURES + ["target"]
        train = selected.loc[selected_month.isin(months[:-1]), columns].copy()
        test = selected.loc[selected_month.eq(months[-1]), columns].copy()
        for split_name, split in [("train", train), ("validation", test)]:
            if set(split["target"].unique()) != {0, 1}:
                raise ValueError(f"{split_name} must contain both binary target classes")
        snapshot_path = directory / "snapshot.joblib"
        atomic_joblib(snapshot_path, {"train": train, "test": test})
        atomic_json(prepared_path, {
            **common, "status": "prepared", "snapshot_path": str(snapshot_path),
            "snapshot_sha256": sha256_file(snapshot_path), "train_rows": len(train),
            "test_rows": len(test), "version": f"{months[-1].replace('-', '')}-{run_key[:12]}",
        })
        return _reference(directory, storage, "prepared")


def train_candidate(prepared=None, **context):
    """Read pinned train rows; write a fitted candidate bundle and checksum.

    The estimator and preprocessing are fitted on the oldest two months only.
    The work-directory candidate is not visible to production. A completed
    training.json is reused on retry; its bytes must still match the checksum.
    """
    reference = _task_input(prepared, context, "prepare_window")
    directory, state = _run_state(reference)
    if state["status"] == "waiting_for_data":
        return reference
    with file_lock(directory / ".run.lock"):
        training_path = directory / "training.json"
        if training_path.exists():
            trained = read_json(training_path)
            if sha256_file(trained["candidate_path"]) != trained["sha256"]:
                raise ValueError("Previously trained candidate was modified")
            return _reference(directory, state["storage_dir"], "trained")
        data = _snapshot(state)
        train = data["train"]
        pipeline = build_pipeline()
        pipeline.fit(train[RAW_FEATURES], train["target"].astype(int))
        candidate_path = directory / "candidate.joblib"
        atomic_joblib(candidate_path, {
            "pipeline": pipeline, "version": state["version"],
            "feature_schema_version": FEATURE_SCHEMA_VERSION,
            "raw_features": RAW_FEATURES, "window_start": state["window_start"],
            "window_end": state["window_end"], "run_id": state["run_id"],
        })
        atomic_json(training_path, {
            "candidate_path": str(candidate_path), "sha256": sha256_file(candidate_path),
            "version": state["version"], "trained_at": _now(),
            "train_rows": len(train), "train_months": state["train_months"],
        })
        return _reference(directory, state["storage_dir"], "trained")


def _metrics(labels, scores):
    if set(np.asarray(labels).tolist()) != {0, 1}:
        raise ValueError("Validation requires both positive and negative labels")
    result = {
        "pr_auc": float(average_precision_score(labels, scores)),
        "roc_auc": float(roc_auc_score(labels, scores)),
    }
    if not all(np.isfinite(value) for value in result.values()):
        raise ValueError("Model metrics must be finite")
    # Floating-point summation can yield AP=1.0000000000000002.
    return {name: float(np.clip(value, 0, 1)) for name, value in result.items()}


def compare_metrics(candidate, incumbent, tolerance=METRIC_TOLERANCE):
    """Accept bootstrap or nonregression on BOTH same-window validation metrics."""
    for values in [candidate] + ([incumbent] if incumbent is not None else []):
        if set(values) != {"pr_auc", "roc_auc"} or not all(np.isfinite(v) and -tolerance <= v <= 1 + tolerance for v in values.values()):
            raise ValueError("Both PR-AUC and ROC-AUC must be finite probabilities")
    return incumbent is None or all(
        candidate[name] + tolerance >= incumbent[name] for name in ("pr_auc", "roc_auc")
    )


def evaluate_candidate(trained=None, **context):
    """Compare candidate and pinned incumbent on the SAME newest-month rows.

    Writes evaluation.json with both PR-AUC (average precision) and ROC-AUC.
    A regression in either metric rejects the candidate. Completed evaluation
    is reused on retry, and never writes current.json or advances the month.
    """
    reference = _task_input(trained, context, "train_candidate")
    directory, state = _run_state(reference)
    if state["status"] == "waiting_for_data":
        return reference
    with file_lock(directory / ".run.lock"):
        evaluation_path = directory / "evaluation.json"
        if evaluation_path.exists():
            return _reference(directory, state["storage_dir"], "evaluated")
        training = read_json(directory / "training.json")
        if sha256_file(training["candidate_path"]) != training["sha256"]:
            raise ValueError("Candidate changed before evaluation")
        test = _snapshot(state)["test"]
        bundle = joblib.load(training["candidate_path"])
        if bundle["version"] != training["version"]:
            raise ValueError("Candidate bundle version mismatch")
        candidate_metrics = _metrics(test["target"], fraud_probabilities(bundle["pipeline"], test[RAW_FEATURES]))
        incumbent_metrics = None
        if state["baseline"] is not None:
            incumbent = ModelRegistry(state["storage_dir"]).load(state["baseline"])
            incumbent_metrics = _metrics(test["target"], fraud_probabilities(incumbent["pipeline"], test[RAW_FEATURES]))
        approved = compare_metrics(candidate_metrics, incumbent_metrics)
        atomic_json(evaluation_path, {
            **training, "approved": approved, "candidate_metrics": candidate_metrics,
            "incumbent_metrics": incumbent_metrics, "test_month": state["test_month"],
            "test_rows": len(test), "snapshot_sha256": state["snapshot_sha256"],
            "tolerance": METRIC_TOLERANCE, "evaluated_at": _now(),
            "reason": "bootstrap" if incumbent_metrics is None else (
                "Both validation metrics are no worse" if approved else "Validation metric regression"
            ),
            "metric_note": "PR-AUC is sklearn average_precision_score; newest month is model-selection validation, not a final holdout",
        })
        return _reference(directory, state["storage_dir"], "evaluated")


def _record_decision(directory, state, decision):
    # Global reports are written first: a retry can finish either interrupted write.
    atomic_json(Path(state["storage_dir"]) / "reports" / "decisions" / f"{state['run_key']}.json", decision)
    atomic_json(directory / "decision.json", decision)
    return decision


def publish_candidate(evaluated=None, **context):
    """Publish only an approved candidate; record rejection as successful work.

    Reads evaluation.json and the pinned baseline. Under a publication flock,
    the registry saves/fsyncs an immutable binary before atomically replacing
    current.json (which contains both version and committed window_end). A
    retried publish reuses its version; stale runs and rejected models cannot
    advance the pointer. decision.json is an idempotent, reviewable outcome.
    """
    reference = _task_input(evaluated, context, "evaluate_candidate")
    directory, state = _run_state(reference)
    with file_lock(directory / ".run.lock"):
        decision_path = directory / "decision.json"
        if decision_path.exists():
            return read_json(decision_path)
        common = {
            "run_id": state["run_id"], "run_key": state["run_key"],
            "window_start": state["window_start"], "window_end": state["window_end"],
            "decided_at": _now(), "run_dir": str(directory),
        }
        if state["status"] == "waiting_for_data":
            return _record_decision(directory, state, {
                **common, "status": "waiting_for_data", "published": False,
                "reason": state["reason"], "missing_months": state["missing_months"],
            })
        evaluation = read_json(directory / "evaluation.json")
        common.update({
            "version": evaluation["version"], "candidate_metrics": evaluation["candidate_metrics"],
            "incumbent_metrics": evaluation["incumbent_metrics"], "reason": evaluation["reason"],
        })
        if not evaluation["approved"]:
            return _record_decision(directory, state, {**common, "status": "rejected", "published": False})
        registry = ModelRegistry(state["storage_dir"])
        manifest = {
            "schema_version": 1, "version": evaluation["version"],
            "artifact": f"model-{evaluation['version']}.joblib", "sha256": evaluation["sha256"],
            "window_start": state["window_start"], "window_end": state["window_end"],
            "metrics": evaluation["candidate_metrics"], "run_id": state["run_id"],
            "published_at": _now(), "validation_month": state["test_month"],
        }
        try:
            committed = registry.publish(evaluation["candidate_path"], manifest, state["baseline"])
        except StaleBaseline as error:
            return _record_decision(directory, state, {
                **common, "status": "stale_baseline", "published": False, "reason": str(error),
            })
        return _record_decision(directory, state, {
            **common, "status": "published", "published": True, "manifest": committed,
        })
