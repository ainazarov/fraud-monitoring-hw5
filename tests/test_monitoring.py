import json
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock
from urllib.request import urlopen

import numpy as np
import pytest

from common.kafka import DeliveryError
from common.metrics import RuntimeMetrics
from fraud_detector.app import process_message as score_transaction
from fraud_detector.scorer import FraudScorer
from pipeline.registry import HotReloadingModel, ModelUnavailable
from scoring_writer.app import process_message as write_score


SCORE = {"transaction_id": "t1", "score": 0.73, "fraud_flag": 1, "model_version": "v1"}


def message(payload):
    result = Mock()
    result.value.return_value = json.dumps(payload).encode()
    result.topic.return_value = "scores"
    result.partition.return_value = 0
    result.offset.return_value = 1
    return result


def connection(rowcount=1):
    result = Mock()
    cursor = Mock(rowcount=rowcount)
    result.cursor.return_value.__enter__ = Mock(return_value=cursor)
    result.cursor.return_value.__exit__ = Mock(return_value=False)
    return result


def producer(error=None):
    result = Mock()
    result.produce.side_effect = lambda *args, **kwargs: kwargs["on_delivery"](error, Mock())
    result.flush.return_value = 0
    return result


def sample(metrics, name, labels=None):
    return metrics.registry.get_sample_value(name, labels or {})


def test_scorer_exports_only_acknowledged_scores_and_propagates_context(monkeypatch):
    metrics = RuntimeMetrics("scorer")
    transaction = {
        "transaction_id": "t1", "emitted_at": "2026-10-23T15:00:00+00:00",
        "data": {"us_state": "tx", "merch": "Shop", "cat_id": "grocery", "amount": 9.5, "transaction_time": "2019-09-14 02:46"},
    }
    monkeypatch.setattr("fraud_detector.app.validate_transaction", lambda value: value)
    scorer = Mock()
    scorer.score.return_value = {key: value for key, value in SCORE.items() if key != "transaction_id"}
    broker = producer()
    consumer = Mock()
    before = datetime.now(timezone.utc)
    score_transaction(message(transaction), consumer, broker, scorer, metrics=metrics)
    output = json.loads(broker.produce.call_args.kwargs["value"])
    assert output["us_state"] == "TX"
    assert output["merch"] == "Shop"
    assert output["amount"] == 9.5
    assert output["emitted_at"] == transaction["emitted_at"]
    assert before <= datetime.fromisoformat(output["processed_at"]) <= datetime.now(timezone.utc)
    assert sample(metrics, "fw_transactions_total") == 1
    assert sample(metrics, "fw_processing_seconds_count") == 1
    assert sample(metrics, "fw_score_sum") == 0.73
    assert sample(metrics, "fw_fraud_total") == 1
    assert sample(metrics, "fw_last_transaction_timestamp_seconds") >= before.timestamp()

    with pytest.raises(DeliveryError):
        score_transaction(message(transaction), Mock(), producer("offline"), scorer, metrics=metrics)
    assert sample(metrics, "fw_transactions_total") == 1
    assert sample(metrics, "fw_processing_seconds_count") == 1


def test_failed_prediction_does_not_count_a_published_score(monkeypatch):
    metrics = RuntimeMetrics("scorer")
    monkeypatch.setattr("fraud_detector.app.validate_transaction", lambda value: value)
    scorer = Mock()
    scorer.score.side_effect = RuntimeError("model unavailable")
    consumer, broker = Mock(), producer()
    with pytest.raises(RuntimeError, match="model unavailable"):
        score_transaction(message({"transaction_id": "t1", "data": {"amount": 1}}), consumer, broker, scorer, metrics=metrics)
    assert sample(metrics, "fw_transactions_total") == 0
    assert sample(metrics, "fw_processing_seconds_count") == 0
    assert sample(metrics, "fw_fraud_total") == 0
    consumer.commit.assert_not_called()
    broker.produce.assert_not_called()


def test_writer_counts_committed_rows_once_and_measures_producer_to_database_latency():
    metrics = RuntimeMetrics("writer")
    rich = {**SCORE, "emitted_at": (datetime.now(timezone.utc) - timedelta(seconds=2)).isoformat(), "transaction_time": "2019-01-01"}
    conn = connection()
    write_score(message(rich), Mock(), producer(), conn, metrics=metrics)
    assert sample(metrics, "fw_scores_written_total") == 1
    assert sample(metrics, "fw_write_seconds_count") == 1
    assert sample(metrics, "fw_written_score_sum") == 0.73
    assert sample(metrics, "fw_written_fraud_total") == 1
    assert sample(metrics, "fw_end_to_end_seconds_count") == 1
    assert 2 <= sample(metrics, "fw_end_to_end_seconds_sum") < 10

    write_score(message(rich), Mock(), producer(), connection(rowcount=0), metrics=metrics)
    assert sample(metrics, "fw_write_duplicates_total") == 1
    assert sample(metrics, "fw_scores_written_total") == 1
    assert sample(metrics, "fw_write_seconds_count") == 1
    assert sample(metrics, "fw_end_to_end_seconds_count") == 1


def test_writer_failure_does_not_count_as_success():
    metrics = RuntimeMetrics("writer")
    conn = connection()
    conn.commit.side_effect = RuntimeError("database unavailable")
    with pytest.raises(RuntimeError):
        write_score(message(SCORE), Mock(), producer(), conn, metrics=metrics)
    assert sample(metrics, "fw_scores_written_total") == 0
    assert sample(metrics, "fw_written_fraud_total") == 0
    assert sample(metrics, "fw_write_seconds_count") == 0


def test_writer_persists_all_normalized_business_context():
    metrics = RuntimeMetrics("writer")
    rich = {
        **SCORE, "us_state": " tx ", "merch": " Shop ", "cat_id": " grocery ", "amount": "9.5",
        "transaction_time": "2019-01-01", "processed_at": "2026-10-23T22:00:02+03:00",
        "emitted_at": "2026-10-23T22:00:00+03:00",
    }
    conn = connection()
    write_score(message(rich), Mock(), producer(), conn, metrics=metrics)
    arguments = conn.cursor.return_value.__enter__.return_value.execute.call_args.args[1]
    assert arguments == (
        "t1", 0.73, 1, "v1", "TX", "Shop", "grocery", 9.5,
        "2019-01-01T00:00:00+00:00", "2026-10-23T19:00:02+00:00", "2026-10-23T19:00:00+00:00",
    )


def test_persisted_row_is_counted_when_kafka_offset_commit_fails():
    metrics = RuntimeMetrics("writer")
    consumer = Mock()
    consumer.commit.side_effect = RuntimeError("broker unavailable")
    with pytest.raises(RuntimeError):
        write_score(message(SCORE), consumer, producer(), connection(), metrics=metrics)
    assert sample(metrics, "fw_scores_written_total") == 1


@pytest.mark.parametrize("extra", [{}, {"emitted_at": (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()}])
def test_legacy_scores_and_clock_skew_do_not_distort_end_to_end_latency(extra):
    metrics = RuntimeMetrics("writer")
    write_score(message({**SCORE, **extra}), Mock(), producer(), connection(), metrics=metrics)
    assert sample(metrics, "fw_scores_written_total") == 1
    assert sample(metrics, "fw_end_to_end_seconds_count") == 0


def test_rejections_count_only_after_dlq_acknowledgement():
    metrics = RuntimeMetrics("writer")
    write_score(message({}), Mock(), producer(), connection(), metrics=metrics)
    assert sample(metrics, "fw_rejected_total", {"service": "writer"}) == 1
    with pytest.raises(DeliveryError):
        write_score(message({}), Mock(), producer("offline"), connection(), metrics=metrics)
    assert sample(metrics, "fw_rejected_total", {"service": "writer"}) == 1
    assert sample(metrics, "fw_scores_written_total") == 0


def test_actual_model_activation_and_failed_reload_keep_version_consistent(tmp_path):
    metrics = RuntimeMetrics("scorer")
    model = HotReloadingModel(tmp_path)
    pipeline = Mock(classes_=np.array([0, 1]))
    pipeline.predict_proba.return_value = np.array([[0.27, 0.73]])
    model.registry.current = Mock(return_value={"version": "v1", "sha256": "hash1", "window_end": "2026-01"})
    model.registry.load = Mock(return_value={"pipeline": pipeline})
    scorer = FraudScorer.__new__(FraudScorer)
    scorer.model, scorer.metrics, scorer.threshold = model, metrics, 0.5

    assert scorer.score({"amount": 1})["model_version"] == "v1"
    activation = sample(metrics, "fw_model_activated_timestamp_seconds", {"version": "v1"})
    assert activation > 0
    scorer.score({"amount": 1})
    assert sample(metrics, "fw_model_activations_total", {"version": "v1"}) == 1
    assert sample(metrics, "fw_model_activated_timestamp_seconds", {"version": "v1"}) == activation

    model.registry.current.return_value = {"version": "v2", "sha256": "hash2", "window_end": "2026-02"}
    model.registry.load.side_effect = ValueError("checksum mismatch")
    assert scorer.score({"amount": 1})["model_version"] == "v1"
    assert sample(metrics, "fw_model_reload_failures_total") == 1
    assert sample(metrics, "fw_model_reload_failed") == 1
    assert sample(metrics, "fw_model_activations_total", {"version": "v2"}) is None

    model.registry.load.side_effect = None
    assert scorer.score({"amount": 1})["model_version"] == "v2"
    assert sample(metrics, "fw_model_activations_total", {"version": "v2"}) == 1
    assert sample(metrics, "fw_model_reload_failed") == 0


def test_missing_initial_model_records_reload_failure_without_fake_activation(tmp_path):
    metrics = RuntimeMetrics("scorer")
    scorer = FraudScorer.__new__(FraudScorer)
    scorer.model, scorer.metrics, scorer.threshold = HotReloadingModel(tmp_path), metrics, 0.5
    with pytest.raises(ModelUnavailable):
        scorer.score({"amount": 1})
    assert sample(metrics, "fw_model_reload_failures_total") == 1
    assert sample(metrics, "fw_model_reload_failed") == 1
    assert not list(metrics.activations.collect())[0].samples


@pytest.mark.parametrize("threshold", ["NaN", "inf", "-inf", "-0.01", "1.01", "invalid", ""])
def test_invalid_fraud_threshold_fails_before_model_initialization(monkeypatch, threshold):
    monkeypatch.setenv("FRAUD_THRESHOLD", threshold)
    model_constructor = Mock()
    monkeypatch.setattr("fraud_detector.scorer.HotReloadingModel", model_constructor)
    with pytest.raises(ValueError, match="FRAUD_THRESHOLD must be a finite number between 0 and 1"):
        FraudScorer(metrics=RuntimeMetrics("scorer"))
    model_constructor.assert_not_called()


@pytest.mark.parametrize("threshold,flag", [("0", 1), ("0.73", 0), ("1", 0)])
def test_valid_fraud_threshold_preserves_strict_greater_than_contract(monkeypatch, threshold, flag):
    monkeypatch.setenv("FRAUD_THRESHOLD", threshold)
    model = Mock(version="v1", last_error=None)
    model.predict.return_value = (np.array([0.73]), "v1")
    monkeypatch.setattr("fraud_detector.scorer.HotReloadingModel", Mock(return_value=model))
    scorer = FraudScorer(metrics=RuntimeMetrics("scorer"))
    result = scorer.score({"amount": 1})
    assert result == {"score": 0.73, "fraud_flag": flag, "model_version": "v1"}


def test_metrics_http_endpoint_serves_prometheus_text():
    metrics = RuntimeMetrics("writer")
    metrics.record_write(SCORE, 0.05, inserted=True)
    server, thread = metrics.start_server(0)
    try:
        with urlopen(f"http://127.0.0.1:{server.server_port}/metrics", timeout=5) as response:
            body = response.read().decode()
            assert response.status == 200
            assert "fw_scores_written_total 1.0" in body
            assert "fw_write_seconds_bucket" in body
            assert "fw_written_fraud_total 1.0" in body
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
