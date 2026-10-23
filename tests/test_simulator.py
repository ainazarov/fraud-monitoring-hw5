"""Simulator exercises real contracts and acknowledgements, without Kafka fakes in production."""
from datetime import datetime, timezone
import json
from threading import Event

import pytest

from common.kafka import DeliveryError
from pipeline.data import read_mart, seed_mart, synthetic_month
from scripts.retrain import run_retraining
from scripts.simulate import TransactionStream, load_rows, publish_stream


class AcknowledgingProducer:
    def __init__(self, error=None):
        self.events = []
        self.error = error

    def produce(self, topic, value, key, on_delivery):
        self.events.append((topic, json.loads(value), key))
        on_delivery(self.error, None)

    def flush(self, timeout):
        return 0


def stream():
    return TransactionStream(synthetic_month("2024-01", rows=50).to_dict("records"), seed=7)


def test_mart_modes_sample_actual_rows_and_do_not_expose_labels(tmp_path):
    seed_mart(tmp_path, rows_per_month=50)
    mart = read_mart(tmp_path)
    normal = load_rows(tmp_path)
    fraud = load_rows(tmp_path, "fraud")
    assert len(normal) == len(mart)
    assert len(fraud) == int(mart.target.sum())
    assert {row["amount"] for row in fraud} == set(mart.loc[mart.target.eq(1), "amount"])
    assert all("target" not in row and "transaction_id" not in row for row in normal)


def test_stream_has_fresh_identity_utc_time_and_deterministic_features():
    rows = synthetic_month("2024-01", rows=50).to_dict("records")
    first = TransactionStream(rows, seed=9, run_id="first")
    second = TransactionStream(rows, seed=9, run_id="second")
    now = datetime(2026, 10, 23, 18, 0, tzinfo=timezone.utc)
    events = [first.next_transaction(now) for _ in range(20)]
    other_events = [second.next_transaction(now) for _ in range(20)]
    assert [event["data"] for event in events] == [event["data"] for event in other_events]
    assert len({event["transaction_id"] for event in events + other_events}) == 40
    assert all(event["emitted_at"] == now.isoformat() for event in events)
    assert all(event["data"]["transaction_time"] == now.isoformat() for event in events)
    assert all("target" not in event["data"] and "transaction_id" not in event["data"] for event in events)
    assert len({event["data"]["amount"] for event in events}) > 1


def test_publish_obeys_count_and_broker_acknowledgements():
    producer = AcknowledgingProducer()
    assert publish_stream(producer, stream(), rate=10000, count=3) == 3
    assert len(producer.events) == 3
    for topic, value, key in producer.events:
        assert topic == "transactions"
        assert key.decode() == value["transaction_id"]
    with pytest.raises(DeliveryError, match="delivery failed"):
        publish_stream(AcknowledgingProducer(error="broker unavailable"), stream(), count=1)


def test_stop_before_publish_and_during_rate_wait():
    stop = Event()
    stop.set()
    producer = AcknowledgingProducer()
    assert publish_stream(producer, stream(), stop=stop) == 0
    assert producer.events == []

    class StopWhileWaiting(Event):
        def wait(self, timeout=None):
            assert timeout > 0
            self.set()
            return True

    assert publish_stream(producer, stream(), rate=0.1, stop=StopWhileWaiting()) == 1


@pytest.mark.parametrize("rate", [0, -1, float("nan"), float("inf")])
def test_rate_must_be_finite_positive(rate):
    with pytest.raises(ValueError, match="rate"):
        publish_stream(AcknowledgingProducer(), stream(), rate=rate, count=1)


def test_missing_mart_fails_without_creating_storage(tmp_path):
    path = tmp_path / "missing"
    with pytest.raises(ValueError, match="seed_data"):
        load_rows(path)
    assert not path.exists()


def test_manual_retraining_publishes_then_preserves_pointer_when_data_missing(tmp_path):
    seed_mart(tmp_path, rows_per_month=50)
    bootstrap = run_retraining(tmp_path, "test-bootstrap")
    assert bootstrap["decision"]["published"] is True
    assert bootstrap["active_manifest"]["window_end"] == "2024-03"
    waiting = run_retraining(tmp_path, "no-new-month")
    assert waiting["decision"]["status"] == "waiting_for_data"
    assert waiting["active_manifest"] == bootstrap["active_manifest"]
    advanced = run_retraining(tmp_path, "next-month", advance=True, batch_size=50)
    assert advanced["supply"]["status"] == "committed"
    assert advanced["decision"]["status"] in {"published", "rejected"}
    # The second invocation must reuse the data receipt and pinned decision.
    retried = run_retraining(tmp_path, "next-month", advance=True, batch_size=50)
    assert retried["supply"]["status"] == "already_committed"
    assert retried["decision"] == advanced["decision"]
    assert retried["active_manifest"] == advanced["active_manifest"]
