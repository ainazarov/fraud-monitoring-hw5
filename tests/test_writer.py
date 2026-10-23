import json
from unittest.mock import Mock

import pytest

from common.kafka import DeliveryError, consumer_config, publish_json
from fraud_detector.app import process_message as process_transaction
from scoring_writer.app import INSERT_SCORE, insert_score, process_message


SCORE = {"transaction_id": "t1", "score": 0.73, "fraud_flag": 1, "model_version": "v1"}


def message(payload=SCORE):
    msg = Mock()
    msg.value.return_value = json.dumps(payload).encode()
    msg.topic.return_value = "scores"
    msg.partition.return_value = 0
    msg.offset.return_value = 17
    return msg


def connection(rowcount=1):
    conn = Mock()
    cursor = Mock(rowcount=rowcount)
    context = Mock()
    context.__enter__ = Mock(return_value=cursor)
    context.__exit__ = Mock(return_value=False)
    conn.cursor.return_value = context
    return conn, cursor


def producer(error=None, pending=0, acknowledge=True):
    kafka_producer = Mock()
    callbacks = []

    def produce(topic, **kwargs):
        callbacks.append(kwargs["on_delivery"])

    def flush(timeout):
        if acknowledge:
            for callback in callbacks:
                callback(error, Mock())
            callbacks.clear()
        return pending

    kafka_producer.produce.side_effect = produce
    kafka_producer.flush.side_effect = flush
    return kafka_producer


def test_database_commit_precedes_kafka_commit():
    conn, cursor = connection()
    consumer = Mock()
    events = []
    conn.commit.side_effect = lambda: events.append("database commit")
    consumer.commit.side_effect = lambda **kw: events.append("offset commit")
    msg = message()

    process_message(msg, consumer, Mock(), conn)

    assert events == ["database commit", "offset commit"]
    cursor.execute.assert_called_once_with(INSERT_SCORE, ("t1", 0.73, 1, "v1", None, None, None, None, None, None, None))
    consumer.commit.assert_called_once_with(message=msg, asynchronous=False)


def test_replayed_transaction_is_recognized_and_committed():
    conn, _ = connection(rowcount=0)
    assert insert_score(conn, SCORE) is False
    conn.commit.assert_called_once()
    assert "ON CONFLICT (transaction_id) DO NOTHING" in INSERT_SCORE


@pytest.mark.parametrize("failure_stage", ["execute", "commit"])
def test_failed_database_work_rolls_back_without_advancing_offset(failure_stage):
    conn, cursor = connection()
    consumer = Mock()
    target = cursor.execute if failure_stage == "execute" else conn.commit
    target.side_effect = RuntimeError("database unavailable")
    with pytest.raises(RuntimeError, match="database unavailable"):
        process_message(message(), consumer, Mock(), conn)
    conn.rollback.assert_called_once()
    consumer.commit.assert_not_called()


def test_failed_offset_commit_propagates_after_persistence_for_safe_replay():
    conn, _ = connection()
    consumer = Mock()
    consumer.commit.side_effect = RuntimeError("broker unavailable")
    with pytest.raises(RuntimeError, match="broker unavailable"):
        process_message(message(), consumer, Mock(), conn)
    conn.commit.assert_called_once()
    conn.rollback.assert_not_called()


def test_malformed_score_is_sent_to_dlq_before_offset_commit_without_private_data():
    bad = {"transaction_id": "private-name", "score": "private-street", "fraud_flag": 1, "model_version": "v1"}
    msg = message(bad)
    conn, _ = connection()
    consumer = Mock()
    kafka_producer = producer()
    events = []
    original_flush = kafka_producer.flush.side_effect

    def acknowledged_flush(timeout):
        result = original_flush(timeout)
        events.append("delivery ack")
        return result

    kafka_producer.flush.side_effect = acknowledged_flush
    consumer.commit.side_effect = lambda **kw: events.append("offset commit")
    process_message(msg, consumer, kafka_producer, conn)

    topic = kafka_producer.produce.call_args.args[0]
    payload = json.loads(kafka_producer.produce.call_args.kwargs["value"])
    assert topic == "dead_letters"
    assert payload["source_offset"] == 17
    assert "private" not in json.dumps(payload)
    assert events == ["delivery ack", "offset commit"]
    conn.commit.assert_not_called()


def test_failed_dlq_delivery_does_not_advance_offset():
    consumer = Mock()
    conn, _ = connection()
    with pytest.raises(DeliveryError):
        process_message(message({}), consumer, producer(error="broker failure"), conn)
    consumer.commit.assert_not_called()
    conn.commit.assert_not_called()


@pytest.mark.parametrize("error,pending,acknowledge", [
    ("broker rejected message", 0, True),
    (None, 1, False),
    (None, 0, False),
])
def test_delivery_success_requires_an_acknowledgement(error, pending, acknowledge):
    with pytest.raises(DeliveryError):
        publish_json(producer(error, pending, acknowledge), "scores", SCORE)


def test_successful_publication_is_one_object_keyed_by_transaction_id():
    kafka_producer = producer()
    publish_json(kafka_producer, "scores", SCORE, key="t1")
    call = kafka_producer.produce.call_args
    assert json.loads(call.kwargs["value"]) == SCORE
    assert call.kwargs["key"] == b"t1"


def test_consumer_never_automatically_commits_or_stores_offsets():
    config = consumer_config("kafka:9092", "test")
    assert config["enable.auto.commit"] is False
    assert config["enable.auto.offset.store"] is False


def test_model_failure_is_retried_instead_of_dead_lettered(monkeypatch):
    transaction = {"transaction_id": "t1", "data": {"amount": 1}}
    monkeypatch.setattr("fraud_detector.app.validate_transaction", lambda value: value)
    scorer = Mock()
    scorer.score.side_effect = RuntimeError("model failure")
    consumer = Mock()
    kafka_producer = producer()
    with pytest.raises(RuntimeError, match="model failure"):
        process_transaction(message(transaction), consumer, kafka_producer, scorer)
    consumer.commit.assert_not_called()
    kafka_producer.produce.assert_not_called()


def test_scoring_waits_for_delivery_ack_before_committing(monkeypatch):
    transaction = {"transaction_id": "t1", "data": {"amount": 1}}
    monkeypatch.setattr("fraud_detector.app.validate_transaction", lambda value: value)
    scorer = Mock()
    scorer.score.return_value = {"score": 0.73, "fraud_flag": 1, "model_version": "v1"}
    consumer = Mock()
    with pytest.raises(DeliveryError):
        process_transaction(message(transaction), consumer, producer(error="offline"), scorer)
    consumer.commit.assert_not_called()
