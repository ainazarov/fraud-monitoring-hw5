import json
import logging
import signal
import sys
from threading import Event
from pathlib import Path

logger = logging.getLogger(__name__)


class DeliveryError(RuntimeError):
    """A message has not been acknowledged and its input must not be committed."""


def consumer_config(bootstrap_servers: str, group_id: str) -> dict:
    return {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    }


def producer_config(bootstrap_servers: str) -> dict:
    return {
        "bootstrap.servers": bootstrap_servers,
        "enable.idempotence": True,
        "acks": "all",
        "delivery.timeout.ms": 30000,
    }


def publish_json(producer, topic: str, payload: dict, key: str | None = None) -> None:
    """Require broker acknowledgement; a completed flush alone does not prove delivery."""
    delivery = []

    def on_delivery(error, message):
        delivery.append(error)

    producer.produce(
        topic,
        value=json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8"),
        key=key.encode("utf-8") if key is not None else None,
        on_delivery=on_delivery,
    )
    pending = producer.flush(35.0)
    if pending or not delivery:
        raise DeliveryError("Kafka delivery acknowledgement timed out")
    if delivery[0] is not None:
        raise DeliveryError(f"Kafka delivery failed: {delivery[0]}")


def reject_message(consumer, producer, message, error: Exception, dlq_topic: str) -> None:
    """Persist only safe error metadata before advancing past malformed input."""
    payload = {
        "source_topic": message.topic(),
        "source_partition": message.partition(),
        "source_offset": message.offset(),
        "error": str(error),
    }
    publish_json(producer, dlq_topic, payload)
    consumer.commit(message=message, asynchronous=False)
    logger.warning("Rejected %s[%s]@%s: %s", message.topic(), message.partition(), message.offset(), error)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )


def stop_event() -> Event:
    stop = Event()

    def request_stop(signum, frame):
        logger.info("Shutdown requested")
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    return stop


def consume_loop(consumer, process_message, stop: Event) -> None:
    """Stop on infrastructure/model failures so the uncommitted record replays."""
    while not stop.is_set():
        message = consumer.poll(1.0)
        Path("/tmp/healthy").touch()
        if message is None:
            continue
        if message.error():
            raise RuntimeError(f"Kafka consumer failed: {message.error()}")
        process_message(message)
