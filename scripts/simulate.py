"""Publish real mart features at a bounded rate for live scoring and monitoring.

Normal mode samples the complete labeled mart, preserving its class mix in
expectation. Fraud mode samples its positive rows. Labels never enter Kafka;
only the deployed model determines scores and fraud flags.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import math
import os
from pathlib import Path
import random
import sys
from threading import Event
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.contracts import validate_transaction
from common.kafka import configure_logging, producer_config, publish_json, stop_event
from pipeline.data import FEATURE_COLUMNS, read_mart, storage_path


LOGGER = logging.getLogger(__name__)


def load_rows(storage_dir=None, mode="normal") -> list[dict]:
    """Read a consistent mart snapshot without exposing targets or identifiers."""
    if mode not in {"normal", "fraud"}:
        raise ValueError("mode must be normal or fraud")
    root = storage_path(storage_dir)
    if not (root / "mart.sqlite").is_file():
        raise ValueError("The labeled mart is missing; run scripts/seed_data.py first")
    frame = read_mart(root)
    if mode == "fraud":
        frame = frame.loc[frame["target"].eq(1)]
    if frame.empty:
        raise ValueError(f"The mart contains no rows for simulator mode {mode}")
    return frame[FEATURE_COLUMNS].to_dict(orient="records")


class TransactionStream:
    """Seeded feature sampling with fresh transport identity and UTC timestamps."""

    def __init__(self, rows, seed=2024, run_id=None):
        if not rows:
            raise ValueError("At least one source row is required")
        self.rows = rows
        self.random = random.Random(seed)
        self.run_id = run_id or uuid.uuid4().hex
        self.sequence = 0

    def next_transaction(self, now=None) -> dict:
        now = now if now is not None else datetime.now(timezone.utc)
        if now.tzinfo is None:
            raise ValueError("Emission time must include a timezone")
        timestamp = now.astimezone(timezone.utc).isoformat()
        row = self.rows[self.random.randrange(len(self.rows))]
        # The mart excludes personal names and addresses. Legacy wire-contract
        # placeholders satisfy required fields and are ignored by the model.
        data = {name: row[name] for name in FEATURE_COLUMNS}
        data.update({
            "transaction_time": timestamp,
            "name_1": "Synthetic", "name_2": "Customer", "street": "Demo address",
        })
        identifier = f"sim-{self.run_id}-{self.sequence:012d}"
        result = validate_transaction({
            "transaction_id": identifier, "data": data, "emitted_at": timestamp,
        })
        self.sequence += 1
        return result


def publish_stream(producer, stream, *, rate=10.0, count=None,
                   topic="transactions", stop=None, clock=time.monotonic) -> int:
    """Count broker-acknowledged events, pace them, and wake promptly on shutdown."""
    if not math.isfinite(rate) or rate <= 0:
        raise ValueError("rate must be a finite positive number")
    if count is not None and count <= 0:
        raise ValueError("count must be positive when supplied")
    stop = stop if stop is not None else Event()
    sent = 0
    deadline = clock()
    while not stop.is_set() and (count is None or sent < count):
        transaction = stream.next_transaction()
        publish_json(producer, topic, transaction, key=transaction["transaction_id"])
        sent += 1
        if sent == 1 or sent % 100 == 0:
            LOGGER.info("Acknowledged %d transactions; requested rate %.2f/s", sent, rate)
        if count is not None and sent >= count:
            break
        # A slow broker reduces throughput instead of causing a catch-up burst.
        deadline = max(deadline + 1.0 / rate, clock())
        if stop.wait(max(0.0, deadline - clock())):
            break
    return sent


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--storage-dir")
    parser.add_argument("--rate", type=float, default=os.getenv("SIMULATOR_RATE", "10"))
    parser.add_argument("--mode", choices=("normal", "fraud"), default=os.getenv("SIMULATOR_MODE", "normal"))
    parser.add_argument("--seed", type=int, default=os.getenv("SIMULATOR_SEED", "2024"))
    parser.add_argument("--count", type=int, default=os.getenv("SIMULATOR_COUNT"), help="Omit for continuous traffic")
    args = parser.parse_args(argv)
    if not math.isfinite(args.rate) or args.rate <= 0:
        parser.error("--rate must be a finite positive number")
    if args.count is not None and args.count <= 0:
        parser.error("--count must be positive")
    configure_logging()
    stop = stop_event()
    rows = load_rows(args.storage_dir, args.mode)
    stream = TransactionStream(rows, seed=args.seed)
    from confluent_kafka import Producer

    producer = Producer(producer_config(os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")))
    LOGGER.info("Starting %s traffic from %d mart rows", args.mode, len(rows))
    started = time.monotonic()
    try:
        sent = publish_stream(
            producer, stream, rate=args.rate, count=args.count, stop=stop,
            topic=os.getenv("TRANSACTIONS_TOPIC", "transactions"),
        )
    finally:
        producer.flush(5.0)
    print(json.dumps({
        "run_id": stream.run_id, "mode": args.mode, "acknowledged": sent,
        "requested_rate": args.rate, "elapsed_seconds": round(time.monotonic() - started, 3),
        "stopped": stop.is_set(),
    }), flush=True)


if __name__ == "__main__":
    main()
