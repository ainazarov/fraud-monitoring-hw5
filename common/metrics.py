"""Low-cardinality Prometheus telemetry for the two streaming services.

Business dimensions belong in PostgreSQL, never metric labels. Each process has
its own registry; tests can inject an isolated instance without global resets.
"""

from datetime import datetime, timezone
import time

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server


LATENCY_BUCKETS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60)
SCORE_BUCKETS = (0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 1)


class RuntimeMetrics:
    def __init__(self, service: str, registry=None):
        if service not in ("scorer", "writer"):
            raise ValueError("Unknown monitored service")
        self.service = service
        self.registry = registry if registry is not None else CollectorRegistry()
        self.rejected = Counter("fw_rejected_total", "Malformed events acknowledged in the dead-letter topic", ["service"], registry=self.registry).labels(service)
        if service == "scorer":
            self.transactions = Counter("fw_transactions_total", "Scores acknowledged by Kafka", registry=self.registry)
            self.processing = Histogram("fw_processing_seconds", "Successful validation, inference and score publication duration", buckets=LATENCY_BUCKETS, registry=self.registry)
            self.score = Histogram("fw_score", "Fraud probabilities published by the scorer", buckets=SCORE_BUCKETS, registry=self.registry)
            self.fraud = Counter("fw_fraud_total", "Published scores flagged as fraud", registry=self.registry)
            self.activations = Counter("fw_model_activations_total", "Successfully activated model versions", ["version"], registry=self.registry)
            self.activated_at = Gauge("fw_model_activated_timestamp_seconds", "Unix time of the most recent actual activation of each version", ["version"], registry=self.registry)
            self.reload_failures = Counter("fw_model_reload_failures_total", "Failed model refresh attempts; the last valid model stays active", registry=self.registry)
            self.reload_failed = Gauge("fw_model_reload_failed", "Whether the most recent model refresh failed", registry=self.registry)
        else:
            self.written = Counter("fw_scores_written_total", "New score rows committed to PostgreSQL", registry=self.registry)
            self.write = Histogram("fw_write_seconds", "Successful new PostgreSQL insert and commit duration", buckets=LATENCY_BUCKETS, registry=self.registry)
            self.score = Histogram("fw_written_score", "Fraud probabilities in newly committed score rows", buckets=SCORE_BUCKETS, registry=self.registry)
            self.fraud = Counter("fw_written_fraud_total", "Newly committed score rows flagged as fraud", registry=self.registry)
            self.duplicates = Counter("fw_write_duplicates_total", "Already stored score rows safely deduplicated", registry=self.registry)
            self.end_to_end = Histogram("fw_end_to_end_seconds", "Producer emission to PostgreSQL commit latency for new rows with valid emitted_at", buckets=LATENCY_BUCKETS + (120, 300, 600), registry=self.registry)
        self.last_transaction = Gauge("fw_last_transaction_timestamp_seconds", "Unix time of the most recent successful new event", registry=self.registry)

    def start_server(self, port: int):
        return start_http_server(port, addr="0.0.0.0", registry=self.registry)

    def record_score(self, result: dict, seconds: float) -> None:
        self.transactions.inc()
        self.processing.observe(seconds)
        self.score.observe(result["score"])
        self.fraud.inc(result["fraud_flag"])
        self.last_transaction.set_to_current_time()

    def record_write(self, result: dict, seconds: float, inserted: bool) -> None:
        if not inserted:
            self.duplicates.inc()
            return
        self.written.inc()
        self.write.observe(seconds)
        self.score.observe(result["score"])
        self.fraud.inc(result["fraud_flag"])
        now = datetime.now(timezone.utc)
        self.last_transaction.set(now.timestamp())
        if result.get("emitted_at"):
            # Validation already normalizes this field. Guard clock skew so the
            # latency sum remains monotonic and rate() remains meaningful.
            delay = (now - datetime.fromisoformat(result["emitted_at"])).total_seconds()
            if delay >= 0:
                self.end_to_end.observe(delay)

    def record_model(self, previous_version: str | None, version: str | None, error: str | None) -> None:
        if version is not None and version != previous_version:
            self.activations.labels(version).inc()
            self.activated_at.labels(version).set(time.time())
        self.reload_failed.set(int(error is not None))
        if error is not None:
            self.reload_failures.inc()


SCORER_METRICS = RuntimeMetrics("scorer")
WRITER_METRICS = RuntimeMetrics("writer")
