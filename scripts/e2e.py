"""Exercise real scheduled infrastructure via Airflow REST, Kafka and PostgreSQL.

The test pauses schedules briefly, drains existing runs, triggers all three DAGs,
and restores their previous pause states even on failure. It writes durable JSON
evidence to storage/evidence and never deletes existing transactions or models.
"""
from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time
import uuid
from urllib.parse import quote

import psycopg2
import requests
from confluent_kafka import Producer

from common.contracts import validate_transaction
from common.kafka import producer_config, publish_json
from pipeline.data import available_months, advance_month
from pipeline.registry import ModelRegistry, atomic_json, storage_root


SESSION = requests.Session()
SESSION.auth = (os.getenv("AIRFLOW_ADMIN_USER", "admin"), os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin"))
BASE = os.getenv("AIRFLOW_URL", "http://airflow-webserver:8080") + "/api/v1"
DAGS = ["advance_month", "retrain_model", "data_quality"]


def api(method, path, **kwargs):
    response = SESSION.request(method, BASE + path, timeout=30, **kwargs)
    if not response.ok:
        raise RuntimeError(f"Airflow {method} {path}: {response.status_code} {response.text[:800]}")
    return response.json()


def wait_until(probe, description, timeout=300):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        result = probe()
        if result:
            return result
        time.sleep(2)
    raise TimeoutError(description)


def trigger(dag, run_id):
    api("POST", f"/dags/{dag}/dagRuns", json={"dag_run_id": run_id, "conf": {}})
    api("PATCH", f"/dags/{dag}", json={"is_paused": False})
    path = f"/dags/{dag}/dagRuns/{quote(run_id, safe='')}"

    def completed():
        run = api("GET", path)
        if run["state"] == "failed":
            raise AssertionError(f"{dag} failed: {api('GET', path + '/taskInstances')}")
        return run if run["state"] == "success" else None

    run = wait_until(completed, f"{dag} did not complete", timeout=300)
    quiesce(dag)
    tasks = api("GET", path + "/taskInstances")["task_instances"]
    assert all(item["state"] == "success" for item in tasks), tasks
    print(f"{dag}: success ({len(tasks)} tasks)", flush=True)
    return {"dag_id": dag, "run_id": run_id, "state": run["state"],
            "tasks": [{"task_id": item["task_id"], "state": item["state"], "try_number": item["try_number"]} for item in tasks]}


def unfinished(dag):
    return api("GET", f"/dags/{dag}/dagRuns?state=running&state=queued")["dag_runs"]


def quiesce(dag):
    # Paused DAGs cannot execute even manual runs in Airflow 2.10. Drain first;
    # recheck after pausing to handle a scheduled run racing with the API call.
    for _ in range(5):
        if unfinished(dag):
            api("PATCH", f"/dags/{dag}", json={"is_paused": False})
            wait_until(lambda: not unfinished(dag), f"{dag} still has unfinished runs", timeout=300)
        api("PATCH", f"/dags/{dag}", json={"is_paused": True})
        if not unfinished(dag):
            return
    raise RuntimeError(f"Could not obtain an idle {dag}")


def score_batch(label, version):
    producer = Producer(producer_config(os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")))
    with Path("data/transactions.csv").open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))[:6]
    identifiers = []
    for number, row in enumerate(rows):
        identifier = f"{label}-{number}"
        transaction = validate_transaction({"transaction_id": identifier, "data": row})
        publish_json(producer, "transactions", transaction, key=identifier)
        identifiers.append(identifier)

    def fetch():
        with psycopg2.connect(connect_timeout=10) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT transaction_id, score, fraud_flag, model_version FROM scores WHERE transaction_id = ANY(%s) ORDER BY transaction_id", (identifiers,))
                results = cursor.fetchall()
        if len(results) != len(identifiers):
            return None
        assert all(result[3] == version for result in results), results
        assert all(0 <= result[1] <= 1 and result[2] == int(result[1] > 0.5) for result in results)
        return [dict(zip(["transaction_id", "score", "fraud_flag", "model_version"], item)) for item in results]

    return wait_until(fetch, "Kafka scoring results did not arrive with the expected model", timeout=90)


def main():
    token = "e2e-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]
    root = storage_root()
    registry = ModelRegistry(root)
    paused = {}
    evidence = {"run_id": token, "started_at": datetime.now(timezone.utc).isoformat(), "dags": []}
    try:
        def ready():
            dags = api("GET", "/dags?limit=100")["dags"]
            return all(any(item["dag_id"] == dag and item["is_active"] for item in dags) for dag in DAGS)
        wait_until(ready, "Airflow DAGs were not discovered", timeout=120)
        imports = api("GET", "/importErrors")
        assert imports["total_entries"] == 0, imports
        for dag in DAGS:
            info = api("GET", f"/dags/{dag}")
            paused[dag] = info["is_paused"]
            quiesce(dag)
        before = registry.current()
        evidence["before"] = before
        evidence["months_before"] = available_months()
        evidence["scores_before"] = score_batch(token + "-before", before["version"])
        advance_id = token + "-advance"
        evidence["dags"].append(trigger("advance_month", advance_id))
        months = available_months()
        replay = advance_month(run_id=advance_id)
        assert available_months() == months, "Replaying the same monthly run shifted the calendar twice"
        evidence["monthly_replay"] = replay
        evidence["dags"].append(trigger("data_quality", token + "-dq"))
        evidence["dags"].append(trigger("retrain_model", token + "-train"))
        after = registry.current()
        assert after["version"] != before["version"], "No model was promoted; inspect the recorded quality-gate decision"
        assert after["window_end"] > before["window_end"], "Training window did not advance"
        registry.load(after)
        evidence["after"] = after
        evidence["months_after"] = available_months()
        evidence["scores_after"] = score_batch(token + "-after", after["version"])
        report = json.loads((root / "dq_reports" / "latest.json").read_text())
        assert report["passed"] and len(report["rules"]) >= 5
        assert (root / "dq_reports" / "latest.html").exists()
        evidence["dq"] = report
        evidence["status"] = "passed"
    except Exception as error:
        evidence["status"] = "failed"
        evidence["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        evidence["finished_at"] = datetime.now(timezone.utc).isoformat()
        atomic_json(root / "evidence" / f"{token}.json", evidence)
        atomic_json(root / "evidence" / "latest.json", evidence)
        for dag, original in paused.items():
            api("PATCH", f"/dags/{dag}", json={"is_paused": original})
    print(json.dumps({"status": "passed", "evidence": str(root / "evidence" / f"{token}.json"),
                      "model_before": before["version"], "model_after": after["version"]}, indent=2))


if __name__ == "__main__":
    main()
