"""Validate the real HW1 CSV in explicitly isolated storage, including the model gate.

Example (inside the project image, with a read-only dataset mount):
  python scripts/real_data_check.py --csv /data/train.csv --storage-dir /tmp/real-check

The existing production storage root is refused. No CSV rows or PII are copied
into the JSON evidence; the script reports provenance, partition counts and
same-month validation metrics. A legitimately rejected next model is successful
evidence of the gate and must not be mistaken for a deployment failure.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.data import advance_month, available_months, month_offset, seed_mart
from pipeline.quality import assert_quality, run_quality_check
from pipeline.registry import ModelRegistry, atomic_json, sha256_file, storage_root
from pipeline.training import prepare_window, train_candidate, evaluate_candidate, publish_candidate


def run_pipeline(run_id: str, root: Path) -> dict:
    """Run the same four retry-safe task functions used by the scheduled DAG."""
    return publish_candidate(evaluate_candidate(train_candidate(prepare_window(run_id, root))))


def run(csv_path: Path, root: Path) -> dict:
    """Import real labels into a dedicated mart and record a bootstrap/next-window gate."""
    csv_path = csv_path.resolve(strict=True)
    root = root.resolve()
    if root == storage_root().resolve():
        raise ValueError("Use an isolated --storage-dir, distinct from production STORAGE_DIR")
    marker = root / "real-check-source.json"
    fingerprint = sha256_file(csv_path)
    provenance = {"kind": "real_labeled_csv", "path": str(csv_path), "sha256": fingerprint,
                  "bytes": csv_path.stat().st_size, "personal_columns_omitted": ["name_1", "name_2", "street"]}
    if marker.exists():
        recorded = json.loads(marker.read_text(encoding="utf-8"))
        if recorded["sha256"] != fingerprint:
            raise ValueError("Isolated storage belongs to another source CSV; select a fresh directory")
    elif root.exists() and any(root.iterdir()):
        raise ValueError("Refusing an existing unmarked directory; select empty isolated storage")
    else:
        atomic_json(marker, provenance)
    evidence = {"started_at": datetime.now(timezone.utc).isoformat(), "storage_dir": str(root),
                "source": provenance, "status": "running"}
    try:
        evidence["seed"] = seed_mart(root, csv_path=csv_path)
        with closing(sqlite3.connect(root / "mart.sqlite")) as connection:
            totals = connection.execute("SELECT substr(transaction_time,1,7), COUNT(*), SUM(target=1), SUM(target=0) FROM transactions GROUP BY 1 ORDER BY 1").fetchall()
        evidence["original_partitions"] = [dict(zip(["month", "rows", "fraud_rows", "nonfraud_rows"], values)) for values in totals]
        if len(totals) < 3:
            raise ValueError("Real training data must contain at least three calendar months")
        evidence["bootstrap"] = run_pipeline("real-data-check-bootstrap", root)
        if evidence["bootstrap"]["status"] != "published":
            raise AssertionError("The initial real-data model was not published")
        registry = ModelRegistry(root)
        before = registry.current()
        # Derive this from the bootstrap receipt so re-running the script is idempotent.
        next_month = month_offset(evidence["bootstrap"]["window_end"], 1)
        if next_month not in available_months(root):
            evidence["month_supply"] = advance_month("real-data-check-supply", root)
        else:
            evidence["month_supply"] = {"status": "original_labeled_month_available", "month": next_month}
        report = run_quality_check("real-data-check-next", end_month=next_month, storage_dir=root)
        evidence["data_quality"] = {"passed": report["passed"], "rules": len(report["rules"]),
                                    "html_path": report["html_path"], "json_path": report["json_path"]}
        assert_quality(report)
        evidence["next_window"] = run_pipeline("real-data-check-next", root)
        decision = evidence["next_window"]
        if decision["status"] not in {"published", "rejected"}:
            raise AssertionError(f"Unexpected next-window outcome: {decision['status']}")
        after = registry.current()
        if decision["status"] == "rejected":
            if after != before:
                raise AssertionError("Rejected real-data candidate changed the production pointer")
        elif after["version"] != decision["version"]:
            raise AssertionError("Published version differs from the committed model pointer")
        registry.load(after)
        evidence["active_model"] = after
        evidence["status"] = "passed"
    except Exception as error:
        evidence["status"] = "failed"
        evidence["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        evidence["finished_at"] = datetime.now(timezone.utc).isoformat()
        atomic_json(root / "evidence" / "real-data-check.json", evidence)
    return evidence


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, required=True)
    parser.add_argument("--storage-dir", type=Path, required=True)
    arguments = parser.parse_args()
    evidence = run(arguments.csv, arguments.storage_dir)
    print(json.dumps(evidence, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
