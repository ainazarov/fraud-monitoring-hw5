"""Deterministic quality gates and self-contained HTML reports for labeled data."""
from __future__ import annotations

from datetime import datetime, timezone
import html
import json
import os
from pathlib import Path
import re
import uuid
from typing import Any

import numpy as np
import pandas as pd

from pipeline.data import MART_COLUMNS, latest_month, month_offset, read_window, storage_path


def validate_frame(frame: pd.DataFrame, end_month: str, window_months: int = 3,
                   min_rows_per_month: int = 50) -> dict[str, Any]:
    """Read a pinned frame and return all DQ findings without mutating data or state."""
    rules: list[dict[str, Any]] = []
    def rule(name: str, passed: bool, details: str) -> None:
        rules.append({"name": name, "passed": bool(passed), "details": details})

    missing = sorted(set(MART_COLUMNS) - set(frame.columns))
    rule("required_schema", not missing, f"Missing columns: {missing}")
    # Reindex so malformed inputs still produce a complete, publishable report.
    data = frame.reindex(columns=MART_COLUMNS)
    rule("required_values_present", len(data) > 0 and not data.isna().any().any(),
         f"Rows={len(data)}; null cells={int(data.isna().sum().sum())}")
    ids = data["transaction_id"]
    rule("unique_transaction_ids", len(data) > 0 and ids.notna().all() and ids.astype(str).str.strip().ne("").all() and not ids.duplicated().any(),
         f"Duplicate IDs={int(ids.duplicated().sum())}")
    timestamps = pd.to_datetime(data["transaction_time"], errors="coerce", utc=True)
    rule("valid_transaction_time", len(data) > 0 and timestamps.notna().all(), f"Unparseable timestamps={int(timestamps.isna().sum())}")
    amount = pd.to_numeric(data["amount"], errors="coerce")
    rule("amount_finite_and_in_range", len(data) > 0 and (np.isfinite(amount) & amount.between(0, 10_000_000)).all(),
         "Every amount must be finite and between 0 and 10,000,000 inclusive")
    coordinates_valid = len(data) > 0
    for column, bound in (("lat", 90), ("merchant_lat", 90), ("lon", 180), ("merchant_lon", 180)):
        values = pd.to_numeric(data[column], errors="coerce")
        coordinates_valid &= bool((np.isfinite(values) & values.between(-bound, bound)).all())
    rule("valid_geographic_coordinates", coordinates_valid, "Finite latitude in [-90,90] and longitude in [-180,180], for customer and merchant")
    target = pd.to_numeric(data["target"], errors="coerce")
    rule("binary_target", len(data) > 0 and target.isin([0, 1]).all(), "Every labeled target is exactly 0 or 1")
    expected = [month_offset(end_month, offset) for offset in range(1 - window_months, 1)]
    observed_months = timestamps.dt.strftime("%Y-%m")
    actual = sorted(observed_months.dropna().unique().tolist())
    rule("consecutive_calendar_window", actual == expected, f"Expected={expected}; observed={actual}")
    counts = {month: int((observed_months == month).sum()) for month in expected}
    rule("usable_monthly_counts", all(value >= min_rows_per_month for value in counts.values()),
         f"Minimum {min_rows_per_month} rows/month; counts={counts}")
    class_counts = {month: sorted(target[observed_months == month].dropna().unique().tolist()) for month in expected}
    rule("both_classes_each_month", all(values == [0, 1] for values in class_counts.values()), f"Observed labels per month={class_counts}")
    training_time = timestamps[observed_months.isin(expected[:-1])]
    holdout_time = timestamps[observed_months == expected[-1]]
    rule("strict_temporal_holdout", len(training_time) > 0 and len(holdout_time) > 0 and training_time.max() < holdout_time.min(),
         "All training timestamps must precede the latest-month holdout; no split overlap")
    categories = ["merch", "cat_id", "gender", "one_city", "us_state", "post_code", "jobs"]
    rule("nonempty_categorical_features", len(data) > 0 and all(data[column].notna().all() and data[column].astype(str).str.strip().ne("").all() for column in categories),
         f"Nonempty categorical values in {categories}")
    population = pd.to_numeric(data["population_city"], errors="coerce")
    rule("valid_city_population", len(data) > 0 and (np.isfinite(population) & population.between(0, 100_000_000) & population.eq(population.round())).all(),
         "Finite integer population in [0,100,000,000]")
    return {"passed": all(entry["passed"] for entry in rules), "end_month": end_month,
            "window_months": window_months, "rows": len(data), "monthly_counts": counts,
            "checked_at": datetime.now(timezone.utc).isoformat(), "rules": rules}


def assert_quality(report: dict[str, Any]) -> None:
    """Raise on failed rules; this read-only gate is safe to retry after report publication."""
    failed = [entry["name"] for entry in report["rules"] if not entry["passed"]]
    if not report["passed"]:
        raise ValueError("Data quality gate failed: " + ", ".join(failed))


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def publish_report(report: dict[str, Any], run_id: str,
                   storage_dir: str | Path | None = None) -> dict[str, Any]:
    """Atomically write escaped self-contained HTML and JSON alongside model storage.

    A retry replaces only the same report receipt and latest aliases. Reports
    contain rule summaries, not customer rows; validation failure remains visible.
    """
    safe_id = re.sub(r"[^a-zA-Z0-9_.-]", "_", run_id)[:160] or "manual"
    root = storage_path(storage_dir) / "dq_reports"
    status = "PASS" if report["passed"] else "FAIL"
    rows = "".join(
        f'<tr><td>{html.escape(str(rule["name"]))}</td><td class="{"pass" if rule["passed"] else "fail"}">{"PASS" if rule["passed"] else "FAIL"}</td><td>{html.escape(str(rule["details"]))}</td></tr>'
        for rule in report["rules"]
    )
    document = f'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Fraud mart data quality — {status}</title><style>body{{font:16px system-ui,sans-serif;max-width:1100px;margin:40px auto;padding:0 20px;color:#1b2937;background:#f5f7fa}}table{{border-collapse:collapse;width:100%;background:white}}th,td{{padding:12px;text-align:left;border:1px solid #ccd5df;overflow-wrap:anywhere}}th{{background:#e8eef5}}.pass{{color:#146c43;font-weight:bold}}.fail{{color:#b02a37;font-weight:bold}}code{{overflow-wrap:anywhere}}</style></head><body><h1>Fraud mart data quality: {status}</h1><p>Run: <code>{html.escape(run_id)}</code></p><p>Checked: {html.escape(str(report.get("checked_at", "")))} · Window end: {html.escape(str(report.get("end_month", "unknown")))} · Rows: {int(report.get("rows", 0))}</p><table><thead><tr><th>Rule</th><th>Result</th><th>Requirement / observation</th></tr></thead><tbody>{rows}</tbody></table><p>This self-contained report is published before the enforcement task, including when validation fails.</p></body></html>'''
    receipt = {**report, "run_id": run_id, "html_path": str(root / f"{safe_id}.html"), "json_path": str(root / f"{safe_id}.json")}
    serialized = json.dumps(receipt, indent=2, allow_nan=False)
    for filename, content in ((f"{safe_id}.html", document), (f"{safe_id}.json", serialized), ("latest.html", document), ("latest.json", serialized)):
        _atomic_write(root / filename, content)
    return receipt


def run_quality_check(run_id: str = "manual", end_month: str | None = None,
                      window_months: int = 3, storage_dir: str | Path | None = None) -> dict[str, Any]:
    """Read the latest fixed window and publish every finding before any failure gate.

    Read errors are converted to a failed report. Writes are atomic and keyed by
    run_id; repeated checks never modify mart rows or the published model pointer.
    """
    try:
        end_month = end_month or latest_month(storage_dir)
        frame = read_window(end_month, window_months, storage_dir)
        report = validate_frame(frame, end_month, window_months)
    except Exception as error:
        report = {"passed": False, "rows": 0, "end_month": end_month,
                  "checked_at": datetime.now(timezone.utc).isoformat(),
                  "rules": [{"name": "readable_labeled_mart", "passed": False, "details": f"{type(error).__name__}: {error}"}]}
    return publish_report(report, run_id, storage_dir)
