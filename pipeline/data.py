"""Durable, retry-safe labeled calendar-month mart shared by the three DAGs."""
from __future__ import annotations

import calendar
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
from typing import Any

import numpy as np
import pandas as pd


FEATURE_COLUMNS = [
    "merch", "cat_id", "amount", "gender", "one_city", "us_state", "post_code",
    "lat", "lon", "population_city", "jobs", "merchant_lat", "merchant_lon",
]
MART_COLUMNS = ["transaction_id", "transaction_time", "target", *FEATURE_COLUMNS]
NUMERIC_COLUMNS = ["amount", "lat", "lon", "population_city", "merchant_lat", "merchant_lon"]


def storage_path(storage_dir: str | Path | None = None) -> Path:
    """Resolve the shared storage root; create nothing on a read-only lookup."""
    return Path(storage_dir or os.environ.get("STORAGE_DIR", "/opt/project/storage"))


def month_offset(month: str, offset: int) -> str:
    """Return a calendar-month offset, validating the YYYY-MM input."""
    if not re.fullmatch(r"\d{4}-\d{2}", month):
        raise ValueError(f"Expected YYYY-MM, got {month!r}")
    year, number = map(int, month.split("-"))
    if not 1 <= number <= 12 or year < 1:
        raise ValueError(f"Invalid calendar month: {month}")
    total = year * 12 + number - 1 + offset
    next_year, zero_month = divmod(total, 12)
    if not 1 <= next_year <= 9999:
        raise ValueError("Month offset outside supported calendar")
    return f"{next_year:04d}-{zero_month + 1:02d}"


@contextmanager
def _connect(storage_dir: str | Path | None = None):
    root = storage_path(storage_dir)
    root.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(root / "mart.sqlite", timeout=60)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=60000")
    connection.execute("PRAGMA synchronous=FULL")
    try:
        with connection:
            yield connection
    finally:
        connection.close()


def _schema(connection: sqlite3.Connection) -> None:
    columns = ["transaction_id TEXT PRIMARY KEY", "transaction_time TEXT NOT NULL", "target INTEGER"]
    columns += [f'"{name}" {"REAL" if name in NUMERIC_COLUMNS else "TEXT"}' for name in FEATURE_COLUMNS]
    connection.execute(f"CREATE TABLE IF NOT EXISTS transactions ({', '.join(columns)})")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_transaction_time ON transactions(transaction_time)")
    connection.execute("CREATE TABLE IF NOT EXISTS month_runs (run_id TEXT PRIMARY KEY, month TEXT NOT NULL, rows_added INTEGER NOT NULL, rows_pruned INTEGER NOT NULL, committed_at TEXT DEFAULT CURRENT_TIMESTAMP)")
    connection.execute("CREATE TABLE IF NOT EXISTS mart_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")


def _insert_frame(connection: sqlite3.Connection, frame: pd.DataFrame) -> None:
    columns = ",".join(f'"{column}"' for column in MART_COLUMNS)
    placeholders = ",".join("?" for _ in MART_COLUMNS)
    clean = frame[MART_COLUMNS].astype(object).where(pd.notna(frame[MART_COLUMNS]), None)
    connection.executemany(
        f"INSERT INTO transactions ({columns}) VALUES ({placeholders})",
        clean.itertuples(index=False, name=None),
    )


def synthetic_month(month: str, rows: int = 1500, seed: int = 2024) -> pd.DataFrame:
    """Build a labeled synthetic, PII-free fixture; labels are a reproducible amount rule."""
    month_offset(month, 0)
    if rows < 20:
        raise ValueError("At least 20 synthetic rows per month are required")
    rng = np.random.default_rng(int(hashlib.sha256(f"{seed}:{month}".encode()).hexdigest()[:16], 16))
    year, number = map(int, month.split("-"))
    seconds = calendar.monthrange(year, number)[1] * 86400
    timestamps = pd.Timestamp(f"{month}-01") + pd.to_timedelta(rng.integers(0, seconds, rows), unit="s")
    # Fraud is intentionally learnable for an orchestration demo, not a quality claim.
    targets = np.zeros(rows, dtype=int)
    targets[rng.choice(rows, max(2, rows // 8), replace=False)] = 1
    amounts = np.where(targets == 1, rng.uniform(650, 1300, rows), rng.uniform(1, 450, rows)).round(2)
    lat = rng.uniform(25, 48, rows)
    lon = rng.uniform(-120, -75, rows)
    return pd.DataFrame({
        "transaction_id": [f"synthetic-{seed}-{month}-{i:07d}" for i in range(rows)],
        "transaction_time": timestamps.strftime("%Y-%m-%d %H:%M:%S"),
        "target": targets, "merch": rng.choice(["demo_market", "demo_travel", "demo_shop"], rows),
        "cat_id": rng.choice(["grocery_pos", "shopping_net", "travel"], rows),
        "amount": amounts, "gender": rng.choice(["F", "M"], rows),
        "one_city": rng.choice(["Demo City A", "Demo City B", "Demo City C"], rows),
        "us_state": rng.choice(["CA", "TX", "NY"], rows), "post_code": "00000",
        "lat": lat, "lon": lon, "population_city": rng.integers(100, 1_000_000, rows),
        "jobs": rng.choice(["demo_engineer", "demo_teacher", "demo_analyst"], rows),
        "merchant_lat": lat + rng.uniform(-0.3, 0.3, rows),
        "merchant_lon": lon + rng.uniform(-0.3, 0.3, rows),
    })[MART_COLUMNS]


def seed_mart(storage_dir: str | Path | None = None, csv_path: str | Path | None = None,
              start_month: str = "2024-01", months: int = 3, rows_per_month: int = 1500,
              seed: int = 2024) -> dict[str, Any]:
    """Read a CSV or reproducible fixture and initialize mart.sqlite atomically.

    Existing rows are never replaced: a retry, parallel initializer, or container
    restart returns the existing row count. CSV imports omit names and addresses.
    All inserts and provenance commit in one transaction; failed imports roll back.
    """
    if months < 3:
        raise ValueError("Seed at least three months for a temporal holdout")
    with _connect(storage_dir) as connection:
        connection.execute("BEGIN IMMEDIATE")
        _schema(connection)
        existing = connection.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        if existing:
            return {"status": "existing", "rows": existing}
        source = "synthetic"
        if csv_path:
            source = "csv"
            offset = 0
            for frame in pd.read_csv(csv_path, chunksize=20_000, dtype={"post_code": str}):
                missing = set(["transaction_time", "target", *FEATURE_COLUMNS]) - set(frame.columns)
                if missing:
                    raise ValueError(f"Labeled CSV missing columns: {sorted(missing)}")
                frame["transaction_time"] = pd.to_datetime(frame["transaction_time"], errors="raise", utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")
                if "transaction_id" not in frame:
                    frame["transaction_id"] = [f"csv-{index:012d}" for index in range(offset, offset + len(frame))]
                offset += len(frame)
                _insert_frame(connection, frame)
            if offset == 0:
                raise ValueError("CSV contains no rows")
        else:
            for index in range(months):
                _insert_frame(connection, synthetic_month(month_offset(start_month, index), rows_per_month, seed))
        count = connection.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        connection.execute("INSERT OR REPLACE INTO mart_metadata(key,value) VALUES ('source',?)", (source,))
        return {"status": "seeded", "rows": count, "source": source}


def read_mart(storage_dir: str | Path | None = None) -> pd.DataFrame:
    """Read a consistent canonical mart snapshot; this function never mutates rows."""
    with _connect(storage_dir) as connection:
        return pd.read_sql_query("SELECT * FROM transactions ORDER BY transaction_time, transaction_id", connection)


def available_months(storage_dir: str | Path | None = None) -> list[str]:
    """Read sorted distinct month partitions without loading all transaction rows."""
    with _connect(storage_dir) as connection:
        return [row[0] for row in connection.execute("SELECT DISTINCT substr(transaction_time,1,7) AS month FROM transactions ORDER BY month")]


def latest_month(storage_dir: str | Path | None = None) -> str:
    """Read the latest mart partition; fail clearly when the mart has no rows."""
    months = available_months(storage_dir)
    if not months:
        raise ValueError("The labeled mart is empty; run scripts/seed_data.py first")
    return months[-1]


def read_window(end_month: str, window_months: int = 3,
                storage_dir: str | Path | None = None) -> pd.DataFrame:
    """Read an inclusive calendar-month snapshot; no state changes on reads/retries."""
    if window_months < 2:
        raise ValueError("A temporal training window needs at least two months")
    start = month_offset(end_month, 1 - window_months) + "-01"
    end = month_offset(end_month, 1) + "-01"
    with _connect(storage_dir) as connection:
        return pd.read_sql_query(
            "SELECT * FROM transactions WHERE transaction_time >= ? AND transaction_time < ? ORDER BY transaction_time, transaction_id",
            connection, params=(start, end),
        )


def advance_month(run_id: str, storage_dir: str | Path | None = None,
                  batch_size: int | None = None, retention_months: int | None = None) -> dict[str, Any]:
    """Read latest labeled rows and published current.json; append one shifted month.

    Writes rows, retention deletion, and a unique run ledger in ONE SQLite
    transaction. Retrying a committed run returns its receipt. Crashes roll back;
    BEGIN IMMEDIATE serializes parallel writers. Supply waits for the current
    month's published model, so a rejected model cannot lose its required history.
    """
    batch_size = int(batch_size if batch_size is not None else os.environ.get("MONTHLY_BATCH_SIZE", "3000"))
    retention_months = int(retention_months if retention_months is not None else os.environ.get("HISTORY_MONTHS", "36"))
    if not run_id or batch_size < 50 or retention_months < 3:
        raise ValueError("run_id required, batch_size >= 50, retention_months >= 3")
    root = storage_path(storage_dir)
    with _connect(root) as connection:
        connection.execute("BEGIN IMMEDIATE")
        _schema(connection)
        previous = connection.execute("SELECT month,rows_added,rows_pruned FROM month_runs WHERE run_id=?", (run_id,)).fetchone()
        if previous:
            return {"status": "already_committed", "run_id": run_id, "month": previous[0], "rows_added": previous[1], "rows_pruned": previous[2]}
        maximum = connection.execute("SELECT MAX(substr(transaction_time,1,7)) FROM transactions").fetchone()[0]
        if not maximum:
            raise ValueError("Seed the mart before advancing its month")
        manifest_path = root / "models" / "current.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
        if manifest.get("window_end") != maximum:
            return {"status": "waiting_for_model", "mart_month": maximum, "published_month": manifest.get("window_end")}
        following = month_offset(maximum, 1)
        base = pd.read_sql_query(
            "SELECT * FROM transactions WHERE transaction_time >= ? AND transaction_time < ? ORDER BY transaction_id",
            connection, params=(maximum + "-01", following + "-01"),
        )
        digest = hashlib.sha256(f"{run_id}:{following}".encode()).hexdigest()
        rng = np.random.default_rng(int(digest[:16], 16))
        indices = rng.choice(len(base), batch_size, replace=True)
        # Preserve both labels even for small or highly imbalanced source months.
        for index, label in enumerate((0, 1)):
            matches = np.flatnonzero(base["target"].to_numpy() == label)
            if not len(matches):
                raise ValueError(f"Source month {maximum} lacks class {label}")
            indices[index] = rng.choice(matches)
        sampled = base.iloc[indices].copy().reset_index(drop=True)
        year, month = map(int, following.split("-"))
        last_day = calendar.monthrange(year, month)[1]
        sampled["transaction_time"] = pd.to_datetime(sampled["transaction_time"]).map(
            lambda value: value.replace(year=year, month=month, day=min(value.day, last_day)).strftime("%Y-%m-%d %H:%M:%S")
        )
        sampled["transaction_id"] = [f"monthly-{following}-{digest[:20]}-{index:09d}" for index in range(batch_size)]
        _insert_frame(connection, sampled)
        cutoff = month_offset(following, 1 - retention_months) + "-01"
        pruned = connection.execute("DELETE FROM transactions WHERE transaction_time < ?", (cutoff,)).rowcount
        connection.execute("INSERT INTO month_runs(run_id,month,rows_added,rows_pruned) VALUES (?,?,?,?)", (run_id, following, batch_size, pruned))
        return {"status": "committed", "run_id": run_id, "month": following, "rows_added": batch_size, "rows_pruned": pruned, "retention_months": retention_months}
