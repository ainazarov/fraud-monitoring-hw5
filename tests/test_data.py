"""Exercise transactional durability, backpressure and calendar edge cases."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from pipeline import data


class DataMartTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)

    def tearDown(self):
        self.directory.cleanup()

    def publish(self, month):
        (self.root / "models").mkdir(exist_ok=True)
        (self.root / "models" / "current.json").write_text(json.dumps({"window_end": month}), encoding="utf-8")

    def seed(self, **kwargs):
        return data.seed_mart(self.root, rows_per_month=100, **kwargs)

    def test_seed_idempotent_and_window_is_calendar_bounded(self):
        self.assertEqual(self.seed()["rows"], 300)
        before = data.read_mart(self.root)
        self.assertEqual(self.seed(start_month="2030-01")["status"], "existing")
        pd.testing.assert_frame_equal(before, data.read_mart(self.root))
        self.assertEqual(data.available_months(self.root), ["2024-01", "2024-02", "2024-03"])
        window = data.read_window("2024-03", 2, self.root)
        self.assertEqual(len(window), 200)
        self.assertEqual(sorted(window.transaction_time.str[:7].unique()), ["2024-02", "2024-03"])
        self.assertNotIn("name_1", window.columns)

    def test_supply_waits_for_successful_publication(self):
        self.seed()
        self.assertEqual(data.advance_month("run-1", self.root, 100)["status"], "waiting_for_model")
        self.publish("2024-03")
        first = data.advance_month("run-1", self.root, 100)
        self.assertEqual(first["month"], "2024-04")
        # Retry remains tied to its receipt even after publication catches up.
        self.publish("2024-04")
        self.assertEqual(data.advance_month("run-1", self.root, 100)["status"], "already_committed")
        self.assertEqual(data.latest_month(self.root), "2024-04")
        self.assertEqual(len(data.read_mart(self.root)), 400)

    def test_failed_insert_rolls_back_without_skipping_month(self):
        self.seed()
        self.publish("2024-03")
        original = data._insert_frame
        def crash_after_insert(connection, frame):
            original(connection, frame)
            raise RuntimeError("simulated crash before commit")
        with patch.object(data, "_insert_frame", side_effect=crash_after_insert):
            with self.assertRaisesRegex(RuntimeError, "simulated crash"):
                data.advance_month("recoverable", self.root, 100)
        self.assertEqual(data.latest_month(self.root), "2024-03")
        self.assertEqual(len(data.read_mart(self.root)), 300)
        self.assertEqual(data.advance_month("recoverable", self.root, 100)["month"], "2024-04")

    def test_parallel_writers_commit_only_one_new_month(self):
        self.seed()
        self.publish("2024-03")
        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(lambda key: data.advance_month(key, self.root, 100), ["parallel-a", "parallel-b"]))
        self.assertEqual(sorted(outcome["status"] for outcome in outcomes), ["committed", "waiting_for_model"])
        self.assertEqual(len(data.read_mart(self.root)), 400)

    def test_calendar_shift_clips_day_for_leap_february(self):
        self.seed(start_month="2023-11")
        with sqlite3.connect(self.root / "mart.sqlite") as connection:
            connection.execute("UPDATE transactions SET transaction_time='2024-01-31 23:59:59' WHERE transaction_time LIKE '2024-01%'")
        connection.close()
        self.publish("2024-01")
        data.advance_month("february", self.root, 100)
        frame = data.read_mart(self.root)
        february = frame[frame.transaction_time.str.startswith("2024-02")]
        self.assertTrue((february.transaction_time == "2024-02-29 23:59:59").all())
        self.assertEqual(set(february.target), {0, 1})

    def test_retention_prunes_in_same_transaction(self):
        self.seed(months=4)
        self.publish("2024-04")
        outcome = data.advance_month("retention", self.root, 100, retention_months=3)
        self.assertEqual(outcome["rows_pruned"], 200)
        self.assertEqual(data.available_months(self.root), ["2024-03", "2024-04", "2024-05"])
        self.assertEqual(data.month_offset("2024-12", 1), "2025-01")

    def test_csv_import_preserves_labels_but_omits_personal_columns(self):
        fixture = pd.concat([data.synthetic_month(month, 100) for month in ["2024-01", "2024-02", "2024-03"]])
        fixture["name_1"] = "sensitive-placeholder"
        fixture = fixture.drop(columns="transaction_id")
        path = self.root / "input.csv"
        fixture.to_csv(path, index=False)
        outcome = data.seed_mart(self.root, csv_path=path)
        self.assertEqual(outcome["source"], "csv")
        imported = data.read_mart(self.root)
        self.assertEqual(len(imported), 300)
        self.assertEqual(imported.transaction_id.nunique(), 300)
        self.assertNotIn("name_1", imported.columns)


if __name__ == "__main__":
    unittest.main()
