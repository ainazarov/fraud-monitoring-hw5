"""DQ failures must be detected, persisted and enforced after report publication."""
import json
from pathlib import Path
import tempfile
import unittest

import numpy as np
import pandas as pd

from pipeline.data import seed_mart, synthetic_month
from pipeline.quality import assert_quality, publish_report, run_quality_check, validate_frame


class QualityTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.frame = pd.concat([synthetic_month(month, 100) for month in ["2024-01", "2024-02", "2024-03"]], ignore_index=True)

    def tearDown(self):
        self.directory.cleanup()

    @staticmethod
    def failures(report):
        return {rule["name"] for rule in report["rules"] if not rule["passed"]}

    def test_fixture_passes_all_thirteen_rules(self):
        report = validate_frame(self.frame, "2024-03")
        self.assertTrue(report["passed"], self.failures(report))
        self.assertEqual(len(report["rules"]), 13)
        assert_quality(report)

    def test_bad_ids_numeric_values_and_labels_are_rejected(self):
        self.frame.loc[1, "transaction_id"] = self.frame.loc[0, "transaction_id"]
        self.frame.loc[0, "amount"] = np.inf
        self.frame.loc[1, "lat"] = 91
        self.frame.loc[2, "target"] = 2
        self.frame.loc[3, "population_city"] = -1
        failures = self.failures(validate_frame(self.frame, "2024-03"))
        self.assertTrue({"unique_transaction_ids", "amount_finite_and_in_range", "valid_geographic_coordinates", "binary_target", "valid_city_population"}.issubset(failures))

    def test_missing_month_and_single_class_fail_temporal_quality(self):
        self.frame = self.frame[~self.frame.transaction_time.str.startswith("2024-02")].copy()
        self.frame["target"] = 0
        failures = self.failures(validate_frame(self.frame, "2024-03"))
        self.assertTrue({"consecutive_calendar_window", "usable_monthly_counts", "both_classes_each_month"}.issubset(failures))

    def test_missing_schema_still_produces_report_then_gate_raises(self):
        self.frame = self.frame.drop(columns=["amount", "transaction_time"])
        report = validate_frame(self.frame, "2024-03")
        receipt = publish_report(report, "broken/manual", self.root)
        self.assertFalse(receipt["passed"])
        self.assertTrue(Path(receipt["html_path"]).exists())
        persisted = json.loads(Path(receipt["json_path"]).read_text(encoding="utf-8"))
        with self.assertRaisesRegex(ValueError, "required_schema"):
            assert_quality(persisted)

    def test_html_escapes_untrusted_text(self):
        report = validate_frame(self.frame, "2024-03")
        report["rules"][0]["details"] = '<script>alert("bad")</script>'
        receipt = publish_report(report, "<img src=x onerror=bad>", self.root)
        content = Path(receipt["html_path"]).read_text(encoding="utf-8")
        self.assertNotIn("<script>", content)
        self.assertNotIn("<img", content)
        self.assertIn("&lt;script&gt;", content)

    def test_read_error_and_normal_check_both_publish_reports(self):
        missing = run_quality_check("uninitialized", storage_dir=self.root)
        self.assertFalse(missing["passed"])
        self.assertTrue(Path(missing["html_path"]).exists())
        seed_mart(self.root, rows_per_month=100)
        valid = run_quality_check("initialized", storage_dir=self.root)
        self.assertTrue(valid["passed"])
        self.assertTrue((self.root / "dq_reports" / "latest.json").exists())


if __name__ == "__main__":
    unittest.main()
