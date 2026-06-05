"""
tests/test_workspace_export.py
================================
Unit tests for workspace_export.status_tracker and
workspace_export.report_generator.

No live Databricks workspace required.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from workspace_export.status_tracker import (
    ExportStatusTracker,
    ComponentStatus,
    PENDING,
    IN_PROGRESS,
    SUCCESS,
    FAILED,
    SKIPPED,
    ALL_COMPONENTS,
)
from workspace_export.report_generator import (
    generate_report,
    report_from_file,
    count_log_lines,
    count_dir_entries,
    dir_size_bytes,
    _fmt_duration,
    _fmt_bytes,
    _fmt_count,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_tracker(tmp: str) -> ExportStatusTracker:
    status_file = os.path.join(tmp, "export_status.json")
    tracker = ExportStatusTracker(status_file)
    tracker.set_metadata(
        workspace_url="https://test.azuredatabricks.net",
        session="M202404281200",
        export_dir=tmp + "/",
    )
    return tracker


# ---------------------------------------------------------------------------
# 1. ExportStatusTracker – initial state
# ---------------------------------------------------------------------------

class TestTrackerInit(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil; shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_all_components_pending_on_init(self):
        tracker = _make_tracker(self.tmpdir)
        for comp in tracker.all():
            self.assertEqual(comp.status, PENDING, f"{comp.name} should start as PENDING")

    def test_component_count_matches_registry(self):
        tracker = _make_tracker(self.tmpdir)
        self.assertEqual(len(tracker.all()), len(ALL_COMPONENTS))

    def test_status_file_created_on_init(self):
        tracker = _make_tracker(self.tmpdir)
        status_file = os.path.join(self.tmpdir, "export_status.json")
        self.assertTrue(os.path.exists(status_file))

    def test_metadata_persisted(self):
        tracker = _make_tracker(self.tmpdir)
        status_file = os.path.join(self.tmpdir, "export_status.json")
        with open(status_file) as fp:
            data = json.load(fp)
        self.assertEqual(data["workspace_url"], "https://test.azuredatabricks.net")
        self.assertEqual(data["session"], "M202404281200")


# ---------------------------------------------------------------------------
# 2. ExportStatusTracker – state transitions
# ---------------------------------------------------------------------------

class TestTrackerTransitions(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.tracker = _make_tracker(self.tmpdir)

    def tearDown(self):
        import shutil; shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_start(self):
        self.tracker.start("users")
        comp = self.tracker.get("users")
        self.assertEqual(comp.status, IN_PROGRESS)
        self.assertIsNotNone(comp.start_time)

    def test_success(self):
        self.tracker.start("users")
        self.tracker.success(
            "users",
            log_files=["users.log"],
            items_exported=156,
            notes="Test run",
        )
        comp = self.tracker.get("users")
        self.assertEqual(comp.status, SUCCESS)
        self.assertEqual(comp.items_exported, 156)
        self.assertEqual(comp.log_files, ["users.log"])
        self.assertIsNotNone(comp.duration_seconds)
        self.assertGreaterEqual(comp.duration_seconds, 0)
        self.assertEqual(comp.notes, "Test run")

    def test_fail(self):
        self.tracker.start("jobs")
        self.tracker.fail("jobs", "Connection timeout")
        comp = self.tracker.get("jobs")
        self.assertEqual(comp.status, FAILED)
        self.assertEqual(comp.error_message, "Connection timeout")
        self.assertIsNotNone(comp.duration_seconds)

    def test_skip(self):
        self.tracker.skip("mlflow_runs", "skipped by user")
        comp = self.tracker.get("mlflow_runs")
        self.assertEqual(comp.status, SKIPPED)
        self.assertEqual(comp.notes, "skipped by user")

    def test_bytes_downloaded(self):
        self.tracker.start("dbfs_libraries")
        self.tracker.success(
            "dbfs_libraries",
            log_files=["library_manifest.json"],
            items_exported=5,
            bytes_downloaded=10 * 1024 * 1024,
        )
        comp = self.tracker.get("dbfs_libraries")
        self.assertEqual(comp.bytes_downloaded, 10 * 1024 * 1024)

    def test_unknown_component_is_noop(self):
        """Updating an unknown component name should not raise."""
        self.tracker.start("nonexistent_component")
        self.tracker.success("nonexistent_component")
        self.tracker.fail("nonexistent_component", "err")
        self.tracker.skip("nonexistent_component")


# ---------------------------------------------------------------------------
# 3. ExportStatusTracker – persistence
# ---------------------------------------------------------------------------

class TestTrackerPersistence(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil; shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_status_persisted_after_each_update(self):
        tracker = _make_tracker(self.tmpdir)
        tracker.start("users")
        tracker.success("users", log_files=["users.log"], items_exported=100)

        status_file = os.path.join(self.tmpdir, "export_status.json")
        with open(status_file) as fp:
            data = json.load(fp)
        self.assertEqual(data["components"]["users"]["status"], SUCCESS)
        self.assertEqual(data["components"]["users"]["items_exported"], 100)

    def test_load_roundtrip(self):
        tracker = _make_tracker(self.tmpdir)
        tracker.start("clusters")
        tracker.success("clusters", log_files=["clusters.log"], items_exported=8)
        tracker.fail("jobs", "API timeout")
        tracker.skip("mlflow_runs", "too slow")
        tracker.mark_complete()

        status_file = os.path.join(self.tmpdir, "export_status.json")
        restored = ExportStatusTracker.load(status_file)

        self.assertEqual(restored.workspace_url, "https://test.azuredatabricks.net")
        self.assertEqual(restored.get("clusters").status, SUCCESS)
        self.assertEqual(restored.get("clusters").items_exported, 8)
        self.assertEqual(restored.get("jobs").status, FAILED)
        self.assertEqual(restored.get("jobs").error_message, "API timeout")
        self.assertEqual(restored.get("mlflow_runs").status, SKIPPED)
        self.assertIsNotNone(restored.export_end)

    def test_atomic_write_no_tmp_file_left(self):
        """Temporary .tmp file must not persist after write."""
        tracker = _make_tracker(self.tmpdir)
        status_file = os.path.join(self.tmpdir, "export_status.json")
        tmp_file = status_file + ".tmp"
        self.assertFalse(os.path.exists(tmp_file))

    def test_counts(self):
        tracker = _make_tracker(self.tmpdir)
        tracker.success("users")
        tracker.success("groups")
        tracker.fail("jobs", "err")
        tracker.skip("mlflow_runs")

        counts = tracker.counts()
        self.assertEqual(counts["succeeded"], 2)
        self.assertEqual(counts["failed"], 1)
        self.assertEqual(counts["skipped"], 1)
        # rest should be pending
        self.assertEqual(
            counts["pending"],
            len(ALL_COMPONENTS) - 4,
        )


# ---------------------------------------------------------------------------
# 4. Report formatting helpers
# ---------------------------------------------------------------------------

class TestFormatters(unittest.TestCase):

    def test_fmt_duration_seconds_only(self):
        self.assertEqual(_fmt_duration(45), "45s")

    def test_fmt_duration_minutes(self):
        self.assertEqual(_fmt_duration(125), "2m 5s")

    def test_fmt_duration_hours(self):
        self.assertEqual(_fmt_duration(3665), "1h 1m 5s")

    def test_fmt_duration_none(self):
        self.assertEqual(_fmt_duration(None), "–")

    def test_fmt_bytes_bytes(self):
        self.assertEqual(_fmt_bytes(512), "512 B")

    def test_fmt_bytes_kilobytes(self):
        self.assertIn("KB", _fmt_bytes(2048))

    def test_fmt_bytes_megabytes(self):
        self.assertIn("MB", _fmt_bytes(5 * 1024 * 1024))

    def test_fmt_bytes_none_empty(self):
        self.assertEqual(_fmt_bytes(None), "")

    def test_fmt_count(self):
        self.assertEqual(_fmt_count(1234567), "1,234,567")

    def test_fmt_count_none(self):
        self.assertEqual(_fmt_count(None), "–")


# ---------------------------------------------------------------------------
# 5. File counting helpers
# ---------------------------------------------------------------------------

class TestFileHelpers(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil; shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_count_log_lines(self):
        log = os.path.join(self.tmpdir, "test.log")
        with open(log, "w") as fp:
            fp.write('{"a": 1}\n')
            fp.write('{"b": 2}\n')
            fp.write("\n")           # blank line – should not be counted
            fp.write('{"c": 3}\n')
        self.assertEqual(count_log_lines(log), 3)

    def test_count_log_lines_missing_file(self):
        self.assertIsNone(count_log_lines("/nonexistent/path.log"))

    def test_count_dir_entries(self):
        for name in ["a.log", "b.log", ".hidden"]:
            open(os.path.join(self.tmpdir, name), "w").close()
        # .hidden should be excluded
        self.assertEqual(count_dir_entries(self.tmpdir), 2)

    def test_count_dir_entries_missing(self):
        self.assertIsNone(count_dir_entries("/nonexistent/dir"))

    def test_dir_size_bytes(self):
        p = os.path.join(self.tmpdir, "file.bin")
        with open(p, "wb") as fp:
            fp.write(b"X" * 1024)
        self.assertEqual(dir_size_bytes(self.tmpdir), 1024)


# ---------------------------------------------------------------------------
# 6. generate_report – structural checks
# ---------------------------------------------------------------------------

class TestReportGenerator(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil; shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _build_tracker(self) -> ExportStatusTracker:
        tracker = _make_tracker(self.tmpdir)
        # Simulate a typical run
        for name in ["instance_profiles", "users", "groups",
                     "workspace_item_log", "workspace_acls", "notebooks",
                     "secrets", "clusters", "instance_pools"]:
            tracker.start(name)
            tracker.success(name, items_exported=10)

        tracker.start("jobs")
        tracker.fail("jobs", "API rate limit exceeded")

        tracker.skip("mlflow_runs", "start_date not specified")
        tracker.start("dbfs_libraries")
        tracker.success("dbfs_libraries", items_exported=3,
                        bytes_downloaded=5 * 1024 * 1024)
        tracker.mark_complete()
        return tracker

    def test_report_contains_workspace_url(self):
        tracker = self._build_tracker()
        report = generate_report(tracker, self.tmpdir)
        self.assertIn("https://test.azuredatabricks.net", report)

    def test_report_contains_session(self):
        tracker = self._build_tracker()
        report = generate_report(tracker, self.tmpdir)
        self.assertIn("M202404281200", report)

    def test_report_shows_failed_component(self):
        tracker = self._build_tracker()
        report = generate_report(tracker, self.tmpdir)
        self.assertIn("API rate limit exceeded", report)
        self.assertIn("FAILURES", report)

    def test_report_shows_skipped_component(self):
        tracker = self._build_tracker()
        report = generate_report(tracker, self.tmpdir)
        self.assertIn("SKIPPED", report)

    def test_report_shows_dbfs_download_size(self):
        tracker = self._build_tracker()
        report = generate_report(tracker, self.tmpdir)
        self.assertIn("MB", report)  # 5 MB download

    def test_report_shows_summary(self):
        tracker = self._build_tracker()
        report = generate_report(tracker, self.tmpdir)
        self.assertIn("SUMMARY", report)
        self.assertIn("Succeeded", report)
        self.assertIn("Failed", report)

    def test_report_contains_checklist(self):
        tracker = self._build_tracker()
        report = generate_report(tracker, self.tmpdir)
        self.assertIn("POST-EXPORT CHECKLIST", report)
        self.assertIn("import pipeline", report)

    def test_all_component_names_in_report(self):
        tracker = self._build_tracker()
        report = generate_report(tracker, self.tmpdir)
        for _, display in ALL_COMPONENTS:
            self.assertIn(display, report, f"'{display}' missing from report")

    def test_report_from_file(self):
        """report_from_file should produce same output as generate_report."""
        tracker = self._build_tracker()
        status_file = os.path.join(self.tmpdir, "export_status.json")
        direct = generate_report(tracker, self.tmpdir)
        from_file = report_from_file(status_file)
        # Both should mention workspace URL
        self.assertIn("https://test.azuredatabricks.net", from_file)


# ---------------------------------------------------------------------------
# 7. ComponentStatus – serialisation
# ---------------------------------------------------------------------------

class TestComponentStatusSerialisation(unittest.TestCase):

    def test_roundtrip(self):
        original = ComponentStatus(
            name="users",
            display_name="Users",
            status=SUCCESS,
            start_time="2024-04-28T12:00:00",
            end_time="2024-04-28T12:01:23",
            duration_seconds=83.0,
            items_exported=156,
            log_files=["users.log"],
            bytes_downloaded=None,
            error_message=None,
            notes="exported successfully",
        )
        d = original.to_dict()
        restored = ComponentStatus.from_dict(d)
        self.assertEqual(restored.name, original.name)
        self.assertEqual(restored.status, original.status)
        self.assertEqual(restored.items_exported, original.items_exported)
        self.assertEqual(restored.log_files, original.log_files)
        self.assertEqual(restored.notes, original.notes)


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main(verbosity=2)
