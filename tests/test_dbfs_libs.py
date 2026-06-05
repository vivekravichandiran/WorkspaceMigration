"""
tests/test_dbfs_libs.py
=======================
Unit tests for the DBFS / library migration utility.

All tests are self-contained: no live Databricks workspace is required.
Databricks API calls are mocked using unittest.mock.
"""

from __future__ import annotations

import base64
import json
import os
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, call, patch

# Allow imports from the workspace root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dbfs_libs.exporter import LibraryExporter, _parse_library_dict
from dbfs_libs.importer import ImportReport, LibraryImporter
from dbfs_libs.models import LibraryEntry, LibraryManifest, LibraryUsage


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_json_lines(path: str, records: list) -> None:
    with open(path, "w", encoding="utf-8") as fp:
        for r in records:
            fp.write(json.dumps(r) + "\n")


def _make_manifest(src_url: str = "https://src.azuredatabricks.net") -> LibraryManifest:
    return LibraryManifest(source_workspace_url=src_url)


# ---------------------------------------------------------------------------
# 1. _parse_library_dict
# ---------------------------------------------------------------------------

class TestParseLibraryDict(unittest.TestCase):

    def test_jar(self):
        entry = _parse_library_dict({"jar": "dbfs:/FileStore/jars/lib.jar"})
        self.assertIsNotNone(entry)
        self.assertEqual(entry.lib_type, "jar")
        self.assertEqual(entry.dbfs_path, "dbfs:/FileStore/jars/lib.jar")

    def test_whl(self):
        entry = _parse_library_dict({"whl": "dbfs:/FileStore/wheels/my.whl"})
        self.assertEqual(entry.lib_type, "whl")
        self.assertEqual(entry.dbfs_path, "dbfs:/FileStore/wheels/my.whl")

    def test_egg(self):
        entry = _parse_library_dict({"egg": "dbfs:/FileStore/eggs/my.egg"})
        self.assertEqual(entry.lib_type, "egg")
        self.assertEqual(entry.dbfs_path, "dbfs:/FileStore/eggs/my.egg")

    def test_pypi_with_repo(self):
        entry = _parse_library_dict(
            {"pypi": {"package": "requests>=2.28", "repo": "https://pypi.org"}}
        )
        self.assertEqual(entry.lib_type, "pypi")
        self.assertEqual(entry.pypi_package, "requests>=2.28")
        self.assertEqual(entry.pypi_repo, "https://pypi.org")

    def test_pypi_without_repo(self):
        entry = _parse_library_dict({"pypi": {"package": "pandas"}})
        self.assertEqual(entry.lib_type, "pypi")
        self.assertIsNone(entry.pypi_repo)

    def test_maven(self):
        entry = _parse_library_dict(
            {
                "maven": {
                    "coordinates": "com.example:foo:1.0",
                    "repo": "https://repo1.maven.org/maven2",
                    "exclusions": ["com.example:bar"],
                }
            }
        )
        self.assertEqual(entry.lib_type, "maven")
        self.assertEqual(entry.maven_coordinates, "com.example:foo:1.0")
        self.assertEqual(entry.maven_exclusions, ["com.example:bar"])

    def test_cran(self):
        entry = _parse_library_dict({"cran": {"package": "ggplot2"}})
        self.assertEqual(entry.lib_type, "cran")
        self.assertEqual(entry.cran_package, "ggplot2")

    def test_unknown_returns_none(self):
        entry = _parse_library_dict({"unknown_key": "value"})
        self.assertIsNone(entry)


# ---------------------------------------------------------------------------
# 2. LibraryEntry – key deduplication
# ---------------------------------------------------------------------------

class TestLibraryEntryKey(unittest.TestCase):

    def test_jar_key(self):
        e = LibraryEntry(lib_type="jar", dbfs_path="dbfs:/FileStore/jars/lib.jar")
        self.assertEqual(e.key(), "dbfs:dbfs:/FileStore/jars/lib.jar")

    def test_pypi_key(self):
        e = LibraryEntry(lib_type="pypi", pypi_package="requests>=2.28")
        self.assertEqual(e.key(), "pypi:requests>=2.28")

    def test_maven_key(self):
        e = LibraryEntry(lib_type="maven", maven_coordinates="com.example:foo:1.0")
        self.assertEqual(e.key(), "maven:com.example:foo:1.0")

    def test_cran_key(self):
        e = LibraryEntry(lib_type="cran", cran_package="ggplot2")
        self.assertEqual(e.key(), "cran:ggplot2")


# ---------------------------------------------------------------------------
# 3. LibraryManifest – JSON round-trip
# ---------------------------------------------------------------------------

class TestLibraryManifestRoundTrip(unittest.TestCase):

    def _build_manifest(self) -> LibraryManifest:
        m = LibraryManifest(source_workspace_url="https://src.azuredatabricks.net")
        jar = LibraryEntry(
            lib_type="jar",
            dbfs_path="dbfs:/FileStore/jars/lib.jar",
            local_file="dbfs_files/FileStore/jars/lib.jar",
            raw={"jar": "dbfs:/FileStore/jars/lib.jar"},
            used_by=[
                LibraryUsage(
                    entity_type="cluster",
                    entity_id="c001",
                    entity_name="prod-cluster",
                )
            ],
        )
        pypi = LibraryEntry(
            lib_type="pypi",
            pypi_package="requests>=2.28",
            pypi_repo=None,
            raw={"pypi": {"package": "requests>=2.28"}},
            used_by=[
                LibraryUsage(
                    entity_type="job",
                    entity_id="42",
                    entity_name="my_job",
                ),
                LibraryUsage(
                    entity_type="job_task",
                    entity_id="42",
                    entity_name="my_job",
                    task_name="ingest",
                ),
            ],
        )
        maven = LibraryEntry(
            lib_type="maven",
            maven_coordinates="io.delta:delta-core_2.12:2.0.0",
            raw={"maven": {"coordinates": "io.delta:delta-core_2.12:2.0.0"}},
        )
        m.libraries = [jar, pypi, maven]
        return m

    def test_roundtrip(self):
        original = self._build_manifest()
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        ) as fp:
            tmp_path = fp.name
            original.to_json(fp)

        try:
            with open(tmp_path, "r", encoding="utf-8") as fp:
                restored = LibraryManifest.from_json(fp)
        finally:
            os.unlink(tmp_path)

        self.assertEqual(restored.source_workspace_url, original.source_workspace_url)
        self.assertEqual(len(restored.libraries), 3)

        jar = restored.libraries[0]
        self.assertEqual(jar.lib_type, "jar")
        self.assertEqual(jar.dbfs_path, "dbfs:/FileStore/jars/lib.jar")
        self.assertEqual(jar.local_file, "dbfs_files/FileStore/jars/lib.jar")
        self.assertEqual(len(jar.used_by), 1)
        self.assertEqual(jar.used_by[0].entity_type, "cluster")

        pypi = restored.libraries[1]
        self.assertEqual(pypi.lib_type, "pypi")
        self.assertEqual(len(pypi.used_by), 2)
        task_usage = pypi.used_by[1]
        self.assertEqual(task_usage.entity_type, "job_task")
        self.assertEqual(task_usage.task_name, "ingest")

        maven = restored.libraries[2]
        self.assertEqual(maven.maven_coordinates, "io.delta:delta-core_2.12:2.0.0")

    def test_file_based_and_coordinate_split(self):
        m = self._build_manifest()
        self.assertEqual(len(m.file_based()), 1)
        self.assertEqual(len(m.coordinate_based()), 2)


# ---------------------------------------------------------------------------
# 4. LibraryExporter – clusters.log scanning
# ---------------------------------------------------------------------------

class TestExporterClustersLog(unittest.TestCase):

    CLUSTERS = [
        {
            "cluster_id": "c001",
            "cluster_name": "prod-cluster",
            "libraries": [
                {"jar": "dbfs:/FileStore/jars/lib.jar"},
                {"pypi": {"package": "requests>=2.28"}},
            ],
        },
        {
            "cluster_id": "c002",
            "cluster_name": "dev-cluster",
            "libraries": [
                {"jar": "dbfs:/FileStore/jars/lib.jar"},  # same jar – should dedup
                {"maven": {"coordinates": "com.example:foo:1.0"}},
            ],
        },
        {
            "cluster_id": "c003",
            "cluster_name": "empty-cluster",
            # no libraries key
        },
    ]

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _make_exporter(self) -> LibraryExporter:
        exporter = LibraryExporter.__new__(LibraryExporter)
        exporter._export_dir = self.tmpdir + "/"
        exporter._dbfs_download_dir = os.path.join(self.tmpdir, "dbfs_files")
        os.makedirs(exporter._dbfs_download_dir, exist_ok=True)
        exporter._logger = MagicMock()
        mock_client = MagicMock()
        mock_client.get_url.return_value = "https://src.azuredatabricks.net"
        exporter._client = mock_client
        return exporter

    def test_cluster_log_deduplication(self):
        exporter = self._make_exporter()
        clusters_log = os.path.join(self.tmpdir, "clusters.log")
        _write_json_lines(clusters_log, self.CLUSTERS)

        manifest = _make_manifest()
        exporter._process_clusters_log(clusters_log, manifest)

        # jar appears in c001 and c002 but should be one entry
        # pypi appears once, maven once → total 3 unique
        self.assertEqual(len(manifest.libraries), 3)
        keys = {l.key() for l in manifest.libraries}
        self.assertIn("dbfs:dbfs:/FileStore/jars/lib.jar", keys)
        self.assertIn("pypi:requests>=2.28", keys)
        self.assertIn("maven:com.example:foo:1.0", keys)

    def test_jar_used_by_two_clusters(self):
        exporter = self._make_exporter()
        clusters_log = os.path.join(self.tmpdir, "clusters.log")
        _write_json_lines(clusters_log, self.CLUSTERS)

        manifest = _make_manifest()
        exporter._process_clusters_log(clusters_log, manifest)

        jar = next(l for l in manifest.libraries if l.lib_type == "jar")
        entity_ids = {u.entity_id for u in jar.used_by}
        self.assertEqual(entity_ids, {"c001", "c002"})


# ---------------------------------------------------------------------------
# 5. LibraryExporter – jobs.log scanning
# ---------------------------------------------------------------------------

class TestExporterJobsLog(unittest.TestCase):

    JOBS = [
        # SINGLE_TASK job with top-level libraries
        {
            "job_id": 1,
            "settings": {
                "name": "ingest_job:::1",
                "format": "SINGLE_TASK",
                "libraries": [{"pypi": {"package": "pandas"}}],
            },
        },
        # MULTI_TASK job with per-task libraries
        {
            "job_id": 2,
            "settings": {
                "name": "pipeline_job:::2",
                "format": "MULTI_TASK",
                "tasks": [
                    {
                        "task_key": "extract",
                        "libraries": [
                            {"jar": "dbfs:/FileStore/jars/etl.jar"},
                            {"maven": {"coordinates": "com.example:foo:1.0"}},
                        ],
                    },
                    {
                        "task_key": "load",
                        "libraries": [{"pypi": {"package": "pandas"}}],
                    },
                ],
            },
        },
    ]

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _make_exporter(self) -> LibraryExporter:
        exporter = LibraryExporter.__new__(LibraryExporter)
        exporter._export_dir = self.tmpdir + "/"
        exporter._dbfs_download_dir = os.path.join(self.tmpdir, "dbfs_files")
        os.makedirs(exporter._dbfs_download_dir, exist_ok=True)
        exporter._logger = MagicMock()
        mock_client = MagicMock()
        mock_client.get_url.return_value = "https://src.azuredatabricks.net"
        exporter._client = mock_client
        return exporter

    def test_single_task_job(self):
        exporter = self._make_exporter()
        jobs_log = os.path.join(self.tmpdir, "jobs.log")
        _write_json_lines(jobs_log, [self.JOBS[0]])

        manifest = _make_manifest()
        exporter._process_jobs_log(jobs_log, manifest)

        self.assertEqual(len(manifest.libraries), 1)
        pandas = manifest.libraries[0]
        self.assertEqual(pandas.lib_type, "pypi")
        self.assertEqual(pandas.used_by[0].entity_type, "job")
        self.assertEqual(pandas.used_by[0].entity_id, "1")
        # name should have :::job_id stripped
        self.assertEqual(pandas.used_by[0].entity_name, "ingest_job")

    def test_multi_task_job(self):
        exporter = self._make_exporter()
        jobs_log = os.path.join(self.tmpdir, "jobs.log")
        _write_json_lines(jobs_log, [self.JOBS[1]])

        manifest = _make_manifest()
        exporter._process_jobs_log(jobs_log, manifest)

        # jar, maven, pandas – 3 unique entries
        self.assertEqual(len(manifest.libraries), 3)
        jar = next(l for l in manifest.libraries if l.lib_type == "jar")
        self.assertEqual(jar.used_by[0].entity_type, "job_task")
        self.assertEqual(jar.used_by[0].task_name, "extract")

    def test_dedup_across_jobs(self):
        exporter = self._make_exporter()
        jobs_log = os.path.join(self.tmpdir, "jobs.log")
        _write_json_lines(jobs_log, self.JOBS)  # both jobs

        manifest = _make_manifest()
        exporter._process_jobs_log(jobs_log, manifest)

        # pandas appears in job 1 and job 2/load – should be 1 entry with 2 usages
        pandas = next(l for l in manifest.libraries if l.pypi_package == "pandas")
        self.assertEqual(len(pandas.used_by), 2)


# ---------------------------------------------------------------------------
# 6. LibraryExporter – DBFS file download
# ---------------------------------------------------------------------------

class TestExporterDbfsDownload(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _make_exporter(self, mock_client: MagicMock) -> LibraryExporter:
        exporter = LibraryExporter.__new__(LibraryExporter)
        exporter._export_dir = self.tmpdir + "/"
        exporter._dbfs_download_dir = os.path.join(self.tmpdir, "dbfs_files")
        os.makedirs(exporter._dbfs_download_dir, exist_ok=True)
        exporter._logger = MagicMock()
        exporter._client = mock_client
        return exporter

    def test_single_chunk_download(self):
        """File fits in one chunk."""
        file_content = b"fake-jar-bytes"
        encoded = base64.b64encode(file_content).decode("utf-8")
        mock_client = MagicMock()
        mock_client.get.return_value = {
            "data": encoded,
            "bytes_read": len(file_content),
        }

        exporter = self._make_exporter(mock_client)
        local_path = os.path.join(self.tmpdir, "dbfs_files", "FileStore", "jars", "lib.jar")
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        exporter._download_file("dbfs:/FileStore/jars/lib.jar", local_path)

        with open(local_path, "rb") as fp:
            result = fp.read()
        self.assertEqual(result, file_content)

    def test_multi_chunk_download(self):
        """File spans two 1-MB chunks."""
        chunk1 = b"A" * (1024 * 1024)
        chunk2 = b"B" * 512

        responses = [
            {"data": base64.b64encode(chunk1).decode(), "bytes_read": len(chunk1)},
            {"data": base64.b64encode(chunk2).decode(), "bytes_read": len(chunk2)},
        ]
        mock_client = MagicMock()
        mock_client.get.side_effect = responses

        exporter = self._make_exporter(mock_client)
        local_path = os.path.join(self.tmpdir, "dbfs_files", "big.jar")
        exporter._download_file("dbfs:/big.jar", local_path)

        with open(local_path, "rb") as fp:
            result = fp.read()
        self.assertEqual(result, chunk1 + chunk2)
        self.assertEqual(mock_client.get.call_count, 2)

    def test_dbfs_path_to_local(self):
        mock_client = MagicMock()
        exporter = self._make_exporter(mock_client)
        local = exporter._dbfs_path_to_local("dbfs:/FileStore/jars/lib.jar")
        expected = os.path.join(self.tmpdir, "dbfs_files", "FileStore", "jars", "lib.jar")
        self.assertEqual(local, expected)

    def test_download_updates_local_file_field(self):
        file_content = b"tiny"
        encoded = base64.b64encode(file_content).decode()
        mock_client = MagicMock()
        mock_client.get.return_value = {"data": encoded, "bytes_read": len(file_content)}

        exporter = self._make_exporter(mock_client)

        manifest = _make_manifest()
        jar = LibraryEntry(
            lib_type="jar",
            dbfs_path="dbfs:/FileStore/jars/lib.jar",
            raw={"jar": "dbfs:/FileStore/jars/lib.jar"},
        )
        manifest.libraries.append(jar)

        os.makedirs(
            os.path.join(self.tmpdir, "dbfs_files", "FileStore", "jars"), exist_ok=True
        )
        exporter._download_dbfs_files(manifest)

        self.assertIsNotNone(jar.local_file)
        self.assertIn("FileStore", jar.local_file)


# ---------------------------------------------------------------------------
# 7. LibraryImporter – DBFS upload
# ---------------------------------------------------------------------------

class TestImporterDbfsUpload(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _make_importer(self, mock_client: MagicMock) -> LibraryImporter:
        importer = LibraryImporter.__new__(LibraryImporter)
        importer._export_dir = self.tmpdir + "/"
        importer._logger = MagicMock()
        importer._client = mock_client
        return importer

    def test_upload_file_calls_api_sequence(self):
        """create → add-block → close must be called in order."""
        mock_client = MagicMock()
        mock_client.post.side_effect = [
            {"handle": 99},  # /dbfs/create
            {},              # /dbfs/add-block
            {},              # /dbfs/close
        ]

        # Create a small fake local file
        local_path = os.path.join(self.tmpdir, "lib.jar")
        with open(local_path, "wb") as fp:
            fp.write(b"fake-jar")

        importer = self._make_importer(mock_client)
        importer._upload_file(local_path, "dbfs:/FileStore/jars/lib.jar")

        calls = mock_client.post.call_args_list
        self.assertEqual(calls[0], call("/dbfs/create", {"path": "dbfs:/FileStore/jars/lib.jar", "overwrite": True}))
        self.assertEqual(calls[1][0][0], "/dbfs/add-block")
        self.assertEqual(calls[2], call("/dbfs/close", {"handle": 99}))

    def test_upload_large_file_multiple_blocks(self):
        """A 2.5 MB file should produce 3 add-block calls."""
        mock_client = MagicMock()
        # create → add-block x3 → close
        mock_client.post.side_effect = [{"handle": 7}] + [{}] * 4

        local_path = os.path.join(self.tmpdir, "big.jar")
        with open(local_path, "wb") as fp:
            fp.write(b"X" * (2 * 1024 * 1024 + 500 * 1024))  # ~2.5 MB

        importer = self._make_importer(mock_client)
        importer._upload_file(local_path, "dbfs:/big.jar")

        add_block_calls = [
            c for c in mock_client.post.call_args_list if c[0][0] == "/dbfs/add-block"
        ]
        self.assertEqual(len(add_block_calls), 3)


# ---------------------------------------------------------------------------
# 8. LibraryImporter – full import_all flow
# ---------------------------------------------------------------------------

class TestImporterImportAll(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self._write_manifest()
        self._write_fake_jar()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _write_manifest(self):
        jar_entry = LibraryEntry(
            lib_type="jar",
            dbfs_path="dbfs:/FileStore/jars/lib.jar",
            local_file="dbfs_files/FileStore/jars/lib.jar",
            raw={"jar": "dbfs:/FileStore/jars/lib.jar"},
            used_by=[
                LibraryUsage(
                    entity_type="cluster",
                    entity_id="old_c001",
                    entity_name="prod-cluster",
                )
            ],
        )
        pypi_entry = LibraryEntry(
            lib_type="pypi",
            pypi_package="requests>=2.28",
            raw={"pypi": {"package": "requests>=2.28"}},
            used_by=[],
        )
        manifest = LibraryManifest(
            source_workspace_url="https://src.azuredatabricks.net",
            libraries=[jar_entry, pypi_entry],
        )
        with open(os.path.join(self.tmpdir, "library_manifest.json"), "w") as fp:
            manifest.to_json(fp)

    def _write_fake_jar(self):
        jar_dir = os.path.join(self.tmpdir, "dbfs_files", "FileStore", "jars")
        os.makedirs(jar_dir, exist_ok=True)
        with open(os.path.join(jar_dir, "lib.jar"), "wb") as fp:
            fp.write(b"fake-jar")

    def _make_importer(self, mock_client: MagicMock) -> LibraryImporter:
        importer = LibraryImporter.__new__(LibraryImporter)
        importer._export_dir = self.tmpdir + "/"
        importer._logger = MagicMock()
        importer._client = mock_client
        return importer

    def test_successful_import(self):
        mock_client = MagicMock()
        # DBFS upload: create → add-block → close
        mock_client.post.side_effect = [{"handle": 1}, {}, {}, {}]
        # cluster list for install
        mock_client.get.return_value = {
            "clusters": [{"cluster_id": "new_c001", "cluster_name": "prod-cluster"}]
        }

        importer = self._make_importer(mock_client)
        report = importer.import_all()

        self.assertEqual(len(report.uploaded_files), 1)
        self.assertIn("dbfs:/FileStore/jars/lib.jar", report.uploaded_files)
        self.assertEqual(len(report.installed_on_clusters), 1)
        self.assertEqual(len(report.failed_uploads), 0)
        self.assertEqual(len(report.failed_installs), 0)
        # pypi should appear in review list
        self.assertEqual(len(report.coordinate_libs_to_review), 1)

    def test_cluster_not_found_skips_install(self):
        mock_client = MagicMock()
        mock_client.post.side_effect = [{"handle": 1}, {}, {}]
        # Destination has no matching cluster
        mock_client.get.return_value = {"clusters": []}

        importer = self._make_importer(mock_client)
        report = importer.import_all()

        self.assertEqual(len(report.skipped_clusters), 1)
        self.assertIn("prod-cluster", report.skipped_clusters)

    def test_missing_local_file_skips_upload(self):
        """If the jar binary is missing, upload should be skipped gracefully."""
        # Remove the fake jar
        os.remove(
            os.path.join(self.tmpdir, "dbfs_files", "FileStore", "jars", "lib.jar")
        )
        mock_client = MagicMock()
        mock_client.get.return_value = {"clusters": []}

        importer = self._make_importer(mock_client)
        report = importer.import_all()

        self.assertEqual(len(report.skipped_files), 1)
        self.assertEqual(len(report.uploaded_files), 0)
        # post should never be called for upload
        upload_calls = [
            c for c in mock_client.post.call_args_list if "/dbfs/" in str(c)
        ]
        self.assertEqual(len(upload_calls), 0)

    def test_manifest_not_found_raises(self):
        mock_client = MagicMock()
        importer = LibraryImporter.__new__(LibraryImporter)
        importer._export_dir = "/nonexistent/path/"
        importer._logger = MagicMock()
        importer._client = mock_client

        with self.assertRaises(FileNotFoundError):
            importer.import_all()


# ---------------------------------------------------------------------------
# 9. ImportReport __str__
# ---------------------------------------------------------------------------

class TestImportReport(unittest.TestCase):

    def test_str_contains_summary(self):
        r = ImportReport(
            uploaded_files=["dbfs:/a.jar"],
            failed_uploads=["dbfs:/b.jar: timeout"],
            installed_on_clusters=["prod (c1) – 2 lib(s)"],
            coordinate_libs_to_review=["[PyPI] requests>=2.28"],
        )
        s = str(r)
        self.assertIn("1 succeeded", s)
        self.assertIn("1 failed", s)
        self.assertIn("Coordinate-based libs", s)
        self.assertIn("requests>=2.28", s)


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main(verbosity=2)
