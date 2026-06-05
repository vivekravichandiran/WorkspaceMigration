"""
Tests for the new workspace-export components:
  • BaseExporter HTTP helpers
  • SimpleExporter (warehouses, DLT, repos, dashboards, genie, serving)
  • UnityCatalogExporter (DDL builders, topo-sort, grants, full flow)
"""
from __future__ import annotations

import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch, call

# ---------------------------------------------------------------------------
# Path setup — allow running without installing the packages
# ---------------------------------------------------------------------------
import sys
_HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(_HERE, ".."))

from workspace_export.exporters.base import BaseExporter, _strip, _WAREHOUSE_RUNTIME_KEYS
from workspace_export.exporters.simple import SimpleExporter
from workspace_export.exporters.unity_catalog import (
    UnityCatalogExporter,
    _catalog_ddl,
    _schema_ddl,
    _table_ddl,
    _view_ddl,
    _volume_ddl,
    _function_ddl,
    _external_location_ddl,
    _grants_to_sql,
    _topo_sort_views,
    _view_deps,
    _sql_escape,
    _fqn,
)


def _make_exporter(cls, tmp_dir):
    """Instantiate an exporter backed by a real temp directory."""
    exp = cls.__new__(cls)
    BaseExporter.__init__(exp, "https://test.azuredatabricks.net", "token123", tmp_dir)
    if cls is UnityCatalogExporter:
        exp._warehouse_id = None
        exp._include_catalogs = None
        exp._include_schemas = None
        exp._owned_only = False
        exp._all_view_fqns = set()
        exp._skipped = []
        exp._manifest_steps = []
        exp._step = 0
        exp._current_user = None
    return exp


# ===========================================================================
# BaseExporter
# ===========================================================================

class TestBaseExporter(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def test_strip_removes_runtime_keys(self):
        obj = {"id": "abc", "name": "My WH", "cluster_size": "X-Small", "state": "RUNNING"}
        result = _strip(obj, _WAREHOUSE_RUNTIME_KEYS)
        self.assertNotIn("id", result)
        self.assertNotIn("state", result)
        self.assertIn("name", result)
        self.assertIn("cluster_size", result)

    def test_save_json_creates_file(self):
        exp = _make_exporter(BaseExporter, self.tmp)
        exp._save_json("sub/test.json", {"key": "value"})
        path = os.path.join(self.tmp, "sub", "test.json")
        self.assertTrue(os.path.exists(path))
        with open(path) as f:
            data = json.load(f)
        self.assertEqual(data["key"], "value")

    def test_save_text_creates_file(self):
        exp = _make_exporter(BaseExporter, self.tmp)
        exp._save_text("uc_export/test.sql", "SELECT 1;")
        path = os.path.join(self.tmp, "uc_export", "test.sql")
        self.assertTrue(os.path.exists(path))
        with open(path) as f:
            self.assertEqual(f.read(), "SELECT 1;")

    def test_paginated_get_single_page(self):
        exp = _make_exporter(BaseExporter, self.tmp)
        exp._get = MagicMock(return_value={"items": [{"a": 1}, {"a": 2}]})
        result = exp._paginated_get("/api/test", "items")
        self.assertEqual(len(result), 2)

    def test_paginated_get_multiple_pages(self):
        exp = _make_exporter(BaseExporter, self.tmp)
        exp._get = MagicMock(side_effect=[
            {"items": [{"a": 1}], "next_page_token": "tok1"},
            {"items": [{"a": 2}], "next_page_token": "tok2"},
            {"items": [{"a": 3}]},
        ])
        result = exp._paginated_get("/api/test", "items")
        self.assertEqual(len(result), 3)
        self.assertEqual(exp._get.call_count, 3)

    def test_paginated_get_empty(self):
        exp = _make_exporter(BaseExporter, self.tmp)
        exp._get = MagicMock(return_value={})
        result = exp._paginated_get("/api/test", "items")
        self.assertEqual(result, [])


# ===========================================================================
# SimpleExporter
# ===========================================================================

class TestSimpleExporterWarehouses(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.exp = _make_exporter(SimpleExporter, self.tmp)
        self.exp._get = MagicMock()

    def test_export_warehouses_saves_json(self):
        self.exp._get.return_value = {
            "warehouses": [
                {"id": "abc123", "name": "Dev WH", "cluster_size": "X-Small", "state": "RUNNING"},
                {"id": "def456", "name": "Prod WH", "cluster_size": "Large", "state": "STOPPED"},
            ]
        }
        result = self.exp.export_sql_warehouses()
        self.assertEqual(result["count"], 2)
        path = os.path.join(self.tmp, "sql_warehouses.json")
        self.assertTrue(os.path.exists(path))
        data = json.load(open(path))
        wh = data["warehouses"][0]
        self.assertNotIn("id", wh)
        self.assertNotIn("state", wh)
        self.assertEqual(wh["name"], "Dev WH")

    def test_export_warehouses_empty(self):
        self.exp._get.return_value = {}
        result = self.exp.export_sql_warehouses()
        self.assertEqual(result["count"], 0)


class TestSimpleExporterPipelines(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.exp = _make_exporter(SimpleExporter, self.tmp)

    def test_export_dlt_pipelines(self):
        self.exp._paginated_get = MagicMock(return_value=[
            {"pipeline_id": "p1", "name": "My Pipeline"},
        ])
        self.exp._get = MagicMock(return_value={
            "spec": {
                "name": "My Pipeline",
                "libraries": [{"notebook": {"path": "/nb"}}],
                "pipeline_id": "p1",
                "state": "RUNNING",
            }
        })
        result = self.exp.export_dlt_pipelines()
        self.assertEqual(result["count"], 1)
        path = os.path.join(self.tmp, "dlt_pipelines.json")
        data = json.load(open(path))
        pl = data["pipelines"][0]
        self.assertNotIn("pipeline_id", pl)
        self.assertEqual(pl["name"], "My Pipeline")


class TestSimpleExporterRepos(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.exp = _make_exporter(SimpleExporter, self.tmp)

    def test_export_repos(self):
        self.exp._paginated_get = MagicMock(return_value=[
            {"id": 1, "url": "https://github.com/org/repo.git", "provider": "gitHub", "path": "/Repos/user/repo"},
        ])
        result = self.exp.export_repos()
        self.assertEqual(result["count"], 1)
        data = json.load(open(os.path.join(self.tmp, "repos.json")))
        repo = data["repos"][0]
        self.assertNotIn("id", repo)
        self.assertEqual(repo["url"], "https://github.com/org/repo.git")


class TestSimpleExporterDashboards(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.exp = _make_exporter(SimpleExporter, self.tmp)

    def test_export_lakeview_dashboards_empty(self):
        self.exp._get = MagicMock(return_value={})
        result = self.exp.export_lakeview_dashboards()
        self.assertEqual(result["count"], 0)

    def test_export_lakeview_dashboards_with_content(self):
        self.exp._get = MagicMock(side_effect=[
            {"dashboards": [{"dashboard_id": "dash1"}]},
            {
                "dashboard_id": "dash1",
                "display_name": "Sales Dashboard",
                "serialized_dashboard": '{"widgets":[]}',
                "warehouse_id": "wh1",
                "parent_path": "/Shared",
            },
        ])
        result = self.exp.export_lakeview_dashboards()
        self.assertEqual(result["count"], 1)
        index = json.load(open(os.path.join(self.tmp, "lakeview_dashboards_index.json")))
        self.assertEqual(index["count"], 1)


class TestSimpleExporterGenie(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.exp = _make_exporter(SimpleExporter, self.tmp)

    def test_export_genie_spaces(self):
        self.exp._get = MagicMock(return_value={
            "spaces": [
                {
                    "space_id": "sp1",
                    "title": "Finance Genie",
                    "description": "Desc",
                    "warehouse_id": "wh1",
                    "create_time": 1234,
                }
            ]
        })
        result = self.exp.export_genie_spaces()
        self.assertEqual(result["count"], 1)
        data = json.load(open(os.path.join(self.tmp, "genie_spaces.json")))
        sp = data["spaces"][0]
        self.assertNotIn("space_id", sp)
        self.assertNotIn("create_time", sp)
        self.assertEqual(sp["title"], "Finance Genie")


class TestSimpleExporterServingEndpoints(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.exp = _make_exporter(SimpleExporter, self.tmp)

    def test_platform_endpoints_skipped(self):
        self.exp._get = MagicMock(return_value={
            "endpoints": [
                {"name": "databricks-llama-3", "id": "e1"},
                {"name": "my-custom-endpoint", "id": "e2", "config": {}},
            ]
        })
        result = self.exp.export_serving_endpoints()
        self.assertEqual(result["count"], 1)
        self.assertEqual(result["platform_skipped"], 1)
        data = json.load(open(os.path.join(self.tmp, "serving_endpoints.json")))
        self.assertEqual(data["endpoints"][0]["name"], "my-custom-endpoint")


# ===========================================================================
# DDL builders
# ===========================================================================

class TestDDLBuilders(unittest.TestCase):

    def test_catalog_ddl_no_comment(self):
        sql = _catalog_ddl({"name": "my_cat"})
        self.assertIn("CREATE CATALOG IF NOT EXISTS", sql)
        self.assertIn("`my_cat`", sql)

    def test_catalog_ddl_with_comment(self):
        sql = _catalog_ddl({"name": "my_cat", "comment": "My catalog"})
        self.assertIn("COMMENT", sql)
        self.assertIn("My catalog", sql)

    def test_schema_ddl(self):
        sql = _schema_ddl({"name": "my_schema", "catalog_name": "my_cat"})
        self.assertIn("CREATE SCHEMA IF NOT EXISTS", sql)
        self.assertIn("`my_cat`.`my_schema`", sql)

    def test_table_ddl_managed(self):
        info = {
            "name": "orders", "catalog_name": "cat", "schema_name": "sch",
            "table_type": "MANAGED",
            "data_source_format": "DELTA",
            "columns": [
                {"name": "id", "type_text": "BIGINT", "nullable": False},
                {"name": "amount", "type_text": "DOUBLE", "comment": "Dollar amount"},
            ],
        }
        sql = _table_ddl(info)
        self.assertIn("CREATE TABLE IF NOT EXISTS", sql)
        self.assertIn("`cat`.`sch`.`orders`", sql)
        self.assertIn("`id` BIGINT NOT NULL", sql)
        self.assertIn("`amount` DOUBLE", sql)
        self.assertIn("Dollar amount", sql)
        self.assertIn("USING DELTA", sql)
        self.assertNotIn("LOCATION", sql)

    def test_table_ddl_external(self):
        info = {
            "name": "ext_t", "catalog_name": "c", "schema_name": "s",
            "table_type": "EXTERNAL",
            "data_source_format": "PARQUET",
            "storage_location": "s3://bucket/path",
            "columns": [],
        }
        sql = _table_ddl(info)
        self.assertIn("LOCATION 's3://bucket/path'", sql)

    def test_view_ddl(self):
        info = {
            "name": "v1", "catalog_name": "c", "schema_name": "s",
            "table_type": "VIEW",
            "view_definition": "SELECT id FROM c.s.orders",
        }
        sql = _view_ddl(info)
        self.assertIn("CREATE OR REPLACE VIEW", sql)
        self.assertIn("`c`.`s`.`v1`", sql)
        self.assertIn("SELECT id FROM c.s.orders", sql)

    def test_view_ddl_missing_definition(self):
        info = {
            "name": "v2", "catalog_name": "c", "schema_name": "s",
            "table_type": "VIEW",
        }
        sql = _view_ddl(info)
        self.assertIn("view definition unavailable", sql)

    def test_volume_ddl_managed(self):
        info = {"name": "vol1", "catalog_name": "c", "schema_name": "s", "volume_type": "MANAGED"}
        sql = _volume_ddl(info)
        self.assertIn("CREATE VOLUME IF NOT EXISTS", sql)
        self.assertNotIn("LOCATION", sql)

    def test_volume_ddl_external(self):
        info = {
            "name": "vol2", "catalog_name": "c", "schema_name": "s",
            "volume_type": "EXTERNAL", "storage_location": "abfss://cont@acct.dfs.core.windows.net/v",
        }
        sql = _volume_ddl(info)
        self.assertIn("CREATE EXTERNAL VOLUME IF NOT EXISTS", sql)
        self.assertIn("LOCATION", sql)

    def test_sql_escape(self):
        self.assertEqual(_sql_escape("it's"), "it''s")
        self.assertEqual(_sql_escape("normal"), "normal")

    def test_fqn(self):
        self.assertEqual(_fqn("cat", "sch", "tbl"), "`cat`.`sch`.`tbl`")


# ===========================================================================
# Grants
# ===========================================================================

class TestGrantsToSQL(unittest.TestCase):

    def test_table_grants(self):
        stmts = _grants_to_sql(
            "table",
            "cat.sch.t1",
            [{"principal": "user@example.com", "privileges": ["SELECT", "MODIFY"]}],
        )
        self.assertEqual(len(stmts), 1)
        self.assertIn("GRANT SELECT, MODIFY ON TABLE", stmts[0])
        self.assertIn("`cat`.`sch`.`t1`", stmts[0])
        self.assertIn("`user@example.com`", stmts[0])

    def test_catalog_grant(self):
        stmts = _grants_to_sql(
            "catalog", "my_cat",
            [{"principal": "grp@domain.com", "privileges": ["USE CATALOG"]}],
        )
        self.assertIn("ON CATALOG", stmts[0])

    def test_empty_privileges_skipped(self):
        stmts = _grants_to_sql(
            "table", "c.s.t",
            [{"principal": "user@x.com", "privileges": []}],
        )
        self.assertEqual(stmts, [])


# ===========================================================================
# Topological sort
# ===========================================================================

class TestTopoSort(unittest.TestCase):

    def _make_view(self, cat, sch, name, view_def=""):
        fqn = f"{cat}.{sch}.{name}".lower()
        return {
            "name": name, "catalog_name": cat, "schema_name": sch,
            "fqn": fqn, "view_definition": view_def, "table_type": "VIEW",
        }

    def test_no_deps_order_preserved(self):
        views = [
            self._make_view("c", "s", "v1"),
            self._make_view("c", "s", "v2"),
        ]
        fqns = {"c.s.v1", "c.s.v2"}
        result = _topo_sort_views(views, fqns)
        self.assertEqual(len(result), 2)

    def test_dep_ordering(self):
        v1 = self._make_view("c", "s", "base", "SELECT 1")
        v2 = self._make_view("c", "s", "derived", "SELECT * FROM `c`.`s`.`base`")
        fqns = {"c.s.base", "c.s.derived"}
        result = _topo_sort_views([v2, v1], fqns)  # pass derived first
        names = [r["name"] for r in result]
        self.assertLess(names.index("base"), names.index("derived"))

    def test_view_deps_extraction(self):
        view_def = "SELECT a.id, b.val FROM `cat`.`sch`.`t1` a JOIN `cat`.`sch`.`v2` b ON a.id=b.id"
        all_views = {"cat.sch.v2", "cat.sch.t1"}
        deps = _view_deps(view_def, all_views)
        self.assertIn("cat.sch.v2", deps)

    def test_circular_deps_handled(self):
        v1 = self._make_view("c", "s", "v1", "SELECT * FROM `c`.`s`.`v2`")
        v2 = self._make_view("c", "s", "v2", "SELECT * FROM `c`.`s`.`v1`")
        fqns = {"c.s.v1", "c.s.v2"}
        result = _topo_sort_views([v1, v2], fqns)
        self.assertEqual(len(result), 2)  # both included, order best-effort


# ===========================================================================
# UnityCatalogExporter full-flow mock
# ===========================================================================

class TestUnityCatalogExporterFlow(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def _make_uc(self):
        exp = UnityCatalogExporter.__new__(UnityCatalogExporter)
        BaseExporter.__init__(exp, "https://ws.azuredatabricks.net", "tok", self.tmp)
        exp._warehouse_id = None
        exp._include_catalogs = None
        exp._include_schemas = None
        exp._owned_only = False      # don't hit SCIM API in tests
        exp._all_view_fqns = set()
        exp._skipped = []
        exp._manifest_steps = []
        exp._step = 0
        exp._current_user = None
        return exp

    def test_skip_mv_and_streaming(self):
        exp = self._make_uc()
        stats = {}
        summaries = [
            {"name": "t1", "table_type": "MANAGED"},
            {"name": "mv1", "table_type": "MATERIALIZED_VIEW"},
            {"name": "st1", "table_type": "STREAMING_TABLE"},
        ]
        exp._get = MagicMock(return_value={
            "name": "t1", "catalog_name": "c", "schema_name": "s",
            "table_type": "MANAGED", "columns": [], "data_source_format": "DELTA",
        })
        exp._paginated_get = MagicMock(return_value=summaries)
        result = exp._list_tables("c", "s", stats)
        self.assertEqual(len(result), 1)  # only MANAGED
        self.assertEqual(stats.get("skipped_materialized_view", 0), 1)
        self.assertEqual(stats.get("skipped_streaming_table", 0), 1)
        self.assertEqual(len(exp._skipped), 2)

    def test_full_export_creates_manifest(self):
        exp = self._make_uc()

        def fake_get(path, params=None):
            if "storage-credentials" in path:
                return {"storage_credentials": []}
            if "external-locations" in path:
                return {"external_locations": []}
            if "tables/" in path:
                return {
                    "name": "orders", "catalog_name": "sales", "schema_name": "public",
                    "table_type": "MANAGED", "data_source_format": "DELTA",
                    "columns": [{"name": "id", "type_text": "BIGINT", "nullable": True}],
                }
            if "permissions" in path:
                return {"privilege_assignments": []}
            if "volumes" in path:
                return {"volumes": []}
            if "functions" in path:
                return {"functions": []}
            return {}

        def fake_paged(path, key, params=None, token_key="next_page_token"):
            if "catalogs" in path:
                return [{"name": "sales", "catalog_name": "sales", "catalog_type": "MANAGED_CATALOG"}]
            if "schemas" in path:
                return [{"name": "public", "catalog_name": "sales"}]
            if "tables" in path:
                return [{"name": "orders", "table_type": "MANAGED"}]
            return []

        exp._get = MagicMock(side_effect=fake_get)
        exp._paginated_get = MagicMock(side_effect=fake_paged)

        stats = exp.export()

        self.assertEqual(stats.get("catalogs"), 1)
        self.assertEqual(stats.get("schemas"), 1)
        self.assertEqual(stats.get("tables"), 1)
        manifest_path = os.path.join(self.tmp, "uc_export", "import_manifest.json")
        self.assertTrue(os.path.exists(manifest_path))
        manifest = json.load(open(manifest_path))
        self.assertIn("deployment_order", manifest)
        self.assertIn("summary", manifest)

    def test_catalog_sql_file_created(self):
        exp = self._make_uc()

        def fake_get(path, params=None):
            if "storage-credentials" in path:
                return {"storage_credentials": []}
            if "external-locations" in path:
                return {"external_locations": []}
            if "permissions" in path:
                return {"privilege_assignments": []}
            return {}

        exp._get = MagicMock(side_effect=fake_get)
        exp._paginated_get = MagicMock(side_effect=lambda p, k, **kw: (
            [{"name": "my_cat", "catalog_type": "MANAGED_CATALOG"}] if "catalogs" in p else []
        ))

        exp.export()

        cat_sql = os.path.join(self.tmp, "uc_export", "03_catalogs.sql")
        self.assertTrue(os.path.exists(cat_sql))
        content = open(cat_sql).read()
        self.assertIn("CREATE CATALOG IF NOT EXISTS", content)
        self.assertIn("`my_cat`", content)


if __name__ == "__main__":
    unittest.main(verbosity=2)
