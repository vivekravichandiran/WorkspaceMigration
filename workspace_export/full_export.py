"""
workspace_export/full_export.py
================================
Python orchestrator that exports every Databricks workspace component and
records per-component status in ``export_status.json``.

Can be invoked directly:

    python3 -m workspace_export.full_export \\
        --workspace-url https://my-ws.azuredatabricks.net \\
        --token dapi... \\
        --session M202404281200

Or used as a library:

    from workspace_export.full_export import WorkspaceExporter
    exporter = WorkspaceExporter(workspace_url, token, export_dir, session)
    exporter.run()
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from typing import Callable, List, Optional

from .status_tracker import ExportStatusTracker
from .report_generator import (
    generate_report,
    count_log_lines,
    count_dir_entries,
    dir_size_bytes,
)

_LOG = logging.getLogger(__name__)

# Inject stubs directory so optional offline dependencies (sqlparse, mlflow, etc.)
# resolve even when they are not installed via pip.
_STUBS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "stubs")
if os.path.isdir(_STUBS_DIR) and _STUBS_DIR not in sys.path:
    sys.path.insert(0, _STUBS_DIR)

# ---------------------------------------------------------------------------
# WorkspaceExporter
# ---------------------------------------------------------------------------

class WorkspaceExporter:
    """
    Exports all Databricks workspace components sequentially, recording
    per-component status for audit and report generation.

    Parameters
    ----------
    workspace_url : str
        Full HTTPS URL of the source Databricks workspace.
    token : str
        Personal Access Token for the source workspace.
    base_export_dir : str
        Root export directory (e.g. ``logs/``).
    session : str
        Session identifier; data is written to ``{base_export_dir}/{session}/``.
    cloud : str
        ``"azure"`` | ``"gcp"``
    num_parallel : int
        Thread parallelism for workspace/ACL downloads.
    notebook_format : str
        ``"DBC"`` | ``"SOURCE"`` | ``"HTML"``
    skip_components : list[str]
        List of component machine keys to skip (see ``ALL_COMPONENTS`` in status_tracker).
    skip_failed : bool
        Pass ``--skip-failed`` to the metastore exporter.
    include_mlflow : bool
        Export MLflow experiments and runs.  Defaults to ``False`` because
        run history can be very large and MLflow Tracking is typically migrated
        separately.  Pass ``True`` (or ``--include-mlflow`` on the CLI) to opt
        in.
    verify_ssl : bool
        Set to False to disable SSL certificate verification.
    retry_total : int
        Total HTTP retries per request.
    retry_backoff : float
        Backoff factor between retries.
    debug : bool
        Enable debug-level logging.
    """

    def __init__(
        self,
        workspace_url: str,
        token: str,
        base_export_dir: str = "logs",
        session: Optional[str] = None,
        cloud: str = "azure",
        num_parallel: int = 4,
        notebook_format: str = "DBC",
        skip_components: Optional[List[str]] = None,
        skip_failed: bool = False,
        include_mlflow: bool = False,
        verify_ssl: bool = True,
        retry_total: int = 10,
        retry_backoff: float = 1.0,
        debug: bool = False,
    ):
        self._workspace_url = workspace_url.rstrip("/")
        self._token = token
        self._session = session or ("M" + datetime.now().strftime("%Y%m%d%H%M"))
        self._cloud = cloud
        self._num_parallel = num_parallel
        self._notebook_format = notebook_format
        self._skip_components = set(skip_components or [])
        # MLflow experiments/runs are skipped by default – they can be very
        # large and are usually migrated via MLflow's own tooling.
        if not include_mlflow:
            self._skip_components.update({"mlflow_experiments", "mlflow_runs"})
        self._skip_failed = skip_failed
        self._verify_ssl = verify_ssl
        self._retry_total = retry_total
        self._retry_backoff = retry_backoff
        self._debug = debug

        self._export_dir = os.path.join(base_export_dir, self._session) + "/"
        os.makedirs(self._export_dir, exist_ok=True)

        # Status tracking
        status_file = os.path.join(self._export_dir, "export_status.json")
        self._tracker = ExportStatusTracker(status_file)
        self._tracker.set_metadata(self._workspace_url, self._session, self._export_dir)

        # Write source_info.txt so migration_pipeline.py --import-pipeline can read it
        with open(os.path.join(self._export_dir, "source_info.txt"), "w") as _f:
            _f.write(self._workspace_url)

        # Build client_config (compatible with databrickslabs/migrate dbclient classes)
        self._client_config = self._build_client_config()

    # ------------------------------------------------------------------
    # Public run method
    # ------------------------------------------------------------------

    def run(self) -> ExportStatusTracker:
        """Export all components and return the populated tracker."""
        _LOG.info(f"Starting workspace export for {self._workspace_url}")
        _LOG.info(f"Session: {self._session}  |  Export dir: {self._export_dir}")

        try:
            self._export_instance_profiles()
            self._export_users()
            self._export_groups()
            self._export_workspace_items()
            self._export_workspace_acls()
            self._export_notebooks()
            self._export_secrets()
            self._export_clusters()
            self._export_instance_pools()
            self._export_jobs()
            self._export_metastore()
            self._export_metastore_table_acls()
            self._export_mlflow_experiments()
            self._export_mlflow_runs()
            self._export_dbfs_libraries()
            # ── New REST-API components ────────────────────────────────
            self._export_sql_warehouses()
            self._export_dlt_pipelines()
            self._export_repos()
            self._export_lakeview_dashboards()
            self._export_genie_spaces()
            self._export_serving_endpoints()
            self._export_unity_catalog()
            # ── Workspace files (non-notebook FILE objects) ────────────
            self._export_workspace_files()
        finally:
            self._tracker.mark_complete()
            self._write_report()

        return self._tracker

    # ------------------------------------------------------------------
    # Component exporters
    # ------------------------------------------------------------------

    def _export_instance_profiles(self):
        name = "instance_profiles"
        if not self._is_aws() or name in self._skip_components:
            self._tracker.skip(name, "AWS only" if not self._is_aws() else "skipped by user")
            return
        self._run(
            name,
            lambda: self._clusters_client().log_instance_profiles(),
            log_files=["instance_profiles.log"],
        )

    def _export_users(self):
        name = "users"
        if name in self._skip_components:
            self._tracker.skip(name, "skipped by user")
            return
        self._run(
            name,
            lambda: self._scim_client().log_all_users(),
            log_files=["users.log"],
        )

    def _export_groups(self):
        name = "groups"
        if name in self._skip_components:
            self._tracker.skip(name, "skipped by user")
            return
        self._run(
            name,
            lambda: self._scim_client().log_all_groups(),
            log_files=["groups"],
            count_fn=lambda: count_dir_entries(self._path("groups")),
        )

    def _export_workspace_items(self):
        name = "workspace_item_log"
        if name in self._skip_components:
            self._tracker.skip(name, "skipped by user")
            return
        self._run(
            name,
            lambda: self._workspace_client().log_all_workspace_items_entry(),
            log_files=["user_workspace.log", "user_dirs.log", "libraries.log"],
        )

    def _export_workspace_acls(self):
        name = "workspace_acls"
        if name in self._skip_components:
            self._tracker.skip(name, "skipped by user")
            return
        self._run(
            name,
            lambda: self._workspace_client().log_all_workspace_acls(
                num_parallel=self._num_parallel
            ),
            log_files=["acl_notebooks.log", "acl_directories.log"],
        )

    def _export_notebooks(self):
        name = "notebooks"
        if name in self._skip_components:
            self._tracker.skip(name, "skipped by user")
            return
        self._run(
            name,
            lambda: self._workspace_client().download_notebooks(
                num_parallel=self._num_parallel
            ),
            log_files=["artifacts"],
            count_fn=lambda: count_dir_entries(self._path("artifacts")),
        )

    def _export_secrets(self):
        name = "secrets"
        if name in self._skip_components:
            self._tracker.skip(name, "skipped by user")
            return

        def _do():
            sc = self._secrets_client()
            # Secrets require a running cluster for the export
            # Try to find one; if none available log the scopes only
            try:
                cluster_name = self._get_any_running_cluster_name()
                if cluster_name:
                    sc.log_all_secrets(cluster_name)
                else:
                    _LOG.warning("No running cluster found – exporting secret scope ACLs only.")
            except Exception as e:
                _LOG.warning(f"Secret value export skipped: {e}")
            sc.log_all_secrets_acls()

        self._run(
            name,
            _do,
            log_files=["secret_scopes_acls.log"],
            count_fn=lambda: count_dir_entries(self._path("secret_scopes")),
        )

    def _export_clusters(self):
        name = "clusters"
        if name in self._skip_components:
            self._tracker.skip(name, "skipped by user")
            return
        self._run(
            name,
            lambda: self._clusters_client().log_cluster_configs(),
            log_files=["clusters.log", "cluster_policies.log"],
        )

    def _export_instance_pools(self):
        name = "instance_pools"
        if name in self._skip_components:
            self._tracker.skip(name, "skipped by user")
            return
        self._run(
            name,
            lambda: self._clusters_client().log_instance_pools(),
            log_files=["instance_pools.log"],
        )

    def _export_jobs(self):
        name = "jobs"
        if name in self._skip_components:
            self._tracker.skip(name, "skipped by user")
            return
        self._run(
            name,
            lambda: self._jobs_client().log_job_configs(),
            log_files=["jobs.log", "acl_jobs.log"],
        )

    def _export_metastore(self):
        name = "metastore"
        if name in self._skip_components:
            self._tracker.skip(name, "skipped by user")
            return
        self._run(
            name,
            lambda: self._hive_client().export_hive_metastore(),
            log_files=["metastore", "success_metastore.log", "database_details.log"],
            count_fn=lambda: count_log_lines(self._path("success_metastore.log")),
        )

    def _export_metastore_table_acls(self):
        name = "metastore_table_acls"
        if name in self._skip_components:
            self._tracker.skip(name, "skipped by user")
            return
        self._run(
            name,
            lambda: self._table_acls_client().export_table_acls(db_name=""),
            log_files=["table_acls"],
            count_fn=lambda: count_dir_entries(self._path("table_acls")),
        )

    def _export_mlflow_experiments(self):
        name = "mlflow_experiments"
        if name in self._skip_components:
            self._tracker.skip(
                name,
                "skipped by user" if name in set(self._skip_components) - {"mlflow_experiments", "mlflow_runs"}
                else "skipped by default – use --include-mlflow to enable",
            )
            return
        self._run(
            name,
            lambda: self._mlflow_client().export_mlflow_experiments(),
            log_files=["mlflow_experiments.log"],
        )

    def _export_mlflow_runs(self):
        name = "mlflow_runs"
        if name in self._skip_components:
            self._tracker.skip(
                name,
                "skipped by default – use --include-mlflow to enable",
            )
            return
        self._run(
            name,
            lambda: self._mlflow_client().export_mlflow_runs(
                start_date=None, num_parallel=self._num_parallel
            ),
            log_files=["mlflow_runs.log"],
        )

    def _export_dbfs_libraries(self):
        name = "dbfs_libraries"
        if name in self._skip_components:
            self._tracker.skip(name, "skipped by user")
            return

        # Import here so the rest of the tool works even without dbfs_libs in path
        try:
            from dbfs_libs.exporter import LibraryExporter
        except ImportError:
            self._tracker.skip(name, "dbfs_libs module not found in path")
            return

        def _do():
            exporter = LibraryExporter(self._client_config)
            manifest = exporter.export()
            # Return bytes downloaded for status recording
            return sum(
                os.path.getsize(os.path.join(self._export_dir, e.local_file))
                for e in manifest.libraries
                if e.local_file
                and os.path.exists(os.path.join(self._export_dir, e.local_file))
            ), len(manifest.libraries)

        self._tracker.start(name)
        try:
            bytes_dl, lib_count = _do()
            self._tracker.success(
                name,
                log_files=["library_manifest.json", "dbfs_files"],
                items_exported=lib_count,
                bytes_downloaded=bytes_dl,
            )
        except Exception as exc:
            _LOG.error(f"{name} failed: {exc}", exc_info=self._debug)
            self._tracker.fail(name, str(exc))

    # ------------------------------------------------------------------
    # New REST-API component exporters
    # ------------------------------------------------------------------

    def _new_exporter(self):
        """Build a SimpleExporter wired to this session's export dir."""
        from workspace_export.exporters.simple import SimpleExporter
        return SimpleExporter(
            workspace_url=self._workspace_url,
            token=self._token,
            export_dir=self._export_dir,
            verify_ssl=self._verify_ssl,
        )

    def _export_sql_warehouses(self):
        name = "sql_warehouses"
        if name in self._skip_components:
            self._tracker.skip(name, "skipped by user"); return
        self._run(
            name,
            lambda: self._new_exporter().export_sql_warehouses(),
            log_files=["sql_warehouses.json"],
            count_fn=lambda: len(
                __import__("json").load(
                    open(os.path.join(self._export_dir, "sql_warehouses.json"))
                ).get("warehouses", [])
            ),
        )

    def _export_dlt_pipelines(self):
        name = "dlt_pipelines"
        if name in self._skip_components:
            self._tracker.skip(name, "skipped by user"); return
        self._run(
            name,
            lambda: self._new_exporter().export_dlt_pipelines(),
            log_files=["dlt_pipelines.json"],
            count_fn=lambda: len(
                __import__("json").load(
                    open(os.path.join(self._export_dir, "dlt_pipelines.json"))
                ).get("pipelines", [])
            ),
        )

    def _export_repos(self):
        name = "repos"
        if name in self._skip_components:
            self._tracker.skip(name, "skipped by user"); return
        self._run(
            name,
            lambda: self._new_exporter().export_repos(),
            log_files=["repos.json"],
            count_fn=lambda: len(
                __import__("json").load(
                    open(os.path.join(self._export_dir, "repos.json"))
                ).get("repos", [])
            ),
        )

    def _export_lakeview_dashboards(self):
        name = "lakeview_dashboards"
        if name in self._skip_components:
            self._tracker.skip(name, "skipped by user"); return
        self._run(
            name,
            lambda: self._new_exporter().export_lakeview_dashboards(),
            log_files=["lakeview_dashboards_index.json", "lakeview_dashboards"],
            count_fn=lambda: __import__("json").load(
                open(os.path.join(self._export_dir, "lakeview_dashboards_index.json"))
            ).get("count", 0),
        )

    def _export_genie_spaces(self):
        name = "genie_spaces"
        if name in self._skip_components:
            self._tracker.skip(name, "skipped by user"); return
        self._run(
            name,
            lambda: self._new_exporter().export_genie_spaces(),
            log_files=["genie_spaces.json"],
            count_fn=lambda: len(
                __import__("json").load(
                    open(os.path.join(self._export_dir, "genie_spaces.json"))
                ).get("spaces", [])
            ),
        )

    def _export_serving_endpoints(self):
        name = "serving_endpoints"
        if name in self._skip_components:
            self._tracker.skip(name, "skipped by user"); return
        self._run(
            name,
            lambda: self._new_exporter().export_serving_endpoints(),
            log_files=["serving_endpoints.json"],
            count_fn=lambda: len(
                __import__("json").load(
                    open(os.path.join(self._export_dir, "serving_endpoints.json"))
                ).get("endpoints", [])
            ),
        )

    def _export_unity_catalog(self):
        name = "unity_catalog"
        if name in self._skip_components:
            self._tracker.skip(name, "skipped by user"); return
        _LOG.info("[unity_catalog] Starting …")
        self._tracker.start(name)
        try:
            from workspace_export.exporters.unity_catalog import UnityCatalogExporter
            exp = UnityCatalogExporter(
                workspace_url=self._workspace_url,
                token=self._token,
                export_dir=self._export_dir,
                verify_ssl=self._verify_ssl,
            )
            stats = exp.export()
            total_objects = (
                stats.get("catalogs", 0)
                + stats.get("schemas", 0)
                + stats.get("tables", 0)
                + stats.get("views", 0)
                + stats.get("functions", 0)
                + stats.get("volumes", 0)
            )
            self._tracker.success(
                name,
                log_files=["uc_export/import_manifest.json", "uc_export/"],
                items_exported=total_objects,
                notes=(
                    f"catalogs={stats.get('catalogs',0)} "
                    f"schemas={stats.get('schemas',0)} "
                    f"tables={stats.get('tables',0)} "
                    f"views={stats.get('views',0)} "
                    f"skipped_mv={stats.get('skipped_mv',0)} "
                    f"skipped_st={stats.get('skipped_streaming',0)}"
                ),
            )
            _LOG.info("[unity_catalog] ✓ Done  %s", stats)
        except Exception as exc:
            _LOG.error("[unity_catalog] ✗ Failed: %s", exc, exc_info=self._debug)
            self._tracker.fail(name, str(exc))

    def _export_workspace_files(self):
        name = "workspace_files"
        if name in self._skip_components:
            self._tracker.skip(name, "skipped by user")
            return
        _LOG.info("[workspace_files] Starting …")
        self._tracker.start(name)
        try:
            from workspace_export.exporters.workspace_files import WorkspaceFilesExporter
            exp = WorkspaceFilesExporter(
                workspace_url=self._workspace_url,
                token=self._token,
                export_dir=self._export_dir,
                extensions=None,   # export ALL FILE-type objects
                verify_ssl=self._verify_ssl,
            )
            stats = exp.export()
            self._tracker.success(
                name,
                log_files=["workspace_files_manifest.json", "workspace_files"],
                items_exported=stats.get("exported", 0),
                notes=(
                    f"exported={stats.get('exported',0)} "
                    f"skipped={stats.get('skipped',0)} "
                    f"failed={stats.get('failed',0)}"
                ),
            )
            _LOG.info("[workspace_files] ✓ Done  %s", stats)
        except Exception as exc:
            _LOG.error("[workspace_files] ✗ Failed: %s", exc, exc_info=self._debug)
            self._tracker.fail(name, str(exc))

    # ------------------------------------------------------------------
    # Generic run wrapper
    # ------------------------------------------------------------------

    def _run(
        self,
        name: str,
        fn: Callable,
        log_files: Optional[List[str]] = None,
        count_fn: Optional[Callable] = None,
    ) -> None:
        """
        Execute ``fn``, record status.  After success, count exported items
        using ``count_fn`` if provided, else fall back to counting lines in
        the first file of ``log_files``.
        """
        _LOG.info(f"[{name}] Starting …")
        self._tracker.start(name)
        try:
            fn()

            # Count items
            items = None
            if count_fn:
                try:
                    items = count_fn()
                except Exception:
                    pass
            elif log_files:
                first = os.path.join(self._export_dir, log_files[0])
                items = count_log_lines(first)

            self._tracker.success(name, log_files=log_files or [], items_exported=items)
            _LOG.info(f"[{name}] ✓ Done  (items={items})")
        except Exception as exc:
            _LOG.error(f"[{name}] ✗ Failed: {exc}", exc_info=self._debug)
            self._tracker.fail(name, str(exc))

    # ------------------------------------------------------------------
    # Report
    # ------------------------------------------------------------------

    def _write_report(self) -> str:
        report = generate_report(self._tracker, self._export_dir)
        report_path = os.path.join(self._export_dir, "export_report.txt")
        with open(report_path, "w", encoding="utf-8") as fp:
            fp.write(report)
        print(report)
        print(f"\nReport saved to: {report_path}")
        print(f"Status JSON  at: {os.path.join(self._export_dir, 'export_status.json')}")
        return report

    # ------------------------------------------------------------------
    # Lazy client constructors
    # ------------------------------------------------------------------

    def _checkpoint_service(self):
        from checkpoint_service import CheckpointService
        return CheckpointService(self._client_config)

    def _scim_client(self):
        from dbclient import ScimClient
        return ScimClient(self._client_config, self._checkpoint_service())

    def _clusters_client(self):
        from dbclient import ClustersClient
        return ClustersClient(self._client_config, self._checkpoint_service())

    def _jobs_client(self):
        from dbclient import JobsClient
        return JobsClient(self._client_config, self._checkpoint_service())

    def _workspace_client(self):
        from dbclient import WorkspaceClient
        wc = WorkspaceClient(self._client_config, self._checkpoint_service())
        wc.init_workspace_logfiles()
        return wc

    def _hive_client(self):
        from dbclient import HiveClient
        return HiveClient(self._client_config, self._checkpoint_service())

    def _table_acls_client(self):
        from dbclient import TableACLsClient
        return TableACLsClient(self._client_config, self._checkpoint_service())

    def _secrets_client(self):
        from dbclient import SecretsClient
        return SecretsClient(self._client_config, self._checkpoint_service())

    def _mlflow_client(self):
        from dbclient import MLFlowClient
        return MLFlowClient(self._client_config, self._checkpoint_service())

    def _get_any_running_cluster_name(self) -> Optional[str]:
        try:
            from dbclient import ClustersClient
            cl = ClustersClient(self._client_config, self._checkpoint_service())
            clusters = cl.get("/clusters/list").get("clusters", [])
            running = [
                c for c in clusters if c.get("state") in ("RUNNING", "PENDING")
            ]
            return running[0]["cluster_name"] if running else None
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Client config construction
    # ------------------------------------------------------------------

    def _build_client_config(self) -> dict:
        return {
            "profile": "workspace_export_tmp",
            "url": self._workspace_url,
            "token": self._token,
            "export_dir": self._export_dir,
            "is_aws": self._cloud == "aws",
            "is_azure": self._cloud == "azure",
            "is_gcp": self._cloud == "gcp",
            "debug": self._debug,
            "verbose": self._debug,
            "verify_ssl": self._verify_ssl,
            "file_format": self._notebook_format,
            "overwrite_notebooks": False,
            "skip_failed": self._skip_failed,
            "use_checkpoint": True,
            "retry_total": self._retry_total,
            "retry_backoff": self._retry_backoff,
            "num_parallel": self._num_parallel,
            "timeout": 300,
            # Additional fields used by some clients
            "groups_to_keep": [],
            "skip_missing_users": False,
            "skip_large_nb": False,
            "hipaa": False,
        }

    def _is_aws(self) -> bool:
        return self._cloud == "aws"

    def _path(self, *parts: str) -> str:
        return os.path.join(self._export_dir, *parts)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Export all Databricks workspace components with status tracking."
    )
    p.add_argument("--workspace-url", "-u", required=True,
                   help="Databricks workspace URL (https://...)")
    p.add_argument("--token", "-t", required=True,
                   help="Personal Access Token")
    p.add_argument("--session", "-s", default=None,
                   help="Session ID (auto-generated if omitted)")
    p.add_argument("--export-dir", "-d", default="logs",
                   help="Base export directory (default: logs/)")
    p.add_argument("--azure", action="store_true", help="Azure workspace")
    p.add_argument("--gcp", action="store_true", help="GCP workspace")
    p.add_argument("--num-parallel", "-p", type=int, default=4,
                   help="Thread parallelism for notebook/ACL downloads (default: 4)")
    p.add_argument("--notebook-format", choices=["DBC", "SOURCE", "HTML"], default="DBC",
                   help="Notebook download format (default: DBC)")
    p.add_argument("--skip", nargs="+", default=[],
                   metavar="COMPONENT",
                   help="Components to skip (space-separated machine keys)")
    p.add_argument("--skip-failed", action="store_true",
                   help="Skip failed metastore exports instead of retrying")
    p.add_argument("--include-mlflow", action="store_true",
                   help="Export MLflow experiments and runs (skipped by default; can be very large)")
    p.add_argument("--no-ssl-verification", action="store_true",
                   help="Disable SSL certificate verification")
    p.add_argument("--retry-total", type=int, default=10,
                   help="Total HTTP retries (default: 10)")
    p.add_argument("--retry-backoff", type=float, default=1.0,
                   help="Retry backoff factor (default: 1.0)")
    p.add_argument("--debug", action="store_true", help="Enable debug logging")
    return p


def main():
    args = _build_arg_parser().parse_args()

    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        format="%(asctime)s;%(levelname)s;%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=log_level,
    )

    if os.name == "nt":
        print("WARNING: Windows path handling may cause issues. Use Linux/macOS or WSL.")

    cloud = "gcp" if args.gcp else "azure"

    exporter = WorkspaceExporter(
        workspace_url=args.workspace_url,
        token=args.token,
        base_export_dir=args.export_dir,
        session=args.session,
        cloud=cloud,
        num_parallel=args.num_parallel,
        notebook_format=args.notebook_format,
        skip_components=args.skip,
        skip_failed=args.skip_failed,
        include_mlflow=args.include_mlflow,
        verify_ssl=not args.no_ssl_verification,
        retry_total=args.retry_total,
        retry_backoff=args.retry_backoff,
        debug=args.debug,
    )

    tracker = exporter.run()
    counts = tracker.counts()
    sys.exit(0 if counts["failed"] == 0 else 1)


if __name__ == "__main__":
    main()
