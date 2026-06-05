#!/usr/bin/env python3
"""
export_unity_catalog.py
=======================
Standalone CLI for exporting Unity Catalog objects from a Databricks workspace
as importable SQL DDL files, organised in correct deployment order.

Generates inside <export-dir>/<session>/uc_export/:

  01_storage_credentials.sql   – Storage credentials (admin; emitted as comments)
  02_external_locations.sql    – External locations
  03_catalogs.sql              – CREATE CATALOG statements
  04_schemas.sql               – CREATE SCHEMA statements
  05_volumes.sql               – CREATE VOLUME statements
  tables/<name>.sql            – CREATE TABLE per table (external first)
  views/<name>.sql             – CREATE OR REPLACE VIEW (topologically sorted)
  06_functions.sql             – CREATE FUNCTION statements
  07_grants.sql                – GRANT statements (catalog → schema → object)
  import_manifest.json         – Ordered deployment plan + summary stats
  raw/                         – Raw API responses (catalogs, schemas, tables)
  skipped_objects.json         – Materialized views & streaming tables skipped

Skips:
  • DELTASHARING / FOREIGN / SYSTEM / INTERNAL / MANAGED_ONLINE catalog types
  • MATERIALIZED_VIEW and STREAMING_TABLE table types (identified via table_type
    field in the UC REST API, which mirrors system.information_schema.tables)

Usage:
  # Export only catalogs you own (default – safest for shared workspaces)
  python3 export_unity_catalog.py \\
      --workspace-url https://my-ws.azuredatabricks.net \\
      --token dapi...

  # Export specific catalogs
  python3 export_unity_catalog.py \\
      --workspace-url https://my-ws.azuredatabricks.net \\
      --token dapi... \\
      --catalogs sales_prod analytics_dev

  # Export all accessible MANAGED catalogs (can be slow on shared workspaces)
  python3 export_unity_catalog.py \\
      --workspace-url https://my-ws.azuredatabricks.net \\
      --token dapi... \\
      --all-catalogs

  # Limit to specific schemas (within selected catalogs)
  python3 export_unity_catalog.py \\
      --workspace-url https://my-ws.azuredatabricks.net \\
      --token dapi... \\
      --catalogs sales_prod \\
      --schemas bronze silver gold

  # Dry-run: show catalog/schema counts without exporting DDL
  python3 export_unity_catalog.py \\
      --workspace-url https://my-ws.azuredatabricks.net \\
      --token dapi... \\
      --all-catalogs \\
      --list-only
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime

# Allow running from the project root or from inside the migrate repo
_HERE = os.path.dirname(os.path.abspath(__file__))
_STUBS = os.path.join(_HERE, "stubs")
for _p in (_STUBS, _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from workspace_export.exporters.unity_catalog import UnityCatalogExporter
from workspace_export.exporters.base import BaseExporter


# ---------------------------------------------------------------------------
# List-only helper (dry-run: show what would be exported)
# ---------------------------------------------------------------------------

def _list_only(args: argparse.Namespace) -> None:
    """Print catalog/schema/table counts without exporting anything."""
    import tempfile, shutil
    tmp = tempfile.mkdtemp()
    try:
        exp = _build_exporter(args, tmp)
        owner = exp._resolve_current_user()
        print(f"\nWorkspace : {args.workspace_url}")
        print(f"Token user: {owner or '(unknown)'}")
        print(f"Mode      : {'selected' if args.catalogs else ('all-accessible' if args.all_catalogs else 'owned-only')}")
        print()

        # Fetch catalogs
        from workspace_export.exporters.unity_catalog import _EXPORTABLE_CATALOG_TYPES
        all_cats = exp._paginated_get("/api/2.1/unity-catalog/catalogs", "catalogs", params={"page_size": 200})
        managed = [c for c in all_cats if c.get("catalog_type") in _EXPORTABLE_CATALOG_TYPES]

        if args.catalogs:
            to_show = [c for c in managed if c["name"] in set(args.catalogs)]
        elif args.all_catalogs:
            to_show = managed
        else:
            to_show = [c for c in managed if c.get("owner") == owner]

        print(f"{'Catalog':<40} {'Owner':<35} Schemas")
        print("-" * 90)
        total_schemas = 0
        for cat in to_show:
            schemas = exp._list_schemas(cat["name"])
            cnt = len(schemas)
            total_schemas += cnt
            print(f"  {cat['name']:<38} {cat.get('owner',''):<35} {cnt}")
        print("-" * 90)
        print(f"  {len(to_show)} catalog(s), {total_schemas} schema(s) would be exported")
        print()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def _build_exporter(args: argparse.Namespace, export_dir: str) -> UnityCatalogExporter:
    return UnityCatalogExporter(
        workspace_url=args.workspace_url,
        token=args.token,
        export_dir=export_dir,
        verify_ssl=not args.no_ssl_verification,
        include_catalogs=args.catalogs or None,
        include_schemas=args.schemas or None,
        owned_only=(not args.all_catalogs and not args.catalogs),
    )


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="export_unity_catalog.py",
        description=(
            "Export Unity Catalog objects (catalogs, schemas, tables, views, "
            "functions, volumes, ACLs) as importable SQL DDL."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # ── Required ────────────────────────────────────────────────────────────
    req = p.add_argument_group("required")
    req.add_argument("-u", "--workspace-url", required=True,
                     help="Databricks workspace URL (https://...)")
    req.add_argument("-t", "--token", required=True,
                     help="Personal Access Token (dapi...)")

    # ── Catalog / schema selection ───────────────────────────────────────────
    sel = p.add_argument_group("catalog / schema selection")
    excl = sel.add_mutually_exclusive_group()
    excl.add_argument(
        "--catalogs", nargs="+", metavar="CATALOG",
        help="Export only these specific catalog name(s). "
             "Example: --catalogs sales_prod analytics_dev",
    )
    excl.add_argument(
        "--all-catalogs", action="store_true",
        help="Export ALL accessible MANAGED catalogs. "
             "Warning: can be slow on shared workspaces with many catalogs.",
    )
    sel.add_argument(
        "--schemas", nargs="+", metavar="SCHEMA",
        help="Restrict to these schema name(s) within the selected catalogs. "
             "Accepts bare names (e.g. bronze) or qualified names (cat.bronze).",
    )

    # ── Output ──────────────────────────────────────────────────────────────
    out = p.add_argument_group("output")
    out.add_argument("-d", "--export-dir", default="logs",
                     help="Base export directory (default: logs/)")
    out.add_argument("-s", "--session", default=None,
                     help="Session identifier sub-directory (auto-generated if omitted)")

    # ── Behaviour ───────────────────────────────────────────────────────────
    beh = p.add_argument_group("behaviour")
    beh.add_argument("--list-only", action="store_true",
                     help="Print catalog/schema counts and exit without exporting.")
    beh.add_argument("--no-ssl-verification", action="store_true",
                     help="Disable SSL certificate verification.")
    beh.add_argument("--debug", action="store_true",
                     help="Enable debug-level logging.")

    return p


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s;%(levelname)s;%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.DEBUG if args.debug else logging.INFO,
    )

    if args.list_only:
        _list_only(args)
        return 0

    session = args.session or ("UC_" + datetime.now().strftime("%Y%m%d%H%M"))
    export_dir = os.path.join(args.export_dir, session)
    os.makedirs(export_dir, exist_ok=True)

    mode = (
        f"catalogs={args.catalogs}" if args.catalogs
        else ("all-accessible" if args.all_catalogs else "owned-only")
    )
    logging.info("Unity Catalog export starting")
    logging.info("  Workspace : %s", args.workspace_url)
    logging.info("  Mode      : %s", mode)
    if args.schemas:
        logging.info("  Schemas   : %s", args.schemas)
    logging.info("  Output    : %s", export_dir)

    exp = _build_exporter(args, export_dir)
    stats = exp.export()

    print("\n" + "═" * 65)
    print(" Unity Catalog Export Complete")
    print("═" * 65)
    for k, v in stats.items():
        if v:
            print(f"  {k:<30} {v:>8,}")
    print("═" * 65)
    print(f"\n  Deployment manifest : {export_dir}/uc_export/import_manifest.json")
    print(f"  SQL files           : {export_dir}/uc_export/")
    print()

    if stats.get("skipped_mv", 0) or stats.get("skipped_streaming", 0):
        print(
            f"  Note: {stats.get('skipped_mv',0)} materialized view(s) and "
            f"{stats.get('skipped_streaming',0)} streaming table(s) were skipped "
            f"(see {export_dir}/uc_export/raw/skipped.json)"
        )
        print()

    print("  To deploy on target workspace:")
    print("    1. Run SQL files in numbered order (01 → 07)")
    print("    2. Deploy tables/ before views/ (already guaranteed by ordering)")
    print("    3. Apply 07_grants.sql last")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
