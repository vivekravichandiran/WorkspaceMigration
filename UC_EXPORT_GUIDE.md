# Unity Catalog Export Utility

## Overview

`export_unity_catalog.py` is a standalone CLI that exports all Unity Catalog objects from a Databricks workspace as **importable SQL DDL files**, organised in the correct deployment order for a target workspace.

### What is exported

| Object type | File(s) | Notes |
|------------|---------|-------|
| Storage credentials | `01_storage_credentials.sql` | Emitted as SQL comments — must be recreated manually |
| External locations | `02_external_locations.sql` | `CREATE EXTERNAL LOCATION` statements |
| Catalogs | `03_catalogs.sql` | `CREATE CATALOG IF NOT EXISTS` |
| Schemas | `04_schemas.sql` | `CREATE SCHEMA IF NOT EXISTS` |
| Volumes | `05_volumes.sql` | Managed and external volumes |
| Tables | `tables/<cat>.<sch>.<tbl>.sql` | One file per table; external tables before managed |
| Views | `views/<cat>.<sch>.<view>.sql` | Topologically sorted by dependency |
| Functions | `06_functions.sql` | UDFs — `CREATE OR REPLACE FUNCTION` |
| ACLs | `07_grants.sql` | All `GRANT` statements in order: catalog → schema → object |
| Manifest | `import_manifest.json` | Ordered deployment plan with summary stats |
| Skipped objects | `raw/skipped.json` | Materialized views and streaming tables |

### What is **NOT** exported (intentionally skipped)

| Skipped type | Why |
|---|---|
| `MATERIALIZED_VIEW` | Managed by DLT/jobs; cannot be re-created as static DDL |
| `STREAMING_TABLE` | Same — re-create via the DLT pipeline definition (`dlt_pipelines.json`) |
| `DELTASHARING_CATALOG` | Read-only shared from another metastore |
| `FOREIGN_CATALOG` | Requires recreating the connection separately |
| `SYSTEM_CATALOG` | Platform-managed (`system`, `samples`) |
| `MANAGED_ONLINE_CATALOG` | Lakebase catalogs — separate migration path |

---

## Installation / Prerequisites

```bash
# In the migrate repo root (after running migrate_workspace.sh once)
cd ~/.databricks-migrate

# Or directly from your WorkspaceMigration directory
cd ~/WorkspaceMigration
```

The utility only requires `requests` (already installed). No additional dependencies.

---

## Quick Start

```bash
# 1. Export only the catalogs you OWN (default – safest)
python3 export_unity_catalog.py \
    --workspace-url https://adb-XXXXXXXX.azuredatabricks.net \
    --token dapi...

# 2. Export specific catalogs by name
python3 export_unity_catalog.py \
    --workspace-url https://adb-XXXXXXXX.azuredatabricks.net \
    --token dapi... \
    --catalogs sales_prod analytics_dev

# 3. Export ALL accessible managed catalogs
python3 export_unity_catalog.py \
    --workspace-url https://adb-XXXXXXXX.azuredatabricks.net \
    --token dapi... \
    --all-catalogs

# 4. Preview what would be exported (no files written)
python3 export_unity_catalog.py \
    --workspace-url https://adb-XXXXXXXX.azuredatabricks.net \
    --token dapi... \
    --all-catalogs \
    --list-only
```

---

## CLI Reference

### Required

| Flag | Description |
|------|-------------|
| `-u`, `--workspace-url` | Databricks workspace URL (`https://...`) |
| `-t`, `--token` | Personal Access Token (`dapi...`) |

### Catalog / Schema Selection (mutually exclusive)

| Flag | Description |
|------|-------------|
| *(default)* | Export only catalogs owned by the current token user |
| `--catalogs CATALOG [CATALOG ...]` | Export these specific catalog(s) only |
| `--all-catalogs` | Export **all** accessible `MANAGED_CATALOG` entries |

> **Tip for shared workspaces:** Always prefer `--catalogs` or the default owned-only mode. Using `--all-catalogs` on a shared workspace with 80+ catalogs and thousands of tables can take 30–60 minutes.

### Schema Filtering

| Flag | Description |
|------|-------------|
| `--schemas SCHEMA [SCHEMA ...]` | Restrict to these schema names within the selected catalogs. Accepts bare names (`bronze`) or qualified names (`sales_prod.bronze`) |

### Output Control

| Flag | Default | Description |
|------|---------|-------------|
| `-d`, `--export-dir` | `logs/` | Base directory; output goes to `<export-dir>/<session>/uc_export/` |
| `-s`, `--session` | auto (`UC_YYYYMMDD_HHMMSS`) | Sub-directory name for this run |

### Behaviour

| Flag | Description |
|------|-------------|
| `--list-only` | Print catalog/schema/table counts and exit without writing any files |
| `--no-ssl-verification` | Disable SSL certificate verification |
| `--debug` | Enable DEBUG-level logging |

---

## Output Structure

```
logs/
└── UC_202604281522/          ← session directory
    └── uc_export/
        ├── 01_storage_credentials.sql   ← run first (manually)
        ├── 02_external_locations.sql
        ├── 03_catalogs.sql
        ├── 04_schemas.sql
        ├── 05_volumes.sql
        ├── tables/
        │   ├── sales.bronze.orders.sql
        │   ├── sales.silver.orders_clean.sql
        │   └── ...
        ├── views/
        │   ├── sales.gold.daily_revenue.sql   ← sorted by dependency
        │   └── ...
        ├── 06_functions.sql
        ├── 07_grants.sql
        ├── import_manifest.json
        └── raw/
            ├── catalogs.json
            ├── schemas.json
            ├── tables.json
            └── skipped.json
```

---

## Deployment Order (Import on Target Workspace)

The numbered SQL files must be applied in order. The `import_manifest.json` contains the exact sequence:

```
Step 01 – storage_credentials  (create manually first – cloud-specific)
Step 02 – external_locations   (requires storage credentials to exist)
Step 03 – catalogs
Step 04 – schemas
Step 05 – volumes
Step 06 – tables/              (external tables first, then managed)
Step 07 – functions
Step 08 – views/               (topologically sorted – dependencies first)
Step 09 – grants               (apply last)
```

### Run on target workspace

```bash
# Option A: Databricks CLI (recommended)
databricks sql execute --warehouse-id <wh-id> --file uc_export/03_catalogs.sql
databricks sql execute --warehouse-id <wh-id> --file uc_export/04_schemas.sql
# ... follow import_manifest.json order

# Option B: paste into a Databricks notebook
# Read and execute each SQL file in manifest order

# Option C: use the import_manifest.json programmatically
python3 - <<'EOF'
import json, subprocess
manifest = json.load(open("uc_export/import_manifest.json"))
for step in manifest["deployment_order"]:
    print(f"Step {step['step']:02d}: {step['file']}")
EOF
```

---

## Understanding the Manifest

`import_manifest.json` contains:

```json
{
  "exported_at": "2026-04-28T10:27:48.123456+00:00",
  "source_workspace": "https://adb-XXXXX.azuredatabricks.net",
  "deployment_order": [
    { "step": 1, "type": "storage_credentials", "file": "uc_export/01_storage_credentials.sql" },
    { "step": 2, "type": "external_locations",  "file": "uc_export/02_external_locations.sql" },
    { "step": 3, "type": "catalogs",            "file": "uc_export/03_catalogs.sql" },
    ...
    { "step": 9, "type": "table",  "file": "uc_export/tables/sales.bronze.orders.sql",
                                   "full_name": "sales.bronze.orders" },
    ...
    { "step": N, "type": "view",   "file": "uc_export/views/sales.gold.revenue.sql",
                                   "full_name": "sales.gold.revenue" }
  ],
  "summary": {
    "catalogs": 2,
    "schemas": 12,
    "tables": 45,
    "views": 18,
    "functions": 3,
    "volumes": 5,
    "acl_entries": 87,
    "skipped_mv": 2,
    "skipped_streaming": 4,
    "storage_credentials": 3,
    "external_locations": 8
  },
  "notes": [
    "Run steps in manifest order for a clean deployment.",
    "Storage credentials (01) must be created manually first.",
    ...
  ]
}
```

---

## Skipped Objects

Materialized views and streaming tables are recorded in `raw/skipped.json`:

```json
[
  { "full_name": "sales.gold.mv_daily_revenue", "reason": "MATERIALIZED_VIEW" },
  { "full_name": "sales.bronze.st_orders",      "reason": "STREAMING_TABLE"   }
]
```

To recreate these in the target workspace:
- **Materialized views** → re-create via their source DLT pipeline (`dlt_pipelines.json`)
- **Streaming tables** → re-create via their source DLT pipeline or Auto Loader job

---

## Integration with `migrate_workspace.sh`

The UC exporter is automatically invoked by `migrate_workspace.sh` as the `unity_catalog` component. To control which catalogs are exported when running the full workspace migration, use these flags when calling the full exporter from `full_export.py` (see `workspace_export/full_export.py` — the `_export_unity_catalog` method accepts `include_catalogs` and `owned_only` via `UnityCatalogExporter`).

For most production migrations you can run the standalone CLI before or after the full workspace export:

```bash
# Run full workspace export
./migrate_workspace.sh --workspace-url ... --token ... --azure

# Then export specific UC catalogs separately
python3 export_unity_catalog.py \
    --workspace-url ... --token ... \
    --catalogs my_catalog_1 my_catalog_2 \
    --export-dir logs --session EXPORT_20260428
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "0 catalog(s) would be exported" in owned-only mode | The token user owns no catalogs. Use `--catalogs <name>` or `--all-catalogs` |
| Export takes very long | Use `--catalogs` to limit scope. `--all-catalogs` on a shared workspace with 80+ catalogs can take 30–60 min |
| Storage credentials show as comments | This is by design — credentials require admin + cloud-specific config. Recreate them manually |
| View DDL shows "view definition unavailable" | The UC API didn't return `view_definition`. This can happen for cross-catalog views; check the source manually |
| `403 Forbidden` on permissions | The token user lacks `MANAGE` on that object. The DDL is still exported; only grants are skipped |
| Streaming tables / MVs not exported | By design. Export the DLT pipeline (`dlt_pipelines.json`) and re-run it in the target workspace to recreate these |
