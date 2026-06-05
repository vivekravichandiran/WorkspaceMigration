# DBFS & Library Migration Utility

A utility built on top of [databrickslabs/migrate](https://github.com/databrickslabs/migrate) to fill the gap in library export/import support. The original tool exports library *metadata* but provides no import implementation. This utility adds:

- Full DBFS binary download and re-upload (JAR, WHL, Egg)
- A structured `library_manifest.json` tracking every library, its type, its DBFS location, and every cluster/job that uses it
- Cluster library re-installation on the destination workspace
- A human-readable import report

---

## Table of Contents

- [Background](#background)
- [File Structure](#file-structure)
- [What Gets Tracked](#what-gets-tracked)
- [Library Types](#library-types)
- [Prerequisites](#prerequisites)
- [Usage](#usage)
  - [Step 1 – Run the main migrate export](#step-1--run-the-main-migrate-export)
  - [Step 2 – Export libraries and DBFS files](#step-2--export-libraries-and-dbfs-files)
  - [Step 3 – Import into destination workspace](#step-3--import-into-destination-workspace)
- [library_manifest.json Schema](#library_manifestjson-schema)
- [Import Report](#import-report)
- [CLI Reference](#cli-reference)
  - [export_dbfs_libs.py](#export_dbfs_libspy)
  - [import_dbfs_libs.py](#import_dbfs_libspy)
- [Architecture](#architecture)
- [Running Tests](#running-tests)
- [Limitations](#limitations)

---

## Background

The `databrickslabs/migrate` tool logs cluster-attached library definitions as part of `clusters.log` and `jobs.log`, but the import side is a stub:

```python
# import_db.py – original tool
if args.libs:
    print("Not supported today")
```

This utility closes that gap for all library types:

| Library Type | Export | Import |
|---|---|---|
| JAR (DBFS) | Download binary from DBFS | Upload binary + re-attach to cluster |
| WHL (DBFS) | Download binary from DBFS | Upload binary + re-attach to cluster |
| Egg (DBFS) | Download binary from DBFS | Upload binary + re-attach to cluster |
| PyPI | Capture package + optional repo | Listed in review report |
| Maven | Capture coordinates + optional repo + exclusions | Listed in review report |
| CRAN | Capture package + optional repo | Listed in review report |

PyPI, Maven, and CRAN libraries are intentionally not auto-installed — they are typically managed through cluster policies or init scripts in the destination workspace and are surfaced in the import report for manual review.

---

## File Structure

```
WorkspaceMigration/
├── dbfs_libs/
│   ├── __init__.py
│   ├── models.py          # LibraryEntry, LibraryUsage, LibraryManifest dataclasses
│   ├── exporter.py        # LibraryExporter – scans logs, calls APIs, downloads binaries
│   └── importer.py        # LibraryImporter – uploads binaries, re-installs on clusters
├── export_dbfs_libs.py    # CLI entry point for export
├── import_dbfs_libs.py    # CLI entry point for import
└── tests/
    └── test_dbfs_libs.py  # 30 unit tests (no live workspace required)
```

---

## What Gets Tracked

Every library is deduplicated across all three sources and stored as a single `LibraryEntry` in the manifest:

| Field | Description |
|---|---|
| `lib_type` | `jar` \| `whl` \| `egg` \| `pypi` \| `maven` \| `cran` |
| `dbfs_path` | Original DBFS path (file-based libs only), e.g. `dbfs:/FileStore/jars/lib.jar` |
| `local_file` | Relative path inside the export directory where the binary was saved |
| `pypi_package` | Package spec, e.g. `requests>=2.28` |
| `pypi_repo` | Custom PyPI repo URL (optional) |
| `maven_coordinates` | Maven coordinate string, e.g. `io.delta:delta-core_2.12:2.0.0` |
| `maven_repo` | Custom Maven repo URL (optional) |
| `maven_exclusions` | List of excluded transitive dependencies |
| `cran_package` | CRAN package name |
| `cran_repo` | CRAN mirror URL (optional) |
| `used_by` | List of `LibraryUsage` objects — see below |
| `raw` | Original Databricks API dict, ready to POST to `/libraries/install` |

Each `LibraryUsage` entry records:

| Field | Description |
|---|---|
| `entity_type` | `cluster` \| `cluster_runtime` \| `job` \| `job_task` |
| `entity_id` | Cluster ID or Job ID |
| `entity_name` | Cluster name or Job name (with `:::job_id` suffix stripped) |
| `task_name` | Task key within a multi-task job (populated when `entity_type == "job_task"`) |

---

## Library Types

### File-based (JAR / WHL / Egg)

Stored in DBFS. The exporter downloads each binary to:

```
{export_dir}/dbfs_files/{original-dbfs-relative-path}
```

For example, `dbfs:/FileStore/jars/my-lib.jar` is saved to:

```
logs/M202401011200/dbfs_files/FileStore/jars/my-lib.jar
```

On import, the binary is uploaded back to the same DBFS path on the destination workspace using the three-step streaming API (`/dbfs/create` → `/dbfs/add-block` → `/dbfs/close`) to support files larger than 1 MB.

### Coordinate-based (PyPI / Maven / CRAN)

No binary transfer is needed. Their coordinates are recorded in the manifest and surfaced in the import report for manual review or scripted re-installation via cluster policies.

---

## Prerequisites

- Python 3.6+
- The `databrickslabs/migrate` repository cloned and set up (run `python3 setup.py install` from its root)
- Databricks CLI configured with profiles for both source and destination workspaces:

```bash
databricks configure --token --profile SRC_PROFILE
databricks configure --token --profile DST_PROFILE
```

- Run `export_dbfs_libs.py` from the `databrickslabs/migrate` repository root (it imports `dbclient` from there)

---

## Usage

### Step 1 – Run the main migrate export

Run the standard migrate pipeline export first so that `clusters.log` and `jobs.log` are present in the session directory:

```bash
python3 migration_pipeline.py \
    --profile SRC_PROFILE \
    --export-pipeline \
    --use-checkpoint \
    --session MY_SESSION
```

### Step 2 – Export libraries and DBFS files

```bash
python3 export_dbfs_libs.py \
    --profile SRC_PROFILE \
    --session MY_SESSION
```

This will:

1. Scan `logs/MY_SESSION/clusters.log` for cluster-embedded library definitions
2. Scan `logs/MY_SESSION/jobs.log` for job-level and task-level library definitions
3. Query the live `/libraries/cluster-status` API for runtime-attached libraries not captured in the static logs
4. Download every JAR / WHL / Egg binary from DBFS into `logs/MY_SESSION/dbfs_files/`
5. Write `logs/MY_SESSION/library_manifest.json`

Output example:

```
======================================================================
Library Manifest  |  source: https://src.azuredatabricks.net
======================================================================
Total unique libraries : 5
  File-based (jar/whl/egg) : 2
  Coordinate-based         : 3
======================================================================
  [JAR] dbfs:/FileStore/jars/etl-lib.jar [local: dbfs_files/FileStore/jars/etl-lib.jar]
    -> cluster: [c001] prod-cluster
    -> job_task: [42] pipeline_job / task=extract
  [WHL] dbfs:/FileStore/wheels/my-pkg.whl [local: dbfs_files/FileStore/wheels/my-pkg.whl]
    -> cluster_runtime: [c002] dev-cluster
  [PyPI] requests>=2.28
    -> job: [1] ingest_job
  [Maven] io.delta:delta-core_2.12:2.0.0
    -> cluster: [c001] prod-cluster
  [CRAN] ggplot2
    -> cluster: [c003] ml-cluster
======================================================================
```

### Step 3 – Import into destination workspace

```bash
python3 import_dbfs_libs.py \
    --profile DST_PROFILE \
    --session MY_SESSION
```

This will:

1. Read `logs/MY_SESSION/library_manifest.json`
2. Upload all JAR / WHL / Egg binaries to the same DBFS paths on the destination
3. Call `/libraries/install` on clusters whose names match those in the source
4. Print an import report

---

## library_manifest.json Schema

```json
{
  "source_workspace_url": "https://src.azuredatabricks.net",
  "libraries": [
    {
      "lib_type": "jar",
      "dbfs_path": "dbfs:/FileStore/jars/etl-lib.jar",
      "local_file": "dbfs_files/FileStore/jars/etl-lib.jar",
      "pypi_package": null,
      "pypi_repo": null,
      "maven_coordinates": null,
      "maven_repo": null,
      "maven_exclusions": null,
      "cran_package": null,
      "cran_repo": null,
      "used_by": [
        {
          "entity_type": "cluster",
          "entity_id": "c001",
          "entity_name": "prod-cluster",
          "task_name": null
        },
        {
          "entity_type": "job_task",
          "entity_id": "42",
          "entity_name": "pipeline_job",
          "task_name": "extract"
        }
      ],
      "raw": { "jar": "dbfs:/FileStore/jars/etl-lib.jar" }
    },
    {
      "lib_type": "pypi",
      "dbfs_path": null,
      "local_file": null,
      "pypi_package": "requests>=2.28",
      "pypi_repo": null,
      "maven_coordinates": null,
      "maven_repo": null,
      "maven_exclusions": null,
      "cran_package": null,
      "cran_repo": null,
      "used_by": [
        {
          "entity_type": "job",
          "entity_id": "1",
          "entity_name": "ingest_job",
          "task_name": null
        }
      ],
      "raw": { "pypi": { "package": "requests>=2.28" } }
    }
  ]
}
```

---

## Import Report

After `import_dbfs_libs.py` completes, a structured report is printed:

```
======================================================================
Library Import Report
======================================================================
DBFS uploads     : 2 succeeded, 0 failed, 0 skipped (file missing)
Cluster installs : 1 succeeded, 0 failed, 1 skipped (cluster not found)
Coordinate libs  : 3 to review (see below)
======================================================================
Uploaded:
  ✓ dbfs:/FileStore/jars/etl-lib.jar
  ✓ dbfs:/FileStore/wheels/my-pkg.whl
Installed on clusters:
  ✓ prod-cluster (new_c001) – 1 lib(s)
Clusters not found in destination (skipped):
  - dev-cluster
Coordinate-based libs (review / attach manually if needed):
  ! [PyPI] requests>=2.28
  ! [Maven] io.delta:delta-core_2.12:2.0.0
  ! [CRAN] ggplot2
======================================================================
```

---

## CLI Reference

### export_dbfs_libs.py

```
python3 export_dbfs_libs.py --help

arguments:
  --profile PROFILE       Databricks CLI profile name (source workspace)  [required]
  --session SESSION       Session ID matching the main migrate export
  --set-export-dir DIR    Base export directory (default: logs/)
  --azure                 Azure workspace
  --gcp                   GCP workspace
  --no-ssl-verification   Disable SSL certificate verification
  --debug                 Enable debug logging
  --skip-download         Build manifest only; do not download DBFS binaries
```

### import_dbfs_libs.py

```
python3 import_dbfs_libs.py --help

arguments:
  --profile PROFILE         Databricks CLI profile name (destination workspace)  [required]
  --session SESSION         Session ID matching the export run
  --set-export-dir DIR      Base export directory (default: logs/)
  --azure                   Azure workspace
  --gcp                     GCP workspace
  --no-ssl-verification     Disable SSL certificate verification
  --debug                   Enable debug logging
  --skip-upload             Skip DBFS uploads; only re-install cluster libraries
  --skip-cluster-install    Skip cluster installs; only upload DBFS binaries
```

---

## Architecture

```
export_dbfs_libs.py
        │
        ▼
  LibraryExporter
        │
        ├── Phase 1: clusters.log  ──────────────────────────────────┐
        │   Read cluster configs exported by the migrate pipeline.   │
        │   Extract libraries[] from each cluster definition.        │
        │                                                            │
        ├── Phase 2: jobs.log  ──────────────────────────────────────┤
        │   Single-task jobs: libraries[] at job level.              │  Deduplicated
        │   Multi-task jobs: libraries[] per task.                   │  into unified
        │                                                            │  LibraryManifest
        ├── Phase 3: Live API  ─────────────────────────────────────-┤
        │   GET /libraries/cluster-status per cluster.               │
        │   Catches runtime-attached libs not in static logs.        │
        │                                                            │
        └── Phase 4: DBFS download  ──────────────────────────────--─┘
            For each jar/whl/egg: stream-download via /dbfs/read
            in 1 MB chunks into dbfs_files/{relative-path}.
            Record local_file path in manifest entry.
                    │
                    ▼
        library_manifest.json   +   dbfs_files/

import_dbfs_libs.py
        │
        ▼
  LibraryImporter
        │
        ├── Phase 1: Upload files
        │   For each file-based entry in manifest:
        │   POST /dbfs/create → POST /dbfs/add-block (chunked) → POST /dbfs/close
        │
        ├── Phase 2: Install on clusters
        │   Match clusters by name. POST /libraries/install.
        │
        └── Phase 3: Report coordinate libs for manual review.
```

The `raw` field stored on each `LibraryEntry` is the exact Databricks API dict
(`{"jar": "..."}`, `{"pypi": {"package": "..."}}`, etc.) so it can be POSTed
directly to `/libraries/install` without any transformation.

---

## Running Tests

No live Databricks workspace is required. All API calls are mocked.

```bash
# From the WorkspaceMigration directory
python3 -m unittest tests.test_dbfs_libs -v
```

Expected output:

```
test_cluster_log_deduplication ... ok
test_jar_used_by_two_clusters ... ok
test_dbfs_path_to_local ... ok
test_download_updates_local_file_field ... ok
test_multi_chunk_download ... ok
test_single_chunk_download ... ok
test_dedup_across_jobs ... ok
test_multi_task_job ... ok
test_single_task_job ... ok
test_str_contains_summary ... ok
test_upload_file_calls_api_sequence ... ok
test_upload_large_file_multiple_blocks ... ok
test_cluster_not_found_skips_install ... ok
test_manifest_not_found_raises ... ok
test_missing_local_file_skips_upload ... ok
test_successful_import ... ok
test_cran_key ... ok
test_jar_key ... ok
test_maven_key ... ok
test_pypi_key ... ok
test_file_based_and_coordinate_split ... ok
test_roundtrip ... ok
test_cran ... ok
test_egg ... ok
test_jar ... ok
test_maven ... ok
test_pypi_with_repo ... ok
test_pypi_without_repo ... ok
test_unknown_returns_none ... ok
test_whl ... ok

Ran 30 tests in 0.073s

OK
```

---

## Limitations

- **DBFS path must match** — binaries are uploaded to the same DBFS path as the source. If the destination workspace uses a different path convention, edit `dbfs_path` in `library_manifest.json` before importing.
- **Cluster matching by name** — clusters are looked up by name in the destination workspace. If a cluster was renamed or not yet created, its library install will be skipped (reported as "skipped").
- **Coordinate libs are not auto-installed** — PyPI, Maven, and CRAN libraries surface in the review report. Attach them via cluster policies, init scripts, or manually through the Databricks UI.
- **Secrets for private repositories** — if a JAR references a private Maven or PyPI repo requiring credentials, those credentials must be configured separately in the destination workspace.
- **DBFS file permissions** — the token used for import must have write access to the target DBFS paths.
- **Cluster must be running for library install** — `/libraries/install` requires the target cluster to be in a running or pending state; otherwise the install is queued and applied at next start.
