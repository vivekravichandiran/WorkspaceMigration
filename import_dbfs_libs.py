#!/usr/bin/env python3
"""
import_dbfs_libs.py
===================
Re-import library metadata (and DBFS binaries) into a destination workspace.

Must be run AFTER export_dbfs_libs.py has been executed against the source
workspace, producing a library_manifest.json in the session directory.

Example
-------
    python3 import_dbfs_libs.py \
        --profile DST_PROFILE \
        --session M202401011200

What this does
--------------
1. Uploads every jar / whl / egg binary to the same DBFS path on the destination.
2. Calls /libraries/install to re-attach libraries to clusters that share the
   same name as in the source workspace.
3. Prints a report listing coordinate-based libs (PyPI / Maven / CRAN) that
   should be reviewed – they are not auto-installed because they are usually
   re-attached through cluster policies or init scripts.
"""

import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dbfs_libs.importer import LibraryImporter


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Import library binaries and cluster attachments into a Databricks workspace."
    )
    p.add_argument("--profile", required=True, help="Databricks CLI profile (destination)")
    p.add_argument(
        "--session",
        default="",
        help="Session ID – must match the value used during export.",
    )
    p.add_argument(
        "--set-export-dir",
        default="logs",
        help="Base export directory (default: logs/)",
    )
    p.add_argument("--azure", action="store_true")
    p.add_argument("--gcp", action="store_true")
    p.add_argument("--no-ssl-verification", action="store_true")
    p.add_argument("--debug", action="store_true")
    p.add_argument(
        "--skip-upload",
        action="store_true",
        help="Skip DBFS uploads; only re-install cluster-attached libraries.",
    )
    p.add_argument(
        "--skip-cluster-install",
        action="store_true",
        help="Skip cluster library installs; only upload DBFS binaries.",
    )
    return p


def main() -> None:
    args = _build_parser().parse_args()

    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        format="%(asctime)s;%(levelname)s;%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=log_level,
    )

    try:
        from dbclient.parser import get_login_credentials, is_azure_creds, is_gcp_creds
    except ImportError:
        logging.error(
            "dbclient not found. Run this script from the databrickslabs/migrate repo root."
        )
        sys.exit(1)

    login_args = get_login_credentials(profile=args.profile)

    if is_azure_creds(login_args) and not args.azure:
        raise ValueError("Azure credentials detected – pass --azure flag.")
    if is_gcp_creds(login_args) and not args.gcp:
        raise ValueError("GCP credentials detected – pass --gcp flag.")

    url = login_args["host"]
    token = login_args.get("token", login_args.get("password"))
    export_dir = os.path.join(args.set_export_dir, args.session) + "/"

    client_config = {
        "profile": args.profile,
        "url": url,
        "token": token,
        "export_dir": export_dir,
        "is_aws": False,
        "is_azure": args.azure,
        "is_gcp": args.gcp,
        "debug": args.debug,
        "verbose": args.debug,
        "verify_ssl": not args.no_ssl_verification,
        "file_format": "DBC",
        "overwrite_notebooks": False,
        "skip_failed": False,
        "use_checkpoint": False,
        "retry_total": 10,
        "retry_backoff": 1.0,
        "timeout": 300,
    }

    importer = LibraryImporter(client_config, export_dir)

    if args.skip_upload:
        importer._upload_files = lambda manifest, report: None  # noqa
    if args.skip_cluster_install:
        importer._install_on_clusters = lambda manifest, report: None  # noqa

    report = importer.import_all()
    print(report)


if __name__ == "__main__":
    main()
