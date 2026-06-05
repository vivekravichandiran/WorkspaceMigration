#!/usr/bin/env python3
"""
export_dbfs_libs.py
===================
Export all library metadata + download DBFS file-based binaries.

Must be run AFTER the main migrate export pipeline so that clusters.log and
jobs.log are already present in the session export directory.

Example
-------
    python3 export_dbfs_libs.py \
        --profile SRC_PROFILE \
        --session M202401011200

The tool reads clusters.log / jobs.log from the session directory,
queries live cluster library statuses, downloads jar/whl/egg binaries
from DBFS, and writes:

    logs/<session>/library_manifest.json
    logs/<session>/dbfs_files/<original-dbfs-relative-path>
"""

import argparse
import logging
import os
import sys

# Allow running from the migrate repo root without installation
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dbfs_libs.exporter import LibraryExporter


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Export library metadata and DBFS binaries from a Databricks workspace."
    )
    p.add_argument("--profile", required=True, help="Databricks CLI profile name")
    p.add_argument(
        "--session",
        default="",
        help="Session ID (subdirectory under --set-export-dir). "
        "Must match the session used by the main export pipeline.",
    )
    p.add_argument(
        "--set-export-dir",
        default="logs",
        help="Base export directory (default: logs/)",
    )
    p.add_argument("--azure", action="store_true", help="Azure workspace")
    p.add_argument("--gcp", action="store_true", help="GCP workspace")
    p.add_argument(
        "--no-ssl-verification", action="store_true", help="Disable SSL verification"
    )
    p.add_argument("--debug", action="store_true", help="Enable debug logging")
    p.add_argument(
        "--skip-download",
        action="store_true",
        help="Build manifest only; do not download DBFS binaries.",
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

    # Build client_config matching the structure expected by dbclient
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
        "is_aws": not args.azure and not args.gcp,
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

    os.makedirs(export_dir, exist_ok=True)

    exporter = LibraryExporter(client_config)

    if args.skip_download:
        # Monkey-patch to skip DBFS download
        exporter._download_dbfs_files = lambda manifest: None  # noqa

    manifest = exporter.export()
    manifest.print_summary()

    print(f"\nManifest saved to: {export_dir}library_manifest.json")
    if not args.skip_download:
        print(f"DBFS files saved to: {export_dir}dbfs_files/")


if __name__ == "__main__":
    main()
