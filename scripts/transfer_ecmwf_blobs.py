#!/usr/bin/env python3
"""
Transfer ECMWF raw XML files from dev to prod blob storage.

Usage:
    python transfer_ecmwf_blobs.py [--dry-run] [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD]

Options:
    --dry-run       List files that would be transferred without actually copying
    --start-date    Only transfer files from this date onwards
    --end-date      Only transfer files up to this date
"""

import argparse
import re
from datetime import datetime

import ocha_stratus as stratus
from dotenv import load_dotenv

load_dotenv()

CONTAINER_NAME = "storm"
BLOB_PREFIX = "xml/raw/"
FILENAME_PATTERN = re.compile(
    r"z_tigge_c_ecmf_(\d{14})_ifs_glob_(?:test|prod)_all_glo\.xml"
)


def parse_timestamp_from_filename(filename):
    """Extract datetime from filename."""
    match = FILENAME_PATTERN.search(filename)
    if match:
        return datetime.strptime(match.group(1), "%Y%m%d%H%M%S")
    return None


def list_blobs(stage):
    """List all ECMWF blobs in the given stage."""
    container_client = stratus.get_container_client(
        container_name=CONTAINER_NAME, stage=stage
    )
    blobs = container_client.list_blobs(name_starts_with=BLOB_PREFIX)
    return {blob.name for blob in blobs}


def transfer_blob(blob_name, source_stage="dev", target_stage="prod"):
    """Copy a single blob from source to target stage."""
    source_client = stratus.get_container_client(
        container_name=CONTAINER_NAME, stage=source_stage
    )
    target_client = stratus.get_container_client(
        container_name=CONTAINER_NAME, stage=target_stage
    )

    # Download from source
    source_blob = source_client.get_blob_client(blob_name)
    data = source_blob.download_blob().readall()

    # Upload to target
    target_blob = target_client.get_blob_client(blob_name)
    target_blob.upload_blob(data, overwrite=True)


def main():
    parser = argparse.ArgumentParser(
        description="Transfer ECMWF files from dev to prod blob storage"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List files without transferring",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Only transfer files from this date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Only transfer files up to this date (YYYY-MM-DD)",
    )
    args = parser.parse_args()

    start_date = (
        datetime.strptime(args.start_date, "%Y-%m-%d")
        if args.start_date
        else None
    )
    end_date = (
        datetime.strptime(args.end_date, "%Y-%m-%d") if args.end_date else None
    )

    print("Listing blobs in dev...")
    dev_blobs = list_blobs("dev")
    print(f"Found {len(dev_blobs)} blobs in dev")

    print("Listing blobs in prod...")
    prod_blobs = list_blobs("prod")
    print(f"Found {len(prod_blobs)} blobs in prod")

    # Find blobs that need to be transferred
    to_transfer = dev_blobs - prod_blobs

    # Filter by date if specified
    if start_date or end_date:
        filtered = set()
        for blob_name in to_transfer:
            ts = parse_timestamp_from_filename(blob_name)
            if ts:
                if start_date and ts < start_date:
                    continue
                if end_date and ts > end_date:
                    continue
                filtered.add(blob_name)
        to_transfer = filtered

    print(f"\nBlobs to transfer: {len(to_transfer)}")

    if not to_transfer:
        print("Nothing to transfer!")
        return

    if args.dry_run:
        print("\n[DRY RUN] Would transfer:")
        for blob_name in sorted(to_transfer)[:20]:
            print(f"  {blob_name}")
        if len(to_transfer) > 20:
            print(f"  ... and {len(to_transfer) - 20} more")
        return

    # Transfer blobs
    print("\nTransferring...")
    for i, blob_name in enumerate(sorted(to_transfer), 1):
        try:
            transfer_blob(blob_name)
            print(f"[{i}/{len(to_transfer)}] Transferred: {blob_name}")
        except Exception as e:
            print(f"[{i}/{len(to_transfer)}] FAILED: {blob_name} - {e}")

    print("\nTransfer complete!")


if __name__ == "__main__":
    main()
