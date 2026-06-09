#!/usr/bin/env python3
"""Strip stale ocha-lens library attachments off an all-purpose cluster.

While the pipeline runs on an all-purpose cluster (existing_cluster_id),
every ocha-lens pin bump leaves the *previous* version attached. They pile
up until a new version can't install over an old one
(ERROR_DUPLICATE_INSTALLATION). Run this after a bump to remove every
ocha-lens entry except the one you want to keep, then restart the cluster.

    uv run python scripts/clean_cluster_lens.py --keep 0.5.1
    databricks clusters restart 0515-161935-i2w5mxhc -p default

Uninstalls only take effect on restart, so the restart is required.
(Delete this script once the bundle moves to an ephemeral job cluster.)
"""

import argparse
import json
import subprocess
import sys

CLUSTER_ID = "0515-161935-i2w5mxhc"
PROFILE = "default"


def _cli(args: list[str]) -> str:
    return subprocess.run(
        ["databricks", *args, "-p", PROFILE],
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--keep",
        required=True,
        help="ocha-lens version to KEEP, e.g. 0.5.1 (everything else is removed)",
    )
    ap.add_argument("--cluster-id", default=CLUSTER_ID)
    args = ap.parse_args()

    keep_pkg = f"ocha-lens=={args.keep}"
    status = json.loads(
        _cli(["libraries", "cluster-status", args.cluster_id, "--output", "json"])
    )
    libs = status if isinstance(status, list) else status.get("library_statuses", [])

    remove = []
    for entry in libs:
        lib = entry.get("library", {})
        pkg = lib.get("pypi", {}).get("package") if lib.get("pypi") else None
        if pkg and pkg.startswith("ocha-lens") and pkg != keep_pkg:
            remove.append({"pypi": {"package": pkg}})

    if not remove:
        print(f"Nothing to remove — only {keep_pkg} (or no ocha-lens) attached.")
        return 0

    print(f"Removing {len(remove)} stale ocha-lens entries (keeping {keep_pkg}):")
    for r in remove:
        print("  -", r["pypi"]["package"])

    payload = json.dumps({"cluster_id": args.cluster_id, "libraries": remove})
    _cli(["libraries", "uninstall", "--json", payload])
    print(
        f"\nMarked for removal. Now restart to apply:\n"
        f"    databricks clusters restart {args.cluster_id} -p {PROFILE}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
