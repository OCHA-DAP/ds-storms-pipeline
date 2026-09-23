"""Copy every blob under a prefix from one stratus stage to another.

Round-trips the bytes through this process (stratus has no server-side
copy). Built for the 2026-09 prod cutover: the FieldMaps adm0/adm1 mirror
only ever existed in the DEV blob account, and `load_adm_units(stage=mode)`
needs it in PROD once the pipelines run with --mode prod.

Usage:
    python scripts/copy_blobs_between_stages.py \
        --container global --prefix fieldmaps/edge-matched/humanitarian/intl/
    python scripts/copy_blobs_between_stages.py --container raster \
        --prefix worldpop/pop_count/global_pop_2026_CN_1km_R2025A_UA_v1.tif
"""

import argparse
import logging

import coloredlogs
import ocha_stratus as stratus
from tqdm import tqdm

logger = logging.getLogger(__name__)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--container", required=True)
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--src-stage", default="dev", choices=["dev", "prod"])
    ap.add_argument("--dst-stage", default="prod", choices=["dev", "prod"])
    ap.add_argument("--overwrite", action="store_true",
                    help="Re-copy blobs already present at the destination")
    args = ap.parse_args()
    coloredlogs.install(level="INFO", logger=logger)

    names = [
        n for n in stratus.list_container_blobs(
            name_starts_with=args.prefix, stage=args.src_stage,
            container_name=args.container,
        )
        if not n.endswith("/")
    ]
    existing = set(stratus.list_container_blobs(
        name_starts_with=args.prefix, stage=args.dst_stage,
        container_name=args.container,
    ))
    todo = names if args.overwrite else [n for n in names if n not in existing]
    logger.info(
        f"{len(names)} source blobs under {args.container}/{args.prefix} "
        f"({args.src_stage}); {len(todo)} to copy to {args.dst_stage}."
    )
    n_ok = n_skip = 0
    for name in tqdm(todo, unit="blob"):
        data = stratus.load_blob_data(
            name, stage=args.src_stage, container_name=args.container
        )
        if not data:
            # Virtual directory markers list as blobs but carry no bytes.
            n_skip += 1
            continue
        stratus.upload_blob_data(
            data, name, stage=args.dst_stage, container_name=args.container
        )
        n_ok += 1
    logger.info(f"Copied {n_ok}, skipped {n_skip} empty.")


if __name__ == "__main__":
    main()
