"""One-off: copy FieldMaps adm0/adm1 blobs from `raster` → `global` container.

stratus doesn't have a server-side copy helper, so we round-trip the bytes
through this process. ~570 small parquets, ~5–10 min wall-clock.

Usage:
    uv run python scripts/copy_fieldmaps_blobs.py --stage dev
    uv run python scripts/copy_fieldmaps_blobs.py --stage prod
"""

import argparse
import logging

import coloredlogs
import ocha_stratus as stratus
from tqdm import tqdm

SRC_CONTAINER = "raster"
SRC_PREFIX = "fieldmaps/"  # old off-convention path in raster
DST_CONTAINER = "global"
# New path in global mirrors the upstream FieldMaps URL structure:
# https://data.fieldmaps.io/edge-matched/humanitarian/intl/adm1_polygons.parquet
DST_PREFIX = "fieldmaps/edge-matched/humanitarian/intl/"

logger = logging.getLogger(__name__)


def copy(stage: str) -> None:
    coloredlogs.install(
        logger=logger,
        fmt="%(asctime)s %(levelname)s %(message)s",
    )
    logger.info(
        f"Listing blobs in {stage}://{SRC_CONTAINER}/{SRC_PREFIX}..."
    )
    names = stratus.list_container_blobs(
        stage=stage,
        container_name=SRC_CONTAINER,
        name_starts_with=SRC_PREFIX,
    )
    # The dst container is HNS-enabled, so a zero-byte blob at
    # `fieldmaps/adm0` would block creation of `fieldmaps/adm0/*.parquet`
    # under it. The list call sometimes surfaces those "directory marker"
    # entries — skip non-parquets.
    names = [n for n in names if n.endswith(".parquet")]
    logger.info(f"Found {len(names)} blobs to copy.")

    for src_name in tqdm(names, unit="blob"):
        # Reshape path: fieldmaps/{adm0|adm1}/{iso3}.parquet
        #            →  fieldmaps/edge-matched/humanitarian/intl/{adm0|adm1}/{iso3}.parquet
        dst_name = src_name.replace(SRC_PREFIX, DST_PREFIX, 1)
        data = stratus.load_blob_data(
            src_name, stage=stage, container_name=SRC_CONTAINER,
        )
        stratus.upload_blob_data(
            data=data,
            blob_name=dst_name,
            stage=stage,
            container_name=DST_CONTAINER,
        )

    logger.info(
        f"Copied {len(names)} blobs to {stage}://{DST_CONTAINER}/{DST_PREFIX}."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=["dev", "prod"], required=True)
    args = parser.parse_args()
    copy(args.stage)
