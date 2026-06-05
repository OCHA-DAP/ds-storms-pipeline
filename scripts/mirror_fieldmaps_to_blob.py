"""Mirror the FieldMaps adm1 polygons parquet to Azure blob storage,
one parquet per iso3, with geometries simplified to ~100m.

Both ds-storms-pipeline and ds-storms-alerts read these blobs; this
script is the one-and-only place that talks to data.fieldmaps.io.
Re-run periodically (annually or when FieldMaps refreshes) to keep
the mirror in sync.

Usage:
    uv run python scripts/mirror_fieldmaps_to_blob.py --stage dev
    uv run python scripts/mirror_fieldmaps_to_blob.py --stage prod
"""

import argparse
import logging
from io import BytesIO

import coloredlogs
import fsspec
import geopandas as gpd
import ocha_stratus as stratus
from shapely.validation import make_valid
from tqdm import tqdm

FIELDMAPS_URL = (
    "https://data.fieldmaps.io/edge-matched/humanitarian/intl/adm1_polygons.parquet"
)
ADM1_COLS = ["iso_3", "adm0_name", "adm1_id", "adm1_name", "geometry"]
SIMPLIFY_TOL_DEG = 0.001  # ~100 m; matches ds-storms-alerts
BLOB_CONTAINER = "global"
# Blob path mirrors the upstream FieldMaps URL structure.
BLOB_BASE = "fieldmaps/edge-matched/humanitarian/intl/"
BLOB_PATH_TPL_ADM1 = BLOB_BASE + "adm1/{iso3}.parquet"
BLOB_PATH_TPL_ADM0 = BLOB_BASE + "adm0/{iso3}.parquet"

logger = logging.getLogger(__name__)


def mirror(stage: str) -> None:
    coloredlogs.install(
        logger=logger,
        fmt="%(asctime)s %(levelname)s %(message)s",
    )
    logger.info(f"Downloading {FIELDMAPS_URL} ...")
    with fsspec.open(FIELDMAPS_URL, "rb") as f:
        gdf = gpd.read_parquet(f, columns=ADM1_COLS)
    logger.info(
        f"Loaded {len(gdf):,} adm1 rows across "
        f"{gdf['iso_3'].nunique()} countries."
    )

    for iso3, sub in tqdm(gdf.groupby("iso_3"), unit="country"):
        sub = sub.copy()
        # simplify() with preserve_topology=True can still emit polygons
        # that aren't OGC-valid (self-intersections, free holes, etc.),
        # which trips downstream dissolve / union_all with a GEOS
        # TopologyException. make_valid() fixes those.
        sub["geometry"] = sub.geometry.simplify(
            SIMPLIFY_TOL_DEG, preserve_topology=True
        ).apply(make_valid)

        # adm1 blob: per-polygon admin1 rows
        buf_adm1 = BytesIO()
        sub.reset_index(drop=True).to_parquet(buf_adm1)
        stratus.upload_blob_data(
            data=buf_adm1.getvalue(),
            blob_name=BLOB_PATH_TPL_ADM1.format(iso3=iso3),
            stage=stage,
            container_name=BLOB_CONTAINER,
        )

        # adm0 blob: one row per iso3, dissolved country geometry. Some
        # iso3s have multiple disjoint adm0 features (e.g. UMI's separate
        # Pacific islands, VEN's Bird Island + mainland) — collapse them
        # into a single MultiPolygon so downstream consumers can treat
        # admin_level=0 as "one unit per iso3". adm0_name is dropped at
        # this point (multiple names per iso3 don't survive the union).
        adm0 = (
            sub[["iso_3", "geometry"]]
            .dissolve(by="iso_3", as_index=False)
        )
        adm0["geometry"] = adm0.geometry.apply(make_valid)
        buf_adm0 = BytesIO()
        adm0.to_parquet(buf_adm0)
        stratus.upload_blob_data(
            data=buf_adm0.getvalue(),
            blob_name=BLOB_PATH_TPL_ADM0.format(iso3=iso3),
            stage=stage,
            container_name=BLOB_CONTAINER,
        )

    logger.info(
        f"Mirrored {gdf['iso_3'].nunique()} country blobs to "
        f"{stage}://{BLOB_CONTAINER}/fieldmaps/{{adm0,adm1}}/."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=["dev", "prod"], required=True)
    args = parser.parse_args()
    mirror(args.stage)
