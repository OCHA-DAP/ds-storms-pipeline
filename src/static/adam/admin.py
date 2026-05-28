"""ge_adm1 boundary loader for the canonical FM↔ADAM lookup.

WFP's ADAM exposure CSV reports per-event population at adm1, but
ships only names — no codes. The names come from WFP's own published
admin-1 layer ``ge_adm1.parquet`` (3,509 polygons globally), which is
an OCHA/SALB-style COD composite. We use it as the spatial reference
for the FM↔ADAM bridge: ADAM ``admin_name`` ↔ ge_adm1 ``adm1_name``
by name, then ge_adm1 polygon ↔ FM polygon by IoU overlay.

Source: https://data.earthobservation.vam.wfp.org/public-share/
boundaries/ge_adm1.parquet

Dev workflow uses a local copy at ``data/static/ge_adm1.parquet``
(gitignored, 278 MB) to avoid pulling 278 MB from blob on every dev
iteration. Once policy is settled, mirror to OCHA-DAP blob and switch
the default to blob streaming. The local-path arg always wins, so
production builds can pass an explicit blob-cached path.
"""

from __future__ import annotations

import logging
from pathlib import Path

import geopandas as gpd
import ocha_stratus as stratus
from azure.core.exceptions import ResourceNotFoundError


# Local fallback path — gitignored under data/. Set by repo convention,
# not by env var, because this file is the developer's working copy and
# the path is stable across the repo.
REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_LOCAL_PATH = REPO_ROOT / "data" / "static" / "ge_adm1.parquet"

# Planned blob mirror location (not yet uploaded). When the local
# fallback is removed, load_ge_adm1 will stream from here by default.
GE_ADM1_CONTAINER = "global"
GE_ADM1_BLOB = "adam/boundaries/ge_adm1.parquet"


logger = logging.getLogger(__name__)


def load_ge_adm1(
    local_path: Path | str | None = None,
    stage: str = "dev",
) -> gpd.GeoDataFrame:
    """Load the global ge_adm1 layer.

    Resolution order:

    1. ``local_path`` if given and exists.
    2. :data:`DEFAULT_LOCAL_PATH` (``data/static/ge_adm1.parquet``)
       if it exists. This is the dev-iteration path — 278 MB local
       file avoids blob round-trips during builds.
    3. Blob fallback at ``{GE_ADM1_CONTAINER}/{GE_ADM1_BLOB}`` —
       NOT yet uploaded. Will raise ``ResourceNotFoundError`` until
       a final-pass commit mirrors the file.

    Returns the full ~3,509-polygon layer with the canonical ``iso3``
    column already populated (ge_adm1's own field). Filter per-country
    with :func:`filter_ge_country`.
    """
    if local_path is not None:
        local_path = Path(local_path)
    elif DEFAULT_LOCAL_PATH.exists():
        local_path = DEFAULT_LOCAL_PATH

    if local_path is not None and local_path.exists():
        logger.info("Loading ge_adm1 from local %s", local_path)
        gdf = gpd.read_parquet(local_path)
    else:
        logger.info(
            "Loading ge_adm1 from blob %s/%s (stage=%s)",
            GE_ADM1_CONTAINER, GE_ADM1_BLOB, stage,
        )
        container = stratus.get_container_client(
            stage=stage, container_name=GE_ADM1_CONTAINER,
        )
        blob_client = container.get_blob_client(GE_ADM1_BLOB)
        try:
            import io
            data = blob_client.download_blob().readall()
            gdf = gpd.read_parquet(io.BytesIO(data))
        except ResourceNotFoundError as e:
            raise RuntimeError(
                f"ge_adm1 not found locally ({DEFAULT_LOCAL_PATH}) "
                f"and not yet mirrored to blob "
                f"({GE_ADM1_CONTAINER}/{GE_ADM1_BLOB}). Download from "
                "https://data.earthobservation.vam.wfp.org/"
                "public-share/boundaries/ge_adm1.parquet and place at "
                f"{DEFAULT_LOCAL_PATH}."
            ) from e

    if gdf is None or len(gdf) == 0:
        raise RuntimeError("ge_adm1 load returned empty")
    return gdf


def filter_ge_country(
    ge: gpd.GeoDataFrame, iso3: str,
) -> gpd.GeoDataFrame:
    """Return ge_adm1 rows for one country."""
    return ge[ge["iso3"] == iso3].copy()
