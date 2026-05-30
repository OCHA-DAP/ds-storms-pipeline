"""ADAM admin boundary loader for the canonical FM↔ADAM lookup.

WFP's ADAM exposure CSV reports per-event population at adm1, but
ships only names — no codes. The names come from WFP's own published
admin-1 layer (``ge_adm1.parquet`` upstream filename), a 3,509-polygon
OCHA/SALB-style COD composite. We use it as the spatial reference
for the FM↔ADAM bridge: ADAM ``admin_name`` ↔ ADAM admin polygon
``adm1_name`` by name, then ADAM polygon ↔ FM polygon by IoU overlay.

Source: https://data.earthobservation.vam.wfp.org/public-share/
boundaries/ge_adm1.parquet  (upstream filename; we keep it as-is on
disk for provenance, but every API into this module uses ``adam_``
naming).

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
# the path is stable across the repo. Upstream filename (ge_adm1.parquet)
# is preserved on disk; the API names this module exposes are adam_.
REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_LOCAL_PATH = REPO_ROOT / "data" / "static" / "ge_adm1.parquet"

# Planned blob mirror location (not yet uploaded). When the local
# fallback is removed, load_adam_admin will stream from here by default.
ADAM_ADMIN_CONTAINER = "global"
ADAM_ADMIN_BLOB = "adam/boundaries/ge_adm1.parquet"

# Narrow allow-list of iso3s where the upstream adm1_name is "N/A" but
# adm0_name holds the real subdivision name. Strict opt-in by iso3
# so we don't accidentally rename real "N/A" rows elsewhere.
ADAM_ADMIN_NAME_FALLBACK_ISOS = {"BES", "UMI"}


logger = logging.getLogger(__name__)


def load_adam_admin(
    local_path: Path | str | None = None,
    stage: str = "dev",
) -> gpd.GeoDataFrame:
    """Load the global ADAM admin-1 boundary layer.

    Resolution order:

    1. ``local_path`` if given and exists.
    2. :data:`DEFAULT_LOCAL_PATH` (``data/static/ge_adm1.parquet``)
       if it exists. This is the dev-iteration path — 278 MB local
       file avoids blob round-trips during builds.
    3. Blob fallback at ``{ADAM_ADMIN_CONTAINER}/{ADAM_ADMIN_BLOB}`` —
       NOT yet uploaded. Will raise ``ResourceNotFoundError`` until
       a final-pass commit mirrors the file.

    Returns the full ~3,509-polygon layer with the canonical ``iso3``
    column already populated (upstream's own field). Filter per-country
    with :func:`filter_adam_country`.
    """
    if local_path is not None:
        local_path = Path(local_path)
    elif DEFAULT_LOCAL_PATH.exists():
        local_path = DEFAULT_LOCAL_PATH

    if local_path is not None and local_path.exists():
        logger.info("Loading ADAM admin layer from local %s", local_path)
        gdf = gpd.read_parquet(local_path)
    else:
        logger.info(
            "Loading ADAM admin layer from blob %s/%s (stage=%s)",
            ADAM_ADMIN_CONTAINER, ADAM_ADMIN_BLOB, stage,
        )
        container = stratus.get_container_client(
            stage=stage, container_name=ADAM_ADMIN_CONTAINER,
        )
        blob_client = container.get_blob_client(ADAM_ADMIN_BLOB)
        try:
            import io
            data = blob_client.download_blob().readall()
            gdf = gpd.read_parquet(io.BytesIO(data))
        except ResourceNotFoundError as e:
            raise RuntimeError(
                f"ADAM admin layer not found locally "
                f"({DEFAULT_LOCAL_PATH}) and not yet mirrored to blob "
                f"({ADAM_ADMIN_CONTAINER}/{ADAM_ADMIN_BLOB}). Download "
                "from https://data.earthobservation.vam.wfp.org/"
                "public-share/boundaries/ge_adm1.parquet and place at "
                f"{DEFAULT_LOCAL_PATH}."
            ) from e

    if gdf is None or len(gdf) == 0:
        raise RuntimeError("ADAM admin layer load returned empty")

    # adm1_name fallback: a handful of iso3s store the *real*
    # adm1 name in adm0_name and leave adm1_name as the literal
    # string "N/A". Same pattern FieldMaps uses for SJM/UMI (see
    # FM_ADM1_NAME_FALLBACK_ISOS in src/static/gdacs/inputs.py).
    # Examples:
    #   BES (Caribbean Netherlands): 3 polygons all named "N/A",
    #        adm0_name = "Bonaire" / "Saba" / "Sint Eustatius"
    #   UMI (US Minor Outlying Islands): 9 polygons all named "N/A",
    #        adm0_name = "Navassa Island" / "Baker Island" / ...
    # Narrow allow-list keeps the fallback from masking real data
    # gaps in iso3s where N/A really does mean unknown.
    fallback_mask = (
        gdf["iso3"].isin(ADAM_ADMIN_NAME_FALLBACK_ISOS)
        & (gdf["adm1_name"] == "N/A")
        & gdf["adm0_name"].notna()
    )
    if fallback_mask.any():
        gdf.loc[fallback_mask, "adm1_name"] = (
            gdf.loc[fallback_mask, "adm0_name"]
        )
        logger.info(
            "Applied adm0_name → adm1_name fallback for %d rows",
            int(fallback_mask.sum()),
        )
    return gdf


def filter_adam_country(
    adam_admin: gpd.GeoDataFrame, iso3: str,
) -> gpd.GeoDataFrame:
    """Return ADAM admin polygons for one country."""
    return adam_admin[adam_admin["iso3"] == iso3].copy()
