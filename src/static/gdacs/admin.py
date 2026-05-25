"""GDACS admin-1 shapefile loader for the canonical FM↔GDACS lookup.

The GDACS impact endpoint reports population at the
``W_ADM_ADMIN2010_V2021`` admin layer — 2,586 polygons globally,
fixed at admin-1 (no hierarchy), carrying ``GMI_ADMIN``,
``FIPS_ADMIN``, ``ADMIN_NAME``. We mirror it once on blob
(``global/gdacs/admin/W_ADM_ADMIN2010_V2021.zip``); this module
streams it directly from blob — no local cache. The shapefile is ~5
MB zipped so download is cheap, and the lookup is built rarely enough
that an extra blob read per build doesn't matter.

Loaded once into memory and filtered per country (whole layer is
~2,586 rows) rather than reading the shapefile 50+ times.

The exploratory crosswalk-runner CLI (which produced per-country
diagnostic parquets used to author ``config/adm_level_config.toml``)
lives at ``artefacts/match_gdacs_fieldmaps.py`` — that's the tool that
authored the policy file once.
"""

from __future__ import annotations

import logging

import geopandas as gpd
import ocha_stratus as stratus


# GDACS-side reference data lives at this blob path. Container is
# ``global`` (shared OCHA-DAP reference data).
GDACS_CONTAINER = "global"
GDACS_BLOB = "gdacs/admin/W_ADM_ADMIN2010_V2021.zip"
GDACS_SHAPEFILE = "W_ADM_ADMIN2010_V2021.shp"


logger = logging.getLogger(__name__)


def _gdacs_to_iso3():
    """Return ocha_lens's ``GMI_CNTRY → ISO3`` mapping function.

    GDACS uses GMI country codes which are mostly ISO3 but X-prefixed
    for some non-sovereign territories (e.g. ``XJE`` → ``JEY``).
    Routing through ``ocha_lens`` keeps the mapping consistent with
    the GDACS ingest pipeline.
    """
    from ocha_lens.datasources.gdacs import to_iso3 as ll_to_iso3
    return ll_to_iso3


def load_full_gdacs(stage: str = "dev") -> gpd.GeoDataFrame:
    """Load the entire GDACS admin layer from blob and add a canonical
    ``_iso3`` column derived from ``GMI_CNTRY``.

    Streams directly via ``stratus.load_shp_from_blob`` — no local
    cache. ~5 MB zipped → fast enough for a build-once workflow.
    """
    logger.info(
        "Loading GDACS admin shapefile from blob %s/%s (stage=%s)",
        GDACS_CONTAINER, GDACS_BLOB, stage,
    )
    gdf = stratus.load_shp_from_blob(
        container_name=GDACS_CONTAINER,
        blob_name=GDACS_BLOB,
        shapefile=GDACS_SHAPEFILE,
        stage=stage,
    )
    if gdf is None or len(gdf) == 0:
        raise RuntimeError(
            "GDACS shapefile load returned empty — check blob path/perms"
        )
    to_iso3 = _gdacs_to_iso3()
    gdf["_iso3"] = gdf["GMI_CNTRY"].map(to_iso3)
    return gdf


def filter_gdacs_country(
    gdacs: gpd.GeoDataFrame, iso3: str,
) -> gpd.GeoDataFrame:
    """Return GDACS admin rows for one country (by canonical ISO3)."""
    return gdacs[gdacs["_iso3"] == iso3].copy()
