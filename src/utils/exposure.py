"""Raster exposure utilities shared across all pipeline datasets."""
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from io import BytesIO

import geopandas as gpd
import ocha_stratus as stratus
import pandas as pd
import xarray as xr

GEO_CRS_ANTIMERIDIAN = "+proj=longlat +datum=WGS84 +lon_wrap=180"
_POP_BLOB = "worldpop/pop_count/global_pop_2026_CN_1km_R2025A_UA_v1.tif"

# FieldMaps boundaries are mirrored to per-country parquet blobs by
# scripts/mirror_fieldmaps_to_blob.py:
#   fieldmaps/adm1/{iso3}.parquet — per-polygon admin1 rows
#   fieldmaps/adm0/{iso3}.parquet — pre-dissolved single-row admin0
# Reading from blob (same Azure region as DBX) is ~50x faster than
# fsspec-ing the 1.4 GB upstream parquet from data.fieldmaps.io over
# DBX's egress link, and the adm0 mirror saves the ~75 s cold dissolve.
_FIELDMAPS_BLOB_CONTAINER = "raster"
_FIELDMAPS_ADM1_PREFIX = "fieldmaps/adm1/"
_FIELDMAPS_ADM0_PREFIX = "fieldmaps/adm0/"
_FIELDMAPS_PARALLEL_WORKERS = 16


@lru_cache(maxsize=None)
def _list_iso3s_in_blob(stage: str) -> tuple[str, ...]:
    """Return the iso3s available in blob (derived from adm1 blob names)."""
    names = stratus.list_container_blobs(
        stage=stage,
        container_name=_FIELDMAPS_BLOB_CONTAINER,
        name_starts_with=_FIELDMAPS_ADM1_PREFIX,
    )
    return tuple(sorted(
        n.removeprefix(_FIELDMAPS_ADM1_PREFIX).removesuffix(".parquet")
        for n in names
        if n.endswith(".parquet")
    ))


@lru_cache(maxsize=1024)
def _load_country_blob(prefix: str, iso3: str, stage: str) -> gpd.GeoDataFrame:
    data = stratus.load_blob_data(
        f"{prefix}{iso3}.parquet",
        stage=stage,
        container_name=_FIELDMAPS_BLOB_CONTAINER,
    )
    return gpd.read_parquet(BytesIO(data))


def _load_in_parallel(prefix: str, countries: list[str], stage: str) -> gpd.GeoDataFrame:
    if not countries:
        return gpd.GeoDataFrame(columns=["iso_3", "geometry"], crs="EPSG:4326")
    with ThreadPoolExecutor(max_workers=_FIELDMAPS_PARALLEL_WORKERS) as ex:
        parts = list(ex.map(
            lambda iso3: _load_country_blob(prefix, iso3, stage), countries,
        ))
    return gpd.GeoDataFrame(
        pd.concat(parts, ignore_index=True), crs=parts[0].crs,
    )


def load_adm1(
    countries: list[str] | None, stage: str = "dev"
) -> gpd.GeoDataFrame:
    if countries is None:
        countries = list(_list_iso3s_in_blob(stage))
    return _load_in_parallel(_FIELDMAPS_ADM1_PREFIX, countries, stage)


def load_adm0(
    countries: list[str] | None, stage: str = "dev"
) -> gpd.GeoDataFrame:
    if countries is None:
        countries = list(_list_iso3s_in_blob(stage))
    return _load_in_parallel(_FIELDMAPS_ADM0_PREFIX, countries, stage)


def load_adm_units(
    countries: list[str] | None,
    admin_level: int,
    stage: str = "dev",
) -> gpd.GeoDataFrame:
    """Return [iso3, pcode, geometry] for the requested admin level.

    admin_level=0: one row per country, geometry = pre-dissolved adm0, pcode=iso3.
    admin_level=1: one row per FieldMaps adm1, pcode=adm1_id.
    """
    if admin_level not in (0, 1):
        raise ValueError(f"admin_level must be 0 or 1, got {admin_level!r}")
    if admin_level == 0:
        gdf = load_adm0(countries, stage=stage)
        # Defensive: some mirror generations stored multiple rows per iso3
        # (e.g. UMI's separate Pacific islands). Collapse to one row per
        # iso3 so the per-country loop downstream sees exactly one unit.
        if gdf["iso_3"].duplicated().any():
            gdf = gdf.dissolve(by="iso_3", as_index=False)
        out = gdf.rename(columns={"iso_3": "iso3"})
        out["pcode"] = out["iso3"]
        return out[["iso3", "pcode", "geometry"]].reset_index(drop=True)
    gdf = load_adm1(countries, stage=stage)
    out = gdf.rename(columns={"iso_3": "iso3", "adm1_id": "pcode"})
    return out[["iso3", "pcode", "geometry"]].reset_index(drop=True)


def load_pop() -> tuple[xr.DataArray, xr.DataArray]:
    """Return (global, antimeridian-wrapped) WorldPop DataArrays."""
    da = stratus.open_blob_cog(_POP_BLOB, container_name="raster").squeeze(drop=True)
    da_wrapped = da.assign_coords({"x": ((da.x + 360) % 360)}).sortby("x")
    return da, da_wrapped


def calculate_exposure(
    gdf: gpd.GeoDataFrame,
    da: xr.DataArray,
    mask_geom=None,
    result_col: str = "pop_exposed",
) -> pd.DataFrame:
    """Population exposure per row in ``gdf``, using exactextract.

    For each row's geometry (optionally intersected with ``mask_geom``,
    e.g. the admin unit), exactextract computes the area-weighted sum
    of ``da`` pixels. Edge pixels contribute ``fraction × value``
    rather than full-or-nothing, so admin1 sums add up to admin0 totals
    (no boundary double-count, unlike chained ``rio.clip`` with
    ``all_touched=True``).

    Returns a DataFrame with all non-geometry columns from ``gdf`` plus
    ``result_col`` (int). Buffers that produce an empty intersection
    (or have an empty geometry) get ``result_col=0``.
    """
    from exactextract import exact_extract

    cols_out = [c for c in gdf.columns if c != gdf.geometry.name]
    if gdf.empty:
        return pd.DataFrame(columns=cols_out + [result_col])

    work = gdf.reset_index(drop=True).copy()
    if mask_geom is not None and not mask_geom.is_empty:
        work["geometry"] = work.geometry.intersection(mask_geom)

    valid = ~(work.geometry.is_empty | work.geometry.isna())
    out = work.loc[:, cols_out].copy()
    out[result_col] = 0

    if valid.any():
        sub = work.loc[valid]
        # Antimeridian handling: if ANY remaining geometry crosses near
        # the dateline, reproject the whole batch (and the raster is
        # expected to be antimeridian-wrapped — caller's responsibility).
        if (sub.geometry.bounds["minx"] < -160).any() or (
            sub.geometry.bounds["maxx"] > 160
        ).any():
            sub = sub.to_crs(GEO_CRS_ANTIMERIDIAN)
        result = exact_extract(da, sub, ops=["sum"], output="pandas")
        out.loc[valid, result_col] = (
            result["sum"].fillna(0).round().astype("int64").values
        )

    return out
