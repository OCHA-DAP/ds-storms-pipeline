"""Raster exposure utilities shared across all pipeline datasets."""
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from io import BytesIO

import geopandas as gpd
import ocha_stratus as stratus
import pandas as pd
import xarray as xr
from rioxarray.exceptions import NoDataInBounds
from tqdm import tqdm

GEO_CRS_ANTIMERIDIAN = "+proj=longlat +datum=WGS84 +lon_wrap=180"
_POP_BLOB = "worldpop/pop_count/global_pop_2026_CN_1km_R2025A_UA_v1.tif"

# FieldMaps adm1 polygons are mirrored once into per-country parquet blobs
# by scripts/mirror_fieldmaps_to_blob.py. Reading from blob (same Azure
# region as DBX) is ~50x faster than fsspec-ing data.fieldmaps.io for
# the 1.4 GB upstream parquet over DBX's egress link.
_FIELDMAPS_BLOB_CONTAINER = "raster"
_FIELDMAPS_BLOB_PREFIX = "fieldmaps/adm1/"
_FIELDMAPS_BLOB_PATH_TPL = _FIELDMAPS_BLOB_PREFIX + "{iso3}.parquet"
_FIELDMAPS_PARALLEL_WORKERS = 16


@lru_cache(maxsize=None)
def _list_iso3s_in_blob(stage: str) -> tuple[str, ...]:
    """Return the iso3s available in blob, derived from blob names."""
    names = stratus.list_container_blobs(
        stage=stage,
        container_name=_FIELDMAPS_BLOB_CONTAINER,
        name_starts_with=_FIELDMAPS_BLOB_PREFIX,
    )
    return tuple(sorted(
        n.removeprefix(_FIELDMAPS_BLOB_PREFIX).removesuffix(".parquet")
        for n in names
        if n.endswith(".parquet")
    ))


@lru_cache(maxsize=1024)
def _load_adm1_country_blob(iso3: str, stage: str) -> gpd.GeoDataFrame:
    data = stratus.load_blob_data(
        _FIELDMAPS_BLOB_PATH_TPL.format(iso3=iso3),
        stage=stage,
        container_name=_FIELDMAPS_BLOB_CONTAINER,
    )
    return gpd.read_parquet(BytesIO(data))


def load_adm1(
    countries: list[str] | None, stage: str = "dev"
) -> gpd.GeoDataFrame:
    if countries is None:
        countries = list(_list_iso3s_in_blob(stage))
    if not countries:
        return gpd.GeoDataFrame(
            columns=["iso_3", "adm1_id", "geometry"], crs="EPSG:4326"
        )
    with ThreadPoolExecutor(max_workers=_FIELDMAPS_PARALLEL_WORKERS) as ex:
        parts = list(ex.map(
            lambda iso3: _load_adm1_country_blob(iso3, stage), countries,
        ))
    return gpd.GeoDataFrame(
        pd.concat(parts, ignore_index=True), crs=parts[0].crs,
    )


def load_adm_units(
    countries: list[str] | None,
    admin_level: int,
    stage: str = "dev",
) -> gpd.GeoDataFrame:
    """Return [iso3, pcode, geometry] for the requested admin level.

    admin_level=0: one row per country, geometry = dissolved adm1s, pcode=iso3.
    admin_level=1: one row per FieldMaps adm1, pcode=adm1_id.
    """
    if admin_level not in (0, 1):
        raise ValueError(f"admin_level must be 0 or 1, got {admin_level!r}")
    gdf = load_adm1(countries, stage=stage)
    if admin_level == 0:
        out = gdf[["iso_3", "geometry"]].dissolve(by="iso_3").reset_index()
        out = out.rename(columns={"iso_3": "iso3"})
        out["pcode"] = out["iso3"]
        return out[["iso3", "pcode", "geometry"]]
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
    result_col: str = "pop_exposed",
) -> pd.DataFrame:
    """
    Calculate raster exposure for each geometry in gdf.

    Clips da to each geometry and sums positive pixel values.
    Handles antimeridian-crossing geometries by reprojecting when needed.

    Returns a DataFrame with all non-geometry columns from gdf plus result_col.
    """
    records = []
    for _, row in tqdm(gdf.iterrows(), total=len(gdf), unit="poly", leave=False):
        row_data = row.drop(labels="geometry").to_dict()
        if not row.geometry or row.geometry.is_empty:
            value = 0
        else:
            geom = row.geometry
            if geom.bounds[0] < -160 or geom.bounds[2] > 160:
                geom = (
                    gpd.GeoSeries([geom], crs=4326)
                    .to_crs(GEO_CRS_ANTIMERIDIAN)
                    .iloc[0]
                )
            try:
                clipped = da.rio.clip([geom])
                value = int(clipped.where(clipped > 0).sum())
            except NoDataInBounds:
                value = 0
        row_data[result_col] = value
        records.append(row_data)
    return pd.DataFrame(records)
