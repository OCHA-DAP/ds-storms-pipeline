"""Raster exposure utilities shared across all pipeline datasets."""
import fsspec
import geopandas as gpd
import ocha_stratus as stratus
import pandas as pd
import xarray as xr
from rioxarray.exceptions import NoDataInBounds
from tqdm import tqdm

GEO_CRS_ANTIMERIDIAN = "+proj=longlat +datum=WGS84 +lon_wrap=180"
_FIELDMAPS_URL = "https://data.fieldmaps.io/edge-matched/humanitarian/intl/adm1_polygons.parquet"
_POP_BLOB = "worldpop/pop_count/global_pop_2026_CN_1km_R2025A_UA_v1.tif"


def load_adm1(countries: list[str] | None) -> gpd.GeoDataFrame:
    filters = [("iso_3", "in", countries)] if countries else None
    with fsspec.open(_FIELDMAPS_URL, "rb") as f:
        return gpd.read_parquet(
            f, columns=["iso_3", "adm1_id", "geometry"], filters=filters
        )


def load_adm_units(
    countries: list[str] | None, admin_level: int
) -> gpd.GeoDataFrame:
    """Return [iso3, pcode, geometry] for the requested admin level.

    admin_level=0: one row per country, geometry = dissolved adm1s, pcode=iso3.
    admin_level=1: one row per FieldMaps adm1, pcode=adm1_id.
    """
    if admin_level not in (0, 1):
        raise ValueError(f"admin_level must be 0 or 1, got {admin_level!r}")
    gdf = load_adm1(countries)
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
