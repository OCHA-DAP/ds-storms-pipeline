"""Raster exposure utilities shared across all pipeline datasets."""
import fsspec
import geopandas as gpd
import ocha_stratus as stratus
import pandas as pd
import xarray as xr
from rioxarray.exceptions import NoDataInBounds

GEO_CRS_ANTIMERIDIAN = "+proj=longlat +datum=WGS84 +lon_wrap=180"
_FIELDMAPS_URL = "https://data.fieldmaps.io/edge-matched/humanitarian/intl/adm1_polygons.parquet"
_POP_BLOB = "worldpop/pop_count/global_pop_2026_CN_1km_R2025A_UA_v1.tif"


def load_adm1(countries: list[str] | None) -> gpd.GeoDataFrame:
    filters = [("iso_3", "in", countries)] if countries else None
    with fsspec.open(_FIELDMAPS_URL, "rb") as f:
        return gpd.read_parquet(f, columns=["iso_3", "geometry"], filters=filters)


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
    for _, row in gdf.iterrows():
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
