"""Raster exposure utilities."""
import geopandas as gpd
import pandas as pd
import xarray as xr
from rioxarray.exceptions import NoDataInBounds

GEO_CRS_ANTIMERIDIAN = "+proj=longlat +datum=WGS84 +lon_wrap=180"


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
