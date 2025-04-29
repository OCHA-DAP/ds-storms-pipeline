import os
import tempfile
import urllib.request
from pathlib import Path
from typing import Literal, Optional
import uuid

import pandas as pd
import xarray as xr


def download_ibtracs(
    dataset: Literal["ALL", "ACTIVE", "last3years"] = "ALL",
    temp_dir: Optional[str] = None,
) -> Path:
    """
    Download IBTrACS data to a specified or temporary directory.

    Args:
        dataset: Which IBTrACS dataset to download ("ALL", "ACTIVE", or "last3years")
        temp_dir: Optional directory to download to. If None, the caller is responsible
        for providing a temporary directory.

    Returns:
        Path to the downloaded file
    """
    # Ensure the directory exists
    if temp_dir is not None:
        os.makedirs(temp_dir, exist_ok=True)

    url = (
        "https://www.ncei.noaa.gov/data/"
        "international-best-track-archive-for-climate-stewardship-ibtracs/"
        f"v04r00/access/netcdf/IBTrACS.{dataset}.v04r00.nc"
    )

    filename = f"IBTrACS.{dataset}.v04r00.nc"
    download_path = Path(temp_dir) / filename

    print(f"Downloading {url} to {download_path}...")
    urllib.request.urlretrieve(url, download_path)
    print(f"Download complete: {download_path}")

    return download_path


def load_ibtracs(
    file_path: Optional[str] = None, dataset: str = "ALL"
) -> xr.Dataset:
    """
    Load IBTrACS data from NetCDF file or download to a temporary directory.

    Args:
        file_path: Path to the IBTrACS NetCDF file. If None, downloads the file to a temp directory.
        dataset: Which IBTrACS dataset to use if downloading ("ALL", "ACTIVE", or "last3years")

    Returns:
        xarray Dataset containing IBTrACS data
    """
    if file_path is None:
        # Use a temporary directory that automatically cleans up
        with tempfile.TemporaryDirectory(prefix="ibtracs_data_") as temp_dir:
            print(f"Created temporary directory: {temp_dir}")
            file_path = download_ibtracs(dataset=dataset, temp_dir=temp_dir)

            # Load the dataset and ensure it's fully loaded into memory
            # since temp_dir will be removed after this block
            print(f"Loading dataset from {file_path}...")
            ds = xr.open_dataset(file_path).load()
            print("Dataset loaded into memory")
            return ds
    else:
        # Load from the specified file
        print(f"Loading dataset from {file_path}...")
        return xr.open_dataset(file_path)


def get_provisional_tracks(ds):
    usa_cols = [
        "usa_r34",
        "usa_r50",
        "usa_r64",
        "usa_lat",
        "usa_lon",
        "usa_wind",
        "usa_pres",
        "usa_rmw",
        "usa_roci",
        "usa_poci",
    ]

    other_cols = ["sid", "track_type", "usa_agency", "basin", "nature"]

    string_cols = ["sid", "track_type", "usa_agency", "basin", "nature"]

    ds_ = ds[usa_cols + other_cols]
    ds_[string_cols] = ds_[string_cols].astype(str)
    provisional_mask = ds.track_type == b"PROVISIONAL"  # If stored as bytes
    ds_ = ds_.where(provisional_mask, drop=True)
    df = ds_.to_dataframe().reset_index()
    df = df.replace(b"", pd.NA).replace("", pd.NA).dropna(subset=["time"])
    cols = usa_cols + other_cols + ["time", "quadrant"]
    df = df[cols]

    r_columns = ["usa_r34", "usa_r50", "usa_r64"]

    group_columns = [
        col for col in df.columns if col not in r_columns and col != "quadrant"
    ]

    result_df = (
        df.drop(columns=r_columns + ["quadrant"])
        .drop_duplicates(subset=group_columns)
        .reset_index(drop=True)
    )

    for col in r_columns:
        pivot_df = df.pivot(
            index=group_columns, columns="quadrant", values=col
        )
        lists = pivot_df.apply(lambda x: x.tolist(), axis=1)
        result_df[col] = (
            result_df.set_index(group_columns).index.map(lists).values
        )

    result_df.rename(
        columns={
            "time": "valid_time",
            "usa_lat": "latitude",
            "usa_lon": "longitude",
            "usa_wind": "wind_speed",
            "usa_pres": "pressure",
            "usa_rmw": "max_wind_radius",
            "usa_roci": "last_closed_isobar_radius",
            "usa_poci": "last_closed_isobar_pressure",
            "usa_agency": "provider",
            "usa_r34": "quadrant_radius_34",
            "usa_r50": "quadrant_radius_50",
            "usa_r64": "quadrant_radius_64",
        },
        inplace=True,
    )
    result_df = result_df.drop(columns=["track_type"])
    result_df["point_id"] = [str(uuid.uuid4()) for _ in range(len(result_df))]
    return result_df


def get_storms(ds):
    storm_cols = [
        "sid",
        "usa_atcf_id",
        "number",
        "season",
        "name",
        "track_type",
    ]
    str_vars = [
        "sid",
        "name",
        "track_type",
        "usa_atcf_id",
    ]

    ds_subset = ds[storm_cols]
    ds_subset[str_vars] = ds_subset[str_vars].astype(str)
    df = ds_subset.to_dataframe().reset_index()
    df = df.replace(b"", pd.NA).replace("", pd.NA)
    cols = storm_cols
    df = df[cols]

    df["provisional"] = df["track_type"].apply(lambda x: x == "PROVISIONAL")
    df_grouped = (
        df.groupby("sid").first().reset_index().drop(columns=["track_type"])
    )

    df_grouped["storm_id"] = [
        str(uuid.uuid4()) for _ in range(len(df_grouped))
    ]
    return df_grouped.rename(columns={"usa_atcf_id": "atcf_id"})
