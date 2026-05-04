#!/usr/bin/env python3
"""
IBTrACS ETL pipeline and wind buffer generation.
"""

import logging
import os
import warnings

import coloredlogs
import geopandas as gpd
import ocha_lens as lens
import pandas as pd
import xarray as xr
from dotenv import load_dotenv
from ocha_lens.utils.storm import calculate_wind_buffers_gdf, expand_quad_col
from sqlalchemy import text
from tqdm import tqdm

load_dotenv()

import ocha_stratus as stratus  # noqa

BUFFER_SPEEDS = [34, 50, 64]
_WIND_BUFFER_BATCH_SIZE = 50


logger = logging.getLogger(__name__)


def retrieve_ibtracs(
    dataset_type, stage="local", save_to_blob=False, save_dir=None
):
    """
    Download IBTrACS Netcdf, upload raw to azure if needed and return loaded Dataset
    """
    logger.info(f"Retrieving {dataset_type} from IBTrACS...")
    filename = f"IBTrACS.{dataset_type}.v04r01.nc"
    file_path = f"{save_dir}/" + filename

    if os.path.exists(file_path):
        logger.info(f"Using file downloaded in {file_path}...")
        path = file_path
    else:
        path = lens.ibtracs.download_ibtracs(
            dataset=dataset_type, save_dir=save_dir
        )
        logger.info(f"Successfully downloaded {dataset_type} to {path}.")

    if save_to_blob:
        logger.info(f"Uploading {path} to Azure blob in {stage}...")
        with open(path, "rb") as file:
            data_to_upload = file.read()
        stratus.upload_blob_data(
            container_name="storm",
            data=data_to_upload,
            blob_name=f"ibtracs/v04r01/{filename}",
            stage=stage,
        )
        logger.info("Successfully uploaded to blob.")

    return xr.open_dataset(path).load()


def process_tracks(dataset, engine, chunksize):
    """
    Retrieve 'best' and 'provisional' tracks and upload them to the database
    """
    logger.info("Extracting tracks...")
    tracks_geo = lens.ibtracs.get_tracks(dataset)

    # In order to comply with the type of object we can apply this function to each geometry
    # and then run the upsert or use to_postgis to a temporary table instead of to_sql and
    # then run another query to do the upsert
    logger.info("Transforming geometry...")
    with warnings.catch_warnings():
        # This is the intended behaviour, suppress the specific warning
        warnings.filterwarnings(
            "ignore", message="Geometry column does not contain geometry"
        )
        tracks_geo["geometry"] = tracks_geo["geometry"].to_wkt()

    logger.info("Updating tracks in database...")
    with engine.connect() as conn:
        tracks_geo.to_sql(
            name="ibtracs_tracks_geo",
            con=conn,
            schema="storms",
            if_exists="append",
            index=False,
            method=stratus.postgres_upsert,
            chunksize=chunksize,
        )
    logger.info("Successfully processed tracks.")

    return tracks_geo


def process_storms(dataset, engine, chunksize):
    """
    Retrieve 'storm' tracks and upload them to the database
    """
    logger.info("Processing storms...")

    storm_tracks = lens.ibtracs.get_storms(dataset)

    with engine.connect() as conn:
        storm_tracks.to_sql(
            "ibtracs_storms",
            con=conn,
            schema="storms",
            if_exists="append",
            index=False,
            method=stratus.postgres_upsert,
            chunksize=chunksize,
        )

    logger.info("Successfully processed storms.")
    return storm_tracks


def run_ibtracs(
    mode, dataset_type, save_to_blob=False, save_dir="/tmp", chunksize=10000
):
    """
    Main function to orchestrate the execution of pipeline functions.

    Parameters
    ----------
    save_to_blob flag to determine whether the netcdf file should be saved
    mode [dev or prod]
    """

    coloredlogs.install(
        logger=logger,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    logger.info("Starting IBTrACS ETL pipeline...")

    # Setting up engine
    engine = stratus.get_engine(stage=mode, write=True)

    try:
        # Retrieve data from source and upload to blob if true
        dataset = retrieve_ibtracs(
            dataset_type=dataset_type,
            stage=mode,
            save_to_blob=save_to_blob,
            save_dir=save_dir,
        )

        # Process storms and add them to the database
        process_storms(dataset=dataset, engine=engine, chunksize=chunksize)

        # Process tracks and add them to the database
        process_tracks(dataset=dataset, engine=engine, chunksize=chunksize)

        logger.info("Pipeline successfully finished!")

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise


# ---------------------------------------------------------------------------
# Wind buffer generation
# ---------------------------------------------------------------------------


def _load_wind_buffer_tracks(
    engine,
    basin: str | None = None,
    start_year: int | None = None,
    sids: list[str] | None = None,
) -> gpd.GeoDataFrame:
    filters = [
        """(usa_quadrant_radius_34 IS NOT NULL
            OR usa_quadrant_radius_50 IS NOT NULL
            OR usa_quadrant_radius_64 IS NOT NULL)"""
    ]
    if basin:
        filters.append(f"basin = '{basin}'")
    if start_year:
        filters.append(f"EXTRACT(YEAR FROM valid_time) >= {start_year}")
    if sids:
        sid_list = ", ".join(f"'{s}'" for s in sids)
        filters.append(f"sid IN ({sid_list})")

    where = " AND ".join(filters)
    query = f"""
        SELECT sid, basin, valid_time,
               usa_quadrant_radius_34,
               usa_quadrant_radius_50,
               usa_quadrant_radius_64,
               geometry
        FROM storms.ibtracs_tracks_geo
        WHERE {where}
        ORDER BY sid, valid_time
    """
    with engine.connect() as conn:
        return gpd.read_postgis(query, conn, geom_col="geometry")


def _load_existing_sids(engine) -> set:
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT DISTINCT sid FROM storms.ibtracs_wind_buffers")
        )
        return {row[0] for row in result}


def _write_wind_buffer_batch(batch, conn, table, cols, chunksize):
    gdf = pd.concat(batch, ignore_index=True).rename(
        columns={"speed": "wind_speed_kt"}
    )
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", message="Geometry column does not contain geometry"
        )
        gdf["geometry"] = gdf["geometry"].apply(
            lambda g: g.wkt if g is not None else None
        )
    gdf[cols].to_sql(
        name=table,
        con=conn,
        schema="storms",
        if_exists="append",
        index=False,
        method=stratus.postgres_upsert,
        chunksize=chunksize,
    )
    conn.commit()


def process_wind_buffers(
    read_engine,
    write_engine,
    chunksize,
    basin=None,
    start_year=None,
    overwrite=False,
    sids=None,
):
    logger.info("Loading IBTrACS tracks with USA wind radii...")
    gdf_tracks = _load_wind_buffer_tracks(
        read_engine, basin=basin, start_year=start_year, sids=sids
    )
    logger.info(
        f"Loaded {len(gdf_tracks)} track points "
        f"across {gdf_tracks['sid'].nunique()} storms."
    )

    if not overwrite:
        existing = _load_existing_sids(write_engine)
        if existing:
            before = gdf_tracks["sid"].nunique()
            gdf_tracks = gdf_tracks[~gdf_tracks["sid"].isin(existing)]
            skipped = before - gdf_tracks["sid"].nunique()
            logger.info(
                f"Skipping {skipped} already-computed storms. "
                f"{gdf_tracks['sid'].nunique()} remaining."
            )
        if gdf_tracks.empty:
            logger.info("All storms already computed. Nothing to do.")
            return

    logger.info("Expanding quadrant radius columns...")
    for speed in BUFFER_SPEEDS:
        gdf_tracks = expand_quad_col(gdf_tracks, f"usa_quadrant_radius_{speed}")

    logger.info("Calculating and writing wind buffers per storm...")
    cols = ["sid", "wind_speed_kt", "geometry"]
    batch = []
    with write_engine.connect() as conn:
        for sid, group in tqdm(gdf_tracks.groupby("sid")):
            gdf_buffers = calculate_wind_buffers_gdf(group)
            gdf_buffers["sid"] = sid
            batch.append(gdf_buffers)

            if len(batch) >= _WIND_BUFFER_BATCH_SIZE:
                _write_wind_buffer_batch(
                    batch, conn, "ibtracs_wind_buffers", cols, chunksize
                )
                batch = []

        if batch:
            _write_wind_buffer_batch(
                batch, conn, "ibtracs_wind_buffers", cols, chunksize
            )

    logger.info("Successfully wrote IBTrACS wind buffers.")


def run_wind_buffers(
    write_mode="dev",
    chunksize=1000,
    basin=None,
    start_year=None,
    overwrite=False,
    sids=None,
):
    coloredlogs.install(
        logger=logger,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger.info("Starting IBTrACS wind buffers pipeline...")
    read_engine = stratus.get_engine(stage="prod")
    write_engine = stratus.get_engine(stage=write_mode, write=True)
    try:
        process_wind_buffers(
            read_engine=read_engine,
            write_engine=write_engine,
            chunksize=chunksize,
            basin=basin,
            start_year=start_year,
            overwrite=overwrite,
            sids=sids,
        )
        logger.info("IBTrACS wind buffers pipeline finished.")
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise
