#!/usr/bin/env python3
"""
IBTrACS ETL pipeline
"""

import os
import logging
import coloredlogs
import ocha_lens as lens
from dotenv import load_dotenv
import xarray as xr

load_dotenv()

import ocha_stratus as stratus  # noqa


logger = logging.getLogger(__name__)


def retrieve_ibtracs(
    dataset_type, stage="local", save_to_blob=False, save_dir=None
):
    """
    Download IBTrACS Netcdf and upload raw to azure
    """
    logger.info(f"Retrieving {dataset_type} from IBTrACS...")
    filename = f"IBTrACS.{dataset_type}.v04r01.nc"
    file_path = "pipelines/storm/raw/" + filename

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
            blob_name=f"ibtracs/v04r01/IBTrACS.{dataset_type}.v04r01.nc",
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
            index=True,
            index_label="index",
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

        # Process tracks and add them to the database
        process_tracks(dataset=dataset, engine=engine, chunksize=chunksize)

        # Process storms and add them to the database
        process_storms(dataset=dataset, engine=engine, chunksize=chunksize)

        logger.info("Pipeline successfully finished!")

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise
