#!/usr/bin/env python3
"""
ECMWF ETL pipeline
"""

import datetime
import logging

import coloredlogs
import ocha_lens as lens
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

load_dotenv()

import ocha_stratus as stratus  # noqa


logger = logging.getLogger(__name__)


def retrieve_ecmwf(date, stage="local", save_to_blob=False):
    """
    Download ECWMF Netcdf and upload raw to azure
    """
    logger.info("Retrieving from ECMWF...")

    try:
        path = lens.ecmwf_storm.download_hindcasts(
            date=date, skip_if_missing=False
        )
        if save_to_blob:
            logger.info(f"Uploading {path} to Azure blob in {stage}...")
            with open(path, "rb") as file:
                data_to_upload = file.read()
            stratus.upload_blob_data(
                data=data_to_upload,
                blob_name=f"storms/ecmwf/{path}",
                stage=stage,
            )
            logger.info("Successfully uploaded to blob.")

        logger.info(f"Successfully downloaded {path}.")
    except Exception as e:
        raise ValueError(f"Failed to retrieve ECWMF data: {e}")

    return path


def process_tracks(dataset, engine, chunksize):
    """
    Retrieve 'best' and 'provisional' tracks and upload them to the database
    """
    logger.info("Extracting tracks...")
    tracks_geo = lens.ecmwf_storm.get_tracks(dataset)

    logger.info("...")
    # TODO This might need to be changed in the df methods in lens
    tracks_geo["geometry"] = tracks_geo["geometry"].apply(lambda x: x.wkt)

    logger.info("Updating tracks in database...")
    tracks_geo.to_sql(
        "ecmwf_tracks_geo_isa",
        con=engine.connect(),
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

    storm_tracks = lens.ecmwf_storm.get_storms(dataset)

    storm_tracks.to_sql(
        "ecmwf_storms_isa",
        con=engine.connect(),
        schema="storms",
        if_exists="append",
        index=True,
        index_label="index",
        method=stratus.postgres_upsert,
        chunksize=chunksize,
    )

    logger.info("Successfully processed storms.")
    return storm_tracks


def run_ecmwf(mode, save_to_blob=False, chunksize=10000):
    """
    Main function to orchestrate the execution of pipeline functions.

    Parameters
    ----------
    date which date to retrieve data from
    mode [dev or prod]
    save_to_blob flag to determine whether the raw file should be saved
    chunksize limit for chunks in to_sql query
    """

    coloredlogs.install(
        logger=logger,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    logger.info("Starting ECWMF ETL pipeline...")

    yesterday = (datetime.now() - relativedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    # Setting up engine
    engine = stratus.get_engine(stage=mode, write=True)

    try:
        # Retrieve data from source and upload to blob if true
        retrieve_ecmwf(date=yesterday, stage=mode, save_to_blob=save_to_blob)
        # TODO add the date parameters
        dataset = lens.ecmwf_storm.load_hindcasts()

        # Process tracks and add them to the database
        process_tracks(dataset=dataset, engine=engine, chunksize=chunksize)

        # Process storms and add them to the database
        process_storms(dataset=dataset, engine=engine, chunksize=chunksize)

        logger.info("Pipeline successfully finished!")

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
