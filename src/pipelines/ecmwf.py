#!/usr/bin/env python3
"""
ECMWF ETL pipeline
"""

import logging

import coloredlogs
import ocha_lens as lens
import ocha_stratus as stratus
from dotenv import load_dotenv

load_dotenv()


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ecmwf_logger = logging.getLogger("ocha_lens.datasources.ecmwf_storm")
ecmwf_logger.setLevel(logging.DEBUG)
coloredlogs.install(
    logger=ecmwf_logger,
    level=logging.DEBUG,
    fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def process_tracks(dataset, engine, chunksize):
    """
    Retrieve 'best' and 'provisional' tracks and upload them to the database
    """
    logger.info("Extracting tracks...")
    tracks_geo = lens.ecmwf_storm.get_tracks(dataset)

    # In order to comply with the type of object we can apply this function to each geometry
    # and then run the upsert or use to_postgis to a temporary table instead of to_sql and
    # then run another query to do the upsert
    logger.info("Transforming geometry...")
    tracks_geo["geometry"] = tracks_geo["geometry"].to_wkt()

    logger.info("Updating tracks in database...")
    with engine.connect() as conn:
        tracks_geo.to_sql(
            name="ecmwf_tracks_geo_hannah",
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

    storm_tracks = lens.ecmwf_storm.get_storms(dataset)

    with engine.connect() as conn:
        storm_tracks.to_sql(
            "ecmwf_storms_hannah",
            con=conn,
            schema="storms",
            if_exists="append",
            index=False,
            method=stratus.postgres_upsert,
            chunksize=chunksize,
        )

    logger.info("Successfully processed storms.")
    return storm_tracks


def run_ecmwf(
    mode,
    start_date,
    end_date,
    save_to_blob=False,
    save_dir="/tmp",
    chunksize=10000,
):
    """
    Main function to orchestrate the execution of pipeline functions.

    Parameters
    ----------
    start_date Which date to start from
    end_date Which date to stop at
    save_to_blob flag to determine whether the netcdf file should be saved
    mode [dev or prod]
    """

    coloredlogs.install(
        logger=logger,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    logger.info("Starting ECMWF ETL pipeline...")

    # Setting up engine
    engine = stratus.get_engine(stage=mode, write=True)

    try:
        dataset = lens.ecmwf_storm.load_hindcasts(
            start_date=start_date,
            end_date=end_date,
            use_cache=False,
            stage=mode,
            skip_if_missing=False,
        )

        # Process storms and add them to the database
        process_storms(dataset=dataset, engine=engine, chunksize=chunksize)

        # Process tracks and add them to the database
        process_tracks(dataset=dataset, engine=engine, chunksize=chunksize)

        logger.info("Pipeline successfully finished!")

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise
