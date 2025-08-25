#!/usr/bin/env python3
"""
IBTrACS ETL pipeline
"""
import os

from dotenv import load_dotenv

load_dotenv()

import ocha_lens as lens
import ocha_stratus as stratus
import logging
import coloredlogs


logger = logging.getLogger(__name__)


def retrieve_ibtracs(dataset_type, stage="local", save_to_blob=False):
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
        path = lens.ibtracs.download_ibtracs(dataset=dataset_type,
                                             save_dir="/dbfs/tmp")

    if save_to_blob:
        logger.info(f"Uploading {path} to Azure blob in {stage}...")
        with open(path, "rb") as file:
            data_to_upload = file.read()
        stratus.upload_blob_data(
            data=data_to_upload,
            blob_name=f"ibtracs/v04r01/IBTrACS.{dataset_type}.v04r01.nc",
            stage=stage,
        )
        logger.info("Successfully uploaded to blob.")

    logger.info(f"Successfully downloaded {dataset_type} to {path}.")

    return path


def process_tracks(dataset, engine):
    """
    Retrieve 'best' and 'provisional' tracks and upload them to the database
    """
    logger.info("Extracting tracks...")
    tracks_geo = lens.ibtracs.get_tracks(dataset)

    # TODO This might need to be changed in the df methods in lens
    tracks_geo["geometry"] = tracks_geo["geometry"].apply(lambda x: x.wkt)

    logger.info("Updating tracks in database...")
    tracks_geo.to_sql(
        "ibtracs_tracks_geo_isa",
        con=engine.connect(),
        schema="storms",
        if_exists="append",
        index=False,
        method=stratus.postgres_upsert,
    )
    logger.info("Successfully processed tracks.")

    return tracks_geo


def process_storms(dataset, engine):
    """
    Retrieve 'storm' tracks and upload them to the database
    """
    logger.info("Processing storms...")

    storm_tracks = lens.ibtracs.get_storms(dataset)

    storm_tracks.to_sql(
        "ibtracs_storms_isa",
        con=engine.connect(),
        schema="storms",
        if_exists="append",
        index=True,
        index_label="index",
        method=stratus.postgres_upsert,
    )

    logger.info("Successfully processed storms.")
    return storm_tracks


def run_ibtracs(mode,
                dataset_type,
                save_to_blob=False):
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
        path = retrieve_ibtracs(
            dataset_type=dataset_type, stage=mode, save_to_blob=save_to_blob
        )
        dataset = lens.ibtracs.load_ibtracs(
            file_path=path, dataset=dataset_type
        )

        # Process tracks and add them to the database
        process_tracks(dataset=dataset, engine=engine)

        # Process storms and add them to the database
        process_storms(dataset=dataset, engine=engine)

        logger.info("Pipeline successfully finished!")

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
