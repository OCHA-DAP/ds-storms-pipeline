#!/usr/bin/env python3
"""
IBTrACS ETL pipeline
"""

import ocha_lens as lens
import ocha_stratus as stratus
import logging
import coloredlogs

from src.schemas.ibtracs import IBTRACS_GEO, IBTRACS_STORMS

logger = logging.getLogger(__name__)

# TODO pick this up from parameter
dataset_type = "ALL"


def retrieve_ibtracs(dataset_type, stage="dev", save_to_blob=False):
    """
    Download IBTrACS Netcdf and upload raw to azure
    """
    logger.info(f"Retrieving {dataset_type} from IBTrACS...")
    path = lens.ibtracs.download_ibtracs(dataset=dataset_type)

    if save_to_blob:
        logger.info(f"Uploading {path} to Azure blob in {stage}...")
        with open(path, "rb") as file:
            data_to_upload = file.read()
        stratus.upload_blob_data(
            data=data_to_upload,
            blob_name="ibtracs/v04r01/IBTrACS.ALL.v04r01.nc",
            stage=stage,
        )
        logger.info("Successfully uploaded to blob.")

    logger.info(f"Successfully downloaded {dataset_type} to {path}.")

    return path


def process_tracks(dataset, engine):
    """
    Retrieve 'best' and 'provisional' tracks and upload them to the database
    """
    logger.info("Processing tracks...")

    tracks_geo = lens.ibtracs.get_tracks(dataset)

    # TODO This might need to be changed in the df methods in lens
    tracks_geo["geometry"] = tracks_geo["geometry"].apply(lambda x: x.wkt)

    tracks_geo.to_sql(
        "ibtracs_tracks_geo_isa",
        con=engine.connect(),
        schema=IBTRACS_GEO.schema,
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
        schema=IBTRACS_STORMS.schema,
        if_exists="append",
        index=False,
        method=stratus.postgres_upsert,
    )

    logger.info("Successfully processed storms.")
    return storm_tracks


def main():
    """
    Main function to orchestrate the execution of pipeline functions.
    """

    coloredlogs.install(
        logger=logger,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    logger.info("Starting IBTrACS ETL pipeline...")
    # TODO pick up mode from parameters
    stage = "dev"

    # Setting up engine
    engine = stratus.get_engine(stage=stage, write=True)

    try:
        # Retrieve data from source and upload to blob if true
        path = retrieve_ibtracs(
            dataset_type=dataset_type, stage=stage, save_to_blob=False
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
        return 1

    return 0


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
