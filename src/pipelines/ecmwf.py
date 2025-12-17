#!/usr/bin/env python3
"""
ECMWF ETL pipeline
"""

import logging

import coloredlogs
import ocha_lens as lens
import ocha_stratus as stratus
from dotenv import load_dotenv
import warnings
from datetime import datetime

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
    with warnings.catch_warnings():
        # This is the intended behaviour, suppress the specific warning
        warnings.filterwarnings(
            "ignore", message="Geometry column does not contain geometry"
        )
        tracks_geo["geometry"] = tracks_geo["geometry"].to_wkt()

    logger.info("Updating tracks in database...")
    with engine.connect() as conn:
        tracks_geo.to_sql(
            name="ecmwf_tracks_geo",
            con=conn,
            schema="storms",
            if_exists="append",
            index=False,
            method=stratus.postgres_upsert,
            chunksize=chunksize,
        )
    logger.info(f"Successfully processed tracks -- {len(tracks_geo)} records.")

    return tracks_geo


def process_storms(dataset, engine, chunksize):
    """
    Retrieve 'storm' tracks and upload them to the database
    """
    logger.info("Processing storms...")

    storm_tracks = lens.ecmwf_storm.get_storms(dataset)
    with engine.connect() as conn:
        storm_tracks.to_sql(
            "ecmwf_storms",
            con=conn,
            schema="storms",
            if_exists="append",
            index=False,
            method=stratus.postgres_upsert,
            chunksize=chunksize,
        )

    logger.info(
        f"Successfully processed storms -- {len(storm_tracks)} records."
    )
    return storm_tracks


def run_ecmwf(
    mode,
    start_date,
    end_date,
    chunksize=10000,
):
    """
    Main function to orchestrate the execution of pipeline functions.

    Parameters
    ----------
    start_date Which date to start from
    end_date Which date to stop at
    mode [dev or prod]
    """

    coloredlogs.install(
        logger=logger,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    logger.info("Starting ECMWF ETL pipeline...")

    # Setting up engine
    engine = stratus.get_engine(stage=mode, write=True)

    # Automatically chunk by year if date range is greater than 1 year
    date_range = end_date - start_date
    if date_range.days > 365:
        logger.info(
            f"Date range spans {date_range.days} days. "
            "Processing year by year to manage memory."
        )

        current_start = start_date
        while current_start <= end_date:
            year_end = datetime(current_start.year, 12, 31)
            current_end = min(year_end, end_date)

            logger.info(
                f"Processing data from {current_start.date()} to {current_end.date()}"
            )

            try:
                dataset = lens.ecmwf_storm.load_forecasts(
                    start_date=current_start,
                    end_date=current_end,
                    use_cache=False,
                    stage=mode,
                    skip_if_missing=False,
                )
                process_storms(
                    dataset=dataset, engine=engine, chunksize=chunksize
                )
                process_tracks(
                    dataset=dataset, engine=engine, chunksize=chunksize
                )

            except Exception as e:
                logger.error(
                    f"An error occurred processing {current_start.year}: {e}",
                    exc_info=True,
                )
                raise

            current_start = datetime(current_start.year + 1, 1, 1)
    else:
        try:
            dataset = lens.ecmwf_storm.load_forecasts(
                start_date=start_date,
                end_date=end_date,
                use_cache=False,
                stage=mode,
                skip_if_missing=False,
            )
            process_storms(dataset=dataset, engine=engine, chunksize=chunksize)
            process_tracks(dataset=dataset, engine=engine, chunksize=chunksize)

        except Exception as e:
            logger.error(f"An error occurred: {e}", exc_info=True)
            raise

    logger.info("Pipeline successfully finished!")
