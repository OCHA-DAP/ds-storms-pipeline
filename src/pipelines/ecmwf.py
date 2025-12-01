#!/usr/bin/env python3
"""
ECMWF ETL pipeline
"""

import logging

import coloredlogs
import ocha_lens as lens
from dotenv import load_dotenv

load_dotenv()

import ocha_stratus as stratus  # noqa


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ecmwf_logger = logging.getLogger("ocha_lens.datasources.ecmwf_storm")
ecmwf_logger.setLevel(logging.DEBUG)
coloredlogs.install(
    logger=ecmwf_logger,
    level=logging.DEBUG,
    fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# def retrieve_ecmwf(
#     start_date, end_date, stage="local", save_to_blob=False, save_dir=None, use_cache=True
# ):
#     """
#     Download ECMWF data, upload raw to azure if needed and return loaded Dataset
#     """

#     logger.info(f"Retrieving from ECMWF {start_date} to {end_date} ...")
#     filename = f"ECMWF_{start_date.date()}_{end_date.date()}.csv"
#     file_path = f"{save_dir}/" + filename

#     if os.path.exists(file_path):
#         logger.info(f"Using file downloaded in {file_path}...")
#         df = pd.read_csv(file_path, parse_dates=["issued_time", "valid_time"])
#     else:
#         if stage == "local":
#             save_dir = Path(save_dir) if save_dir else Path("temp")
#             os.makedirs(save_dir, exist_ok=True)

#         date_list = rrule.rrule(
#             rrule.HOURLY,
#             dtstart=start_date,
#             until=end_date + timedelta(hours=12),
#             interval=12,
#         )

#         dfs = []
#         for date in date_list:
#             logger.info(f"Processing for {date}...")
#             raw_file = download_hindcasts(
#                 date, "storm", use_cache, False, stage
#             )
#             if raw_file:
#                 df = _process_cxml_to_df(raw_file, stage, "storm")
#                 if df is not None:
#                     dfs.append(df)
#         if len(dfs) > 0:
#             df = pd.concat(dfs)
#             df.to_csv(file_path, index=False, na_rep=None)

#             logger.info(f"Successfully wrote ECMWF data to {file_path}.")
#             return df
#         logger.error("No data available for input dates")
#         return None
#         """
#         df = lens.ecmwf_storm.load_hindcasts(
#             start_date=start_date,
#             end_date=end_date,
#             stage=stage,
#             temp_dir=save_dir,
#             use_cache=False
#         )
#         """


# if save_to_blob:
#     logger.info(f"Uploading {file_path} to Azure blob in {stage}...")
#     with open(file_path, "rb") as file:
#         data_to_upload = file.read()
#     stratus.upload_blob_data(
#         container_name="storm",
#         data=data_to_upload,
#         blob_name=f"ecmwf/{filename}",
#         stage=stage,
#     )
#     logger.info("Successfully uploaded to blob.")

# return df


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
        # Retrieve data from source and upload to blob if true
        # dataset = retrieve_ecmwf(
        #     start_date=start_date,
        #     end_date=end_date,
        #     stage=mode,
        #     save_to_blob=save_to_blob,
        #     save_dir=save_dir,
        # )
        dataset = lens.ecmwf_storm.load_hindcasts(
            start_date=start_date,
            end_date=end_date,
            use_cache=True,
            stage=mode,
            skip_if_missing=False,
            temp_dir="storm/",
        )

        # Process storms and add them to the database
        process_storms(dataset=dataset, engine=engine, chunksize=chunksize)

        # Process tracks and add them to the database
        process_tracks(dataset=dataset, engine=engine, chunksize=chunksize)

        logger.info("Pipeline successfully finished!")

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise
