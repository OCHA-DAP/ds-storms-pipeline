#!/usr/bin/env python3
"""
NHC (National Hurricane Center) ETL pipeline

Supports two modes:
1. Current storms: Real-time active tropical cyclones and WSP polygons
2. Archive: Historical ATCF A-deck data and WSP shapefiles
"""

import json
import logging
import warnings

import coloredlogs
import geopandas as gpd
import ocha_lens as lens
import pandas as pd
from dotenv import load_dotenv
from ocha_lens.utils.storm import calculate_wind_buffers_gdf, expand_quad_col
from sqlalchemy import text
from tqdm import tqdm

load_dotenv()

import ocha_stratus as stratus  # noqa

BUFFER_SPEEDS = [34, 50, 64]
_WIND_BUFFER_BATCH_SIZE = 50


logger = logging.getLogger(__name__)


def retrieve_nhc_current(stage="local", save_to_blob=False, save_dir=None):
    """
    Download current NHC storms JSON, optionally upload to Azure, and return loaded DataFrame.

    Parameters
    ----------
    stage : str
        Environment stage (local, dev, prod)
    save_to_blob : bool
        Whether to upload raw JSON to Azure blob storage
    save_dir : str
        Directory to save downloaded file

    Returns
    -------
    pd.DataFrame
        Raw NHC data with all track points, or None if no active storms
    """
    logger.info("Retrieving current storms from NHC...")

    path = lens.nhc.download_nhc(cache_dir=save_dir, use_cache=False)

    if path is None:
        logger.warning("No active storms currently.")
        return None

    logger.info(f"Successfully downloaded current storms to {path}.")

    if save_to_blob:
        logger.info(f"Uploading {path} to Azure blob in {stage}...")
        with open(path, "rb") as file:
            data_to_upload = file.read()

        blob_name = f"nhc/current/{path.name}"
        stratus.upload_blob_data(
            container_name="storm",
            data=data_to_upload,
            blob_name=blob_name,
            stage=stage,
        )
        logger.info("Successfully uploaded to blob.")

    return lens.nhc.load_nhc(file_path=path, use_cache=False)


def retrieve_nhc_archive(
    year, basin, stage="local", save_to_blob=False, save_dir=None
):
    """
    Download NHC archive data for a specific year and basin.

    Parameters
    ----------
    year : int
        Year to download (e.g., 2023, 2024)
    basin : str
        Basin code: "AL" (Atlantic), "EP" (Eastern Pacific), or "CP" (Central Pacific)
    stage : str
        Environment stage (local, dev, prod)
    save_to_blob : bool
        Whether to upload raw ATCF files to Azure blob storage
    save_dir : str
        Directory to save downloaded files

    Returns
    -------
    pd.DataFrame
        Raw NHC archive data with all track points, or None if no storms found
    """
    logger.info(f"Retrieving {year} {basin} archive from NHC...")

    paths = lens.nhc.download_nhc_archive(
        year=year, basin=basin, cache_dir=save_dir, use_cache=False
    )

    if not paths:
        logger.warning(f"No storms found for {year} {basin}.")
        return None

    logger.info(f"Successfully downloaded {len(paths)} archive files.")

    if save_to_blob:
        logger.info(
            f"Uploading {len(paths)} files to Azure blob in {stage}..."
        )
        for path in paths:
            with open(path, "rb") as file:
                data_to_upload = file.read()

            blob_name = f"nhc/archive/{year}/{path.name}"
            stratus.upload_blob_data(
                container_name="storm",
                data=data_to_upload,
                blob_name=blob_name,
                stage=stage,
            )
        logger.info("Successfully uploaded all files to blob.")

    return lens.nhc.load_nhc(
        year=year, basin=basin, cache_dir=save_dir, use_cache=False
    )


def process_tracks(df_raw, engine, chunksize):
    """
    Extract track data from raw NHC data and upload to database.

    Parameters
    ----------
    df_raw : pd.DataFrame
        Raw NHC data
    engine : sqlalchemy.Engine
        Database engine
    chunksize : int
        Number of rows per batch insert

    Returns
    -------
    gpd.GeoDataFrame
        Processed track data, or None if no data to process
    """
    if df_raw is None or len(df_raw) == 0:
        logger.warning("No raw data to process for tracks. Skipping.")
        return None

    logger.info("Extracting tracks...")
    tracks_geo = lens.nhc.get_tracks(df_raw)

    if len(tracks_geo) == 0:
        logger.warning("No tracks extracted. Skipping database upload.")
        return tracks_geo

    logger.info(f"Extracted {len(tracks_geo)} track points.")

    # Transform geometry to WKT for database compatibility
    logger.info("Transforming geometry to WKT...")
    with warnings.catch_warnings():
        # This is the intended behaviour, suppress the specific warning
        warnings.filterwarnings(
            "ignore", message="Geometry column does not contain geometry"
        )
        tracks_geo["geometry"] = tracks_geo["geometry"].to_wkt()

    # Convert quadrant radii lists to JSON strings
    logger.info("Converting quadrant radii to JSON strings...")
    for col in [
        "quadrant_radius_34",
        "quadrant_radius_50",
        "quadrant_radius_64",
    ]:
        tracks_geo[col] = tracks_geo[col].apply(
            lambda x: json.dumps(x) if isinstance(x, list) else None
        )

    logger.info("Updating tracks in database...")
    with engine.connect() as conn:
        tracks_geo.to_sql(
            name="nhc_tracks_geo",
            con=conn,
            schema="storms",
            if_exists="append",
            index=False,
            method=stratus.postgres_upsert,
            chunksize=chunksize,
        )
    logger.info("Successfully processed tracks.")

    return tracks_geo


def process_storms(df_raw, engine, chunksize):
    """
    Extract storm metadata from raw NHC data and upload to database.

    Parameters
    ----------
    df_raw : pd.DataFrame
        Raw NHC data
    engine : sqlalchemy.Engine
        Database engine
    chunksize : int
        Number of rows per batch insert

    Returns
    -------
    pd.DataFrame
        Storm metadata, or None if no data to process
    """
    if df_raw is None or len(df_raw) == 0:
        logger.warning("No raw data to process for storms. Skipping.")
        return None

    logger.info("Processing storms...")
    storms = lens.nhc.get_storms(df_raw)

    if len(storms) == 0:
        logger.warning("No storms extracted. Skipping database upload.")
        return storms

    logger.info(f"Extracted {len(storms)} storms.")

    with engine.connect() as conn:
        storms.to_sql(
            "nhc_storms",
            con=conn,
            schema="storms",
            if_exists="append",
            index=False,
            method=stratus.postgres_upsert,
            chunksize=chunksize,
        )

    logger.info("Successfully processed storms.")
    return storms


def process_wsp_polygons(gdf, engine, chunksize):
    """
    Upload WSP polygon data to the database.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        WSP polygons from lens.nhc.get_wsp()
    engine : sqlalchemy.Engine
        Database engine
    chunksize : int
        Number of rows per batch insert

    Returns
    -------
    gpd.GeoDataFrame
        Processed data, or None if no data to process
    """
    if gdf is None or len(gdf) == 0:
        logger.warning("No WSP polygon data to process. Skipping.")
        return None

    logger.info(f"Processing {len(gdf)} WSP polygon rows...")

    from shapely.geometry import box as shapely_box
    _world = shapely_box(-180, -90, 180, 90)

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", message="Geometry column does not contain geometry"
        )
        gdf = gdf.copy()
        gdf["geometry"] = gdf["geometry"].apply(
            lambda g: g.intersection(_world) if g is not None and not g.is_empty else g
        )
        gdf["geometry"] = gdf["geometry"].apply(
            lambda g: g.wkt if g is not None else None
        )

    with engine.connect() as conn:
        gdf.to_sql(
            name="nhc_wsp_polygon",
            con=conn,
            schema="storms",
            if_exists="append",
            index=False,
            method=stratus.postgres_upsert,
            chunksize=chunksize,
        )

    logger.info("Successfully processed WSP polygons.")
    return gdf


def run_nhc_current(
    mode="local", save_to_blob=False, save_dir="storm", chunksize=10000
):
    """
    Main function to process current NHC storms.

    Parameters
    ----------
    mode : str
        Environment stage: "local", "dev", or "prod"
    save_to_blob : bool
        Whether to upload raw files to Azure blob storage
    save_dir : str
        Directory to save downloaded files
    chunksize : int
        Number of rows per batch insert to database
    """
    coloredlogs.install(
        logger=logger,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    logger.info("Starting NHC Current Storms ETL pipeline...")

    # Setting up engine
    engine = stratus.get_engine(stage=mode, write=True)

    try:
        # Retrieve current storms data
        df_raw = retrieve_nhc_current(
            stage=mode,
            save_to_blob=save_to_blob,
            save_dir=save_dir,
        )

        if df_raw is None:
            logger.info("No active storms. Pipeline finished.")
            return

        # Process storms and add them to the database
        process_storms(df_raw=df_raw, engine=engine, chunksize=chunksize)

        # Process tracks and add them to the database
        process_tracks(df_raw=df_raw, engine=engine, chunksize=chunksize)

        # Process wind speed probability polygons
        logger.info("Fetching current WSP polygons...")
        wsp_gdf = lens.nhc.get_wsp()
        if wsp_gdf is not None and len(wsp_gdf) > 0:
            process_wsp_polygons(
                gdf=wsp_gdf, engine=engine, chunksize=chunksize
            )
        else:
            logger.info("No current WSP data available.")

        logger.info("Pipeline successfully finished!")

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise


def run_nhc_archive(
    start_year,
    end_year=None,
    mode="local",
    save_to_blob=False,
    save_dir="storm",
    chunksize=10000,
):
    """
    Main function to process NHC archive data for a year range (all basins).

    Parameters
    ----------
    start_year : int
        First year to process (e.g., 2020)
    end_year : int, optional
        Last year to process (e.g., 2024). If not provided, only processes start_year.
    mode : str
        Environment stage: "local", "dev", or "prod"
    save_to_blob : bool
        Whether to upload raw files to Azure blob storage
    save_dir : str
        Directory to save downloaded files
    chunksize : int
        Number of rows per batch insert to database
    """
    coloredlogs.install(
        logger=logger,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Determine year range
    if end_year is None:
        end_year = start_year
        logger.info(
            f"Starting NHC Archive ETL pipeline for {start_year} (all basins)..."
        )
    else:
        logger.info(
            f"Starting NHC Archive ETL pipeline for {start_year}-{end_year} (all basins)..."
        )

    # Setting up engine
    engine = stratus.get_engine(stage=mode, write=True)

    # Process all years in range
    basins = ["AL", "EP", "CP"]
    total_storms_processed = 0
    total_tracks_processed = 0

    for year in range(start_year, end_year + 1):
        logger.info(f"Processing year: {year}")

        year_storms = 0
        year_tracks = 0

        for basin in basins:
            logger.info(f"  Processing basin: {basin}")
            # Retrieve archive data for this basin/year
            df_raw = retrieve_nhc_archive(
                year=year,
                basin=basin,
                stage=mode,
                save_to_blob=save_to_blob,
                save_dir=save_dir,
            )

            if df_raw is None or len(df_raw) == 0:
                logger.info(f"  No storms found for {year} {basin}. Skipping.")
                continue

            # Process storms and add them to the database
            storms = process_storms(
                df_raw=df_raw, engine=engine, chunksize=chunksize
            )
            if storms is not None:
                year_storms += len(storms)
                total_storms_processed += len(storms)

            # Process tracks and add them to the database
            tracks = process_tracks(
                df_raw=df_raw, engine=engine, chunksize=chunksize
            )
            if tracks is not None:
                year_tracks += len(tracks)
                total_tracks_processed += len(tracks)

        logger.info(
            f"  Year {year} complete: {year_storms} storms, {year_tracks} track points"
        )

    logger.info(
        f"Processed {total_storms_processed} storms "
        f"and {total_tracks_processed} track points across {end_year - start_year + 1} year(s)."
    )

    # Process wind speed probability polygons for the full year range
    wsp_start = f"{start_year}-01-01"
    wsp_end = f"{end_year}-12-31"
    logger.info(f"Fetching WSP polygons for {wsp_start} to {wsp_end}...")
    wsp_gdf = lens.nhc.get_wsp(start=wsp_start, end=wsp_end)
    if wsp_gdf is not None and len(wsp_gdf) > 0:
        logger.info(
            f"Loaded {len(wsp_gdf)} WSP rows across {wsp_gdf['issued_time'].nunique()} issuances."
        )
        process_wsp_polygons(gdf=wsp_gdf, engine=engine, chunksize=chunksize)
    else:
        logger.info("No WSP data found for the specified date range.")

    logger.info("Pipeline successfully finished!")


# ---------------------------------------------------------------------------
# Wind buffer generation
# ---------------------------------------------------------------------------


def _load_nhc_tracks_fcast_buffer_tracks(
    engine,
    basin: str | None = None,
    start_year: int | None = None,
    issued_time=None,
) -> gpd.GeoDataFrame:
    filters = [
        """(quadrant_radius_34 IS NOT NULL
            OR quadrant_radius_50 IS NOT NULL
            OR quadrant_radius_64 IS NOT NULL)"""
    ]
    if basin:
        filters.append(f"basin = '{basin}'")
    if start_year:
        filters.append(f"EXTRACT(YEAR FROM issued_time) >= {start_year}")
    if issued_time is not None:
        filters.append(f"issued_time = '{issued_time}'")

    where = " AND ".join(filters)
    query = f"""
        SELECT atcf_id, basin, issued_time, valid_time,
               quadrant_radius_34,
               quadrant_radius_50,
               quadrant_radius_64,
               geometry
        FROM storms.nhc_tracks_geo
        WHERE {where}
        ORDER BY atcf_id, issued_time, valid_time
    """
    with engine.connect() as conn:
        return gpd.read_postgis(query, conn, geom_col="geometry")


def _load_existing_nhc_keys(engine) -> set:
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT DISTINCT atcf_id, issued_time FROM storms.nhc_tracks_fcast_buffers")
        )
        return {(row[0], row[1]) for row in result}


def _write_nhc_tracks_fcast_buffer_batch(batch, conn, cols, chunksize):
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
        name="nhc_tracks_fcast_buffers",
        con=conn,
        schema="storms",
        if_exists="append",
        index=False,
        method=stratus.postgres_upsert,
        chunksize=chunksize,
    )
    conn.commit()


def process_nhc_tracks_fcast_buffers(
    read_engine,
    write_engine,
    chunksize,
    basin=None,
    start_year=None,
    overwrite=False,
    issued_time=None,
):
    logger.info("Loading NHC tracks with wind radii...")
    gdf_tracks = _load_nhc_tracks_fcast_buffer_tracks(
        read_engine, basin=basin, start_year=start_year, issued_time=issued_time
    )
    n_issuances = gdf_tracks.groupby(["atcf_id", "issued_time"]).ngroups
    logger.info(
        f"Loaded {len(gdf_tracks)} track points across "
        f"{gdf_tracks['atcf_id'].nunique()} storms, {n_issuances} forecast issuances."
    )

    if not overwrite:
        existing = _load_existing_nhc_keys(write_engine)
        if existing:
            before = gdf_tracks.groupby(["atcf_id", "issued_time"]).ngroups
            mask = gdf_tracks.apply(
                lambda r: (r["atcf_id"], r["issued_time"]) not in existing, axis=1
            )
            gdf_tracks = gdf_tracks[mask]
            after = (
                gdf_tracks.groupby(["atcf_id", "issued_time"]).ngroups
                if not gdf_tracks.empty
                else 0
            )
            logger.info(
                f"Skipping {before - after} already-computed issuances. "
                f"{after} remaining."
            )
        if gdf_tracks.empty:
            logger.info("All issuances already computed. Nothing to do.")
            return

    logger.info("Expanding quadrant radius columns...")
    for speed in BUFFER_SPEEDS:
        gdf_tracks = expand_quad_col(gdf_tracks, f"quadrant_radius_{speed}")

    logger.info("Calculating and writing NHC wind buffers per forecast issuance...")
    cols = ["atcf_id", "issued_time", "wind_speed_kt", "geometry"]
    batch = []
    with write_engine.connect() as conn:
        for (atcf_id, it), group in tqdm(
            gdf_tracks.groupby(["atcf_id", "issued_time"]),
            unit="issuance",
            leave=False,
        ):
            gdf_buffers = calculate_wind_buffers_gdf(
                group, quad_cols_format="quadrant_radius_{speed}_{quad}"
            )
            gdf_buffers["atcf_id"] = atcf_id
            gdf_buffers["issued_time"] = it
            batch.append(gdf_buffers)

            if len(batch) >= _WIND_BUFFER_BATCH_SIZE:
                _write_nhc_tracks_fcast_buffer_batch(batch, conn, cols, chunksize)
                batch = []

        if batch:
            _write_nhc_tracks_fcast_buffer_batch(batch, conn, cols, chunksize)

    logger.info("Successfully wrote NHC wind buffers.")


def run_nhc_tracks_fcast_buffers(
    write_mode="dev",
    chunksize=1000,
    basin=None,
    start_year=None,
    overwrite=False,
    issued_time=None,
):
    coloredlogs.install(
        logger=logger,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger.info("Starting NHC wind buffers pipeline...")
    read_engine = stratus.get_engine(stage="prod")
    write_engine = stratus.get_engine(stage=write_mode, write=True)
    try:
        process_nhc_tracks_fcast_buffers(
            read_engine=read_engine,
            write_engine=write_engine,
            chunksize=chunksize,
            basin=basin,
            start_year=start_year,
            overwrite=overwrite,
            issued_time=issued_time,
        )
        logger.info("NHC wind buffers pipeline finished.")
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise


# ---------------------------------------------------------------------------
# Observational track cumulative buffers
# ---------------------------------------------------------------------------


def _load_nhc_tracks_obsv_buffer_tracks(
    engine,
    basin: str | None = None,
    start_year: int | None = None,
    issued_time=None,
) -> gpd.GeoDataFrame:
    base_filters = [
        "leadtime = 0",
        "(quadrant_radius_34 IS NOT NULL OR quadrant_radius_50 IS NOT NULL OR quadrant_radius_64 IS NOT NULL)",
    ]
    if issued_time is not None:
        # Load full history for storms that have a new advisory at this time
        filters = base_filters + [
            f"atcf_id IN ("
            f"SELECT DISTINCT atcf_id FROM storms.nhc_tracks_geo"
            f" WHERE leadtime = 0 AND issued_time = '{issued_time}')",
            f"issued_time <= '{issued_time}'",
        ]
    else:
        filters = base_filters[:]
        if basin:
            filters.append(f"basin = '{basin}'")
        if start_year:
            filters.append(f"EXTRACT(YEAR FROM issued_time) >= {start_year}")

    where = " AND ".join(filters)
    query = f"""
        SELECT atcf_id, basin, issued_time, issued_time AS valid_time,
               quadrant_radius_34, quadrant_radius_50, quadrant_radius_64,
               geometry
        FROM storms.nhc_tracks_geo
        WHERE {where}
        ORDER BY atcf_id, issued_time
    """
    with engine.connect() as conn:
        return gpd.read_postgis(query, conn, geom_col="geometry")


def _load_existing_nhc_obsv_keys(engine) -> set:
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT DISTINCT atcf_id, valid_time FROM storms.nhc_tracks_obsv_buffers")
        )
        return {(row[0], row[1]) for row in result}


def _write_nhc_tracks_obsv_buffer_batch(batch, conn, cols, chunksize):
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
        name="nhc_tracks_obsv_buffers",
        con=conn,
        schema="storms",
        if_exists="append",
        index=False,
        method=stratus.postgres_upsert,
        chunksize=chunksize,
    )
    conn.commit()


def process_nhc_tracks_obsv_buffers(
    read_engine,
    write_engine,
    chunksize,
    basin=None,
    start_year=None,
    overwrite=False,
    issued_time=None,
):
    logger.info("Loading NHC observational (leadtime=0) track points...")
    gdf_obsv = _load_nhc_tracks_obsv_buffer_tracks(
        read_engine, basin=basin, start_year=start_year, issued_time=issued_time
    )
    if gdf_obsv.empty:
        logger.info("No observational track points found. Nothing to do.")
        return
    logger.info(
        f"Loaded {len(gdf_obsv)} obs points across {gdf_obsv['atcf_id'].nunique()} storms."
    )

    existing = set() if overwrite else _load_existing_nhc_obsv_keys(write_engine)

    logger.info("Expanding quadrant radius columns...")
    for speed in BUFFER_SPEEDS:
        gdf_obsv = expand_quad_col(gdf_obsv, f"quadrant_radius_{speed}")

    logger.info("Calculating and writing cumulative NHC observational track buffers...")
    cols = ["atcf_id", "valid_time", "wind_speed_kt", "geometry"]
    batch = []
    with write_engine.connect() as conn:
        for atcf_id, storm_gdf in tqdm(
            gdf_obsv.groupby("atcf_id"),
            unit="storm",
            leave=False,
        ):
            sorted_times = sorted(storm_gdf["issued_time"].unique())
            for t in sorted_times:
                if issued_time is not None and t != issued_time:
                    continue
                if (atcf_id, t) in existing:
                    continue
                cumulative = storm_gdf[storm_gdf["issued_time"] <= t]
                gdf_buffers = calculate_wind_buffers_gdf(
                    cumulative, quad_cols_format="quadrant_radius_{speed}_{quad}"
                )
                gdf_buffers["atcf_id"] = atcf_id
                gdf_buffers["valid_time"] = t
                batch.append(gdf_buffers)

                if len(batch) >= _WIND_BUFFER_BATCH_SIZE:
                    _write_nhc_tracks_obsv_buffer_batch(batch, conn, cols, chunksize)
                    batch = []

        if batch:
            _write_nhc_tracks_obsv_buffer_batch(batch, conn, cols, chunksize)

    logger.info("Successfully wrote NHC observational track buffers.")


def run_nhc_tracks_obsv_buffers(
    write_mode="dev",
    chunksize=1000,
    basin=None,
    start_year=None,
    overwrite=False,
    issued_time=None,
):
    coloredlogs.install(
        logger=logger,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger.info("Starting NHC observational track buffers pipeline...")
    read_engine = stratus.get_engine(stage="prod")
    write_engine = stratus.get_engine(stage=write_mode, write=True)
    try:
        process_nhc_tracks_obsv_buffers(
            read_engine=read_engine,
            write_engine=write_engine,
            chunksize=chunksize,
            basin=basin,
            start_year=start_year,
            overwrite=overwrite,
            issued_time=issued_time,
        )
        logger.info("NHC observational track buffers pipeline finished.")
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise


# ---------------------------------------------------------------------------
# Forecast-only buffers (fcast minus observed swath)
# ---------------------------------------------------------------------------

_FCASTONLY_BATCH_SIZE = _WIND_BUFFER_BATCH_SIZE * 3


def _load_nhc_fcastonly_inputs(
    engine,
    basin: str | None = None,
    start_year: int | None = None,
    issued_time=None,
) -> pd.DataFrame:
    filters = []
    joins = ""
    if issued_time is not None:
        filters.append(f"f.issued_time = '{issued_time}'")
    else:
        if basin:
            joins = " JOIN storms.nhc_storms s ON f.atcf_id = s.atcf_id"
            filters.append(f"s.genesis_basin = '{basin}'")
        if start_year:
            filters.append(f"EXTRACT(YEAR FROM f.issued_time) >= {start_year}")

    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    query = f"""
        SELECT f.atcf_id, f.issued_time, f.wind_speed_kt,
               ST_AsText(f.geometry) AS fcast_geom,
               ST_AsText(o.geometry) AS obsv_geom
        FROM storms.nhc_tracks_fcast_buffers f{joins}
        LEFT JOIN storms.nhc_tracks_obsv_buffers o
            ON f.atcf_id = o.atcf_id
           AND f.issued_time = o.valid_time
           AND f.wind_speed_kt = o.wind_speed_kt
        {where}
        ORDER BY f.atcf_id, f.issued_time, f.wind_speed_kt
    """
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)


def _load_existing_nhc_fcastonly_keys(engine) -> set:
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT DISTINCT atcf_id, issued_time FROM storms.nhc_tracks_fcastonly_buffers")
        )
        return {(row[0], row[1]) for row in result}


def _write_nhc_tracks_fcastonly_buffer_batch(batch: list[dict], conn, chunksize: int):
    df = pd.DataFrame(batch)
    df.to_sql(
        name="nhc_tracks_fcastonly_buffers",
        con=conn,
        schema="storms",
        if_exists="append",
        index=False,
        method=stratus.postgres_upsert,
        chunksize=chunksize,
    )
    conn.commit()


def process_nhc_tracks_fcastonly_buffers(
    read_engine,
    write_engine,
    chunksize,
    basin=None,
    start_year=None,
    overwrite=False,
    issued_time=None,
):
    from shapely import wkt as shapely_wkt

    logger.info("Loading fcast and obsv buffer inputs...")
    df = _load_nhc_fcastonly_inputs(
        read_engine, basin=basin, start_year=start_year, issued_time=issued_time
    )
    if df.empty:
        logger.info("No forecast buffers found. Nothing to do.")
        return
    logger.info(f"Loaded {len(df)} rows across {df['atcf_id'].nunique()} storms.")

    existing = set() if overwrite else _load_existing_nhc_fcastonly_keys(write_engine)
    warned: set = set()
    batch: list[dict] = []

    with write_engine.connect() as conn:
        for _, row in df.iterrows():
            key = (row["atcf_id"], row["issued_time"])
            if key in existing:
                continue

            fcast_geom = shapely_wkt.loads(row["fcast_geom"])

            if pd.isna(row["obsv_geom"]) or row["obsv_geom"] is None:
                if key not in warned:
                    logger.warning(
                        f"No obsv buffer for {row['atcf_id']} at {row['issued_time']} "
                        f"— storing full forecast geometry"
                    )
                    warned.add(key)
                result_geom = fcast_geom
            else:
                obsv_geom = shapely_wkt.loads(row["obsv_geom"])
                diff = fcast_geom.difference(obsv_geom)
                result_geom = None if diff.is_empty else diff

            batch.append({
                "atcf_id": row["atcf_id"],
                "issued_time": row["issued_time"],
                "wind_speed_kt": row["wind_speed_kt"],
                "geometry": result_geom.wkt if result_geom is not None else None,
            })

            if len(batch) >= _FCASTONLY_BATCH_SIZE:
                _write_nhc_tracks_fcastonly_buffer_batch(batch, conn, chunksize)
                batch = []

        if batch:
            _write_nhc_tracks_fcastonly_buffer_batch(batch, conn, chunksize)

    logger.info("Successfully wrote NHC forecast-only track buffers.")


def run_nhc_tracks_fcastonly_buffers(
    write_mode="dev",
    chunksize=1000,
    basin=None,
    start_year=None,
    overwrite=False,
    issued_time=None,
):
    coloredlogs.install(
        logger=logger,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger.info("Starting NHC forecast-only track buffers pipeline...")
    # Both input tables (fcast_buffers, obsv_buffers) live in the same DB as the output
    engine = stratus.get_engine(stage=write_mode, write=True)
    try:
        process_nhc_tracks_fcastonly_buffers(
            read_engine=engine,
            write_engine=engine,
            chunksize=chunksize,
            basin=basin,
            start_year=start_year,
            overwrite=overwrite,
            issued_time=issued_time,
        )
        logger.info("NHC forecast-only track buffers pipeline finished.")
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise


# ---------------------------------------------------------------------------
# Population exposure — NHC wind buffers
# ---------------------------------------------------------------------------

_TRACK_EXP_KEY_COLS = ["atcf_id", "issued_time", "wind_speed_kt", "admin_level", "pcode"]
_WSP_EXP_KEY_COLS = ["issued_time", "wind_threshold_kt", "percentage", "atcf_id", "admin_level", "pcode"]
_EXP_ADMIN_LEVEL = 0


def _load_nhc_tracks_fcast_exp_buffers(
    engine,
    since: str | None = None,
    basin: str | None = None,
    issued_time=None,
) -> gpd.GeoDataFrame:
    filters = []
    if since:
        filters.append(f"b.issued_time >= '{since}'")
    if basin:
        filters.append(f"s.genesis_basin = '{basin}'")
    if issued_time is not None:
        filters.append(f"b.issued_time = '{issued_time}'")

    if basin:
        where = "WHERE " + " AND ".join(filters)
        query = (
            f"SELECT b.atcf_id, b.issued_time, b.wind_speed_kt, b.geometry"
            f" FROM storms.nhc_tracks_fcast_buffers b"
            f" JOIN storms.nhc_storms s ON b.atcf_id = s.atcf_id"
            f" {where}"
        )
    else:
        where = ("WHERE " + " AND ".join(filters)) if filters else ""
        query = (
            f"SELECT atcf_id, issued_time, wind_speed_kt, geometry"
            f" FROM storms.nhc_tracks_fcast_buffers {where}"
        )

    with engine.connect() as conn:
        return gpd.read_postgis(query, conn, geom_col="geometry")


def _load_done_nhc_tracks_fcast_exp(engine) -> pd.DataFrame:
    try:
        with engine.connect() as conn:
            return pd.read_sql(
                text(
                    "SELECT atcf_id, issued_time, wind_speed_kt, admin_level, pcode"
                    " FROM storms.nhc_tracks_fcast_exposure"
                    f" WHERE admin_level = {_EXP_ADMIN_LEVEL}"
                ),
                conn,
            )
    except Exception:
        return pd.DataFrame(columns=_TRACK_EXP_KEY_COLS)


def _filter_done_nhc_tracks_fcast(buffers: gpd.GeoDataFrame, done_country: pd.DataFrame) -> gpd.GeoDataFrame:
    merge_cols = ["atcf_id", "issued_time", "wind_speed_kt"]
    merged = buffers[merge_cols].merge(
        done_country[merge_cols].drop_duplicates().assign(_done=True),
        on=merge_cols,
        how="left",
    )
    return buffers[merged["_done"].isna().values].reset_index(drop=True)


def run_nhc_tracks_fcast_exp(
    countries: list[str] | None = None,
    since: str | None = None,
    basin: str | None = None,
    overwrite: bool = False,
    mode: str = "dev",
    issued_time=None,
) -> None:
    import warnings
    from rasterio.errors import ShapeSkipWarning
    from src.utils.exposure import GEO_CRS_ANTIMERIDIAN, calculate_exposure, load_adm1, load_pop

    warnings.filterwarnings("ignore", category=ShapeSkipWarning)
    engine = stratus.get_engine(stage=mode, write=True)

    logger.info("Loading NHC wind buffers for exposure calculation...")
    gdf_buffers = _load_nhc_tracks_fcast_exp_buffers(engine, since=since, basin=basin, issued_time=issued_time)
    if gdf_buffers.empty:
        logger.info("No wind buffers found for the given filters. Skipping.")
        return
    gdf_buffers_anti = gdf_buffers.to_crs(GEO_CRS_ANTIMERIDIAN)

    gdf_adm1 = load_adm1(countries)
    country_list = sorted(gdf_adm1["iso_3"].unique())
    logger.info(f"Processing {len(country_list)} countries...")

    da_wp_global, da_wp_wrapped = load_pop()

    done_df = pd.DataFrame(columns=_TRACK_EXP_KEY_COLS) if overwrite else _load_done_nhc_tracks_fcast_exp(engine)

    buffers_sindex = gdf_buffers.sindex
    processed = skipped = 0
    for i, iso3 in enumerate(country_list, 1):
        prefix = f"[{i}/{len(country_list)}] {iso3}"

        adm_geom = gdf_adm1[gdf_adm1["iso_3"] == iso3][["geometry"]].dissolve().iloc[0].geometry
        minx, _, maxx, _ = adm_geom.bounds
        wrap = maxx > 160 or minx < -160

        if wrap:
            da_wp = da_wp_wrapped
            adm_geom = gpd.GeoSeries([adm_geom], crs=4326).to_crs(GEO_CRS_ANTIMERIDIAN).iloc[0]
            buf_in = gdf_buffers_anti[gdf_buffers_anti.intersects(adm_geom)]
        else:
            da_wp = da_wp_global
            candidate_idx = list(buffers_sindex.intersection(adm_geom.bounds))
            if not candidate_idx:
                skipped += 1
                continue
            candidates = gdf_buffers.iloc[candidate_idx]
            buf_in = candidates[candidates.intersects(adm_geom)]

        if buf_in.empty:
            skipped += 1
            continue

        if not overwrite and not done_df.empty:
            done_country = done_df[done_df["pcode"] == iso3]
            if not done_country.empty:
                buf_in = _filter_done_nhc_tracks_fcast(buf_in, done_country)
                if buf_in.empty:
                    skipped += 1
                    logger.info(f"{prefix} — all done, skipping")
                    continue

        logger.info(f"{prefix} — {len(buf_in)} intersecting")

        da_wp_country = da_wp.rio.clip([adm_geom], all_touched=True)
        df = calculate_exposure(buf_in, da_wp_country)
        df["iso3"] = iso3
        df["pcode"] = iso3
        df["admin_level"] = _EXP_ADMIN_LEVEL
        del da_wp_country

        out = df.drop_duplicates(subset=_TRACK_EXP_KEY_COLS, keep="last")
        with engine.connect() as conn:
            out.to_sql(
                "nhc_tracks_fcast_exposure",
                conn,
                schema="storms",
                if_exists="append",
                index=False,
                method=stratus.postgres_upsert,
            )
            conn.commit()
        processed += 1

    logger.info(f"NHC wind exposure done: {processed} written, {skipped} skipped.")
    engine.dispose()


# ---------------------------------------------------------------------------
# Population exposure — NHC observed track buffers
# ---------------------------------------------------------------------------

_OBSV_EXP_KEY_COLS = ["atcf_id", "valid_time", "wind_speed_kt", "admin_level", "pcode"]


def _load_nhc_tracks_obsv_exp_buffers(
    engine,
    since: str | None = None,
    basin: str | None = None,
    valid_time=None,
) -> gpd.GeoDataFrame:
    filters = []
    if since:
        filters.append(f"b.valid_time >= '{since}'")
    if basin:
        filters.append(f"s.genesis_basin = '{basin}'")
    if valid_time is not None:
        filters.append(f"b.valid_time = '{valid_time}'")

    if basin:
        where = "WHERE " + " AND ".join(filters)
        query = (
            f"SELECT b.atcf_id, b.valid_time, b.wind_speed_kt, b.geometry"
            f" FROM storms.nhc_tracks_obsv_buffers b"
            f" JOIN storms.nhc_storms s ON b.atcf_id = s.atcf_id"
            f" {where}"
        )
    else:
        where = ("WHERE " + " AND ".join(filters)) if filters else ""
        query = (
            f"SELECT atcf_id, valid_time, wind_speed_kt, geometry"
            f" FROM storms.nhc_tracks_obsv_buffers {where}"
        )

    with engine.connect() as conn:
        return gpd.read_postgis(query, conn, geom_col="geometry")


def _load_done_nhc_tracks_obsv_exp(engine) -> pd.DataFrame:
    try:
        with engine.connect() as conn:
            return pd.read_sql(
                text(
                    "SELECT atcf_id, valid_time, wind_speed_kt, admin_level, pcode"
                    " FROM storms.nhc_tracks_obsv_exposure"
                    f" WHERE admin_level = {_EXP_ADMIN_LEVEL}"
                ),
                conn,
            )
    except Exception:
        return pd.DataFrame(columns=_OBSV_EXP_KEY_COLS)


def _filter_done_nhc_tracks_obsv(buffers: gpd.GeoDataFrame, done_country: pd.DataFrame) -> gpd.GeoDataFrame:
    merge_cols = ["atcf_id", "valid_time", "wind_speed_kt"]
    merged = buffers[merge_cols].merge(
        done_country[merge_cols].drop_duplicates().assign(_done=True),
        on=merge_cols,
        how="left",
    )
    return buffers[merged["_done"].isna().values].reset_index(drop=True)


def run_nhc_tracks_obsv_exp(
    countries: list[str] | None = None,
    since: str | None = None,
    basin: str | None = None,
    overwrite: bool = False,
    mode: str = "dev",
    valid_time=None,
) -> None:
    import warnings
    from rasterio.errors import ShapeSkipWarning
    from src.utils.exposure import GEO_CRS_ANTIMERIDIAN, calculate_exposure, load_adm1, load_pop

    warnings.filterwarnings("ignore", category=ShapeSkipWarning)
    engine = stratus.get_engine(stage=mode, write=True)

    logger.info("Loading NHC observed track buffers for exposure calculation...")
    gdf_buffers = _load_nhc_tracks_obsv_exp_buffers(engine, since=since, basin=basin, valid_time=valid_time)
    if gdf_buffers.empty:
        logger.info("No observed track buffers found for the given filters. Skipping.")
        return
    gdf_buffers_anti = gdf_buffers.to_crs(GEO_CRS_ANTIMERIDIAN)

    gdf_adm1 = load_adm1(countries)
    country_list = sorted(gdf_adm1["iso_3"].unique())
    logger.info(f"Processing {len(country_list)} countries...")

    da_wp_global, da_wp_wrapped = load_pop()

    done_df = pd.DataFrame(columns=_OBSV_EXP_KEY_COLS) if overwrite else _load_done_nhc_tracks_obsv_exp(engine)

    buffers_sindex = gdf_buffers.sindex
    processed = skipped = 0
    for i, iso3 in enumerate(country_list, 1):
        prefix = f"[{i}/{len(country_list)}] {iso3}"

        adm_geom = gdf_adm1[gdf_adm1["iso_3"] == iso3][["geometry"]].dissolve().iloc[0].geometry
        minx, _, maxx, _ = adm_geom.bounds
        wrap = maxx > 160 or minx < -160

        if wrap:
            da_wp = da_wp_wrapped
            adm_geom = gpd.GeoSeries([adm_geom], crs=4326).to_crs(GEO_CRS_ANTIMERIDIAN).iloc[0]
            buf_in = gdf_buffers_anti[gdf_buffers_anti.intersects(adm_geom)]
        else:
            da_wp = da_wp_global
            candidate_idx = list(buffers_sindex.intersection(adm_geom.bounds))
            if not candidate_idx:
                skipped += 1
                continue
            candidates = gdf_buffers.iloc[candidate_idx]
            buf_in = candidates[candidates.intersects(adm_geom)]

        if buf_in.empty:
            skipped += 1
            continue

        if not overwrite and not done_df.empty:
            done_country = done_df[done_df["pcode"] == iso3]
            if not done_country.empty:
                buf_in = _filter_done_nhc_tracks_obsv(buf_in, done_country)
                if buf_in.empty:
                    skipped += 1
                    logger.info(f"{prefix} — all done, skipping")
                    continue

        logger.info(f"{prefix} — {len(buf_in)} intersecting")

        da_wp_country = da_wp.rio.clip([adm_geom], all_touched=True)
        df = calculate_exposure(buf_in, da_wp_country)
        df["iso3"] = iso3
        df["pcode"] = iso3
        df["admin_level"] = _EXP_ADMIN_LEVEL
        del da_wp_country

        out = df.drop_duplicates(subset=_OBSV_EXP_KEY_COLS, keep="last")
        with engine.connect() as conn:
            out.to_sql(
                "nhc_tracks_obsv_exposure",
                conn,
                schema="storms",
                if_exists="append",
                index=False,
                method=stratus.postgres_upsert,
            )
            conn.commit()
        processed += 1

    logger.info(f"NHC observed track exposure done: {processed} written, {skipped} skipped.")
    engine.dispose()


# ---------------------------------------------------------------------------
# Population exposure — NHC forecast-only track buffers
# ---------------------------------------------------------------------------

_FCASTONLY_EXP_KEY_COLS = ["atcf_id", "issued_time", "wind_speed_kt", "admin_level", "pcode"]


def _load_nhc_tracks_fcastonly_exp_buffers(
    engine,
    since: str | None = None,
    basin: str | None = None,
    issued_time=None,
) -> gpd.GeoDataFrame:
    filters = []
    if since:
        filters.append(f"b.issued_time >= '{since}'")
    if basin:
        filters.append(f"s.genesis_basin = '{basin}'")
    if issued_time is not None:
        filters.append(f"b.issued_time = '{issued_time}'")

    if basin:
        where = "WHERE " + " AND ".join(filters)
        query = (
            f"SELECT b.atcf_id, b.issued_time, b.wind_speed_kt, b.geometry"
            f" FROM storms.nhc_tracks_fcastonly_buffers b"
            f" JOIN storms.nhc_storms s ON b.atcf_id = s.atcf_id"
            f" {where}"
        )
    else:
        where = ("WHERE " + " AND ".join(filters)) if filters else ""
        query = (
            f"SELECT atcf_id, issued_time, wind_speed_kt, geometry"
            f" FROM storms.nhc_tracks_fcastonly_buffers {where}"
        )

    with engine.connect() as conn:
        return gpd.read_postgis(query, conn, geom_col="geometry")


def _load_done_nhc_tracks_fcastonly_exp(engine) -> pd.DataFrame:
    try:
        with engine.connect() as conn:
            return pd.read_sql(
                text(
                    "SELECT atcf_id, issued_time, wind_speed_kt, admin_level, pcode"
                    " FROM storms.nhc_tracks_fcastonly_exposure"
                    f" WHERE admin_level = {_EXP_ADMIN_LEVEL}"
                ),
                conn,
            )
    except Exception:
        return pd.DataFrame(columns=_FCASTONLY_EXP_KEY_COLS)


def _filter_done_nhc_tracks_fcastonly(buffers: gpd.GeoDataFrame, done_country: pd.DataFrame) -> gpd.GeoDataFrame:
    merge_cols = ["atcf_id", "issued_time", "wind_speed_kt"]
    merged = buffers[merge_cols].merge(
        done_country[merge_cols].drop_duplicates().assign(_done=True),
        on=merge_cols,
        how="left",
    )
    return buffers[merged["_done"].isna().values].reset_index(drop=True)


def run_nhc_tracks_fcastonly_exp(
    countries: list[str] | None = None,
    since: str | None = None,
    basin: str | None = None,
    overwrite: bool = False,
    mode: str = "dev",
    issued_time=None,
) -> None:
    import warnings
    from rasterio.errors import ShapeSkipWarning
    from src.utils.exposure import GEO_CRS_ANTIMERIDIAN, calculate_exposure, load_adm1, load_pop

    warnings.filterwarnings("ignore", category=ShapeSkipWarning)
    engine = stratus.get_engine(stage=mode, write=True)

    logger.info("Loading NHC forecast-only track buffers for exposure calculation...")
    gdf_buffers = _load_nhc_tracks_fcastonly_exp_buffers(engine, since=since, basin=basin, issued_time=issued_time)
    if gdf_buffers.empty:
        logger.info("No forecast-only track buffers found for the given filters. Skipping.")
        return
    gdf_buffers_anti = gdf_buffers.to_crs(GEO_CRS_ANTIMERIDIAN)

    gdf_adm1 = load_adm1(countries)
    country_list = sorted(gdf_adm1["iso_3"].unique())
    logger.info(f"Processing {len(country_list)} countries...")

    da_wp_global, da_wp_wrapped = load_pop()

    done_df = pd.DataFrame(columns=_FCASTONLY_EXP_KEY_COLS) if overwrite else _load_done_nhc_tracks_fcastonly_exp(engine)

    buffers_sindex = gdf_buffers.sindex
    processed = skipped = 0
    for i, iso3 in enumerate(country_list, 1):
        prefix = f"[{i}/{len(country_list)}] {iso3}"

        adm_geom = gdf_adm1[gdf_adm1["iso_3"] == iso3][["geometry"]].dissolve().iloc[0].geometry
        minx, _, maxx, _ = adm_geom.bounds
        wrap = maxx > 160 or minx < -160

        if wrap:
            da_wp = da_wp_wrapped
            adm_geom = gpd.GeoSeries([adm_geom], crs=4326).to_crs(GEO_CRS_ANTIMERIDIAN).iloc[0]
            buf_in = gdf_buffers_anti[gdf_buffers_anti.intersects(adm_geom)]
        else:
            da_wp = da_wp_global
            candidate_idx = list(buffers_sindex.intersection(adm_geom.bounds))
            if not candidate_idx:
                skipped += 1
                continue
            candidates = gdf_buffers.iloc[candidate_idx]
            buf_in = candidates[candidates.intersects(adm_geom)]

        if buf_in.empty:
            skipped += 1
            continue

        if not overwrite and not done_df.empty:
            done_country = done_df[done_df["pcode"] == iso3]
            if not done_country.empty:
                buf_in = _filter_done_nhc_tracks_fcastonly(buf_in, done_country)
                if buf_in.empty:
                    skipped += 1
                    logger.info(f"{prefix} — all done, skipping")
                    continue

        logger.info(f"{prefix} — {len(buf_in)} intersecting")

        da_wp_country = da_wp.rio.clip([adm_geom], all_touched=True)
        df = calculate_exposure(buf_in, da_wp_country)
        df["iso3"] = iso3
        df["pcode"] = iso3
        df["admin_level"] = _EXP_ADMIN_LEVEL
        del da_wp_country

        out = df.drop_duplicates(subset=_FCASTONLY_EXP_KEY_COLS, keep="last")
        with engine.connect() as conn:
            out.to_sql(
                "nhc_tracks_fcastonly_exposure",
                conn,
                schema="storms",
                if_exists="append",
                index=False,
                method=stratus.postgres_upsert,
            )
            conn.commit()
        processed += 1

    logger.info(f"NHC forecast-only track exposure done: {processed} written, {skipped} skipped.")
    engine.dispose()


# ---------------------------------------------------------------------------
# Population exposure — NHC WSP polygons
# ---------------------------------------------------------------------------


def _load_wsp_for_exposure(
    engine,
    since: str | None = None,
    basin: str | None = None,
    issued_time=None,
) -> gpd.GeoDataFrame:
    from ocha_lens.utils.storm import match_wsp_to_tracks

    track_filters = []
    if since:
        track_filters.append(f"issued_time >= '{since}'")
    if basin:
        track_filters.append(f"basin = '{basin}'")
    if issued_time is not None:
        track_filters.append(f"issued_time = '{issued_time}'")

    track_where = ("WHERE " + " AND ".join(track_filters)) if track_filters else ""
    wsp_time_filter = (
        f"WHERE issued_time IN (SELECT DISTINCT issued_time FROM storms.nhc_tracks_geo {track_where})"
    )

    with engine.connect() as conn:
        gdf_wsp_raw = gpd.read_postgis(
            f"SELECT id, issued_time, wind_threshold_kt, percentage, geometry"
            f" FROM storms.nhc_wsp_polygon {wsp_time_filter}",
            conn,
            geom_col="geometry",
        )
        track_query = (
            f"SELECT atcf_id, issued_time, geometry FROM storms.nhc_tracks_geo"
            f" {track_where}"
            f" AND issued_time IN (SELECT DISTINCT issued_time FROM storms.nhc_wsp_polygon {wsp_time_filter})"
            if track_where
            else
            f"SELECT atcf_id, issued_time, geometry FROM storms.nhc_tracks_geo"
            f" WHERE issued_time IN (SELECT DISTINCT issued_time FROM storms.nhc_wsp_polygon)"
        )
        gdf_tracks = gpd.read_postgis(track_query, conn, geom_col="geometry")

    logger.info(f"  {len(gdf_wsp_raw)} WSP polygons, {len(gdf_tracks)} track points")
    gdf_wsp = match_wsp_to_tracks(gdf_wsp_raw, gdf_tracks)
    n_matched = gdf_wsp["atcf_id"].notna().sum()
    logger.info(f"  {n_matched}/{len(gdf_wsp)} polygons matched to an ATCF ID")
    return gdf_wsp


def _load_done_nhc_wsp_exp(engine) -> pd.DataFrame:
    try:
        with engine.connect() as conn:
            return pd.read_sql(
                text(
                    "SELECT issued_time, wind_threshold_kt, percentage, atcf_id, admin_level, pcode"
                    " FROM storms.nhc_wsp_exposure"
                    f" WHERE admin_level = {_EXP_ADMIN_LEVEL}"
                ),
                conn,
            )
    except Exception:
        return pd.DataFrame(columns=_WSP_EXP_KEY_COLS)


def _filter_done_nhc_wsp(wsp: gpd.GeoDataFrame, done_country: pd.DataFrame) -> gpd.GeoDataFrame:
    merge_cols = ["issued_time", "wind_threshold_kt", "percentage", "atcf_id"]
    SENTINEL = "__null__"
    wsp_keys = wsp[merge_cols].assign(atcf_id=wsp["atcf_id"].fillna(SENTINEL))
    done_keys = done_country[merge_cols].assign(atcf_id=done_country["atcf_id"].fillna(SENTINEL))
    merged = wsp_keys.merge(
        done_keys.drop_duplicates().assign(_done=True),
        on=merge_cols,
        how="left",
    )
    return wsp[merged["_done"].isna().values].reset_index(drop=True)


def run_nhc_wsp_exp(
    countries: list[str] | None = None,
    since: str | None = None,
    basin: str | None = None,
    overwrite: bool = False,
    mode: str = "dev",
    issued_time=None,
) -> None:
    import warnings
    from rasterio.errors import ShapeSkipWarning
    from src.utils.exposure import GEO_CRS_ANTIMERIDIAN, calculate_exposure, load_adm1, load_pop

    warnings.filterwarnings("ignore", category=ShapeSkipWarning)
    engine = stratus.get_engine(stage=mode, write=True)

    logger.info("Loading WSP polygons for exposure calculation...")
    gdf_wsp = _load_wsp_for_exposure(engine, since=since, basin=basin, issued_time=issued_time)
    if gdf_wsp.empty:
        logger.info("No WSP polygons found for the given filters. Skipping.")
        return
    gdf_wsp_anti = gdf_wsp.to_crs(GEO_CRS_ANTIMERIDIAN)

    gdf_adm1 = load_adm1(countries)
    country_list = sorted(gdf_adm1["iso_3"].unique())
    logger.info(f"Processing {len(country_list)} countries...")

    da_wp_global, da_wp_wrapped = load_pop()

    done_df = pd.DataFrame(columns=_WSP_EXP_KEY_COLS) if overwrite else _load_done_nhc_wsp_exp(engine)

    wsp_sindex = gdf_wsp.sindex
    processed = skipped = 0
    for i, iso3 in enumerate(country_list, 1):
        prefix = f"[{i}/{len(country_list)}] {iso3}"

        adm_geom = gdf_adm1[gdf_adm1["iso_3"] == iso3][["geometry"]].dissolve().iloc[0].geometry
        minx, _, maxx, _ = adm_geom.bounds
        wrap = maxx > 160 or minx < -160

        if wrap:
            da_wp = da_wp_wrapped
            adm_geom = gpd.GeoSeries([adm_geom], crs=4326).to_crs(GEO_CRS_ANTIMERIDIAN).iloc[0]
            wsp_in = gdf_wsp_anti[gdf_wsp_anti.intersects(adm_geom)]
        else:
            da_wp = da_wp_global
            candidate_idx = list(wsp_sindex.intersection(adm_geom.bounds))
            if not candidate_idx:
                skipped += 1
                continue
            candidates = gdf_wsp.iloc[candidate_idx]
            wsp_in = candidates[candidates.intersects(adm_geom)]

        if wsp_in.empty:
            skipped += 1
            continue

        if not overwrite and not done_df.empty:
            done_country = done_df[done_df["pcode"] == iso3]
            if not done_country.empty:
                wsp_in = _filter_done_nhc_wsp(wsp_in, done_country)
                if wsp_in.empty:
                    skipped += 1
                    logger.info(f"{prefix} — all done, skipping")
                    continue

        logger.info(f"{prefix} — {len(wsp_in)} intersecting, calculating...")

        da_wp_country = da_wp.rio.clip([adm_geom], all_touched=True)
        df = calculate_exposure(wsp_in, da_wp_country)
        df["iso3"] = iso3
        df["pcode"] = iso3
        df["admin_level"] = _EXP_ADMIN_LEVEL
        del da_wp_country

        out = df.drop(columns=["id"], errors="ignore")
        out = out.drop_duplicates(subset=_WSP_EXP_KEY_COLS, keep="last")
        with engine.connect() as conn:
            out.to_sql(
                "nhc_wsp_exposure",
                conn,
                schema="storms",
                if_exists="append",
                index=False,
                method=stratus.postgres_upsert,
            )
            conn.commit()
        n_exposed = int((df["pop_exposed"] > 0).sum())
        logger.info(f"{prefix} — done ({n_exposed} rows with pop > 0)")
        processed += 1

    logger.info(f"WSP exposure done: {processed} written, {skipped} already done.")
    engine.dispose()


# ---------------------------------------------------------------------------
# WSP forecast-only polygons (WSP minus observed track swath)
# ---------------------------------------------------------------------------

_WSP_FCASTONLY_BATCH_SIZE = 500
_WSP_OBSV_OFFSET_HOURS = 3


def _load_obsv_buffer_lookup(engine, atcf_ids: list[str]) -> dict:
    """Load nhc_tracks_obsv_buffers for the given storms into a dict keyed by
    (atcf_id, valid_time, wind_speed_kt)."""
    from shapely import wkt as shapely_wkt

    if not atcf_ids:
        return {}
    ids_sql = ", ".join(f"'{a}'" for a in atcf_ids)
    query = (
        f"SELECT atcf_id, valid_time, wind_speed_kt, ST_AsText(geometry) AS geom_wkt"
        f" FROM storms.nhc_tracks_obsv_buffers"
        f" WHERE atcf_id IN ({ids_sql})"
    )
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    lookup = {}
    for _, row in df.iterrows():
        if row["geom_wkt"] is not None:
            lookup[(row["atcf_id"], row["valid_time"], int(row["wind_speed_kt"]))] = (
                shapely_wkt.loads(row["geom_wkt"])
            )
    return lookup


def process_nhc_wsp_fcastonly_polygons(
    engine,
    since: str | None = None,
    basin: str | None = None,
    issued_time=None,
    overwrite: bool = False,
    chunksize: int = 500,
) -> None:
    from datetime import timedelta
    from shapely import wkt as shapely_wkt
    from shapely.geometry import box as shapely_box
    _world = shapely_box(-180, -90, 180, 90)

    logger.info("Loading WSP polygons for fcastonly cut-out...")
    gdf_wsp = _load_wsp_for_exposure(engine, since=since, basin=basin, issued_time=issued_time)
    if gdf_wsp.empty:
        logger.info("No WSP polygons found. Skipping.")
        return
    logger.info(f"Loaded {len(gdf_wsp)} WSP rows.")

    atcf_ids = [a for a in gdf_wsp["atcf_id"].dropna().unique()]
    logger.info(f"Loading obsv buffers for {len(atcf_ids)} storms...")
    obsv_lookup = _load_obsv_buffer_lookup(engine, atcf_ids)
    logger.info(f"Loaded {len(obsv_lookup)} obsv buffer entries.")

    if not overwrite:
        with engine.connect() as conn:
            existing = pd.read_sql(
                text(
                    "SELECT issued_time, wind_threshold_kt, percentage, atcf_id"
                    " FROM storms.nhc_wsp_fcastonly_polygon"
                ),
                conn,
            )
        SENTINEL = "__null__"
        existing_keys = set(
            zip(
                existing["issued_time"],
                existing["wind_threshold_kt"],
                existing["percentage"],
                existing["atcf_id"].fillna(SENTINEL),
            )
        )
    else:
        existing_keys = set()

    offset = timedelta(hours=_WSP_OBSV_OFFSET_HOURS)
    no_obsv = offset_used = exact_used = already_done = 0
    batch = []

    for _, row in gdf_wsp.iterrows():
        it = row["issued_time"]
        kt = int(row["wind_threshold_kt"])
        pct = int(row["percentage"])
        atcf_id = row["atcf_id"] if pd.notna(row["atcf_id"]) else None
        SENTINEL = "__null__"
        key = (it, kt, pct, atcf_id if atcf_id is not None else SENTINEL)

        if key in existing_keys:
            already_done += 1
            continue

        wsp_geom = row.geometry.intersection(_world)
        if wsp_geom.is_empty:
            continue
        obsv_valid_time = None
        result_geom = wsp_geom

        if atcf_id is not None:
            obsv_geom = obsv_lookup.get((atcf_id, it + offset, kt))
            if obsv_geom is not None:
                obsv_valid_time = it + offset
                offset_used += 1
            else:
                obsv_geom = obsv_lookup.get((atcf_id, it, kt))
                if obsv_geom is not None:
                    obsv_valid_time = it
                    exact_used += 1
                else:
                    no_obsv += 1

            if obsv_geom is not None:
                diff = wsp_geom.difference(obsv_geom)
                result_geom = None if diff.is_empty else diff
        else:
            no_obsv += 1

        batch.append({
            "issued_time": it,
            "wind_threshold_kt": kt,
            "percentage": pct,
            "atcf_id": atcf_id,
            "obsv_valid_time": obsv_valid_time,
            "geometry": result_geom.wkt if result_geom is not None else None,
        })

        if len(batch) >= _WSP_FCASTONLY_BATCH_SIZE:
            _write_wsp_fcastonly_batch(batch, engine, chunksize)
            batch = []

    if batch:
        _write_wsp_fcastonly_batch(batch, engine, chunksize)

    logger.info(
        f"WSP fcastonly polygons done: {offset_used} with +3h offset, "
        f"{exact_used} exact-time fallback, {no_obsv} no obsv buffer, "
        f"{already_done} skipped (already done)."
    )


def _write_wsp_fcastonly_batch(batch: list[dict], engine, chunksize: int) -> None:
    df = pd.DataFrame(batch)
    key_cols = ["issued_time", "wind_threshold_kt", "percentage", "atcf_id"]
    df = df.drop_duplicates(subset=key_cols, keep="last")
    with engine.connect() as conn:
        df.to_sql(
            name="nhc_wsp_fcastonly_polygon",
            con=conn,
            schema="storms",
            if_exists="append",
            index=False,
            method=stratus.postgres_upsert,
            chunksize=chunksize,
        )
        conn.commit()


def run_nhc_wsp_fcastonly_polygons(
    mode: str = "dev",
    since: str | None = None,
    basin: str | None = None,
    issued_time=None,
    overwrite: bool = False,
    chunksize: int = 500,
) -> None:
    coloredlogs.install(
        logger=logger,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger.info("Starting NHC WSP forecast-only polygon pipeline...")
    engine = stratus.get_engine(stage=mode, write=True)
    try:
        process_nhc_wsp_fcastonly_polygons(
            engine=engine,
            since=since,
            basin=basin,
            issued_time=issued_time,
            overwrite=overwrite,
            chunksize=chunksize,
        )
        logger.info("NHC WSP forecast-only polygon pipeline finished.")
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise


# ---------------------------------------------------------------------------
# Population exposure — NHC WSP forecast-only polygons
# ---------------------------------------------------------------------------


def _load_wsp_fcastonly_for_exposure(
    engine,
    since: str | None = None,
    basin: str | None = None,
    issued_time=None,
) -> gpd.GeoDataFrame:
    filters = []
    if since:
        filters.append(f"p.issued_time >= '{since}'")
    if basin:
        filters.append(f"s.genesis_basin = '{basin}'")
    if issued_time is not None:
        filters.append(f"p.issued_time = '{issued_time}'")

    if basin:
        where = "WHERE " + " AND ".join(filters)
        query = (
            f"SELECT p.issued_time, p.wind_threshold_kt, p.percentage, p.atcf_id, p.geometry"
            f" FROM storms.nhc_wsp_fcastonly_polygon p"
            f" JOIN storms.nhc_storms s ON p.atcf_id = s.atcf_id"
            f" {where}"
        )
    else:
        where = ("WHERE " + " AND ".join(filters)) if filters else ""
        query = (
            f"SELECT issued_time, wind_threshold_kt, percentage, atcf_id, geometry"
            f" FROM storms.nhc_wsp_fcastonly_polygon {where}"
        )

    with engine.connect() as conn:
        return gpd.read_postgis(query, conn, geom_col="geometry")


def _load_done_nhc_wsp_fcastonly_exp(engine) -> pd.DataFrame:
    try:
        with engine.connect() as conn:
            return pd.read_sql(
                text(
                    "SELECT issued_time, wind_threshold_kt, percentage, atcf_id, admin_level, pcode"
                    " FROM storms.nhc_wsp_fcastonly_exposure"
                    f" WHERE admin_level = {_EXP_ADMIN_LEVEL}"
                ),
                conn,
            )
    except Exception:
        return pd.DataFrame(columns=_WSP_EXP_KEY_COLS)


def _filter_done_nhc_wsp_fcastonly(wsp: gpd.GeoDataFrame, done_country: pd.DataFrame) -> gpd.GeoDataFrame:
    merge_cols = ["issued_time", "wind_threshold_kt", "percentage", "atcf_id"]
    SENTINEL = "__null__"
    wsp_keys = wsp[merge_cols].assign(atcf_id=wsp["atcf_id"].fillna(SENTINEL))
    done_keys = done_country[merge_cols].assign(atcf_id=done_country["atcf_id"].fillna(SENTINEL))
    merged = wsp_keys.merge(
        done_keys.drop_duplicates().assign(_done=True),
        on=merge_cols,
        how="left",
    )
    return wsp[merged["_done"].isna().values].reset_index(drop=True)


def run_nhc_wsp_fcastonly_exp(
    countries: list[str] | None = None,
    since: str | None = None,
    basin: str | None = None,
    overwrite: bool = False,
    mode: str = "dev",
    issued_time=None,
) -> None:
    import warnings
    from rasterio.errors import ShapeSkipWarning
    from src.utils.exposure import GEO_CRS_ANTIMERIDIAN, calculate_exposure, load_adm1, load_pop

    warnings.filterwarnings("ignore", category=ShapeSkipWarning)
    engine = stratus.get_engine(stage=mode, write=True)

    logger.info("Loading WSP fcastonly polygons for exposure calculation...")
    gdf_wsp = _load_wsp_fcastonly_for_exposure(engine, since=since, basin=basin, issued_time=issued_time)
    if gdf_wsp.empty:
        logger.info("No WSP fcastonly polygons found for the given filters. Skipping.")
        return
    gdf_wsp_anti = gdf_wsp.to_crs(GEO_CRS_ANTIMERIDIAN)

    gdf_adm1 = load_adm1(countries)
    country_list = sorted(gdf_adm1["iso_3"].unique())
    logger.info(f"Processing {len(country_list)} countries...")

    da_wp_global, da_wp_wrapped = load_pop()

    done_df = pd.DataFrame(columns=_WSP_EXP_KEY_COLS) if overwrite else _load_done_nhc_wsp_fcastonly_exp(engine)

    wsp_sindex = gdf_wsp.sindex
    processed = skipped = 0
    for i, iso3 in enumerate(country_list, 1):
        prefix = f"[{i}/{len(country_list)}] {iso3}"

        adm_geom = gdf_adm1[gdf_adm1["iso_3"] == iso3][["geometry"]].dissolve().iloc[0].geometry
        minx, _, maxx, _ = adm_geom.bounds
        wrap = maxx > 160 or minx < -160

        if wrap:
            da_wp = da_wp_wrapped
            adm_geom = gpd.GeoSeries([adm_geom], crs=4326).to_crs(GEO_CRS_ANTIMERIDIAN).iloc[0]
            wsp_in = gdf_wsp_anti[gdf_wsp_anti.intersects(adm_geom)]
        else:
            da_wp = da_wp_global
            candidate_idx = list(wsp_sindex.intersection(adm_geom.bounds))
            if not candidate_idx:
                skipped += 1
                continue
            candidates = gdf_wsp.iloc[candidate_idx]
            wsp_in = candidates[candidates.intersects(adm_geom)]

        if wsp_in.empty:
            skipped += 1
            continue

        if not overwrite and not done_df.empty:
            done_country = done_df[done_df["pcode"] == iso3]
            if not done_country.empty:
                wsp_in = _filter_done_nhc_wsp_fcastonly(wsp_in, done_country)
                if wsp_in.empty:
                    skipped += 1
                    logger.info(f"{prefix} — all done, skipping")
                    continue

        logger.info(f"{prefix} — {len(wsp_in)} intersecting, calculating...")

        da_wp_country = da_wp.rio.clip([adm_geom], all_touched=True)
        df = calculate_exposure(wsp_in, da_wp_country)
        df["iso3"] = iso3
        df["pcode"] = iso3
        df["admin_level"] = _EXP_ADMIN_LEVEL
        del da_wp_country

        out = df.drop(columns=["id"], errors="ignore")
        out = out.drop_duplicates(subset=_WSP_EXP_KEY_COLS, keep="last")
        with engine.connect() as conn:
            out.to_sql(
                "nhc_wsp_fcastonly_exposure",
                conn,
                schema="storms",
                if_exists="append",
                index=False,
                method=stratus.postgres_upsert,
            )
            conn.commit()
        n_exposed = int((df["pop_exposed"] > 0).sum())
        logger.info(f"{prefix} — done ({n_exposed} rows with pop > 0)")
        processed += 1

    logger.info(f"WSP fcastonly exposure done: {processed} written, {skipped} already done.")
    engine.dispose()


# ---------------------------------------------------------------------------
# Realtime orchestration
# ---------------------------------------------------------------------------


def run_nhc_realtime(
    mode: str = "prod",
    save_to_blob: bool = False,
    save_dir: str = "/tmp",
    chunksize: int = 10000,
) -> None:
    coloredlogs.install(
        logger=logger,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger.info("Starting NHC realtime pipeline...")

    engine = stratus.get_engine(stage=mode, write=True)

    df_raw = retrieve_nhc_current(stage=mode, save_to_blob=save_to_blob, save_dir=save_dir)
    if df_raw is None:
        logger.info("No active storms. Realtime pipeline finished.")
        return

    process_storms(df_raw=df_raw, engine=engine, chunksize=chunksize)
    process_tracks(df_raw=df_raw, engine=engine, chunksize=chunksize)

    logger.info("Fetching current WSP polygons...")
    wsp_gdf = lens.nhc.get_wsp()
    if wsp_gdf is not None and len(wsp_gdf) > 0:
        process_wsp_polygons(gdf=wsp_gdf, engine=engine, chunksize=chunksize)
    else:
        logger.info("No current WSP data available.")
        wsp_gdf = None

    with engine.connect() as conn:
        track_issued_time = pd.read_sql(
            text("SELECT MAX(issued_time) FROM storms.nhc_tracks_geo"), conn
        ).iloc[0, 0]
        wsp_issued_time = (
            pd.read_sql(text("SELECT MAX(issued_time) FROM storms.nhc_wsp_polygon"), conn).iloc[0, 0]
            if wsp_gdf is not None
            else None
        )

    logger.info(f"Track issued_time: {track_issued_time}, WSP issued_time: {wsp_issued_time}")

    read_engine = stratus.get_engine(stage="prod")
    write_engine = stratus.get_engine(stage=mode, write=True)

    try:
        logger.info("Running NHC wind buffers...")
        process_nhc_tracks_fcast_buffers(
            read_engine=read_engine,
            write_engine=write_engine,
            chunksize=chunksize,
            issued_time=track_issued_time,
        )
    except Exception as e:
        logger.error(f"NHC wind buffers failed: {e}", exc_info=True)

    try:
        logger.info("Running NHC observational track buffers...")
        process_nhc_tracks_obsv_buffers(
            read_engine=read_engine,
            write_engine=write_engine,
            chunksize=chunksize,
            issued_time=track_issued_time,
        )
    except Exception as e:
        logger.error(f"NHC observational track buffers failed: {e}", exc_info=True)

    try:
        logger.info("Running NHC forecast-only track buffers...")
        process_nhc_tracks_fcastonly_buffers(
            read_engine=write_engine,
            write_engine=write_engine,
            chunksize=chunksize,
            issued_time=track_issued_time,
        )
    except Exception as e:
        logger.error(f"NHC forecast-only track buffers failed: {e}", exc_info=True)

    try:
        logger.info("Running NHC track exposure...")
        run_nhc_tracks_fcast_exp(mode=mode, issued_time=track_issued_time)
    except Exception as e:
        logger.error(f"NHC track exposure failed: {e}", exc_info=True)

    try:
        logger.info("Running NHC observed track exposure...")
        run_nhc_tracks_obsv_exp(mode=mode, valid_time=track_issued_time)
    except Exception as e:
        logger.error(f"NHC observed track exposure failed: {e}", exc_info=True)

    try:
        logger.info("Running NHC forecast-only track exposure...")
        run_nhc_tracks_fcastonly_exp(mode=mode, issued_time=track_issued_time)
    except Exception as e:
        logger.error(f"NHC forecast-only track exposure failed: {e}", exc_info=True)

    if wsp_issued_time is not None:
        try:
            logger.info("Running NHC WSP exposure...")
            run_nhc_wsp_exp(mode=mode, issued_time=wsp_issued_time)
        except Exception as e:
            logger.error(f"NHC WSP exposure failed: {e}", exc_info=True)

        try:
            logger.info("Running NHC WSP forecast-only polygons...")
            process_nhc_wsp_fcastonly_polygons(engine=write_engine, issued_time=wsp_issued_time)
        except Exception as e:
            logger.error(f"NHC WSP fcastonly polygons failed: {e}", exc_info=True)

        try:
            logger.info("Running NHC WSP forecast-only exposure...")
            run_nhc_wsp_fcastonly_exp(mode=mode, issued_time=wsp_issued_time)
        except Exception as e:
            logger.error(f"NHC WSP fcastonly exposure failed: {e}", exc_info=True)
    else:
        logger.info("No WSP data — skipping WSP exposure.")

    logger.info("NHC realtime pipeline finished.")
