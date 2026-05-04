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

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", message="Geometry column does not contain geometry"
        )
        gdf = gdf.copy()
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


def _load_nhc_wind_buffer_tracks(
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
            text("SELECT DISTINCT atcf_id, issued_time FROM storms.nhc_wind_buffers")
        )
        return {(row[0], row[1]) for row in result}


def _write_nhc_wind_buffer_batch(batch, conn, cols, chunksize):
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
        name="nhc_wind_buffers",
        con=conn,
        schema="storms",
        if_exists="append",
        index=False,
        method=stratus.postgres_upsert,
        chunksize=chunksize,
    )
    conn.commit()


def process_nhc_wind_buffers(
    read_engine,
    write_engine,
    chunksize,
    basin=None,
    start_year=None,
    overwrite=False,
    issued_time=None,
):
    logger.info("Loading NHC tracks with wind radii...")
    gdf_tracks = _load_nhc_wind_buffer_tracks(
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
            gdf_tracks.groupby(["atcf_id", "issued_time"])
        ):
            gdf_buffers = calculate_wind_buffers_gdf(
                group, quad_cols_format="quadrant_radius_{speed}_{quad}"
            )
            gdf_buffers["atcf_id"] = atcf_id
            gdf_buffers["issued_time"] = it
            batch.append(gdf_buffers)

            if len(batch) >= _WIND_BUFFER_BATCH_SIZE:
                _write_nhc_wind_buffer_batch(batch, conn, cols, chunksize)
                batch = []

        if batch:
            _write_nhc_wind_buffer_batch(batch, conn, cols, chunksize)

    logger.info("Successfully wrote NHC wind buffers.")


def run_nhc_wind_buffers(
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
        process_nhc_wind_buffers(
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
# Population exposure — NHC wind buffers
# ---------------------------------------------------------------------------

_TRACK_EXP_KEY_COLS = ["atcf_id", "issued_time", "wind_speed_kt", "admin_level", "pcode"]
_WSP_EXP_KEY_COLS = ["issued_time", "wind_threshold_kt", "percentage", "atcf_id", "admin_level", "pcode"]
_EXP_ADMIN_LEVEL = 0


def _load_nhc_wind_exp_buffers(
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
            f" FROM storms.nhc_wind_buffers b"
            f" JOIN storms.nhc_storms s ON b.atcf_id = s.atcf_id"
            f" {where}"
        )
    else:
        where = ("WHERE " + " AND ".join(filters)) if filters else ""
        query = (
            f"SELECT atcf_id, issued_time, wind_speed_kt, geometry"
            f" FROM storms.nhc_wind_buffers {where}"
        )

    with engine.connect() as conn:
        return gpd.read_postgis(query, conn, geom_col="geometry")


def _load_done_nhc_wind_exp(engine) -> pd.DataFrame:
    try:
        with engine.connect() as conn:
            return pd.read_sql(
                text(
                    "SELECT atcf_id, issued_time, wind_speed_kt, admin_level, pcode"
                    " FROM storms.nhc_wind_exposure"
                    f" WHERE admin_level = {_EXP_ADMIN_LEVEL}"
                ),
                conn,
            )
    except Exception:
        return pd.DataFrame(columns=_TRACK_EXP_KEY_COLS)


def _filter_done_nhc_wind(buffers: gpd.GeoDataFrame, done_country: pd.DataFrame) -> gpd.GeoDataFrame:
    merge_cols = ["atcf_id", "issued_time", "wind_speed_kt"]
    merged = buffers[merge_cols].merge(
        done_country[merge_cols].drop_duplicates().assign(_done=True),
        on=merge_cols,
        how="left",
    )
    return buffers[merged["_done"].isna().values].reset_index(drop=True)


def run_nhc_wind_exp(
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
    gdf_buffers = _load_nhc_wind_exp_buffers(engine, since=since, basin=basin, issued_time=issued_time)
    if gdf_buffers.empty:
        logger.info("No wind buffers found for the given filters. Skipping.")
        return
    gdf_buffers_anti = gdf_buffers.to_crs(GEO_CRS_ANTIMERIDIAN)

    gdf_adm1 = load_adm1(countries)
    country_list = sorted(gdf_adm1["iso_3"].unique())
    logger.info(f"Processing {len(country_list)} countries...")

    da_wp_global, da_wp_wrapped = load_pop()

    done_df = pd.DataFrame(columns=_TRACK_EXP_KEY_COLS) if overwrite else _load_done_nhc_wind_exp(engine)

    processed = skipped = 0
    for i, iso3 in enumerate(country_list, 1):
        prefix = f"[{i}/{len(country_list)}] {iso3}"

        adm_geom = gdf_adm1[gdf_adm1["iso_3"] == iso3][["geometry"]].dissolve().iloc[0].geometry
        minx, _, maxx, _ = adm_geom.bounds
        wrap = maxx > 160 or minx < -160

        if wrap:
            da_wp = da_wp_wrapped
            adm_geom = gpd.GeoSeries([adm_geom], crs=4326).to_crs(GEO_CRS_ANTIMERIDIAN).iloc[0]
            buffers = gdf_buffers_anti
        else:
            da_wp = da_wp_global
            buffers = gdf_buffers

        intersects_mask = buffers.intersects(adm_geom)
        buf_in = buffers[intersects_mask]
        buf_zero = buffers[~intersects_mask]

        if not overwrite and not done_df.empty:
            done_country = done_df[done_df["pcode"] == iso3]
            if not done_country.empty:
                buf_in = _filter_done_nhc_wind(buf_in, done_country)
                buf_zero = _filter_done_nhc_wind(buf_zero, done_country)
                if buf_in.empty and buf_zero.empty:
                    skipped += 1
                    logger.info(f"{prefix} — all done, skipping")
                    continue

        logger.info(f"{prefix} — {len(buf_in)} intersecting, {len(buf_zero)} zeros")

        if not buf_in.empty:
            da_wp_country = da_wp.rio.clip([adm_geom], all_touched=True)
            df = calculate_exposure(buf_in, da_wp_country)
            df["iso3"] = iso3
            df["pcode"] = iso3
            df["admin_level"] = _EXP_ADMIN_LEVEL
            del da_wp_country
        else:
            df = pd.DataFrame(columns=_TRACK_EXP_KEY_COLS + ["iso3", "pop_exposed"])

        if not buf_zero.empty:
            df_zeros = buf_zero.drop(columns=["geometry"], errors="ignore").copy()
            df_zeros["iso3"] = iso3
            df_zeros["pcode"] = iso3
            df_zeros["admin_level"] = _EXP_ADMIN_LEVEL
            df_zeros["pop_exposed"] = 0
            df = pd.concat([df, df_zeros], ignore_index=True)

        out = df.drop_duplicates(subset=_TRACK_EXP_KEY_COLS, keep="last")
        with engine.connect() as conn:
            out.to_sql(
                "nhc_wind_exposure",
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


def _load_done_nhc_wsp_exp(engine) -> list[str]:
    try:
        with engine.connect() as conn:
            return pd.read_sql(
                text("SELECT DISTINCT pcode FROM storms.nhc_wsp_adm0_exp"),
                conn,
            )["pcode"].tolist()
    except Exception:
        return []


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

    done = [] if overwrite else _load_done_nhc_wsp_exp(engine)
    if done:
        logger.info(f"{len(done)} countries already in DB — skipping")

    processed = skipped_done = skipped_no_overlap = 0
    for i, iso3 in enumerate(country_list, 1):
        prefix = f"[{i}/{len(country_list)}] {iso3}"

        if iso3 in done:
            skipped_done += 1
            logger.info(f"{prefix} — already done, skipping")
            continue

        adm_geom = gdf_adm1[gdf_adm1["iso_3"] == iso3][["geometry"]].dissolve().iloc[0].geometry
        minx, _, maxx, _ = adm_geom.bounds
        wrap = maxx > 160 or minx < -160

        if wrap:
            da_wp = da_wp_wrapped
            adm_geom = gpd.GeoSeries([adm_geom], crs=4326).to_crs(GEO_CRS_ANTIMERIDIAN).iloc[0]
            wsp = gdf_wsp_anti
        else:
            da_wp = da_wp_global
            wsp = gdf_wsp

        wsp_in_country = wsp[wsp.intersects(adm_geom)]
        if wsp_in_country.empty:
            skipped_no_overlap += 1
            logger.info(f"{prefix} — no WSP overlap, skipping")
            continue

        logger.info(f"{prefix} — {len(wsp_in_country)} WSP polygons")
        da_wp_country = da_wp.rio.clip([adm_geom], all_touched=True)
        df = calculate_exposure(wsp_in_country, da_wp_country)
        df["adm0_pcode"] = iso3
        del da_wp_country

        out = df.drop(columns=["id"], errors="ignore")
        key_cols = ["issued_time", "wind_threshold_kt", "percentage", "atcf_id", "adm0_pcode"]
        out = out.drop_duplicates(subset=key_cols, keep="last")
        with engine.connect() as conn:
            out.to_sql(
                "nhc_wsp_adm0_exp",
                conn,
                schema="storms",
                if_exists="append",
                index=False,
                method=stratus.postgres_upsert,
            )
            conn.commit()
        processed += 1

    logger.info(
        f"WSP exposure done: {processed} written, "
        f"{skipped_done} already done, {skipped_no_overlap} no overlap."
    )
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
        process_nhc_wind_buffers(
            read_engine=read_engine,
            write_engine=write_engine,
            chunksize=chunksize,
            issued_time=track_issued_time,
        )
    except Exception as e:
        logger.error(f"NHC wind buffers failed: {e}", exc_info=True)

    try:
        logger.info("Running NHC track exposure...")
        run_nhc_wind_exp(mode=mode, issued_time=track_issued_time)
    except Exception as e:
        logger.error(f"NHC track exposure failed: {e}", exc_info=True)

    if wsp_issued_time is not None:
        try:
            logger.info("Running NHC WSP exposure...")
            run_nhc_wsp_exp(mode=mode, issued_time=wsp_issued_time)
        except Exception as e:
            logger.error(f"NHC WSP exposure failed: {e}", exc_info=True)
    else:
        logger.info("No WSP data — skipping WSP exposure.")

    logger.info("NHC realtime pipeline finished.")
