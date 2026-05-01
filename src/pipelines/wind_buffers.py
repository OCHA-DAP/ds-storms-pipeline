#!/usr/bin/env python3
"""
IBTrACS wind buffer pipeline.

Reads track data from storms.ibtracs_tracks_geo (PROD), computes per-storm
wind buffer polygons using ocha-lens, and writes to storms.ibtracs_wind_buffers (DEV).
"""

import logging
import warnings

import coloredlogs
import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
from ocha_lens.utils.storm import calculate_wind_buffers_gdf, expand_quad_col
from tqdm import tqdm

load_dotenv()

import ocha_stratus as stratus  # noqa

BUFFER_SPEEDS = [34, 50, 64]
logger = logging.getLogger(__name__)


def load_tracks(
    engine,
    basin: str = None,
    start_year: int = None,
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


def load_existing_sids(engine) -> set:
    from sqlalchemy import text

    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT DISTINCT sid FROM storms.ibtracs_wind_buffers")
        )
        return {row[0] for row in result}


def _write_batch(batch, conn, table, cols, chunksize):
    gdf = pd.concat(batch, ignore_index=True).rename(columns={"speed": "wind_speed_kt"})
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


WRITE_BATCH_SIZE = 50


def process_wind_buffers(
    read_engine, write_engine, chunksize, basin=None, start_year=None, overwrite=False
):
    logger.info("Loading IBTrACS tracks with USA wind radii from PROD...")
    gdf_tracks = load_tracks(read_engine, basin=basin, start_year=start_year)
    logger.info(
        f"Loaded {len(gdf_tracks)} track points "
        f"across {gdf_tracks['sid'].nunique()} storms."
    )

    if not overwrite:
        existing = load_existing_sids(write_engine)
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

            if len(batch) >= WRITE_BATCH_SIZE:
                _write_batch(batch, conn, "ibtracs_wind_buffers", cols, chunksize)
                batch = []

        if batch:
            _write_batch(batch, conn, "ibtracs_wind_buffers", cols, chunksize)

    logger.info("Successfully wrote wind buffers.")


def load_nhc_tracks(
    engine,
    basin: str = None,
    start_year: int = None,
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


def load_existing_nhc_keys(engine) -> set:
    from sqlalchemy import text

    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT DISTINCT atcf_id, issued_time FROM storms.nhc_wind_buffers")
        )
        return {(row[0], row[1]) for row in result}


def process_nhc_wind_buffers(
    read_engine, write_engine, chunksize, basin=None, start_year=None, overwrite=False
):
    logger.info("Loading NHC tracks with wind radii from PROD...")
    gdf_tracks = load_nhc_tracks(read_engine, basin=basin, start_year=start_year)
    n_issuances = gdf_tracks.groupby(["atcf_id", "issued_time"]).ngroups
    logger.info(
        f"Loaded {len(gdf_tracks)} track points across "
        f"{gdf_tracks['atcf_id'].nunique()} storms, {n_issuances} forecast issuances."
    )

    if not overwrite:
        existing = load_existing_nhc_keys(write_engine)
        if existing:
            before = gdf_tracks.groupby(["atcf_id", "issued_time"]).ngroups
            mask = gdf_tracks.apply(
                lambda r: (r["atcf_id"], r["issued_time"]) not in existing, axis=1
            )
            gdf_tracks = gdf_tracks[mask]
            after = gdf_tracks.groupby(["atcf_id", "issued_time"]).ngroups if not gdf_tracks.empty else 0
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

    logger.info("Calculating and writing wind buffers per forecast issuance...")
    cols = ["atcf_id", "issued_time", "wind_speed_kt", "geometry"]
    batch = []
    with write_engine.connect() as conn:
        for (atcf_id, issued_time), group in tqdm(
            gdf_tracks.groupby(["atcf_id", "issued_time"])
        ):
            gdf_buffers = calculate_wind_buffers_gdf(
                group, quad_cols_format="quadrant_radius_{speed}_{quad}"
            )
            gdf_buffers["atcf_id"] = atcf_id
            gdf_buffers["issued_time"] = issued_time
            batch.append(gdf_buffers)

            if len(batch) >= WRITE_BATCH_SIZE:
                _write_batch(batch, conn, "nhc_wind_buffers", cols, chunksize)
                batch = []

        if batch:
            _write_batch(batch, conn, "nhc_wind_buffers", cols, chunksize)

    logger.info("Successfully wrote NHC wind buffers.")


def run_wind_buffers(
    write_mode="dev", chunksize=1000, basin=None, start_year=None, overwrite=False
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
        )
        logger.info("Pipeline successfully finished!")
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise


def run_nhc_wind_buffers(
    write_mode="dev", chunksize=1000, basin=None, start_year=None, overwrite=False
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
        )
        logger.info("Pipeline successfully finished!")
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise
