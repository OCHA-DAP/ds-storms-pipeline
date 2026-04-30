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

    logger.info("Calculating wind buffers per storm...")
    results = []
    for sid, group in tqdm(gdf_tracks.groupby("sid")):
        gdf_buffers = calculate_wind_buffers_gdf(group)
        gdf_buffers["sid"] = sid
        results.append(gdf_buffers)

    gdf_all = pd.concat(results, ignore_index=True)
    gdf_all = gdf_all.rename(columns={"speed": "wind_speed_kt"})
    logger.info(f"Calculated {len(gdf_all)} buffer polygons.")

    logger.info("Writing wind buffers to DEV database...")
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", message="Geometry column does not contain geometry"
        )
        gdf_all["geometry"] = gdf_all["geometry"].apply(
            lambda g: g.wkt if g is not None else None
        )

    with write_engine.connect() as conn:
        gdf_all[["sid", "wind_speed_kt", "geometry"]].to_sql(
            name="ibtracs_wind_buffers",
            con=conn,
            schema="storms",
            if_exists="append",
            index=False,
            method=stratus.postgres_upsert,
            chunksize=chunksize,
        )
    logger.info("Successfully wrote wind buffers.")


def run_wind_buffers(write_mode="dev", chunksize=1000, basin=None, start_year=None, overwrite=False):
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
