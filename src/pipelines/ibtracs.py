#!/usr/bin/env python3
"""
IBTrACS ETL pipeline and wind buffer generation.
"""

import logging
import os
import warnings

import coloredlogs
import geopandas as gpd
import ocha_lens as lens
import pandas as pd
import xarray as xr
from dotenv import load_dotenv
from ocha_lens.utils.storm import calculate_wind_buffers_gdf, expand_quad_col
from sqlalchemy import text
from tqdm import tqdm

load_dotenv()

import ocha_stratus as stratus  # noqa

BUFFER_SPEEDS = [34, 50, 64]
_WIND_BUFFER_BATCH_SIZE = 50


logger = logging.getLogger(__name__)


def retrieve_ibtracs(
    dataset_type, stage="local", save_to_blob=False, save_dir=None
):
    """
    Download IBTrACS Netcdf, upload raw to azure if needed and return loaded Dataset
    """
    logger.info(f"Retrieving {dataset_type} from IBTrACS...")
    filename = f"IBTrACS.{dataset_type}.v04r01.nc"
    file_path = f"{save_dir}/" + filename

    if os.path.exists(file_path):
        logger.info(f"Using file downloaded in {file_path}...")
        path = file_path
    else:
        path = lens.ibtracs.download_ibtracs(
            dataset=dataset_type, save_dir=save_dir
        )
        logger.info(f"Successfully downloaded {dataset_type} to {path}.")

    if save_to_blob:
        logger.info(f"Uploading {path} to Azure blob in {stage}...")
        with open(path, "rb") as file:
            data_to_upload = file.read()
        stratus.upload_blob_data(
            container_name="storm",
            data=data_to_upload,
            blob_name=f"ibtracs/v04r01/{filename}",
            stage=stage,
        )
        logger.info("Successfully uploaded to blob.")

    return xr.open_dataset(path).load()


def process_tracks(dataset, engine, chunksize):
    """
    Retrieve 'best' and 'provisional' tracks and upload them to the database
    """
    logger.info("Extracting tracks...")
    tracks_geo = lens.ibtracs.get_tracks(dataset)

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
            name="ibtracs_tracks_geo",
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

    storm_tracks = lens.ibtracs.get_storms(dataset)

    with engine.connect() as conn:
        storm_tracks.to_sql(
            "ibtracs_storms",
            con=conn,
            schema="storms",
            if_exists="append",
            index=False,
            method=stratus.postgres_upsert,
            chunksize=chunksize,
        )

    logger.info("Successfully processed storms.")
    return storm_tracks


def run_ibtracs(
    mode, dataset_type, save_to_blob=False, save_dir="/tmp", chunksize=10000
):
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
        dataset = retrieve_ibtracs(
            dataset_type=dataset_type,
            stage=mode,
            save_to_blob=save_to_blob,
            save_dir=save_dir,
        )

        # Process storms and add them to the database
        process_storms(dataset=dataset, engine=engine, chunksize=chunksize)

        # Process tracks and add them to the database
        process_tracks(dataset=dataset, engine=engine, chunksize=chunksize)

        logger.info("Pipeline successfully finished!")

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise


# ---------------------------------------------------------------------------
# Wind buffer generation
# ---------------------------------------------------------------------------


def _load_wind_buffer_tracks(
    engine,
    basin: str | None = None,
    start_year: int | None = None,
    sids: list[str] | None = None,
) -> gpd.GeoDataFrame:
    # Subquery: find SIDs that have at least one track point with real radii.
    # Zero arrays {0,0,0,0} are treated as no-data (same as NULL).
    inner_filters = [
        """(
            (usa_quadrant_radius_34 IS NOT NULL AND usa_quadrant_radius_34 != '{0,0,0,0}')
            OR (usa_quadrant_radius_50 IS NOT NULL AND usa_quadrant_radius_50 != '{0,0,0,0}')
            OR (usa_quadrant_radius_64 IS NOT NULL AND usa_quadrant_radius_64 != '{0,0,0,0}')
        )"""
    ]
    if basin:
        inner_filters.append(f"basin = '{basin}'")
    if start_year:
        inner_filters.append(f"EXTRACT(YEAR FROM valid_time) >= {start_year}")

    inner_where = " AND ".join(inner_filters)

    # Outer query: load ALL track points for qualifying SIDs so that zero rows
    # between non-zero rows are included for correct interpolation.
    outer_filters = [f"sid IN (SELECT DISTINCT sid FROM storms.ibtracs_tracks_geo WHERE {inner_where})"]
    if basin:
        outer_filters.append(f"basin = '{basin}'")
    if start_year:
        outer_filters.append(f"EXTRACT(YEAR FROM valid_time) >= {start_year}")
    if sids:
        sid_list = ", ".join(f"'{s}'" for s in sids)
        outer_filters.append(f"sid IN ({sid_list})")

    outer_where = " AND ".join(outer_filters)
    query = f"""
        SELECT sid, basin, valid_time,
               usa_quadrant_radius_34,
               usa_quadrant_radius_50,
               usa_quadrant_radius_64,
               geometry
        FROM storms.ibtracs_tracks_geo
        WHERE {outer_where}
        ORDER BY sid, valid_time
    """
    with engine.connect() as conn:
        return gpd.read_postgis(query, conn, geom_col="geometry")


def _load_existing_sids(engine) -> set:
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT DISTINCT sid FROM storms.ibtracs_wind_buffers")
        )
        return {row[0] for row in result}


def _write_wind_buffer_batch(batch, conn, table, cols, chunksize):
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
        name=table,
        con=conn,
        schema="storms",
        if_exists="append",
        index=False,
        method=stratus.postgres_upsert,
        chunksize=chunksize,
    )
    conn.commit()


def process_wind_buffers(
    read_engine,
    write_engine,
    chunksize,
    basin=None,
    start_year=None,
    overwrite=False,
    sids=None,
):
    logger.info("Loading IBTrACS tracks with USA wind radii...")
    gdf_tracks = _load_wind_buffer_tracks(
        read_engine, basin=basin, start_year=start_year, sids=sids
    )
    logger.info(
        f"Loaded {len(gdf_tracks)} track points "
        f"across {gdf_tracks['sid'].nunique()} storms."
    )

    if not overwrite:
        existing = _load_existing_sids(write_engine)
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

            if len(batch) >= _WIND_BUFFER_BATCH_SIZE:
                _write_wind_buffer_batch(
                    batch, conn, "ibtracs_wind_buffers", cols, chunksize
                )
                batch = []

        if batch:
            _write_wind_buffer_batch(
                batch, conn, "ibtracs_wind_buffers", cols, chunksize
            )

    logger.info("Successfully wrote IBTrACS wind buffers.")


def run_wind_buffers(
    write_mode="dev",
    chunksize=1000,
    basin=None,
    start_year=None,
    overwrite=False,
    sids=None,
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
            sids=sids,
        )
        logger.info("IBTrACS wind buffers pipeline finished.")
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise


# ---------------------------------------------------------------------------
# Population exposure
# ---------------------------------------------------------------------------

_EXP_KEY_COLS = ["sid", "wind_speed_kt", "admin_level", "pcode"]
_EXP_ADMIN_LEVEL = 0


def _load_ibtracs_exp_buffers(
    engine,
    since: int | None = None,
    basin: str | None = None,
    sids: list[str] | None = None,
) -> gpd.GeoDataFrame:
    filters = []
    if since:
        filters.append(f"s.season >= {since}")
    if basin:
        filters.append(f"s.genesis_basin = '{basin}'")
    if sids:
        sid_list = ", ".join(f"'{s}'" for s in sids)
        filters.append(f"b.sid IN ({sid_list})")

    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    query = (
        f"SELECT b.sid, b.wind_speed_kt, b.geometry"
        f" FROM storms.ibtracs_wind_buffers b"
        f" JOIN storms.ibtracs_storms s ON b.sid = s.sid"
        f" {where}"
    )
    with engine.connect() as conn:
        return gpd.read_postgis(query, conn, geom_col="geometry")


def _load_done_ibtracs_exp(engine) -> pd.DataFrame:
    try:
        with engine.connect() as conn:
            return pd.read_sql(
                text(
                    "SELECT sid, wind_speed_kt, admin_level, pcode"
                    " FROM storms.ibtracs_wind_exposure"
                    f" WHERE admin_level = {_EXP_ADMIN_LEVEL}"
                ),
                conn,
            )
    except Exception:
        return pd.DataFrame(columns=_EXP_KEY_COLS)


def _filter_done_ibtracs(buffers: gpd.GeoDataFrame, done_country: pd.DataFrame) -> gpd.GeoDataFrame:
    merge_cols = ["sid", "wind_speed_kt"]
    merged = buffers[merge_cols].merge(
        done_country[merge_cols].drop_duplicates().assign(_done=True),
        on=merge_cols,
        how="left",
    )
    return buffers[merged["_done"].isna().values].reset_index(drop=True)


def run_ibtracs_exp(
    countries: list[str] | None = None,
    since: int | None = None,
    basin: str | None = None,
    overwrite: bool = False,
    mode: str = "dev",
    sids: list[str] | None = None,
) -> None:
    import warnings
    from rasterio.errors import ShapeSkipWarning
    from src.utils.exposure import GEO_CRS_ANTIMERIDIAN, calculate_exposure, load_adm1, load_pop

    warnings.filterwarnings("ignore", category=ShapeSkipWarning)
    engine = stratus.get_engine(stage=mode, write=True)

    logger.info("Loading IBTrACS wind buffers for exposure calculation...")
    gdf_buffers = _load_ibtracs_exp_buffers(engine, since=since, basin=basin, sids=sids)
    if gdf_buffers.empty:
        logger.info("No wind buffers found for the given filters. Skipping.")
        return
    gdf_buffers_anti = gdf_buffers.to_crs(GEO_CRS_ANTIMERIDIAN)

    gdf_adm1 = load_adm1(countries, stage=mode)
    country_list = sorted(gdf_adm1["iso_3"].unique())
    logger.info(f"Processing {len(country_list)} countries...")

    da_wp_global, da_wp_wrapped = load_pop()

    done_df = pd.DataFrame(columns=_EXP_KEY_COLS) if overwrite else _load_done_ibtracs_exp(engine)
    if not done_df.empty:
        logger.info(f"{done_df['pcode'].nunique()} countries with existing data in DB")

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
                buf_in = _filter_done_ibtracs(buf_in, done_country)
                buf_zero = _filter_done_ibtracs(buf_zero, done_country)
                if buf_in.empty and buf_zero.empty:
                    skipped += 1
                    logger.info(f"{prefix} — all done, skipping")
                    continue

        logger.info(f"{prefix} — {len(buf_in)} intersecting, {len(buf_zero)} zeros")

        if not buf_in.empty:
            # Country-level pre-clip is just a raster window restriction;
            # exact_extract handles area-weighted sums over (country ∩ buffer).
            da_wp_country = da_wp.rio.clip([adm_geom], all_touched=True)
            df = calculate_exposure(buf_in, da_wp_country, mask_geom=adm_geom)
            df["iso3"] = iso3
            df["pcode"] = iso3
            df["admin_level"] = _EXP_ADMIN_LEVEL
            del da_wp_country
        else:
            df = pd.DataFrame(columns=_EXP_KEY_COLS + ["iso3", "pop_exposed"])

        if not buf_zero.empty:
            df_zeros = buf_zero.drop(columns=["geometry"], errors="ignore").copy()
            df_zeros["iso3"] = iso3
            df_zeros["pcode"] = iso3
            df_zeros["admin_level"] = _EXP_ADMIN_LEVEL
            df_zeros["pop_exposed"] = 0
            df = pd.concat([df, df_zeros], ignore_index=True)

        out = df.drop_duplicates(subset=_EXP_KEY_COLS, keep="last")
        with engine.connect() as conn:
            out.to_sql(
                "ibtracs_wind_exposure",
                conn,
                schema="storms",
                if_exists="append",
                index=False,
                method=stratus.postgres_upsert,
            )
            conn.commit()
        processed += 1

    logger.info(f"Exposure done: {processed} written, {skipped} skipped (already done).")
    engine.dispose()


# ---------------------------------------------------------------------------
# Realtime orchestration
# ---------------------------------------------------------------------------


def run_ibtracs_realtime(
    mode: str = "prod",
    dataset_type: str = "ACTIVE",
    save_to_blob: bool = False,
    save_dir: str = "/tmp",
    chunksize: int = 10000,
) -> None:
    coloredlogs.install(
        logger=logger,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger.info("Starting IBTrACS realtime pipeline...")

    engine = stratus.get_engine(stage=mode, write=True)

    dataset = retrieve_ibtracs(
        dataset_type=dataset_type,
        stage=mode,
        save_to_blob=save_to_blob,
        save_dir=save_dir,
    )
    process_storms(dataset=dataset, engine=engine, chunksize=chunksize)
    process_tracks(dataset=dataset, engine=engine, chunksize=chunksize)

    storm_df = lens.ibtracs.get_storms(dataset)
    active_sids = storm_df["sid"].tolist()
    logger.info(f"Active SIDs this run: {active_sids}")

    if not active_sids:
        logger.info("No active storms — skipping downstream steps.")
        return

    read_engine = stratus.get_engine(stage="prod")
    write_engine = stratus.get_engine(stage=mode, write=True)

    try:
        logger.info("Running IBTrACS wind buffers...")
        process_wind_buffers(
            read_engine=read_engine,
            write_engine=write_engine,
            chunksize=chunksize,
            sids=active_sids,
        )
    except Exception as e:
        logger.error(f"Wind buffers failed: {e}", exc_info=True)

    try:
        logger.info("Running IBTrACS exposure...")
        run_ibtracs_exp(mode=mode, sids=active_sids)
    except Exception as e:
        logger.error(f"Exposure failed: {e}", exc_info=True)

    logger.info("IBTrACS realtime pipeline finished.")
