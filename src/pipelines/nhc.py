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
from contextlib import contextmanager, nullcontext
from functools import partial

import coloredlogs
import geopandas as gpd
import ocha_lens as lens
import ocha_lens.datasources.nhc as _lens_nhc
import pandas as pd
import requests
from dotenv import load_dotenv
from ocha_lens.utils.storm import calculate_wind_buffers_gdf, expand_quad_col
from sqlalchemy import text
from tqdm import tqdm

load_dotenv()

# Silence noisy geopandas/pandas warnings that fire repeatedly in the WSP
# matching/dissolve loops.
warnings.filterwarnings(
    "ignore",
    message="Geometry column does not contain geometry",
    category=UserWarning,
)
warnings.filterwarnings(
    "ignore",
    message=".*GeoSeries.notna.*",
    category=UserWarning,
)

import ocha_stratus as stratus  # noqa

BUFFER_SPEEDS = [34, 50, 64]
_WIND_BUFFER_BATCH_SIZE = 50


logger = logging.getLogger(__name__)


NHC_SAMPLE_JSON_URL = (
    "https://www.nhc.noaa.gov/productexamples/NHC_JSON_Sample.json"
)


@contextmanager
def _patch_current_storms_url(url: str):
    """Redirect lens.nhc._fetch_current_storms_json to read from `url`.

    Both the tracks fetch (lens.nhc.download_nhc) and the WSP fetch
    (lens.nhc._load_nhc_wsp_current) call _fetch_current_storms_json
    internally — patching it once covers both. WSP itself then follows
    the windSpeedProbabilitiesGIS.zipFile5km URL embedded in the (sample)
    JSON, identical to realtime behaviour.
    """
    original = _lens_nhc._fetch_current_storms_json

    def _patched():
        logger.info(f"Test mode: fetching CurrentStorms JSON from {url}")
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()

    _lens_nhc._fetch_current_storms_json = _patched
    try:
        yield
    finally:
        _lens_nhc._fetch_current_storms_json = original


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
        # Conflict-target the PK (atcf_id) rather than the redundant
        # nhc_storms_unique(atcf_id, storm_id). storm_id is derived from
        # the storm's current name, so it changes when NHC renames a
        # system (e.g. placeholder "Nine" → "Harold"). Keying on the PK
        # lets those renames overwrite the row instead of tripping a
        # plain-INSERT PK violation.
        storms.to_sql(
            "nhc_storms",
            con=conn,
            schema="storms",
            if_exists="append",
            index=False,
            method=partial(
                stratus.postgres_upsert, constraint="nhc_storms_pkey"
            ),
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
            name="nhc_wsp_polygon_raw",
            con=conn,
            schema="storms",
            if_exists="append",
            index=False,
            method=stratus.postgres_upsert,
            chunksize=chunksize,
        )

    logger.info("Successfully processed WSP polygons.")
    return gdf


def _to_multipolygon(g):
    """Coerce a shapely geometry into a MultiPolygon (or None if empty/non-polygonal)."""
    from shapely.geometry import MultiPolygon, Polygon
    if g is None or g.is_empty:
        return None
    if isinstance(g, MultiPolygon):
        return g
    if isinstance(g, Polygon):
        return MultiPolygon([g])
    # Rare: dissolve may emit a GeometryCollection if inputs are mixed.
    polys = [p for p in g.geoms if p.geom_type in ("Polygon", "MultiPolygon")]
    flat: list[Polygon] = []
    for p in polys:
        if isinstance(p, Polygon):
            flat.append(p)
        else:
            flat.extend(p.geoms)
    return MultiPolygon(flat) if flat else None


def _list_wsp_issued_times(
    engine,
    since: str | None = None,
    basin: str | None = None,
    issued_time=None,
) -> list:
    """Return the issued_times in nhc_wsp_polygon_raw that also have tracks,
    after applying the user filters. Sorted ascending."""
    filters = []
    params: dict = {}
    if since:
        filters.append("t.issued_time >= :since")
        params["since"] = since
    if basin:
        filters.append("t.basin = :basin")
        params["basin"] = basin
    if issued_time is not None:
        filters.append("t.issued_time = :issued_time")
        params["issued_time"] = issued_time
    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    sql = (
        "SELECT DISTINCT r.issued_time"
        " FROM storms.nhc_wsp_polygon_raw r"
        " INNER JOIN storms.nhc_tracks_geo t"
        " ON t.issued_time = r.issued_time"
        f" {where}"
        " ORDER BY r.issued_time"
    )
    with engine.connect() as conn:
        return [row[0] for row in conn.execute(text(sql), params)]


def _build_matched_for_issued_time(engine, it, chunksize: int = 500) -> int:
    """Build one issued_time's worth of nhc_wsp_polygon_matched rows.

    Returns the number of rows written.
    """
    from ocha_lens.utils.storm import match_wsp_to_tracks
    from shapely.validation import make_valid

    with engine.connect() as conn:
        gdf_wsp_raw = gpd.read_postgis(
            text(
                "SELECT id, issued_time, wind_threshold_kt, percentage, geometry"
                " FROM storms.nhc_wsp_polygon_raw"
                " WHERE issued_time = :it"
            ),
            conn, geom_col="geometry", params={"it": it},
        )
        gdf_tracks = gpd.read_postgis(
            text(
                "SELECT atcf_id, issued_time, geometry"
                " FROM storms.nhc_tracks_geo WHERE issued_time = :it"
            ),
            conn, geom_col="geometry", params={"it": it},
        )

    if gdf_wsp_raw.empty or gdf_tracks.empty:
        return 0

    mask = ~gdf_wsp_raw.geometry.is_empty & gdf_wsp_raw.geometry.notna()
    gdf_wsp_raw = gdf_wsp_raw[mask].copy()
    gdf_wsp_raw["geometry"] = gdf_wsp_raw["geometry"].apply(
        lambda g: g if g.is_valid else make_valid(g)
    )

    matched = match_wsp_to_tracks(gdf_wsp_raw, gdf_tracks)
    key_cols = ["issued_time", "wind_threshold_kt", "percentage", "atcf_id"]
    dissolved = matched.dissolve(by=key_cols, dropna=False, as_index=False)
    dissolved["geometry"] = dissolved["geometry"].apply(_to_multipolygon)
    dissolved = dissolved[dissolved["geometry"].notna()].copy()
    if dissolved.empty:
        return 0

    out = dissolved[key_cols + ["geometry"]].copy()
    out["geometry"] = out["geometry"].apply(lambda g: g.wkt)
    with engine.connect() as conn:
        out.to_sql(
            name="nhc_wsp_polygon_matched",
            con=conn,
            schema="storms",
            if_exists="append",
            index=False,
            method=stratus.postgres_upsert,
            chunksize=chunksize,
        )
        conn.commit()
    return len(out)


def process_nhc_wsp_polygon_matched(
    engine,
    since: str | None = None,
    basin: str | None = None,
    issued_time=None,
    overwrite: bool = False,
    chunksize: int = 500,
) -> None:
    """Build storms.nhc_wsp_polygon_matched from nhc_wsp_polygon_raw + tracks.

    Processes one issued_time at a time, committing after each. Keeps peak
    memory bounded by the largest single issuance.
    """
    issued_times = _list_wsp_issued_times(
        engine, since=since, basin=basin, issued_time=issued_time,
    )
    if not issued_times:
        logger.info("No raw WSP issued_times match filters. Skipping.")
        return

    # Build the skip-set once when overwrite=False.
    existing_its: set = set()
    if not overwrite:
        with engine.connect() as conn:
            r = conn.execute(text(
                "SELECT DISTINCT issued_time FROM storms.nhc_wsp_polygon_matched"
            ))
            existing_its = {row[0] for row in r}

    n_done = n_written = n_skipped = 0
    total = len(issued_times)
    for it in issued_times:
        if not overwrite and it in existing_its:
            n_skipped += 1
            continue
        rows = _build_matched_for_issued_time(engine, it, chunksize=chunksize)
        n_done += 1
        n_written += rows
        if n_done % 25 == 0 or n_done == total - n_skipped:
            logger.info(
                f"matched: {n_done}/{total - n_skipped} issued_times done, "
                f"{n_written} rows written, {n_skipped} skipped"
            )
    logger.info(
        f"nhc_wsp_polygon_matched done: {n_done} issued_times processed, "
        f"{n_written} rows written, {n_skipped} skipped."
    )


def run_nhc_wsp_polygon_matched(
    mode: str = "dev",
    since: str | None = None,
    basin: str | None = None,
    issued_time=None,
    overwrite: bool = False,
    chunksize: int = 500,
) -> None:
    """CLI wrapper for process_nhc_wsp_polygon_matched."""
    engine = stratus.get_engine(stage=mode, write=True)
    process_nhc_wsp_polygon_matched(
        engine=engine,
        since=since,
        basin=basin,
        issued_time=issued_time,
        overwrite=overwrite,
        chunksize=chunksize,
    )


def _list_null_issued_times(
    engine,
    since: str | None = None,
    issued_time=None,
) -> list:
    """Return issued_times in nhc_wsp_polygon_matched that have at least one
    atcf_id IS NULL row, sorted ascending."""
    filters = ["atcf_id IS NULL"]
    params: dict = {}
    if since:
        filters.append("issued_time >= :since")
        params["since"] = since
    if issued_time is not None:
        filters.append("issued_time = :issued_time")
        params["issued_time"] = issued_time
    where = "WHERE " + " AND ".join(filters)
    sql = (
        f"SELECT DISTINCT issued_time FROM storms.nhc_wsp_polygon_matched"
        f" {where} ORDER BY issued_time"
    )
    with engine.connect() as conn:
        return [row[0] for row in conn.execute(text(sql), params)]


def _fill_nulls_for_issued_time(engine, it) -> tuple[int, int, int]:
    """For one issued_time, re-match NULL parts using existing non-NULL rows
    as containment donors, then reconcile the matched table in a single
    transaction.

    Returns (parts_promoted, parts_still_null, rows_touched).
    """
    from ocha_lens.utils.storm import match_wsp_to_tracks
    from shapely.validation import make_valid

    offset = pd.Timedelta(hours=3)

    with engine.connect() as conn:
        # Existing matched (atcf_id NOT NULL) → containment donors.
        gdf_containers = gpd.read_postgis(
            text(
                "SELECT issued_time, wind_threshold_kt, percentage, atcf_id,"
                " geometry FROM storms.nhc_wsp_polygon_matched"
                " WHERE issued_time = :it AND atcf_id IS NOT NULL"
            ),
            conn,
            geom_col="geometry",
            params={"it": it},
        )
        # NULL rows we want to refine.
        gdf_null = gpd.read_postgis(
            text(
                "SELECT issued_time, wind_threshold_kt, percentage, geometry"
                " FROM storms.nhc_wsp_polygon_matched"
                " WHERE issued_time = :it AND atcf_id IS NULL"
            ),
            conn,
            geom_col="geometry",
            params={"it": it},
        )
        # Tracks at issued_time and at issued_time+3h (the matcher's window).
        gdf_tracks = gpd.read_postgis(
            text(
                "SELECT atcf_id, issued_time, valid_time, geometry"
                " FROM storms.nhc_tracks_geo"
                " WHERE issued_time IN (:it, :it_plus)"
            ),
            conn,
            geom_col="geometry",
            params={"it": it, "it_plus": it + offset},
        )

    if gdf_null.empty:
        return 0, 0, 0

    # Explode NULL rows into individual parts.
    mask = ~gdf_null.geometry.is_empty & gdf_null.geometry.notna()
    gdf_null = gdf_null[mask].copy()
    gdf_null["geometry"] = gdf_null["geometry"].apply(
        lambda g: g if g.is_valid else make_valid(g)
    )
    null_parts = gdf_null.explode(index_parts=False).reset_index(drop=True)
    if null_parts.empty:
        return 0, 0, 0

    # Run the matcher. With no track-line intersect (otherwise these rows
    # would have matched the first time), promotions come from the
    # containment fallback against extra_containers (and from any new
    # matches a lower NULL band might produce within this same call).
    matched = match_wsp_to_tracks(
        null_parts,
        gdf_tracks,
        extra_containers=gdf_containers if not gdf_containers.empty else None,
    )

    key_cols = ["issued_time", "wind_threshold_kt", "percentage", "atcf_id"]
    dissolved = matched.dissolve(by=key_cols, dropna=False, as_index=False)
    dissolved["geometry"] = dissolved["geometry"].apply(_to_multipolygon)
    dissolved = dissolved[dissolved["geometry"].notna()].copy()
    if dissolved.empty:
        return 0, 0, 0

    # Split into newly-matched (atcf_id NOT NULL) and residual-NULL groups.
    promoted = dissolved[dissolved["atcf_id"].notna()].copy()
    residual = dissolved[dissolved["atcf_id"].isna()].copy()

    n_promoted = sum(g.geoms.__len__() if g.geom_type == "MultiPolygon" else 1
                     for g in promoted.geometry)
    n_residual = sum(g.geoms.__len__() if g.geom_type == "MultiPolygon" else 1
                     for g in residual.geometry)
    n_touched = 0

    # All DB mutations for this issued_time in a single transaction.
    with engine.begin() as conn:
        # 1. Drop every existing NULL row for this issued_time so we can
        #    re-insert just the residual (if any).
        conn.execute(
            text(
                "DELETE FROM storms.nhc_wsp_polygon_matched"
                " WHERE issued_time = :it AND atcf_id IS NULL"
            ),
            {"it": it},
        )

        # 2. Re-insert residual NULL rows (parts that still didn't match).
        for _, r in residual.iterrows():
            conn.execute(
                text(
                    "INSERT INTO storms.nhc_wsp_polygon_matched"
                    " (issued_time, wind_threshold_kt, percentage, atcf_id, geometry)"
                    " VALUES (:it, :kt, :pct, NULL, ST_GeomFromText(:wkt, 4326))"
                ),
                {
                    "it": r["issued_time"], "kt": int(r["wind_threshold_kt"]),
                    "pct": int(r["percentage"]), "wkt": r.geometry.wkt,
                },
            )
            n_touched += 1

        # 3. For each newly-promoted (kt, pct, atcf_id), either ST_Union into
        #    the existing row or INSERT a new one.
        for _, r in promoted.iterrows():
            params = {
                "it": r["issued_time"], "kt": int(r["wind_threshold_kt"]),
                "pct": int(r["percentage"]), "aid": r["atcf_id"],
                "wkt": r.geometry.wkt,
            }
            existing = conn.execute(
                text(
                    "SELECT 1 FROM storms.nhc_wsp_polygon_matched"
                    " WHERE issued_time = :it AND wind_threshold_kt = :kt"
                    " AND percentage = :pct AND atcf_id = :aid"
                ),
                params,
            ).first()
            if existing:
                conn.execute(
                    text(
                        "UPDATE storms.nhc_wsp_polygon_matched"
                        " SET geometry = ST_Multi(ST_Union(geometry,"
                        " ST_GeomFromText(:wkt, 4326)))"
                        " WHERE issued_time = :it AND wind_threshold_kt = :kt"
                        " AND percentage = :pct AND atcf_id = :aid"
                    ),
                    params,
                )
            else:
                conn.execute(
                    text(
                        "INSERT INTO storms.nhc_wsp_polygon_matched"
                        " (issued_time, wind_threshold_kt, percentage, atcf_id, geometry)"
                        " VALUES (:it, :kt, :pct, :aid,"
                        " ST_Multi(ST_GeomFromText(:wkt, 4326)))"
                    ),
                    params,
                )
            n_touched += 1

    return n_promoted, n_residual, n_touched


def fill_null_wsp_polygon_matched(
    engine,
    since: str | None = None,
    issued_time=None,
) -> None:
    """Re-match the NULL parts in storms.nhc_wsp_polygon_matched using the
    containment fallback + band ordering provided by
    ``ocha_lens.utils.storm.match_wsp_to_tracks``.

    Surgical: existing non-NULL rows are only ever extended (ST_Union) with
    newly-matched parts; nothing is deleted or re-keyed. Existing NULL rows
    are shrunk (or removed) as their parts find homes.

    The work is per-issued_time, each in its own transaction.
    """
    issued_times = _list_null_issued_times(
        engine, since=since, issued_time=issued_time,
    )
    if not issued_times:
        logger.info(
            "No issued_times have atcf_id IS NULL rows. Nothing to fill."
        )
        return

    total = len(issued_times)
    n_done = n_promoted = n_residual = 0
    for it in issued_times:
        try:
            p, r, _t = _fill_nulls_for_issued_time(engine, it)
        except Exception as e:
            logger.error(f"fill-nulls failed at {it}: {e}", exc_info=True)
            continue
        n_done += 1
        n_promoted += p
        n_residual += r
        if n_done % 25 == 0 or n_done == total:
            logger.info(
                f"fill-nulls: {n_done}/{total} issued_times — "
                f"promoted parts: {n_promoted}, still NULL parts: {n_residual}"
            )
    logger.info(
        f"fill-nulls done: {n_done}/{total} issued_times processed, "
        f"{n_promoted} parts promoted, {n_residual} parts still NULL."
    )


def run_fill_null_wsp_polygon_matched(
    mode: str = "dev",
    since: str | None = None,
    issued_time=None,
) -> None:
    """CLI wrapper for fill_null_wsp_polygon_matched."""
    engine = stratus.get_engine(stage=mode, write=True)
    fill_null_wsp_polygon_matched(
        engine=engine, since=since, issued_time=issued_time,
    )


def run_nhc_current(
    mode="local",
    save_to_blob=False,
    save_dir="storm",
    chunksize=10000,
    sample_json: str | None = None,
) -> dict:
    """Main function to process current NHC storms.

    Returns a dict with the issued_times of the just-fetched data:
        {"track_issued_time": pd.Timestamp | None,
         "wsp_issued_time":   pd.Timestamp | None}
    Both values come directly from the scraped JSON / shapefile — not
    from a post-write DB query — so downstream tasks can use them
    without races against concurrent writers.

    When ``sample_json`` is set, the CurrentStorms JSON is read from that
    URL instead of the live NHC endpoint (test mode). WSP polygons still
    flow from the GIS URL embedded in the JSON, exactly like realtime.
    """
    coloredlogs.install(
        logger=logger,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    logger.info("Starting NHC Current Storms ETL pipeline...")
    engine = stratus.get_engine(stage=mode, write=True)

    track_issued_time = None
    wsp_issued_time = None

    ctx = (
        _patch_current_storms_url(sample_json) if sample_json else nullcontext()
    )

    try:
        with ctx:
            df_raw = retrieve_nhc_current(
                stage=mode,
                save_to_blob=save_to_blob,
                save_dir=save_dir,
            )

            if df_raw is None:
                logger.info("No active storms. Pipeline finished.")
                return {
                    "track_issued_time": None,
                    "wsp_issued_time": None,
                }

            process_storms(df_raw=df_raw, engine=engine, chunksize=chunksize)
            process_tracks(df_raw=df_raw, engine=engine, chunksize=chunksize)
            if "issued_time" in df_raw.columns:
                track_issued_time = pd.to_datetime(df_raw["issued_time"]).max()

            logger.info("Fetching current WSP polygons...")
            wsp_gdf = lens.nhc.get_wsp()
            if wsp_gdf is not None and len(wsp_gdf) > 0:
                process_wsp_polygons(
                    gdf=wsp_gdf, engine=engine, chunksize=chunksize
                )
                if "issued_time" in wsp_gdf.columns:
                    wsp_issued_time = pd.to_datetime(
                        wsp_gdf["issued_time"]
                    ).max()
            else:
                logger.info("No current WSP data available.")

        logger.info("Pipeline successfully finished!")
        logger.info(
            f"TRACK_ISSUED_TIME={track_issued_time}  "
            f"WSP_ISSUED_TIME={wsp_issued_time}"
        )

        return {
            "track_issued_time": track_issued_time,
            "wsp_issued_time": wsp_issued_time,
        }

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise


# Atcf-keyed NHC tables in scrub order: downstream first, parents last,
# so foreign-key references (if any) clear before the rows they point to.
_NHC_SCRUB_ATCF_TABLES = [
    "nhc_tracks_fcast_exposure",
    "nhc_tracks_obsv_exposure",
    "nhc_tracks_fcastonly_exposure",
    "nhc_wsp_exposure",
    "nhc_wsp_fcastonly_exposure",
    "nhc_tracks_fcast_buffers",
    "nhc_tracks_obsv_buffers",
    "nhc_tracks_fcastonly_buffers",
    "nhc_wsp_polygon_matched",
    "nhc_wsp_fcastonly_polygon",
    "nhc_tracks_geo",
    "nhc_storms",
]


def run_nhc_scrub(
    atcf_ids: list[str],
    issued_times: list[pd.Timestamp] | list[str] | None = None,
    mode: str = "local",
    dry_run: bool = False,
) -> None:
    """Delete rows for the given atcf_ids from every NHC table.

    ``issued_times`` is required ONLY to scrub ``nhc_wsp_polygon_raw``
    (which has no atcf_id — it's a basin-wide MultiPolygon keyed on
    issued_time / threshold / band). If omitted, that table is skipped.

    All deletes commit as a single transaction; any failure rolls the
    whole scrub back.
    """
    coloredlogs.install(
        logger=logger,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    if not atcf_ids:
        raise ValueError("atcf_ids must be non-empty")

    atcf_ids = sorted(set(atcf_ids))
    logger.info(
        f"{'[dry-run] ' if dry_run else ''}Scrubbing atcf_ids={atcf_ids} "
        f"issued_times={list(issued_times) if issued_times else []}"
    )

    engine = stratus.get_engine(stage=mode, write=True)
    with engine.begin() as conn:
        for tbl in _NHC_SCRUB_ATCF_TABLES:
            if dry_run:
                n = conn.execute(
                    text(
                        f"SELECT COUNT(*) FROM storms.{tbl} "
                        "WHERE atcf_id = ANY(:ids)"
                    ),
                    {"ids": atcf_ids},
                ).scalar()
                logger.info(f"[dry-run] would delete {n} rows from {tbl}")
            else:
                result = conn.execute(
                    text(
                        f"DELETE FROM storms.{tbl} "
                        "WHERE atcf_id = ANY(:ids)"
                    ),
                    {"ids": atcf_ids},
                )
                logger.info(f"deleted {result.rowcount} rows from {tbl}")

        if issued_times:
            ts_list = [pd.Timestamp(t) for t in issued_times]
            if dry_run:
                n = conn.execute(
                    text(
                        "SELECT COUNT(*) FROM storms.nhc_wsp_polygon_raw "
                        "WHERE issued_time = ANY(:ts)"
                    ),
                    {"ts": ts_list},
                ).scalar()
                logger.info(
                    f"[dry-run] would delete {n} rows from nhc_wsp_polygon_raw"
                )
            else:
                result = conn.execute(
                    text(
                        "DELETE FROM storms.nhc_wsp_polygon_raw "
                        "WHERE issued_time = ANY(:ts)"
                    ),
                    {"ts": ts_list},
                )
                logger.info(
                    f"deleted {result.rowcount} rows from nhc_wsp_polygon_raw"
                )
        else:
            logger.info(
                "No issued_times supplied — skipping nhc_wsp_polygon_raw"
            )

    logger.info(f"{'[dry-run] ' if dry_run else ''}Scrub complete.")


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
_DEFAULT_ADMIN_LEVELS = [0, 1]


def _process_buffer_exposure_country(
    *,
    iso3: str,
    country_units: gpd.GeoDataFrame,    # rows for this iso3, cols [pcode, geometry]
    admin_level: int,
    gdf_buffers,                         # epsg:4326
    gdf_buffers_anti,                    # antimeridian-wrapped
    buffers_sindex,                      # gdf_buffers.sindex
    da_wp_global,
    da_wp_wrapped,
    done_df: pd.DataFrame,
    overwrite: bool,
    key_cols: list[str],
    done_filter,
    out_table: str,
    engine,
    drop_cols: list[str] | None = None,
) -> int:
    """Compute and write per-unit exposure for all admin units in one country.

    Country geometry (union of its units) is used to pre-clip WorldPop and
    spatially prefilter buffers; per-unit work then sub-clips that smaller
    raster and intersects against the smaller buffer set.

    Returns number of units written (0 if nothing intersected or all done).
    """
    from src.utils.exposure import GEO_CRS_ANTIMERIDIAN, calculate_exposure

    country_geom = country_units.geometry.union_all()
    minx, _, maxx, _ = country_geom.bounds
    wrap = maxx > 160 or minx < -160

    if wrap:
        da_wp = da_wp_wrapped
        country_geom_local = (
            gpd.GeoSeries([country_geom], crs=4326)
            .to_crs(GEO_CRS_ANTIMERIDIAN).iloc[0]
        )
        country_buffers = gdf_buffers_anti[
            gdf_buffers_anti.intersects(country_geom_local)
        ]
    else:
        da_wp = da_wp_global
        country_geom_local = country_geom
        candidate_idx = list(buffers_sindex.intersection(country_geom.bounds))
        if not candidate_idx:
            return 0
        candidates = gdf_buffers.iloc[candidate_idx]
        country_buffers = candidates[candidates.intersects(country_geom)]

    if country_buffers.empty:
        return 0

    da_wp_country = da_wp.rio.clip([country_geom_local], all_touched=True)

    # For admin1, build a small sindex over the country-clipped buffers so
    # per-unit intersect is cheap. For admin0 we skip — the only unit IS
    # the country.
    unit_sindex = country_buffers.sindex if admin_level > 0 else None

    writes = 0
    for _, unit in country_units.iterrows():
        pcode = unit["pcode"]

        if admin_level == 0:
            buf_in = country_buffers
            unit_geom_local = country_geom_local
            da_wp_unit = da_wp_country
        else:
            unit_geom = unit.geometry
            if wrap:
                unit_geom_local = (
                    gpd.GeoSeries([unit_geom], crs=4326)
                    .to_crs(GEO_CRS_ANTIMERIDIAN).iloc[0]
                )
                buf_in = country_buffers[country_buffers.intersects(unit_geom_local)]
            else:
                unit_geom_local = unit_geom
                idx2 = list(unit_sindex.intersection(unit_geom.bounds))
                if not idx2:
                    continue
                unit_candidates = country_buffers.iloc[idx2]
                buf_in = unit_candidates[unit_candidates.intersects(unit_geom)]
            if buf_in.empty:
                continue
            try:
                da_wp_unit = da_wp_country.rio.clip([unit_geom_local], all_touched=True)
            except Exception:
                continue

        if not overwrite and not done_df.empty:
            done_unit = done_df[done_df["pcode"] == pcode]
            if not done_unit.empty:
                buf_in = done_filter(buf_in, done_unit)
                if buf_in.empty:
                    continue

        df = calculate_exposure(buf_in, da_wp_unit)
        df["iso3"] = iso3
        df["pcode"] = pcode
        df["admin_level"] = admin_level

        if drop_cols:
            df = df.drop(columns=drop_cols, errors="ignore")

        out = df.drop_duplicates(subset=key_cols, keep="last")
        with engine.connect() as conn:
            out.to_sql(
                out_table,
                conn,
                schema="storms",
                if_exists="append",
                index=False,
                method=stratus.postgres_upsert,
            )
            conn.commit()
        writes += 1

    return writes


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
        join = " JOIN storms.nhc_storms s ON b.atcf_id = s.atcf_id"
    else:
        join = ""
    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    query = (
        "SELECT b.atcf_id, b.issued_time, b.wind_speed_kt, b.geometry"
        " FROM storms.nhc_tracks_fcast_buffers b"
        f"{join} {where}"
    )

    with engine.connect() as conn:
        return gpd.read_postgis(query, conn, geom_col="geometry")


def _load_done_nhc_tracks_fcast_exp(engine, admin_level: int) -> pd.DataFrame:
    try:
        with engine.connect() as conn:
            return pd.read_sql(
                text(
                    "SELECT atcf_id, issued_time, wind_speed_kt, admin_level, pcode"
                    " FROM storms.nhc_tracks_fcast_exposure"
                    " WHERE admin_level = :al"
                ),
                conn,
                params={"al": admin_level},
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
    admin_levels: list[int] | None = None,
) -> None:
    import warnings
    from rasterio.errors import ShapeSkipWarning
    from src.utils.exposure import GEO_CRS_ANTIMERIDIAN, load_adm_units, load_pop

    warnings.filterwarnings("ignore", category=ShapeSkipWarning)
    engine = stratus.get_engine(stage=mode, write=True)

    admin_levels = admin_levels or _DEFAULT_ADMIN_LEVELS

    logger.info("Loading NHC wind buffers for exposure calculation...")
    gdf_buffers = _load_nhc_tracks_fcast_exp_buffers(engine, since=since, basin=basin, issued_time=issued_time)
    if gdf_buffers.empty:
        logger.info("No wind buffers found for the given filters. Skipping.")
        return
    gdf_buffers_anti = gdf_buffers.to_crs(GEO_CRS_ANTIMERIDIAN)
    buffers_sindex = gdf_buffers.sindex

    da_wp_global, da_wp_wrapped = load_pop()

    for admin_level in admin_levels:
        gdf_units = load_adm_units(countries, admin_level)
        country_groups = list(gdf_units.groupby("iso3"))
        logger.info(
            f"admin_level={admin_level}: {len(country_groups)} countries, "
            f"{len(gdf_units)} units"
        )

        done_df = (
            pd.DataFrame(columns=_TRACK_EXP_KEY_COLS) if overwrite
            else _load_done_nhc_tracks_fcast_exp(engine, admin_level)
        )

        processed = skipped = 0
        for i, (iso3, country_units) in enumerate(country_groups, 1):
            prefix = f"[adm{admin_level}][{i}/{len(country_groups)}] {iso3}"
            n = _process_buffer_exposure_country(
                iso3=iso3,
                country_units=country_units,
                admin_level=admin_level,
                gdf_buffers=gdf_buffers,
                gdf_buffers_anti=gdf_buffers_anti,
                buffers_sindex=buffers_sindex,
                da_wp_global=da_wp_global,
                da_wp_wrapped=da_wp_wrapped,
                done_df=done_df,
                overwrite=overwrite,
                key_cols=_TRACK_EXP_KEY_COLS,
                done_filter=_filter_done_nhc_tracks_fcast,
                out_table="nhc_tracks_fcast_exposure",
                engine=engine,
            )
            if n:
                processed += 1
                logger.info(f"{prefix} — {n} unit writes")
            else:
                skipped += 1

        logger.info(
            f"admin_level={admin_level} done: {processed} countries written, "
            f"{skipped} skipped."
        )
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
        join = " JOIN storms.nhc_storms s ON b.atcf_id = s.atcf_id"
    else:
        join = ""
    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    query = (
        "SELECT b.atcf_id, b.valid_time, b.wind_speed_kt, b.geometry"
        " FROM storms.nhc_tracks_obsv_buffers b"
        f"{join} {where}"
    )

    with engine.connect() as conn:
        return gpd.read_postgis(query, conn, geom_col="geometry")


def _load_done_nhc_tracks_obsv_exp(engine, admin_level: int) -> pd.DataFrame:
    try:
        with engine.connect() as conn:
            return pd.read_sql(
                text(
                    "SELECT atcf_id, valid_time, wind_speed_kt, admin_level, pcode"
                    " FROM storms.nhc_tracks_obsv_exposure"
                    " WHERE admin_level = :al"
                ),
                conn,
                params={"al": admin_level},
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
    admin_levels: list[int] | None = None,
    final_only: bool = False,
) -> None:
    import warnings
    from rasterio.errors import ShapeSkipWarning
    from src.utils.exposure import GEO_CRS_ANTIMERIDIAN, load_adm_units, load_pop

    warnings.filterwarnings("ignore", category=ShapeSkipWarning)
    engine = stratus.get_engine(stage=mode, write=True)

    admin_levels = admin_levels or _DEFAULT_ADMIN_LEVELS

    logger.info("Loading NHC observed track buffers for exposure calculation...")
    gdf_buffers = _load_nhc_tracks_obsv_exp_buffers(engine, since=since, basin=basin, valid_time=valid_time)
    if gdf_buffers.empty:
        logger.info("No observed track buffers found for the given filters. Skipping.")
        return

    if final_only:
        before = len(gdf_buffers)
        idx = gdf_buffers.groupby(["atcf_id", "wind_speed_kt"])["valid_time"].idxmax()
        gdf_buffers = gdf_buffers.loc[idx].reset_index(drop=True)
        logger.info(
            f"final_only: trimmed {before:,} buffers → {len(gdf_buffers):,} "
            f"(one per atcf_id × wind_speed_kt at max(valid_time))"
        )

    gdf_buffers_anti = gdf_buffers.to_crs(GEO_CRS_ANTIMERIDIAN)
    buffers_sindex = gdf_buffers.sindex

    da_wp_global, da_wp_wrapped = load_pop()

    for admin_level in admin_levels:
        gdf_units = load_adm_units(countries, admin_level)
        country_groups = list(gdf_units.groupby("iso3"))
        logger.info(
            f"admin_level={admin_level}: {len(country_groups)} countries, "
            f"{len(gdf_units)} units"
        )

        done_df = (
            pd.DataFrame(columns=_OBSV_EXP_KEY_COLS) if overwrite
            else _load_done_nhc_tracks_obsv_exp(engine, admin_level)
        )

        processed = skipped = 0
        for i, (iso3, country_units) in enumerate(country_groups, 1):
            prefix = f"[adm{admin_level}][{i}/{len(country_groups)}] {iso3}"
            n = _process_buffer_exposure_country(
                iso3=iso3,
                country_units=country_units,
                admin_level=admin_level,
                gdf_buffers=gdf_buffers,
                gdf_buffers_anti=gdf_buffers_anti,
                buffers_sindex=buffers_sindex,
                da_wp_global=da_wp_global,
                da_wp_wrapped=da_wp_wrapped,
                done_df=done_df,
                overwrite=overwrite,
                key_cols=_OBSV_EXP_KEY_COLS,
                done_filter=_filter_done_nhc_tracks_obsv,
                out_table="nhc_tracks_obsv_exposure",
                engine=engine,
            )
            if n:
                processed += 1
                logger.info(f"{prefix} — {n} unit writes")
            else:
                skipped += 1

        logger.info(
            f"admin_level={admin_level} obsv exposure done: {processed} countries "
            f"written, {skipped} skipped."
        )
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
        join = " JOIN storms.nhc_storms s ON b.atcf_id = s.atcf_id"
    else:
        join = ""
    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    query = (
        "SELECT b.atcf_id, b.issued_time, b.wind_speed_kt, b.geometry"
        " FROM storms.nhc_tracks_fcastonly_buffers b"
        f"{join} {where}"
    )

    with engine.connect() as conn:
        return gpd.read_postgis(query, conn, geom_col="geometry")


def _load_done_nhc_tracks_fcastonly_exp(engine, admin_level: int) -> pd.DataFrame:
    try:
        with engine.connect() as conn:
            return pd.read_sql(
                text(
                    "SELECT atcf_id, issued_time, wind_speed_kt, admin_level, pcode"
                    " FROM storms.nhc_tracks_fcastonly_exposure"
                    " WHERE admin_level = :al"
                ),
                conn,
                params={"al": admin_level},
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
    admin_levels: list[int] | None = None,
) -> None:
    import warnings
    from rasterio.errors import ShapeSkipWarning
    from src.utils.exposure import GEO_CRS_ANTIMERIDIAN, load_adm_units, load_pop

    warnings.filterwarnings("ignore", category=ShapeSkipWarning)
    engine = stratus.get_engine(stage=mode, write=True)

    admin_levels = admin_levels or _DEFAULT_ADMIN_LEVELS

    logger.info("Loading NHC forecast-only track buffers for exposure calculation...")
    gdf_buffers = _load_nhc_tracks_fcastonly_exp_buffers(engine, since=since, basin=basin, issued_time=issued_time)
    if gdf_buffers.empty:
        logger.info("No forecast-only track buffers found for the given filters. Skipping.")
        return
    gdf_buffers_anti = gdf_buffers.to_crs(GEO_CRS_ANTIMERIDIAN)
    buffers_sindex = gdf_buffers.sindex

    da_wp_global, da_wp_wrapped = load_pop()

    for admin_level in admin_levels:
        gdf_units = load_adm_units(countries, admin_level)
        country_groups = list(gdf_units.groupby("iso3"))
        logger.info(
            f"admin_level={admin_level}: {len(country_groups)} countries, "
            f"{len(gdf_units)} units"
        )

        done_df = (
            pd.DataFrame(columns=_FCASTONLY_EXP_KEY_COLS) if overwrite
            else _load_done_nhc_tracks_fcastonly_exp(engine, admin_level)
        )

        processed = skipped = 0
        for i, (iso3, country_units) in enumerate(country_groups, 1):
            prefix = f"[adm{admin_level}][{i}/{len(country_groups)}] {iso3}"
            n = _process_buffer_exposure_country(
                iso3=iso3,
                country_units=country_units,
                admin_level=admin_level,
                gdf_buffers=gdf_buffers,
                gdf_buffers_anti=gdf_buffers_anti,
                buffers_sindex=buffers_sindex,
                da_wp_global=da_wp_global,
                da_wp_wrapped=da_wp_wrapped,
                done_df=done_df,
                overwrite=overwrite,
                key_cols=_FCASTONLY_EXP_KEY_COLS,
                done_filter=_filter_done_nhc_tracks_fcastonly,
                out_table="nhc_tracks_fcastonly_exposure",
                engine=engine,
            )
            if n:
                processed += 1
                logger.info(f"{prefix} — {n} unit writes")
            else:
                skipped += 1

        logger.info(
            f"admin_level={admin_level} fcastonly exposure done: {processed} "
            f"countries written, {skipped} skipped."
        )
    engine.dispose()


# ---------------------------------------------------------------------------
# Population exposure — NHC WSP polygons
# ---------------------------------------------------------------------------


def _load_wsp_for_exposure(
    engine,
    since: str | None = None,
    basin: str | None = None,
    issued_time=None,
    year: int | None = None,
) -> gpd.GeoDataFrame:
    """Load matched-per-storm WSP polygons.

    Reads from storms.nhc_wsp_polygon_matched (already one MultiPolygon per
    (issued_time, wind_threshold_kt, percentage, atcf_id)), so no
    match_wsp_to_tracks call or post-load dissolve is needed here. To filter
    by basin, joins on storms.nhc_storms.genesis_basin.

    Pass ``year=YYYY`` to restrict the load to one calendar year — useful
    for chunking large historical scans.
    """
    filters: list[str] = []
    params: dict = {}
    if since:
        filters.append("m.issued_time >= :since")
        params["since"] = since
    if issued_time is not None:
        filters.append("m.issued_time = :issued_time")
        params["issued_time"] = issued_time
    if basin:
        filters.append("s.genesis_basin = :basin")
        params["basin"] = basin
    if year is not None:
        filters.append("EXTRACT(YEAR FROM m.issued_time) = :y")
        params["y"] = year

    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    join = "LEFT JOIN storms.nhc_storms s ON s.atcf_id = m.atcf_id" if basin else ""

    sql = (
        "SELECT m.issued_time, m.wind_threshold_kt, m.percentage,"
        " m.atcf_id, m.geometry"
        " FROM storms.nhc_wsp_polygon_matched m"
        f" {join}"
        f" {where}"
    )

    with engine.connect() as conn:
        gdf_wsp = gpd.read_postgis(
            text(sql), conn, geom_col="geometry", params=params,
        )

    n_matched = gdf_wsp["atcf_id"].notna().sum()
    logger.info(
        f"  Loaded {len(gdf_wsp)} matched WSP polygons; "
        f"{n_matched} with atcf_id, {len(gdf_wsp) - n_matched} unmatched"
    )
    return gdf_wsp


def _load_done_nhc_wsp_exp(engine, admin_level: int) -> pd.DataFrame:
    try:
        with engine.connect() as conn:
            return pd.read_sql(
                text(
                    "SELECT issued_time, wind_threshold_kt, percentage, atcf_id, admin_level, pcode"
                    " FROM storms.nhc_wsp_exposure"
                    " WHERE admin_level = :al"
                ),
                conn,
                params={"al": admin_level},
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


def _list_years(
    engine,
    table: str,
    since: str | None = None,
    basin: str | None = None,
) -> list[int]:
    """List distinct years in a WSP-shaped table (issued_time TIMESTAMP)."""
    filters = []
    params: dict = {}
    if since:
        filters.append("t.issued_time >= :since")
        params["since"] = since
    if basin:
        filters.append("s.genesis_basin = :basin")
        params["basin"] = basin
    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    join = (
        "LEFT JOIN storms.nhc_storms s ON s.atcf_id = t.atcf_id"
        if basin else ""
    )
    sql = (
        "SELECT DISTINCT EXTRACT(YEAR FROM t.issued_time)::int AS y"
        f" FROM storms.{table} t {join} {where}"
        " ORDER BY y"
    )
    with engine.connect() as conn:
        return [row[0] for row in conn.execute(text(sql), params)]


def _list_matched_issued_times(
    engine, since: str | None = None, basin: str | None = None,
) -> list:
    """List distinct issued_times in nhc_wsp_polygon_matched, filtered."""
    filters = []
    params: dict = {}
    if since:
        filters.append("m.issued_time >= :since")
        params["since"] = since
    if basin:
        filters.append("s.genesis_basin = :basin")
        params["basin"] = basin
    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    join = "LEFT JOIN storms.nhc_storms s ON s.atcf_id = m.atcf_id" if basin else ""
    sql = (
        "SELECT DISTINCT m.issued_time"
        " FROM storms.nhc_wsp_polygon_matched m"
        f" {join}"
        f" {where}"
        " ORDER BY m.issued_time"
    )
    with engine.connect() as conn:
        return [row[0] for row in conn.execute(text(sql), params)]


def _run_exp_year_chunk(
    *,
    table_label: str,
    load_chunk,                    # callable(engine, year) -> gpd.GeoDataFrame
    out_table: str,
    done_loader,                   # callable(engine, admin_level) -> done_df
    done_filter,                   # callable(wsp_in, done_country) -> wsp_in
    countries,
    since: str | None,
    basin: str | None,
    overwrite: bool,
    mode: str,
    issued_time,
    chunk_source_table: str,
    single_year: int | None = None,
    admin_levels: list[int] | None = None,
) -> None:
    """Shared year-chunked WSP exposure loop.

    Loads one calendar year of WSP polygons at a time, builds a spatial
    index across that year, then iterates admin units (tqdm). adm/pop are
    loaded once and reused across all years.
    """
    import warnings
    from rasterio.errors import ShapeSkipWarning
    from tqdm import tqdm
    from src.utils.exposure import GEO_CRS_ANTIMERIDIAN, load_adm_units, load_pop

    warnings.filterwarnings("ignore", category=ShapeSkipWarning)
    engine = stratus.get_engine(stage=mode, write=True)

    admin_levels = admin_levels or _DEFAULT_ADMIN_LEVELS

    if single_year is not None:
        years = [int(single_year)]
    elif issued_time is not None:
        try:
            it_dt = pd.Timestamp(issued_time)
            years = [int(it_dt.year)]
        except Exception:
            years = _list_years(
                engine, chunk_source_table, since=since, basin=basin,
            )
    else:
        years = _list_years(
            engine, chunk_source_table, since=since, basin=basin,
        )

    if not years:
        logger.info(f"{table_label}: no years match filters. Skipping.")
        return
    logger.info(f"{table_label}: {len(years)} year chunks: {years}")

    # Pre-load admin units per level once — reused across all years.
    units_by_level = {al: load_adm_units(countries, al) for al in admin_levels}
    country_groups_by_level = {
        al: list(units_by_level[al].groupby("iso3")) for al in admin_levels
    }
    da_wp_global, da_wp_wrapped = load_pop()

    done_by_level = {
        al: (
            pd.DataFrame(columns=_WSP_EXP_KEY_COLS) if overwrite
            else done_loader(engine, al)
        )
        for al in admin_levels
    }

    total_processed = 0
    for year in years:
        logger.info(f"{table_label}: loading year {year}…")
        gdf_wsp = load_chunk(engine, year)
        if gdf_wsp.empty:
            logger.info(f"{table_label}: year {year} has no polygons; skipping")
            continue
        if issued_time is not None:
            gdf_wsp = gdf_wsp[gdf_wsp["issued_time"] == pd.Timestamp(issued_time)]
            if gdf_wsp.empty:
                continue
        logger.info(
            f"{table_label}: year {year} → {len(gdf_wsp)} polygons; "
            f"running per-admin-level country loop"
        )
        gdf_wsp_anti = gdf_wsp.to_crs(GEO_CRS_ANTIMERIDIAN)
        wsp_sindex = gdf_wsp.sindex

        for admin_level in admin_levels:
            country_groups = country_groups_by_level[admin_level]
            done_df = done_by_level[admin_level]
            year_writes = 0
            pbar = tqdm(
                country_groups,
                desc=f"{table_label} {year} adm{admin_level}",
                unit="country",
                leave=False,
            )
            for iso3, country_units in pbar:
                n = _process_buffer_exposure_country(
                    iso3=iso3,
                    country_units=country_units,
                    admin_level=admin_level,
                    gdf_buffers=gdf_wsp,
                    gdf_buffers_anti=gdf_wsp_anti,
                    buffers_sindex=wsp_sindex,
                    da_wp_global=da_wp_global,
                    da_wp_wrapped=da_wp_wrapped,
                    done_df=done_df,
                    overwrite=overwrite,
                    key_cols=_WSP_EXP_KEY_COLS,
                    done_filter=done_filter,
                    out_table=out_table,
                    engine=engine,
                    drop_cols=["id"],
                )
                if n:
                    year_writes += 1
                    total_processed += 1
                    pbar.set_postfix(writes=year_writes)
            pbar.close()
            logger.info(
                f"{table_label}: year {year} adm{admin_level} done — "
                f"{year_writes} country writes ({total_processed} cumulative)"
            )
        del gdf_wsp, gdf_wsp_anti, wsp_sindex

    logger.info(f"{table_label}: all years done; {total_processed} writes.")
    engine.dispose()


def run_nhc_wsp_exp(
    countries: list[str] | None = None,
    since: str | None = None,
    basin: str | None = None,
    overwrite: bool = False,
    mode: str = "dev",
    issued_time=None,
    admin_levels: list[int] | None = None,
) -> None:
    """WSP exposure, chunked by year so peak memory stays bounded."""
    _run_exp_year_chunk(
        table_label="WSP exposure",
        load_chunk=lambda eng, year: _load_wsp_for_exposure(
            eng, basin=basin, year=year,
        ),
        out_table="nhc_wsp_exposure",
        done_loader=_load_done_nhc_wsp_exp,
        done_filter=_filter_done_nhc_wsp,
        countries=countries,
        since=since,
        basin=basin,
        overwrite=overwrite,
        mode=mode,
        issued_time=issued_time,
        chunk_source_table="nhc_wsp_polygon_matched",
        admin_levels=admin_levels,
    )


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
    from shapely.geometry import box as shapely_box
    from shapely.validation import make_valid
    _world = shapely_box(-180, -90, 180, 90)

    # Iterate per issued_time so memory only holds one issuance's polygons +
    # its obsv-buffer lookup at a time. The function loops by recursing into
    # itself for each issued_time when none is specified.
    if issued_time is None:
        issued_times = _list_matched_issued_times(
            engine, since=since, basin=basin,
        )
        if not issued_times:
            logger.info(
                "No matched WSP issued_times match filters. Skipping."
            )
            return
        logger.info(
            f"WSP fcastonly: {len(issued_times)} issued_times to process"
        )
        existing_its_skip: set = set()
        if not overwrite:
            with engine.connect() as conn:
                r = conn.execute(text(
                    "SELECT DISTINCT issued_time"
                    " FROM storms.nhc_wsp_fcastonly_polygon"
                ))
                existing_its_skip = {row[0] for row in r}
        for n, it in enumerate(issued_times, 1):
            if not overwrite and it in existing_its_skip:
                continue
            process_nhc_wsp_fcastonly_polygons(
                engine=engine, issued_time=it,
                overwrite=overwrite, chunksize=chunksize,
            )
            if n % 25 == 0 or n == len(issued_times):
                logger.info(
                    f"WSP fcastonly: {n}/{len(issued_times)} issued_times"
                )
        logger.info("WSP fcastonly polygons: all issued_times processed.")
        return

    logger.info(f"Loading WSP polygons for fcastonly cut-out @ {issued_time}...")
    gdf_wsp = _load_wsp_for_exposure(engine, issued_time=issued_time, basin=basin)
    if gdf_wsp.empty:
        return

    atcf_ids = [a for a in gdf_wsp["atcf_id"].dropna().unique()]
    obsv_lookup = _load_obsv_buffer_lookup(engine, atcf_ids)

    if not overwrite:
        with engine.connect() as conn:
            existing = pd.read_sql(
                text(
                    "SELECT issued_time, wind_threshold_kt, percentage, atcf_id"
                    " FROM storms.nhc_wsp_fcastonly_polygon"
                    " WHERE issued_time = :it"
                ),
                conn,
                params={"it": issued_time},
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
        # Clean-and-rebuild for this issued_time: drop any existing
        # fcastonly rows first so orphans (keys present in fcastonly but
        # no longer present in the matched table — e.g. after a fill-
        # nulls pass promoted a NULL atcf_id to a real one) get removed.
        with engine.begin() as conn:
            conn.execute(
                text(
                    "DELETE FROM storms.nhc_wsp_fcastonly_polygon"
                    " WHERE issued_time = :it"
                ),
                {"it": issued_time},
            )

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

        raw_geom = row.geometry
        if not raw_geom.is_valid:
            raw_geom = make_valid(raw_geom)
        wsp_geom = raw_geom.intersection(_world)
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

        # Coerce single Polygon results into a 1-part MultiPolygon so the
        # nhc_wsp_fcastonly_polygon column type stays uniform.
        if result_geom is not None:
            result_geom = _to_multipolygon(result_geom)

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

    # When called for a single issued_time, the parent loop logs progress —
    # only log a per-issuance summary when the run touched something.
    if offset_used + exact_used + no_obsv > 0:
        logger.debug(
            f"  {issued_time}: {offset_used} offset, {exact_used} exact, "
            f"{no_obsv} no-obsv, {already_done} skipped"
        )


def _write_wsp_fcastonly_batch(batch: list[dict], engine, chunksize: int) -> None:
    df = pd.DataFrame(batch)
    key_cols = ["issued_time", "wind_threshold_kt", "percentage", "atcf_id"]
    # Invariant: matched-table input gives one row per key; the writer should
    # never see duplicates. Fail loudly if it ever does.
    dups = df[df.duplicated(subset=key_cols, keep=False)]
    assert dups.empty, (
        f"Duplicate keys reached _write_wsp_fcastonly_batch: {dups[key_cols].to_dict('records')}"
    )
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
    year: int | None = None,
) -> gpd.GeoDataFrame:
    filters = []
    if since:
        filters.append(f"p.issued_time >= '{since}'")
    if basin:
        filters.append(f"s.genesis_basin = '{basin}'")
    if issued_time is not None:
        filters.append(f"p.issued_time = '{issued_time}'")
    if year is not None:
        filters.append(f"EXTRACT(YEAR FROM p.issued_time) = {year}")
    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    if basin:
        query = (
            "SELECT p.issued_time, p.wind_threshold_kt, p.percentage,"
            " p.atcf_id, p.geometry"
            " FROM storms.nhc_wsp_fcastonly_polygon p"
            " JOIN storms.nhc_storms s ON p.atcf_id = s.atcf_id"
            f" {where}"
        )
    else:
        query = (
            "SELECT p.issued_time, p.wind_threshold_kt, p.percentage,"
            " p.atcf_id, p.geometry"
            f" FROM storms.nhc_wsp_fcastonly_polygon p {where}"
        )

    with engine.connect() as conn:
        return gpd.read_postgis(query, conn, geom_col="geometry")


def _load_done_nhc_wsp_fcastonly_exp(engine, admin_level: int) -> pd.DataFrame:
    try:
        with engine.connect() as conn:
            return pd.read_sql(
                text(
                    "SELECT issued_time, wind_threshold_kt, percentage, atcf_id, admin_level, pcode"
                    " FROM storms.nhc_wsp_fcastonly_exposure"
                    " WHERE admin_level = :al"
                ),
                conn,
                params={"al": admin_level},
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


def _list_fcastonly_issued_times(
    engine, since: str | None = None, basin: str | None = None,
) -> list:
    """List distinct issued_times in nhc_wsp_fcastonly_polygon, filtered."""
    filters = []
    params: dict = {}
    if since:
        filters.append("p.issued_time >= :since")
        params["since"] = since
    if basin:
        filters.append("s.genesis_basin = :basin")
        params["basin"] = basin
    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    join = "JOIN storms.nhc_storms s ON s.atcf_id = p.atcf_id" if basin else ""
    sql = (
        "SELECT DISTINCT p.issued_time"
        " FROM storms.nhc_wsp_fcastonly_polygon p"
        f" {join}"
        f" {where}"
        " ORDER BY p.issued_time"
    )
    with engine.connect() as conn:
        return [row[0] for row in conn.execute(text(sql), params)]


def run_nhc_wsp_fcastonly_exp(
    countries: list[str] | None = None,
    since: str | None = None,
    basin: str | None = None,
    overwrite: bool = False,
    mode: str = "dev",
    issued_time=None,
    year: int | None = None,
    admin_levels: list[int] | None = None,
) -> None:
    """WSP fcastonly exposure, chunked by year.

    Pass year=YYYY to restrict to a single calendar year — useful for
    parallelizing across years from the shell.
    """
    _run_exp_year_chunk(
        table_label="WSP fcastonly exposure",
        load_chunk=lambda eng, y: _load_wsp_fcastonly_for_exposure(
            eng, basin=basin, year=y,
        ),
        out_table="nhc_wsp_fcastonly_exposure",
        done_loader=_load_done_nhc_wsp_fcastonly_exp,
        done_filter=_filter_done_nhc_wsp_fcastonly,
        countries=countries,
        since=since,
        basin=basin,
        overwrite=overwrite,
        mode=mode,
        issued_time=issued_time,
        chunk_source_table="nhc_wsp_fcastonly_polygon",
        single_year=year,
        admin_levels=admin_levels,
    )


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
            pd.read_sql(text("SELECT MAX(issued_time) FROM storms.nhc_wsp_polygon_raw"), conn).iloc[0, 0]
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
            logger.info("Building NHC WSP polygon matched table...")
            process_nhc_wsp_polygon_matched(
                engine=write_engine, issued_time=wsp_issued_time
            )
        except Exception as e:
            logger.error(
                f"NHC WSP polygon matched build failed: {e}", exc_info=True
            )

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
