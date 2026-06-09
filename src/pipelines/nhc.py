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


def _fix_antimeridian(geom):
    """Defensive: split a polygon crossing the antimeridian into a proper
    MultiPolygon. Mostly redundant now that ocha-lens does this internally
    via calculate_wind_buffers_gdf; kept as a safety net for the
    fcast.difference(obsv) path which can also produce wraparounds.

    Per-part handling tolerates degenerate sub-polygons (<4 unique vertices
    after dedup) that would otherwise blow up antimeridian.fix_polygon.
    """
    import antimeridian
    from shapely.geometry import MultiPolygon
    if geom is None or geom.is_empty:
        return geom

    def _fix_one(p):
        try:
            return antimeridian.fix_polygon(p, fix_winding=True)
        except ValueError:
            return p

    if geom.geom_type == "Polygon":
        return _fix_one(geom)
    if geom.geom_type == "MultiPolygon":
        parts = []
        for p in geom.geoms:
            fixed = _fix_one(p)
            if fixed.geom_type == "Polygon":
                parts.append(fixed)
            elif fixed.geom_type == "MultiPolygon":
                parts.extend(list(fixed.geoms))
        return MultiPolygon(parts) if parts else geom
    return geom


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


def _peek_wsp_issuance_from_current_storms_json() -> pd.Timestamp | None:
    """Read just the WSP issuance from CurrentStorms.json, without
    downloading the (multi-MB) zip. Honors any _patch_current_storms_url
    override. Returns a naive UTC Timestamp to match the DB schema."""
    data = _lens_nhc._fetch_current_storms_json()
    if data is None:
        return None
    for s in data.get("activeStorms", []):
        gis = s.get("windSpeedProbabilitiesGIS") or {}
        iss = gis.get("issuance")
        if iss:
            return pd.to_datetime(iss, utc=True).tz_localize(None)
    return None


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
    until: str | None = None,
    basin: str | None = None,
    issued_time=None,
) -> list:
    """Return WSP raw issued_times that have at least one matching tracks
    advisory within [r.issued_time, r.issued_time + 3h], sorted ascending.

    The 3-hour forward window reflects NHC's actual publishing cadence:
    WSP products are issued at synoptic hours (00/06/12/18 UTC) but
    typically don't appear on the public mirror until the next major
    advisory (03/09/15/21 UTC), at which point the tracks_geo row for
    that later advisory is the closest available track context. For
    storms with no active watches/warnings (no intermediate advisories),
    this is the *only* tracks-to-WSP mapping that exists.
    """
    filters = []
    params: dict = {}
    if since:
        filters.append("r.issued_time >= :since")
        params["since"] = since
    if until:
        filters.append("r.issued_time < :until")
        params["until"] = until
    if basin:
        filters.append("t.basin = :basin")
        params["basin"] = basin
    if issued_time is not None:
        filters.append("r.issued_time = :issued_time")
        params["issued_time"] = issued_time
    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    sql = (
        "SELECT DISTINCT r.issued_time"
        " FROM storms.nhc_wsp_polygon_raw r"
        " INNER JOIN storms.nhc_tracks_geo t"
        " ON t.issued_time BETWEEN r.issued_time"
        " AND r.issued_time + INTERVAL '3 hours'"
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
        # For each storm, take the earliest tracks advisory within
        # [WSP issued_time, WSP issued_time + 3h]. Exact-match wins
        # (ORDER BY issued_time ASC), with the next-later advisory as
        # the fallback when no intermediate-advisory tracks at the WSP's
        # synoptic hour are available. See _list_wsp_issued_times for
        # the why.
        gdf_tracks = gpd.read_postgis(
            text(
                "SELECT DISTINCT ON (atcf_id) atcf_id, issued_time, geometry"
                " FROM storms.nhc_tracks_geo"
                " WHERE issued_time BETWEEN :it"
                " AND :it + INTERVAL '3 hours'"
                " ORDER BY atcf_id, issued_time ASC"
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
    until: str | None = None,
    basin: str | None = None,
    issued_time=None,
    overwrite: bool = False,
    chunksize: int = 500,
) -> None:
    """Build storms.nhc_wsp_polygon_matched from nhc_wsp_polygon_raw + tracks.

    Processes one issued_time at a time, committing after each. Keeps peak
    memory bounded by the largest single issuance.
    """
    # Realtime short-circuit.
    if issued_time is not None and not overwrite:
        with engine.connect() as conn:
            exists = bool(conn.execute(
                text(
                    "SELECT 1 FROM storms.nhc_wsp_polygon_matched "
                    "WHERE issued_time = :it LIMIT 1"
                ),
                {"it": issued_time},
            ).scalar())
        if exists:
            logger.info(
                f"nhc_wsp_polygon_matched already has rows for "
                f"issued_time={issued_time}; skipping."
            )
            return

    issued_times = _list_wsp_issued_times(
        engine, since=since, until=until, basin=basin, issued_time=issued_time,
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
    until: str | None = None,
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
        until=until,
        basin=basin,
        issued_time=issued_time,
        overwrite=overwrite,
        chunksize=chunksize,
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

            # Peek at the JSON's WSP issuance and short-circuit the
            # (multi-MB) zip download when those rows are already in DB.
            # The 3h realtime cron commonly fires before NHC publishes
            # the cycle's WSP zip, so a manual rerun 30 min later
            # otherwise wastes the entire WSP fetch + parse just to
            # upsert no-op rows.
            advertised_wsp_iss = _peek_wsp_issuance_from_current_storms_json()
            already_have_wsp = False
            if advertised_wsp_iss is not None:
                with engine.connect() as conn:
                    already_have_wsp = bool(conn.execute(
                        text(
                            "SELECT 1 FROM storms.nhc_wsp_polygon_raw "
                            "WHERE issued_time = :it LIMIT 1"
                        ),
                        {"it": advertised_wsp_iss.to_pydatetime()},
                    ).scalar())

            if already_have_wsp:
                logger.info(
                    f"WSP issuance {advertised_wsp_iss} already in DB; "
                    "skipping download."
                )
                wsp_issued_time = advertised_wsp_iss
            else:
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
    since: str | None = None,
    until: str | None = None,
    issued_time=None,
) -> gpd.GeoDataFrame:
    filters = [
        """(quadrant_radius_34 IS NOT NULL
            OR quadrant_radius_50 IS NOT NULL
            OR quadrant_radius_64 IS NOT NULL)"""
    ]
    if basin:
        filters.append(f"basin = '{basin}'")
    if since:
        filters.append(f"issued_time >= '{since}'")
    if until:
        filters.append(f"issued_time < '{until}'")
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
    since=None,
    until=None,
    overwrite=False,
    issued_time=None,
):
    # Realtime short-circuit: if a single issued_time was requested and
    # buffer rows for it already exist, skip the source load + per-issuance
    # geometry work entirely. Range backfills (--since/--until) fall
    # through to the load-then-filter path below.
    if issued_time is not None and not overwrite:
        with write_engine.connect() as conn:
            exists = bool(conn.execute(
                text(
                    "SELECT 1 FROM storms.nhc_tracks_fcast_buffers "
                    "WHERE issued_time = :it LIMIT 1"
                ),
                {"it": issued_time},
            ).scalar())
        if exists:
            logger.info(
                f"nhc_tracks_fcast_buffers already has rows for "
                f"issued_time={issued_time}; skipping."
            )
            return

    logger.info("Loading NHC tracks with wind radii...")
    gdf_tracks = _load_nhc_tracks_fcast_buffer_tracks(
        read_engine, basin=basin, since=since, until=until, issued_time=issued_time
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
            gdf_buffers["geometry"] = gdf_buffers.geometry.apply(_fix_antimeridian)
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
    since=None,
    until=None,
    overwrite=False,
    issued_time=None,
):
    coloredlogs.install(
        logger=logger,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger.info("Starting NHC wind buffers pipeline...")
    read_engine = stratus.get_engine(stage=write_mode)
    write_engine = stratus.get_engine(stage=write_mode, write=True)
    try:
        process_nhc_tracks_fcast_buffers(
            read_engine=read_engine,
            write_engine=write_engine,
            chunksize=chunksize,
            basin=basin,
            since=since,
            until=until,
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
    since: str | None = None,
    until: str | None = None,
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
        if since:
            filters.append(f"issued_time >= '{since}'")
        if until:
            filters.append(f"issued_time < '{until}'")

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
    since=None,
    until=None,
    overwrite=False,
    issued_time=None,
):
    # Realtime short-circuit. In realtime, valid_time == issued_time for
    # the just-fetched observation, so an existing row at that valid_time
    # means there's nothing to add.
    if issued_time is not None and not overwrite:
        with write_engine.connect() as conn:
            exists = bool(conn.execute(
                text(
                    "SELECT 1 FROM storms.nhc_tracks_obsv_buffers "
                    "WHERE valid_time = :it LIMIT 1"
                ),
                {"it": issued_time},
            ).scalar())
        if exists:
            logger.info(
                f"nhc_tracks_obsv_buffers already has rows for "
                f"valid_time={issued_time}; skipping."
            )
            return

    logger.info("Loading NHC observational (leadtime=0) track points...")
    gdf_obsv = _load_nhc_tracks_obsv_buffer_tracks(
        read_engine, basin=basin, since=since, until=until, issued_time=issued_time
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
                gdf_buffers["geometry"] = gdf_buffers.geometry.apply(_fix_antimeridian)
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
    since=None,
    until=None,
    overwrite=False,
    issued_time=None,
):
    coloredlogs.install(
        logger=logger,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger.info("Starting NHC observational track buffers pipeline...")
    read_engine = stratus.get_engine(stage=write_mode)
    write_engine = stratus.get_engine(stage=write_mode, write=True)
    try:
        process_nhc_tracks_obsv_buffers(
            read_engine=read_engine,
            write_engine=write_engine,
            chunksize=chunksize,
            basin=basin,
            since=since,
            until=until,
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
    since: str | None = None,
    until: str | None = None,
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
        if since:
            filters.append(f"f.issued_time >= '{since}'")
        if until:
            filters.append(f"f.issued_time < '{until}'")

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
    since=None,
    until=None,
    overwrite=False,
    issued_time=None,
):
    from shapely import wkt as shapely_wkt

    # Realtime short-circuit.
    if issued_time is not None and not overwrite:
        with write_engine.connect() as conn:
            exists = bool(conn.execute(
                text(
                    "SELECT 1 FROM storms.nhc_tracks_fcastonly_buffers "
                    "WHERE issued_time = :it LIMIT 1"
                ),
                {"it": issued_time},
            ).scalar())
        if exists:
            logger.info(
                f"nhc_tracks_fcastonly_buffers already has rows for "
                f"issued_time={issued_time}; skipping."
            )
            return

    logger.info("Loading fcast and obsv buffer inputs...")
    df = _load_nhc_fcastonly_inputs(
        read_engine, basin=basin, since=since, until=until, issued_time=issued_time
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

            # Defensive: if fcast had a wraparound that snuck past, or the
            # difference produced one, split at the dateline so downstream
            # exposure intersections don't false-match far-away countries.
            if result_geom is not None:
                result_geom = _fix_antimeridian(result_geom)

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
    since=None,
    until=None,
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
            since=since,
            until=until,
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


def _exposure_already_done(
    *,
    out_table: str,
    key_col: str,
    key_val,
    admin_levels: list[int] | None,
    mode: str,
    session: "_ExposureSession | None" = None,
) -> bool:
    """Realtime short-circuit helper for exposure pipelines.

    Returns True only when EVERY requested admin_level has at least one
    row in `out_table` at `key_col = key_val`. The all-levels check is
    what makes this safe to call before kicking off an exposure run —
    a half-done state (adm0 done, adm1 killed mid-way) is not skipped.
    """
    requested = set(admin_levels or _DEFAULT_ADMIN_LEVELS)
    own_engine = session is None
    engine = session.engine if session is not None else stratus.get_engine(stage=mode)
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    f"SELECT DISTINCT admin_level "
                    f"FROM storms.{out_table} "
                    f"WHERE {key_col} = :v"
                ),
                {"v": key_val},
            )
            present = {row[0] for row in rows}
    finally:
        if own_engine:
            engine.dispose()
    return requested.issubset(present)


# ---------------------------------------------------------------------------
# Shared exposure session — load WorldPop / FieldMaps / engine ONCE and
# reuse across the realtime exposure cascade's 5 sub-pipelines instead of
# paying ~5 minutes of cold setup per subprocess.
# ---------------------------------------------------------------------------
from dataclasses import dataclass


@dataclass
class _ExposureSession:
    engine: object
    da_wp: object
    units_by_level: dict          # admin_level → GeoDataFrame
    country_groups_by_level: dict  # admin_level → [(iso3, units_gdf, bbox_tuple)]
    admin_levels: list[int]


def _build_country_groups_with_bbox(units):
    """[(iso3, units_gdf, (minx, miny, maxx, maxy))] for cheap bbox prefilter."""
    return [
        (iso3, sub, tuple(sub.total_bounds))
        for iso3, sub in units.groupby("iso3")
    ]


def build_exposure_session(
    mode: str = "dev",
    countries: list[str] | None = None,
    admin_levels: list[int] | None = None,
) -> _ExposureSession:
    """Load all the per-run-invariant state: engine, WorldPop, FieldMaps adm units."""
    from src.utils.exposure import load_adm_units, load_pop
    admin_levels = admin_levels or _DEFAULT_ADMIN_LEVELS
    engine = stratus.get_engine(stage=mode, write=True)
    da_wp = load_pop()
    units_by_level = {
        al: load_adm_units(countries, al, stage=mode) for al in admin_levels
    }
    country_groups_by_level = {
        al: _build_country_groups_with_bbox(units_by_level[al])
        for al in admin_levels
    }
    return _ExposureSession(
        engine=engine,
        da_wp=da_wp,
        units_by_level=units_by_level,
        country_groups_by_level=country_groups_by_level,
        admin_levels=admin_levels,
    )


def _bbox_overlaps(country_bbox, buffers_bbox) -> bool:
    """True if (minx, miny, maxx, maxy) tuples overlap."""
    return not (
        country_bbox[2] < buffers_bbox[0]
        or country_bbox[0] > buffers_bbox[2]
        or country_bbox[3] < buffers_bbox[1]
        or country_bbox[1] > buffers_bbox[3]
    )


def _process_buffer_exposure_country(
    *,
    iso3: str,
    country_units: gpd.GeoDataFrame,    # rows for this iso3, cols [pcode, geometry]
    admin_level: int,
    gdf_buffers,                         # epsg:4326
    buffers_sindex,                      # gdf_buffers.sindex
    da_wp,
    done_df: pd.DataFrame,
    overwrite: bool,
    key_cols: list[str],
    done_filter,
    out_table: str,
    engine,
    drop_cols: list[str] | None = None,
    buffers_union_prep=None,            # shapely PreparedGeometry over union of gdf_buffers
) -> int:
    """Compute and write per-unit exposure for all admin units in one country.

    Buffers and admin units are expected to be split at the dateline upstream
    (antimeridian package in buffer pipelines, FieldMaps' native multi-part
    adm files), so this works entirely in standard EPSG:4326 — no wrap-around
    CRS reprojection needed.

    Country geometry (union of its units) is used to pre-clip WorldPop and
    spatially prefilter buffers; per-unit work then sub-clips that smaller
    raster and intersects against the smaller buffer set.

    Returns number of units written (0 if nothing intersected or all done).
    """
    from src.utils.exposure import calculate_exposure

    country_geom = country_units.geometry.union_all()
    # Prefilter against the prepared union of all buffers. For tracks this
    # is near-redundant with the bbox check (localized buffers); for WSP
    # it's the actual win, since basin-wide bboxes let most countries pass
    # the bbox check even when no polygon touches them.
    if buffers_union_prep is not None and not buffers_union_prep.intersects(country_geom):
        return 0
    candidate_idx = list(buffers_sindex.intersection(country_geom.bounds))
    if not candidate_idx:
        return 0
    candidates = gdf_buffers.iloc[candidate_idx]
    country_buffers = candidates[candidates.intersects(country_geom)]
    if country_buffers.empty:
        return 0

    da_wp_country = da_wp.rio.clip([country_geom], all_touched=True)

    # For admin1, build a small sindex over the country-clipped buffers so
    # per-unit intersect is cheap. For admin0 we skip — the only unit IS
    # the country.
    unit_sindex = country_buffers.sindex if admin_level > 0 else None

    writes = 0
    for _, unit in country_units.iterrows():
        pcode = unit["pcode"]

        if admin_level == 0:
            # Admin0: country is the unit; reuse country buffers + geom.
            buf_in = country_buffers
            unit_geom = country_geom
        else:
            unit_geom = unit.geometry
            idx2 = list(unit_sindex.intersection(unit_geom.bounds))
            if not idx2:
                continue
            unit_candidates = country_buffers.iloc[idx2]
            buf_in = unit_candidates[unit_candidates.intersects(unit_geom)]
            if buf_in.empty:
                continue

        if not overwrite and not done_df.empty:
            done_unit = done_df[done_df["pcode"] == pcode]
            if not done_unit.empty:
                buf_in = done_filter(buf_in, done_unit)
                if buf_in.empty:
                    continue

        # exactextract on (unit_geom ∩ buffer_geom). Country-level
        # pre-clip da_wp_country is just a window restriction; exact_extract
        # handles per-pair area-weighted sums.
        df = calculate_exposure(buf_in, da_wp_country, mask_geom=unit_geom)
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
    until: str | None = None,
    basin: str | None = None,
    issued_time=None,
) -> gpd.GeoDataFrame:
    filters = []
    if since:
        filters.append(f"b.issued_time >= '{since}'")
    if until:
        filters.append(f"b.issued_time < '{until}'")
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


def _load_done_nhc_tracks_fcast_exp(
    engine, admin_level: int, issued_time=None,
    since: str | None = None, until: str | None = None,
) -> pd.DataFrame:
    filters = ["admin_level = :al"]
    params: dict = {"al": admin_level}
    if issued_time is not None:
        filters.append("issued_time = :it")
        params["it"] = issued_time
    if since:
        filters.append("issued_time >= :since")
        params["since"] = since
    if until:
        filters.append("issued_time < :until")
        params["until"] = until
    where = " AND ".join(filters)
    try:
        with engine.connect() as conn:
            return pd.read_sql(
                text(
                    "SELECT atcf_id, issued_time, wind_speed_kt, admin_level, pcode"
                    f" FROM storms.nhc_tracks_fcast_exposure WHERE {where}"
                ),
                conn,
                params=params,
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


def _run_track_exp(
    *,
    load_buffers,            # callable(engine) -> gpd.GeoDataFrame
    out_table: str,
    key_cols: list[str],
    done_loader,             # callable(engine, admin_level) -> done_df
    done_filter,             # callable(buffers, done_country) -> buffers
    countries,
    overwrite: bool,
    mode: str,
    admin_levels,
    session: _ExposureSession | None,
    buffers_log_label: str,
) -> None:
    """Shared body for the 3 inline track-exposure runners.

    Builds (or borrows) a session, loads the per-pipeline buffer set, and
    iterates countries with a cheap bbox prefilter before the heavy
    union_all in `_process_buffer_exposure_country`.
    """
    import gc
    import warnings
    from rasterio.errors import ShapeSkipWarning

    warnings.filterwarnings("ignore", category=ShapeSkipWarning)

    own_session = session is None
    if own_session:
        session = build_exposure_session(
            mode=mode, countries=countries, admin_levels=admin_levels,
        )
    engine = session.engine
    admin_levels = session.admin_levels

    try:
        from shapely.prepared import prep
        logger.info(f"Loading {buffers_log_label} for exposure calculation...")
        gdf_buffers = load_buffers(engine)
        if gdf_buffers.empty:
            logger.info(
                f"No {buffers_log_label.lower()} found for the given filters. Skipping."
            )
            return
        buffers_sindex = gdf_buffers.sindex
        buffers_bbox = tuple(gdf_buffers.total_bounds)
        buffers_union_prep = prep(gdf_buffers.geometry.union_all())

        for admin_level in admin_levels:
            country_groups = session.country_groups_by_level[admin_level]
            n_units = len(session.units_by_level[admin_level])
            logger.info(
                f"admin_level={admin_level}: {len(country_groups)} countries, "
                f"{n_units} units"
            )

            done_df = (
                pd.DataFrame(columns=key_cols) if overwrite
                else done_loader(engine, admin_level)
            )

            processed = no_intersect = bbox_skipped = 0
            for i, (iso3, country_units, cb) in enumerate(country_groups, 1):
                if not _bbox_overlaps(cb, buffers_bbox):
                    bbox_skipped += 1
                    continue
                prefix = f"[adm{admin_level}][{i}/{len(country_groups)}] {iso3}"
                n = _process_buffer_exposure_country(
                    iso3=iso3,
                    country_units=country_units,
                    admin_level=admin_level,
                    gdf_buffers=gdf_buffers,
                    buffers_sindex=buffers_sindex,
                    buffers_union_prep=buffers_union_prep,
                    da_wp=session.da_wp,
                    done_df=done_df,
                    overwrite=overwrite,
                    key_cols=key_cols,
                    done_filter=done_filter,
                    out_table=out_table,
                    engine=engine,
                )
                if n:
                    processed += 1
                    logger.info(f"{prefix} — {n} unit writes")
                else:
                    no_intersect += 1
                # rasterio's per-clip buffers and intermediate xarray
                # arrays aren't cheap; without an explicit collect they
                # can sit around long enough for cgroup OOM to fire on
                # long backfills (see the May 2026 adm1 SIGKILL incident).
                gc.collect()

            logger.info(
                f"admin_level={admin_level} done: {processed} written, "
                f"{no_intersect} no-intersect, {bbox_skipped} bbox-prefiltered"
            )
    finally:
        if own_session:
            engine.dispose()


def run_nhc_tracks_fcast_exp(
    countries: list[str] | None = None,
    since: str | None = None,
    until: str | None = None,
    basin: str | None = None,
    overwrite: bool = False,
    mode: str = "dev",
    issued_time=None,
    admin_levels: list[int] | None = None,
    session: _ExposureSession | None = None,
) -> None:
    if issued_time is not None and not overwrite and _exposure_already_done(
        out_table="nhc_tracks_fcast_exposure",
        key_col="issued_time",
        key_val=issued_time,
        admin_levels=admin_levels,
        mode=mode,
        session=session,
    ):
        logger.info(
            f"nhc_tracks_fcast_exposure already has rows for all requested "
            f"admin_levels at issued_time={issued_time}; skipping."
        )
        return

    _run_track_exp(
        load_buffers=lambda eng: _load_nhc_tracks_fcast_exp_buffers(
            eng, since=since, until=until, basin=basin, issued_time=issued_time,
        ),
        out_table="nhc_tracks_fcast_exposure",
        key_cols=_TRACK_EXP_KEY_COLS,
        done_loader=lambda eng, al: _load_done_nhc_tracks_fcast_exp(
            eng, al, issued_time=issued_time, since=since, until=until,
        ),
        done_filter=_filter_done_nhc_tracks_fcast,
        countries=countries, overwrite=overwrite, mode=mode,
        admin_levels=admin_levels, session=session,
        buffers_log_label="NHC wind buffers",
    )


# ---------------------------------------------------------------------------
# Population exposure — NHC observed track buffers
# ---------------------------------------------------------------------------

_OBSV_EXP_KEY_COLS = ["atcf_id", "valid_time", "wind_speed_kt", "admin_level", "pcode"]


def _load_nhc_tracks_obsv_exp_buffers(
    engine,
    since: str | None = None,
    until: str | None = None,
    basin: str | None = None,
    valid_time=None,
) -> gpd.GeoDataFrame:
    filters = []
    if since:
        filters.append(f"b.valid_time >= '{since}'")
    if until:
        filters.append(f"b.valid_time < '{until}'")
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


def _load_done_nhc_tracks_obsv_exp(
    engine, admin_level: int, valid_time=None,
    since: str | None = None, until: str | None = None,
) -> pd.DataFrame:
    filters = ["admin_level = :al"]
    params: dict = {"al": admin_level}
    if valid_time is not None:
        filters.append("valid_time = :vt")
        params["vt"] = valid_time
    if since:
        filters.append("valid_time >= :since")
        params["since"] = since
    if until:
        filters.append("valid_time < :until")
        params["until"] = until
    where = " AND ".join(filters)
    try:
        with engine.connect() as conn:
            return pd.read_sql(
                text(
                    "SELECT atcf_id, valid_time, wind_speed_kt, admin_level, pcode"
                    f" FROM storms.nhc_tracks_obsv_exposure WHERE {where}"
                ),
                conn,
                params=params,
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
    until: str | None = None,
    basin: str | None = None,
    overwrite: bool = False,
    mode: str = "dev",
    valid_time=None,
    admin_levels: list[int] | None = None,
    final_only: bool = False,
    session: _ExposureSession | None = None,
) -> None:
    if valid_time is not None and not overwrite and _exposure_already_done(
        out_table="nhc_tracks_obsv_exposure",
        key_col="valid_time",
        key_val=valid_time,
        admin_levels=admin_levels,
        mode=mode,
        session=session,
    ):
        logger.info(
            f"nhc_tracks_obsv_exposure already has rows for all requested "
            f"admin_levels at valid_time={valid_time}; skipping."
        )
        return

    def _load(engine):
        gdf = _load_nhc_tracks_obsv_exp_buffers(
            engine, since=since, until=until, basin=basin, valid_time=valid_time,
        )
        if final_only and not gdf.empty:
            before = len(gdf)
            idx = gdf.groupby(["atcf_id", "wind_speed_kt"])["valid_time"].idxmax()
            gdf = gdf.loc[idx].reset_index(drop=True)
            logger.info(
                f"final_only: trimmed {before:,} buffers → {len(gdf):,} "
                f"(one per atcf_id × wind_speed_kt at max(valid_time))"
            )
        return gdf

    _run_track_exp(
        load_buffers=_load,
        out_table="nhc_tracks_obsv_exposure",
        key_cols=_OBSV_EXP_KEY_COLS,
        done_loader=lambda eng, al: _load_done_nhc_tracks_obsv_exp(
            eng, al, valid_time=valid_time, since=since, until=until,
        ),
        done_filter=_filter_done_nhc_tracks_obsv,
        countries=countries, overwrite=overwrite, mode=mode,
        admin_levels=admin_levels, session=session,
        buffers_log_label="NHC observed track buffers",
    )


# ---------------------------------------------------------------------------
# Population exposure — NHC forecast-only track buffers
# ---------------------------------------------------------------------------

_FCASTONLY_EXP_KEY_COLS = ["atcf_id", "issued_time", "wind_speed_kt", "admin_level", "pcode"]


def _load_nhc_tracks_fcastonly_exp_buffers(
    engine,
    since: str | None = None,
    until: str | None = None,
    basin: str | None = None,
    issued_time=None,
) -> gpd.GeoDataFrame:
    filters = []
    if since:
        filters.append(f"b.issued_time >= '{since}'")
    if until:
        filters.append(f"b.issued_time < '{until}'")
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


def _load_done_nhc_tracks_fcastonly_exp(
    engine, admin_level: int, issued_time=None,
    since: str | None = None, until: str | None = None,
) -> pd.DataFrame:
    filters = ["admin_level = :al"]
    params: dict = {"al": admin_level}
    if issued_time is not None:
        filters.append("issued_time = :it")
        params["it"] = issued_time
    if since:
        filters.append("issued_time >= :since")
        params["since"] = since
    if until:
        filters.append("issued_time < :until")
        params["until"] = until
    where = " AND ".join(filters)
    try:
        with engine.connect() as conn:
            return pd.read_sql(
                text(
                    "SELECT atcf_id, issued_time, wind_speed_kt, admin_level, pcode"
                    f" FROM storms.nhc_tracks_fcastonly_exposure WHERE {where}"
                ),
                conn,
                params=params,
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
    until: str | None = None,
    basin: str | None = None,
    overwrite: bool = False,
    mode: str = "dev",
    issued_time=None,
    admin_levels: list[int] | None = None,
    session: _ExposureSession | None = None,
) -> None:
    if issued_time is not None and not overwrite and _exposure_already_done(
        out_table="nhc_tracks_fcastonly_exposure",
        key_col="issued_time",
        key_val=issued_time,
        admin_levels=admin_levels,
        mode=mode,
        session=session,
    ):
        logger.info(
            f"nhc_tracks_fcastonly_exposure already has rows for all requested "
            f"admin_levels at issued_time={issued_time}; skipping."
        )
        return

    _run_track_exp(
        load_buffers=lambda eng: _load_nhc_tracks_fcastonly_exp_buffers(
            eng, since=since, until=until, basin=basin, issued_time=issued_time,
        ),
        out_table="nhc_tracks_fcastonly_exposure",
        key_cols=_FCASTONLY_EXP_KEY_COLS,
        done_loader=lambda eng, al: _load_done_nhc_tracks_fcastonly_exp(
            eng, al, issued_time=issued_time, since=since, until=until,
        ),
        done_filter=_filter_done_nhc_tracks_fcastonly,
        countries=countries, overwrite=overwrite, mode=mode,
        admin_levels=admin_levels, session=session,
        buffers_log_label="NHC forecast-only track buffers",
    )


# ---------------------------------------------------------------------------
# Population exposure — NHC WSP polygons
# ---------------------------------------------------------------------------


def _load_wsp_for_exposure(
    engine,
    since: str | None = None,
    until: str | None = None,
    basin: str | None = None,
    issued_time=None,
    year: int | None = None,
) -> gpd.GeoDataFrame:
    """Load matched-per-storm WSP polygons.

    Reads from storms.nhc_wsp_polygon_matched (already one MultiPolygon per
    (issued_time, wind_threshold_kt, percentage, atcf_id)), so no
    match_wsp_to_tracks call or post-load dissolve is needed here. To filter
    by basin, joins on storms.nhc_storms.genesis_basin.

    Pass ``year=YYYY`` to restrict the load to one calendar year — used
    internally by _run_exp_year_chunk for chunked historical scans.
    """
    filters: list[str] = []
    params: dict = {}
    if since:
        filters.append("m.issued_time >= :since")
        params["since"] = since
    if until:
        filters.append("m.issued_time < :until")
        params["until"] = until
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


def _load_done_nhc_wsp_exp(
    engine, admin_level: int, issued_time=None,
    since: str | None = None, until: str | None = None,
) -> pd.DataFrame:
    filters = ["admin_level = :al"]
    params: dict = {"al": admin_level}
    if issued_time is not None:
        filters.append("issued_time = :it")
        params["it"] = issued_time
    if since:
        filters.append("issued_time >= :since")
        params["since"] = since
    if until:
        filters.append("issued_time < :until")
        params["until"] = until
    where = " AND ".join(filters)
    try:
        with engine.connect() as conn:
            return pd.read_sql(
                text(
                    "SELECT issued_time, wind_threshold_kt, percentage, atcf_id, admin_level, pcode"
                    f" FROM storms.nhc_wsp_exposure WHERE {where}"
                ),
                conn,
                params=params,
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
    until: str | None = None,
    basin: str | None = None,
) -> list[int]:
    """List distinct years in a WSP-shaped table (issued_time TIMESTAMP)."""
    filters = []
    params: dict = {}
    if since:
        filters.append("t.issued_time >= :since")
        params["since"] = since
    if until:
        filters.append("t.issued_time < :until")
        params["until"] = until
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
    engine,
    since: str | None = None,
    until: str | None = None,
    basin: str | None = None,
) -> list:
    """List distinct issued_times in nhc_wsp_polygon_matched, filtered."""
    filters = []
    params: dict = {}
    if since:
        filters.append("m.issued_time >= :since")
        params["since"] = since
    if until:
        filters.append("m.issued_time < :until")
        params["until"] = until
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
    done_loader,                   # callable(engine, admin_level, issued_time=, since=, until=) -> done_df
    done_filter,                   # callable(wsp_in, done_country) -> wsp_in
    countries,
    since: str | None,
    until: str | None,
    basin: str | None,
    overwrite: bool,
    mode: str,
    issued_time,
    chunk_source_table: str,
    single_year: int | None = None,
    admin_levels: list[int] | None = None,
    session: _ExposureSession | None = None,
) -> None:
    """Shared year-chunked WSP exposure loop.

    Loads one calendar year of WSP polygons at a time, builds a spatial
    index across that year, then iterates admin units. Adm/pop are loaded
    once via the shared exposure session (or built if not provided) and
    reused across all years.
    """
    import warnings
    from rasterio.errors import ShapeSkipWarning
    from shapely.prepared import prep
    from tqdm import tqdm

    warnings.filterwarnings("ignore", category=ShapeSkipWarning)

    own_session = session is None
    if own_session:
        session = build_exposure_session(
            mode=mode, countries=countries, admin_levels=admin_levels,
        )
    engine = session.engine
    admin_levels = session.admin_levels

    # Compact description of the active filters — surfaced in every load
    # log line so it's obvious what's being pulled from the DB. Without
    # this, "loading year 2025" can read as a full-year scan when in
    # realtime mode we're really pulling a single issued_time.
    filter_parts: list[str] = []
    if issued_time is not None:
        filter_parts.append(f"issued_time={issued_time}")
    if since:
        filter_parts.append(f"since={since}")
    if until:
        filter_parts.append(f"until={until}")
    if basin:
        filter_parts.append(f"basin={basin}")
    filters_desc = (
        ", ".join(filter_parts) if filter_parts else "no filters (full table)"
    )

    try:
        if single_year is not None:
            years = [int(single_year)]
        elif issued_time is not None:
            try:
                it_dt = pd.Timestamp(issued_time)
                years = [int(it_dt.year)]
            except Exception:
                years = _list_years(
                    engine, chunk_source_table,
                    since=since, until=until, basin=basin,
                )
        else:
            years = _list_years(
                engine, chunk_source_table,
                since=since, until=until, basin=basin,
            )

        if not years:
            logger.info(f"{table_label}: no years match filters. Skipping.")
            return
        logger.info(
            f"{table_label}: {len(years)} year chunks: {years} ({filters_desc})"
        )

        done_by_level = {
            al: (
                pd.DataFrame(columns=_WSP_EXP_KEY_COLS) if overwrite
                else done_loader(
                    engine, al, issued_time=issued_time,
                    since=since, until=until,
                )
            )
            for al in admin_levels
        }

        total_processed = 0
        for year in years:
            logger.info(
                f"{table_label}: loading year {year} ({filters_desc})…"
            )
            gdf_wsp = load_chunk(engine, year)
            if gdf_wsp.empty:
                logger.info(f"{table_label}: year {year} has no polygons; skipping")
                continue
            if issued_time is not None:
                gdf_wsp = gdf_wsp[gdf_wsp["issued_time"] == pd.Timestamp(issued_time)]
                if gdf_wsp.empty:
                    continue
            n_distinct_it = gdf_wsp["issued_time"].nunique()
            logger.info(
                f"{table_label}: year {year} → {len(gdf_wsp)} polygons across "
                f"{n_distinct_it} issued_time(s); running per-admin-level country loop"
            )
            wsp_sindex = gdf_wsp.sindex
            buffers_bbox = tuple(gdf_wsp.total_bounds)
            buffers_union_prep = prep(gdf_wsp.geometry.union_all())

            for admin_level in admin_levels:
                country_groups = session.country_groups_by_level[admin_level]
                done_df = done_by_level[admin_level]
                year_writes = bbox_skipped = 0
                pbar = tqdm(
                    country_groups,
                    desc=f"{table_label} {year} adm{admin_level}",
                    unit="country",
                    leave=False,
                )
                for iso3, country_units, cb in pbar:
                    if not _bbox_overlaps(cb, buffers_bbox):
                        bbox_skipped += 1
                        continue
                    n = _process_buffer_exposure_country(
                        iso3=iso3,
                        country_units=country_units,
                        admin_level=admin_level,
                        gdf_buffers=gdf_wsp,
                        buffers_sindex=wsp_sindex,
                        buffers_union_prep=buffers_union_prep,
                        da_wp=session.da_wp,
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
                    f"{year_writes} country writes, {bbox_skipped} bbox-prefiltered "
                    f"({total_processed} cumulative)"
                )
            del gdf_wsp, wsp_sindex, buffers_union_prep

        logger.info(f"{table_label}: all years done; {total_processed} writes.")
    finally:
        if own_session:
            engine.dispose()


def run_nhc_wsp_exp(
    countries: list[str] | None = None,
    since: str | None = None,
    until: str | None = None,
    basin: str | None = None,
    overwrite: bool = False,
    mode: str = "dev",
    issued_time=None,
    admin_levels: list[int] | None = None,
    session: _ExposureSession | None = None,
) -> None:
    """WSP exposure, chunked by year so peak memory stays bounded."""
    if issued_time is not None and not overwrite and _exposure_already_done(
        out_table="nhc_wsp_exposure",
        key_col="issued_time",
        key_val=issued_time,
        admin_levels=admin_levels,
        mode=mode,
        session=session,
    ):
        logger.info(
            f"nhc_wsp_exposure already has rows for all requested "
            f"admin_levels at issued_time={issued_time}; skipping."
        )
        return

    _run_exp_year_chunk(
        table_label="WSP exposure",
        load_chunk=lambda eng, year: _load_wsp_for_exposure(
            eng, basin=basin, year=year,
            since=since, until=until, issued_time=issued_time,
        ),
        out_table="nhc_wsp_exposure",
        done_loader=_load_done_nhc_wsp_exp,
        done_filter=_filter_done_nhc_wsp,
        countries=countries,
        since=since,
        until=until,
        basin=basin,
        overwrite=overwrite,
        mode=mode,
        issued_time=issued_time,
        chunk_source_table="nhc_wsp_polygon_matched",
        admin_levels=admin_levels,
        session=session,
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
    until: str | None = None,
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
            engine, since=since, until=until, basin=basin,
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

    # Realtime short-circuit: bail before the WSP load + per-storm obsv
    # lookup if this issuance is already done.
    if not overwrite:
        with engine.connect() as conn:
            exists = bool(conn.execute(
                text(
                    "SELECT 1 FROM storms.nhc_wsp_fcastonly_polygon "
                    "WHERE issued_time = :it LIMIT 1"
                ),
                {"it": issued_time},
            ).scalar())
        if exists:
            logger.info(
                f"nhc_wsp_fcastonly_polygon already has rows for "
                f"issued_time={issued_time}; skipping."
            )
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
    until: str | None = None,
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
            until=until,
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
    until: str | None = None,
    basin: str | None = None,
    issued_time=None,
    year: int | None = None,
) -> gpd.GeoDataFrame:
    filters = []
    if since:
        filters.append(f"p.issued_time >= '{since}'")
    if until:
        filters.append(f"p.issued_time < '{until}'")
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


def _load_done_nhc_wsp_fcastonly_exp(
    engine, admin_level: int, issued_time=None,
    since: str | None = None, until: str | None = None,
) -> pd.DataFrame:
    filters = ["admin_level = :al"]
    params: dict = {"al": admin_level}
    if issued_time is not None:
        filters.append("issued_time = :it")
        params["it"] = issued_time
    if since:
        filters.append("issued_time >= :since")
        params["since"] = since
    if until:
        filters.append("issued_time < :until")
        params["until"] = until
    where = " AND ".join(filters)
    try:
        with engine.connect() as conn:
            return pd.read_sql(
                text(
                    "SELECT issued_time, wind_threshold_kt, percentage, atcf_id, admin_level, pcode"
                    f" FROM storms.nhc_wsp_fcastonly_exposure WHERE {where}"
                ),
                conn,
                params=params,
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
    until: str | None = None,
    basin: str | None = None,
    overwrite: bool = False,
    mode: str = "dev",
    issued_time=None,
    admin_levels: list[int] | None = None,
    session: _ExposureSession | None = None,
) -> None:
    """WSP fcastonly exposure, chunked by year so peak memory stays bounded."""
    if issued_time is not None and not overwrite and _exposure_already_done(
        out_table="nhc_wsp_fcastonly_exposure",
        key_col="issued_time",
        key_val=issued_time,
        admin_levels=admin_levels,
        mode=mode,
        session=session,
    ):
        logger.info(
            f"nhc_wsp_fcastonly_exposure already has rows for all requested "
            f"admin_levels at issued_time={issued_time}; skipping."
        )
        return

    _run_exp_year_chunk(
        table_label="WSP fcastonly exposure",
        load_chunk=lambda eng, y: _load_wsp_fcastonly_for_exposure(
            eng, basin=basin, year=y,
            since=since, until=until, issued_time=issued_time,
        ),
        out_table="nhc_wsp_fcastonly_exposure",
        done_loader=_load_done_nhc_wsp_fcastonly_exp,
        done_filter=_filter_done_nhc_wsp_fcastonly,
        countries=countries,
        since=since,
        until=until,
        basin=basin,
        overwrite=overwrite,
        mode=mode,
        issued_time=issued_time,
        chunk_source_table="nhc_wsp_fcastonly_polygon",
        admin_levels=admin_levels,
        session=session,
    )


# ---------------------------------------------------------------------------
# Realtime exposure composites — one process for 3 (tracks) / 2 (wsp)
# pipelines, sharing WorldPop + FieldMaps + engine via _ExposureSession.
# ---------------------------------------------------------------------------


def run_nhc_tracks_exp_realtime(
    *,
    mode: str = "dev",
    issued_time=None,
    countries: list[str] | None = None,
    since: str | None = None,
    until: str | None = None,
    basin: str | None = None,
    overwrite: bool = False,
    admin_levels: list[int] | None = None,
) -> None:
    """Single-process tracks-exposure cascade: fcast + obsv + fcastonly.

    Realtime use: pass ``issued_time`` (single advisory).
    Backfill use: pass ``since``/``until`` (range); inner runners scan
    over their respective buffer tables.
    obsv keys on valid_time which in realtime equals track_issued_time.
    """
    session = build_exposure_session(
        mode=mode, countries=countries, admin_levels=admin_levels,
    )
    try:
        logger.info("=== nhc-track-exp ===")
        run_nhc_tracks_fcast_exp(
            session=session, countries=countries,
            since=since, until=until, basin=basin,
            overwrite=overwrite, mode=mode, issued_time=issued_time,
            admin_levels=admin_levels,
        )
        logger.info("=== nhc-obsv-exp ===")
        run_nhc_tracks_obsv_exp(
            session=session, countries=countries,
            since=since, until=until, basin=basin,
            overwrite=overwrite, mode=mode, valid_time=issued_time,
            admin_levels=admin_levels,
        )
        logger.info("=== nhc-fcastonly-exp ===")
        run_nhc_tracks_fcastonly_exp(
            session=session, countries=countries,
            since=since, until=until, basin=basin,
            overwrite=overwrite, mode=mode, issued_time=issued_time,
            admin_levels=admin_levels,
        )
    finally:
        session.engine.dispose()


def run_nhc_wsp_exp_realtime(
    *,
    mode: str = "dev",
    issued_time=None,
    countries: list[str] | None = None,
    since: str | None = None,
    until: str | None = None,
    basin: str | None = None,
    overwrite: bool = False,
    admin_levels: list[int] | None = None,
) -> None:
    """Single-process WSP-exposure cascade: wsp + wsp-fcastonly.

    Realtime: pass ``issued_time``. Backfill: pass ``since``/``until``.
    """
    session = build_exposure_session(
        mode=mode, countries=countries, admin_levels=admin_levels,
    )
    try:
        logger.info("=== nhc-wsp-exp ===")
        run_nhc_wsp_exp(
            session=session, countries=countries,
            since=since, until=until, basin=basin,
            overwrite=overwrite, mode=mode, issued_time=issued_time,
            admin_levels=admin_levels,
        )
        logger.info("=== nhc-wsp-fcastonly-exp ===")
        run_nhc_wsp_fcastonly_exp(
            session=session, countries=countries,
            since=since, until=until, basin=basin,
            overwrite=overwrite, mode=mode, issued_time=issued_time,
            admin_levels=admin_levels,
        )
    finally:
        session.engine.dispose()


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

    read_engine = stratus.get_engine(stage=mode)
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
