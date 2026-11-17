"""South-West Indian Ocean (RSMC La Réunion / Météo-France) wind swaths and
population exposure.

The counterpart for this basin of the NHC and South Pacific buffer/exposure
stages, writing the tables in src/schemas/sql/swio_tables.sql from the
Météo-France tables that src/pipelines/mfr.py fills.

Realtime, from each Météo-France forecast (storms.mfr_tracks_geo):
  forecast swath (leadtime 0 to 120 h), observed swath (the analysis points
  of every forecast up to then), forecast-only swath (forecast minus
  observed), and 36 uncertainty scenarios: every forecast point moved by its
  Météo-France position_accuracy towards one compass bearing.

Historical, one track per system:
  1985/86 to 2022/23  Réunion's best track (dev blob copy of the file Météo-
                      France shared; Réunion's 48 kt radii from 2003/04, its
                      34 and 64 kt radii from 2010/11)
  later seasons       the system's final track (track-only file), else the
                      analysis points of its forecasts, else IBTrACS
                      (Réunion's position, wind and radii; IBTrACS's "R50"
                      for Réunion is its 48 kt radius)

Choices, on purpose:
  * Réunion's own thresholds and winds: 34/48/64 kt, 10-minute means. No
    JTWC data anywhere.
  * Where Réunion's wind reaches a threshold but it gives no radius, the
    radius is modelled (src/pipelines/swio_radii.py). Before 2003/04 that
    is every radius.
  * Geodesic buffers; admin units extended 500 m into the sea
    (src/utils/regional_exposure.py, same method as the South Pacific).
"""

import argparse
import json
import logging
import os
import re

import geopandas as gpd
import numpy as np
import ocha_stratus as stratus
import pandas as pd
import shapely
from ocha_lens.utils.storm import (
    build_merged_wind_buffer,
    interpolate_track,
    make_geodesic_quadrant_disk,
)
from pyproj import Geod
from sqlalchemy import bindparam, text

from src.pipelines.mfr import clean_name
from src.pipelines.swio_radii import QUADS, SPEEDS, quadrant_radii
from src.utils.regional_exposure import (
    ExposureContext,
    compute_exposure,
    compute_unit_population,
    replace_rows,
)

logger = logging.getLogger(__name__)

# Countries and territories a Réunion swath has reached (34 kt, history
# 1985/86 on) or could reach from the basin.
SWIO_ISO3S = [
    "MOZ", "MDG", "COM", "MYT", "REU", "MUS", "SYC", "TZA", "MWI", "ZWE",
    "ZAF", "SWZ", "LSO", "ZMB", "BWA", "KEN", "SOM", "IOT", "NAM", "MDV",
]
ADMIN_LEVELS = [0, 1]

MAX_LEADTIME_H = 120
SCENARIO_STEP_DEG = 10
# The scheduled run looks back this far for forecasts not yet processed (the
# table also holds the 2010/11-2023/24 archive, which it leaves alone).
REALTIME_LOOKBACK = pd.Timedelta(days=5)

BESTTRACK_BLOB = (
    "ds-storms-pipeline/raw/mfr/drive_copies/"
    "data_cyclone_SWIO_19851986_to_20222023.csv"
)
BESTTRACK_LAST_SEASON = 2022
FIRST_SEASON = 1985

# A swath wider than this (degrees of longitude) is a geometry failure, not
# a storm: lens 0.6.2 Mercator buffers turned three storms globe-sized.
MAX_SWATH_LON_SPAN = 120.0

NM_TO_M = 1852.0
_GEOD = Geod(ellps="WGS84")
_RADII_COLS = {34: "quadrant_radius_34", 48: "quadrant_radius_48", 64: "quadrant_radius_64"}


# ---------------------------------------------------------------------------
# Track points
# ---------------------------------------------------------------------------


def season_of_disturbance(disturbance_id: str) -> int:
    """'10-20252026' -> 2025 (season start year)."""
    m = re.fullmatch(r"\d{2}-(\d{4})(\d{4})", disturbance_id)
    if not m or int(m.group(2)) != int(m.group(1)) + 1:
        raise ValueError(f"not a Météo-France disturbance_id: {disturbance_id!r}")
    return int(m.group(1))


def read_besttrack(raw: bytes) -> pd.DataFrame:
    """Réunion's best-track text file: space-separated, quoted header,
    decimal commas in some values, 'null' for missing, latitude in degrees
    south as a positive number.

    Returns one row per point: disturbance_id ('NN-SSSSSSSS'), storm_id
    (None while unnamed), season, valid_time, lat (negative south), lon,
    wind, mslp and r{k}_{quad} radii (nm) at 34, 48 and 64 kt.
    """
    import shlex

    lines = raw.decode("utf-8").splitlines()
    header = shlex.split(lines[0])
    rows = [ln.split() for ln in lines[1:] if ln.strip()]
    bad = [i + 2 for i, r in enumerate(rows) if len(r) != len(header)]
    if bad:
        raise ValueError(f"best track: {len(bad)} malformed lines, first at line {bad[0]}")
    t = pd.DataFrame(rows, columns=header)

    def num(col):
        return pd.to_numeric(
            t[col].str.replace(",", ".", regex=False).replace("null", None), errors="coerce"
        )

    df = pd.DataFrame({"name_as_issued": t["Name"]})
    df["valid_time"] = pd.to_datetime(dict(
        year=num("Year"), month=num("Month"), day=num("Day"), hour=num("UTC")))
    df["lat"] = -num("Lat")
    df["lon"] = num("Lon")
    df["wind"] = num("Max wind (kt)")
    df["mslp"] = num("MSLP (hPa)")
    for k in SPEEDS:
        for q in QUADS:
            df[f"r{k}_{q}"] = num(f"{k}kt_{q.upper()}_mn")
    # One system per (season, number): the season of its first point.
    num_ = num("Depr Num").astype(int)
    first_time = df.groupby([df.name_as_issued, num_]).valid_time.transform("min")
    season = np.where(first_time.dt.month >= 7, first_time.dt.year, first_time.dt.year - 1)
    df["season"] = season
    df["disturbance_id"] = [f"{n:02d}-{s}{s + 1}" for n, s in zip(num_, season)]
    names = df.groupby("disturbance_id").name_as_issued.agg(lambda s: s.iloc[-1])
    storm_ids = {
        d: (f"{clean_name(n).lower()}_si_{season_of_disturbance(d) + 1}" if clean_name(n) else None)
        for d, n in names.items()
    }
    df["storm_id"] = df.disturbance_id.map(storm_ids)
    if df.duplicated(["disturbance_id", "valid_time"]).any():
        raise ValueError("best track: a system has two points at one time")
    return df.sort_values(["disturbance_id", "valid_time"]).reset_index(drop=True)


def _quads(value) -> list:
    """[NE, SE, SW, NW] floats from a stored radii value, NaN where absent.

    Accepts the JSON arrays of mfr_tracks_geo (null for an omitted sector)
    and the Postgres array text of ibtracs_tracks_geo ('{75.0,NaN,...}').
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return [np.nan] * 4
    if isinstance(value, str):
        s = value.strip()
        if s.startswith("{"):
            vals = [x.strip() for x in s.strip("{}").split(",")]
            vals = [None if x in ("", "NULL", "NaN", "nan") else float(x) for x in vals]
        else:
            vals = json.loads(s)
    else:
        vals = list(value)
    if not isinstance(vals, list) or len(vals) != 4:
        return [np.nan] * 4
    return [np.nan if v is None else float(v) for v in vals]


def radii_columns(df: pd.DataFrame, cols: dict) -> pd.DataFrame:
    """Add r{k}_{quad} from stored quadrant arrays; ``cols``: {k: column}.

    A point that gives some sectors at a speed but leaves others out has 0 in
    the missing ones (Météo-France omits sectors the wind does not reach); a
    point with no sector at all keeps NaN everywhere (no radius given).
    """
    df = df.copy()
    for k, col in cols.items():
        arr = np.array([_quads(v) for v in df[col]], dtype=float).reshape(len(df), 4)
        given = np.isfinite(arr).any(axis=1)
        arr = np.where(given[:, None], np.nan_to_num(arr, nan=0.0), np.nan)
        for i, q in enumerate(QUADS):
            df[f"r{k}_{q}"] = arr[:, i]
    return df


def _mfr_radii(df: pd.DataFrame) -> pd.DataFrame:
    """r{k}_{quad} for Météo-France points: 48 kt from the JSON product's
    48 kt contour, else the CXML product's "50 kt" one (the same radius)."""
    df = df.copy()
    df["q48"] = df["quadrant_radius_48"].where(df["quadrant_radius_48"].notna(), df["quadrant_radius_50"])
    return radii_columns(df, {34: "quadrant_radius_34", 48: "q48", 64: "quadrant_radius_64"})


def to_points(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Track points with modelled-where-missing quadrant radii q_{k}_{quad}
    and modelled_{k} flags (see swio_radii.quadrant_radii)."""
    df = df.sort_values("valid_time").reset_index(drop=True)
    q, modelled = quadrant_radii(df)
    out = pd.concat([df, q, modelled], axis=1)
    lon = (df["lon"].to_numpy(float) + 180.0) % 360.0 - 180.0
    return gpd.GeoDataFrame(out, geometry=gpd.points_from_xy(lon, df["lat"]), crs="EPSG:4326")


# ---------------------------------------------------------------------------
# Swaths
# ---------------------------------------------------------------------------


def swaths(points: gpd.GeoDataFrame, time_col: str = "valid_time") -> gpd.GeoDataFrame:
    """Geodesic 34/48/64 kt swaths of one track with q_{k}_{quad} radii (nm).

    The track is interpolated to 30 minutes (lens interpolate_track) and a
    geodesic quadrant disk drawn at every step (lens
    make_geodesic_quadrant_disk). Built from lens's parts rather than
    calculate_wind_buffers_gdf, whose speeds are limited to 34/50/64 (the
    NHC and IBTrACS table schemas). The basin never reaches ±180°, so no
    dateline handling is needed; a swath wider than MAX_SWATH_LON_SPAN
    raises.
    """
    cols = [time_col, *(f"q_{k}_{q}" for k in SPEEDS for q in QUADS)]
    interp = interpolate_track(points[[*cols, "geometry"]], time_col=time_col)
    geoms = []
    for k in SPEEDS:
        g = build_merged_wind_buffer(
            interp, tuple(f"q_{k}_{q}" for q in QUADS), make_disk=make_geodesic_quadrant_disk
        )
        if g is not None and not g.is_empty:
            minx, _, maxx, _ = g.bounds
            if maxx - minx > MAX_SWATH_LON_SPAN or maxx > 180 or minx < -180:
                raise ValueError(f"{k} kt swath spans {minx:.1f} to {maxx:.1f} degrees")
            if not g.is_valid:
                g = shapely.make_valid(g)
        geoms.append(g)
    return gpd.GeoDataFrame({"wind_speed_kt": SPEEDS}, geometry=geoms, crs="EPSG:4326")


def _fcastonly(fcast, obsv):
    if fcast is None or fcast.is_empty:
        return None
    if obsv is None or obsv.is_empty:
        return fcast
    diff = fcast.difference(obsv)
    return None if diff.is_empty else diff


def _geoms_by_speed(gdf: gpd.GeoDataFrame) -> dict:
    return dict(zip(gdf["wind_speed_kt"], gdf.geometry))


def shift_points(points: gpd.GeoDataFrame, bearing_deg: float) -> gpd.GeoDataFrame:
    """Move every point by its position_accuracy (nm) towards a bearing."""
    out = points.copy()
    dist = np.nan_to_num(points["position_accuracy"].to_numpy(float), nan=0.0) * NM_TO_M
    lon, lat, _ = _GEOD.fwd(
        points.geometry.x.to_numpy(), points.geometry.y.to_numpy(),
        np.full(len(points), float(bearing_deg)), dist,
    )
    out["geometry"] = gpd.points_from_xy(lon, lat)
    return out.set_crs("EPSG:4326", allow_override=True)


# ---------------------------------------------------------------------------
# Realtime: Météo-France forecasts
# ---------------------------------------------------------------------------

_POINT_COLS = """
    p.disturbance_id, p.issued_time, p.kind, p.data_type, p.valid_time, p.leadtime,
    p.wind_speed AS wind, p.pressure AS mslp, p.position_accuracy,
    p.quadrant_radius_34, p.quadrant_radius_48, p.quadrant_radius_50, p.quadrant_radius_64,
    ST_X(p.geometry) AS lon, ST_Y(p.geometry) AS lat
"""


def _read(engine, sql: str, params: dict) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def load_forecast_points(engine, disturbance_id: str, issued_time) -> pd.DataFrame:
    """The forecast points (leadtime 0 to MAX_LEADTIME_H) of one forecast."""
    df = _read(engine, f"""
        SELECT {_POINT_COLS} FROM storms.mfr_tracks_geo p
        WHERE p.disturbance_id = :d AND p.issued_time = :t AND p.kind = 'forecast'
          AND p.data_type = 'forecast' AND p.leadtime BETWEEN 0 AND :maxlt
        ORDER BY p.valid_time""",
        {"d": disturbance_id, "t": issued_time, "maxlt": MAX_LEADTIME_H})
    return _mfr_radii(df)


def latest_per_time(df: pd.DataFrame) -> pd.DataFrame:
    """One point per valid_time: the latest forecast's, and within a forecast
    its analysis point over its leadtime-0 forecast point."""
    df = df.assign(_rank=(df["data_type"] == "analysis").astype(int))
    return (df.sort_values(["valid_time", "issued_time", "_rank"])
              .drop_duplicates("valid_time", keep="last")
              .drop(columns="_rank")
              .reset_index(drop=True))


def load_observed_points(engine, disturbance_id: str, upto) -> pd.DataFrame:
    """The observed track up to ``upto``: analysis points (and the leadtime-0
    point) of every forecast issued up to then. Where forecasts disagree
    about a time, the latest forecast's analysis wins."""
    df = _read(engine, f"""
        SELECT {_POINT_COLS} FROM storms.mfr_tracks_geo p
        WHERE p.disturbance_id = :d AND p.kind = 'forecast' AND p.leadtime <= 0
          AND p.issued_time <= :t AND p.valid_time <= :t
        ORDER BY p.valid_time""",
        {"d": disturbance_id, "t": upto})
    if df.empty:
        return df
    return _mfr_radii(latest_per_time(df))


def process_mfr_forecast(
    engine, ctx: ExposureContext, disturbance_id: str, issued_time, scenarios: bool = True
) -> None:
    """All swaths and exposure for one Météo-France forecast."""
    key_it = {"disturbance_id": disturbance_id, "issued_time": issued_time}
    key_vt = {"disturbance_id": disturbance_id, "valid_time": issued_time}
    fc = load_forecast_points(engine, disturbance_id, issued_time)
    if fc.empty:
        logger.warning(f"No forecast points for {disturbance_id} {issued_time}")
        return
    fc_pts = to_points(fc)
    an = load_observed_points(engine, disturbance_id, issued_time)

    fcast = swaths(fc_pts)
    obsv = swaths(to_points(an)) if not an.empty else None
    fcast_g = _geoms_by_speed(fcast)
    obsv_g = _geoms_by_speed(obsv) if obsv is not None else {}
    fco = gpd.GeoDataFrame(
        {"wind_speed_kt": SPEEDS},
        geometry=[_fcastonly(fcast_g.get(k), obsv_g.get(k)) for k in SPEEDS],
        crs="EPSG:4326",
    )

    def _frame(gdf, key):
        f = gdf.copy()
        for c, v in key.items():
            f[c] = v
        return f[[*key, "wind_speed_kt", "geometry"]]

    replace_rows(engine, "mfr_tracks_fcast_buffers", _frame(fcast, key_it), key_it)
    if obsv is not None:
        replace_rows(engine, "mfr_tracks_obsv_buffers", _frame(obsv, key_vt), key_vt)
    replace_rows(engine, "mfr_tracks_fcastonly_buffers", _frame(fco, key_it), key_it)
    _mark_done(engine, disturbance_id, issued_time, "buffers")

    for table, gdf, key in (
        ("mfr_tracks_fcast_exposure", fcast, key_it),
        ("mfr_tracks_obsv_exposure", obsv, key_vt),
        ("mfr_tracks_fcastonly_exposure", fco, key_it),
    ):
        if gdf is None:
            continue
        replace_rows(engine, table, compute_exposure(_frame(gdf, key), ctx), key)
    _mark_done(engine, disturbance_id, issued_time, "exposure")

    if not scenarios:
        return
    exps, envelope = [], {k: [] for k in SPEEDS}
    bearings = list(range(0, 360, SCENARIO_STEP_DEG))
    for brg in bearings:
        sg = _geoms_by_speed(swaths(shift_points(fc_pts, brg)))
        sco = gpd.GeoDataFrame(
            {"wind_speed_kt": SPEEDS},
            geometry=[_fcastonly(sg.get(k), obsv_g.get(k)) for k in SPEEDS],
            crs="EPSG:4326",
        )
        for k, g in zip(sco.wind_speed_kt, sco.geometry):
            if g is not None and not g.is_empty:
                envelope[k].append(g)
        f = _frame(sco, key_it)
        f["shift_deg"] = brg
        e = compute_exposure(f, ctx, admin_levels=[0])
        if not e.empty:
            exps.append(e)
    for k in SPEEDS:
        g = fco.loc[fco.wind_speed_kt == k, "geometry"].iloc[0]
        if g is not None and not g.is_empty:
            envelope[k].append(g)
    env = gpd.GeoDataFrame(
        {"wind_speed_kt": SPEEDS, "n_scenarios": len(bearings)},
        geometry=[shapely.union_all(envelope[k]) if envelope[k] else None for k in SPEEDS],
        crs="EPSG:4326",
    )
    env = env.assign(**key_it)[[*key_it, "wind_speed_kt", "n_scenarios", "geometry"]]
    replace_rows(engine, "mfr_tracks_scenario_envelope", env, key_it)
    replace_rows(engine, "mfr_tracks_scenario_exposure",
                 pd.concat(exps, ignore_index=True) if exps else None, key_it)
    _mark_done(engine, disturbance_id, issued_time, "scenarios")


def _mark_done(engine, disturbance_id: str, issued_time, stage: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO storms.mfr_processing_completion "
                "(disturbance_id, issued_time, stage) VALUES (:d, :t, :s) "
                "ON CONFLICT (disturbance_id, issued_time, stage) "
                "DO UPDATE SET completed_at = now() AT TIME ZONE 'utc'"
            ),
            {"d": disturbance_id, "t": issued_time, "s": stage},
        )


def pending_forecasts(
    engine, disturbance_id=None, issued_time=None, since=None, overwrite=False,
    last_stage="scenarios",
) -> pd.DataFrame:
    """Forecasts (kind 'forecast') not yet processed up to ``last_stage``.

    A forecast ingested again after it was processed (a corrected file
    replaces its points) is processed again.
    """
    filters = ["f.kind = 'forecast'", "f.n_forecast > 0"]
    params = {}
    if disturbance_id:
        filters.append("f.disturbance_id = :d")
        params["d"] = disturbance_id
    if issued_time is not None:
        filters.append("f.issued_time = :t")
        params["t"] = issued_time
    if since is not None:
        filters.append("f.issued_time >= :since")
        params["since"] = since
    if not overwrite:
        filters.append(
            "NOT EXISTS (SELECT 1 FROM storms.mfr_processing_completion c "
            "WHERE c.disturbance_id = f.disturbance_id "
            "AND c.issued_time = f.issued_time AND c.stage = :last_stage "
            "AND c.completed_at >= f.ingested_at)"
        )
        params["last_stage"] = last_stage
    return _read(engine, f"""
        SELECT f.disturbance_id, f.issued_time FROM storms.mfr_forecasts f
        WHERE {' AND '.join(filters)} ORDER BY f.issued_time, f.disturbance_id""", params)


def run_swio_realtime(
    mode: str = "dev",
    disturbance_id: str | None = None,
    issued_time=None,
    since=None,
    overwrite: bool = False,
    scenarios: bool = True,
) -> list[tuple]:
    """Process every recent Météo-France forecast not yet fully processed (or
    the one asked for), in issue order. Without ``since`` or a forecast named,
    only forecasts issued in the last REALTIME_LOOKBACK are considered."""
    engine = stratus.get_engine(stage=mode, write=True)
    if since is None and disturbance_id is None and issued_time is None:
        since = pd.Timestamp.now(tz="UTC").tz_localize(None) - REALTIME_LOOKBACK
    todo = pending_forecasts(
        engine, disturbance_id, issued_time, since, overwrite,
        last_stage="scenarios" if scenarios else "exposure",
    )
    if todo.empty:
        logger.info("No Météo-France forecasts to process.")
        return []
    logger.info(f"{len(todo)} Météo-France forecast(s) to process")
    ctx = ExposureContext(SWIO_ISO3S, stage=mode, admin_levels=ADMIN_LEVELS)
    done = []
    for row in todo.itertuples():
        logger.info(f"SWIO {row.disturbance_id} {row.issued_time}")
        process_mfr_forecast(engine, ctx, row.disturbance_id, row.issued_time, scenarios=scenarios)
        done.append((row.disturbance_id, row.issued_time))
    return done


# ---------------------------------------------------------------------------
# Historical
# ---------------------------------------------------------------------------


def load_besttrack(stage: str = "dev") -> pd.DataFrame:
    return read_besttrack(stratus.load_blob_data(BESTTRACK_BLOB, stage=stage, container_name="projects"))


def load_mfr_history(engine, first_season: int) -> dict:
    """{disturbance_id: (points, track_source)} for seasons after the best
    track: the latest final track (kind 'track') where there is one, else the
    analysis points of the system's forecasts."""
    first = max(first_season, BESTTRACK_LAST_SEASON + 1)
    sql = f"""
        SELECT {_POINT_COLS}, s.storm_id
        FROM storms.mfr_tracks_geo p JOIN storms.mfr_storms s USING (disturbance_id)
        WHERE substring(p.disturbance_id from 4 for 4)::int >= :first
          AND (p.kind = 'track' OR (p.leadtime <= 0 AND p.valid_time <= p.issued_time))
    """
    df = _read(engine, sql, {"first": first})
    out = {}
    for did, g in df.groupby("disturbance_id"):
        tracks = g[g.kind == "track"]
        if not tracks.empty:
            pts = tracks[tracks.issued_time == tracks.issued_time.max()]
            pts = pts[pts.data_type == "analysis"]
            source = "mfr_track"
        else:
            pts = latest_per_time(g[g.kind == "forecast"])
            source = "mfr_forecasts"
        pts = _mfr_radii(pts.drop_duplicates("valid_time", keep="last"))
        pts["season"] = season_of_disturbance(did)
        out[did] = (pts, source)
    return out


def load_ibtracs_history(engine, first_season: int, exclude_storm_ids) -> dict:
    """{sid: (points, 'ibtracs')} for named systems after the best track that
    Météo-France data doesn't cover (e.g. the Feb-Oct 2025 gap): Réunion's
    rows of IBTrACS, deduplicated on (sid, valid_time) — unnamed and renamed
    storms have repeated rows in ibtracs_tracks_geo — and keeping the rows
    whose storm_id is the storm's current one."""
    first = max(first_season, BESTTRACK_LAST_SEASON + 1)
    df = _read(engine, """
        SELECT DISTINCT ON (g.sid, g.valid_time)
               g.sid, s.storm_id, s.season, g.valid_time, g.wind_speed AS wind,
               g.pressure AS mslp, g.quadrant_radius_34, g.quadrant_radius_50,
               g.quadrant_radius_64, ST_X(g.geometry) AS lon, ST_Y(g.geometry) AS lat
        FROM storms.ibtracs_tracks_geo g JOIN storms.ibtracs_storms s USING (sid)
        WHERE g.basin = 'SI' AND g.provider = 'reunion' AND s.season >= :season_end
          AND s.storm_id IS NOT NULL AND g.storm_id = s.storm_id
        ORDER BY g.sid, g.valid_time""", {"season_end": first + 1})
    exclude = set(exclude_storm_ids)
    out = {}
    for sid, g in df.groupby("sid"):
        if g.storm_id.iloc[0] in exclude or not (g.wind >= SPEEDS[0]).any():
            continue
        # IBTrACS's Réunion "R50" is Réunion's 48 kt radius.
        pts = radii_columns(g, {34: "quadrant_radius_34", 48: "quadrant_radius_50",
                                64: "quadrant_radius_64"})
        pts["season"] = int(g.season.iloc[0]) - 1
        pts["disturbance_id"] = sid
        out[sid] = (pts, "ibtracs")
    return out


def history_tracks(engine, stage: str, first_season: int = FIRST_SEASON, ibtracs_engine=None) -> dict:
    """{key: (points, track_source, storm_id, season)}, one track per system
    with any point at 34 kt or more. IBTrACS is read with ``ibtracs_engine``
    (the IBTrACS tables are kept current in prod only)."""
    tracks = {}
    bt = load_besttrack(stage)
    bt = bt[(bt.season >= first_season) & (bt.season <= BESTTRACK_LAST_SEASON)]
    for did, g in bt.groupby("disturbance_id"):
        tracks[did] = (g, "besttrack", g.storm_id.iloc[0], int(g.season.iloc[0]))
    for did, (g, src) in load_mfr_history(engine, first_season).items():
        tracks[did] = (g, src, g.storm_id.dropna().iloc[-1] if g.storm_id.notna().any() else None,
                       season_of_disturbance(did))
    have = {v[2] for v in tracks.values() if v[2]}
    for sid, (g, src) in load_ibtracs_history(ibtracs_engine or engine, first_season, have).items():
        tracks[sid] = (g, src, g.storm_id.iloc[0], int(g.season.iloc[0]))
    return {k: v for k, v in tracks.items() if (v[0]["wind"] >= SPEEDS[0]).any()}


def history_buffers(key: str, pts: pd.DataFrame, source: str, storm_id, season: int) -> gpd.GeoDataFrame:
    p = to_points(pts)
    b = swaths(p)
    b["disturbance_id"] = key
    b["storm_id"] = storm_id
    b["season"] = season
    b["track_source"] = source
    wind = p["wind"].to_numpy(float)
    b["n_points"] = [int(np.sum(wind >= k)) for k in SPEEDS]
    b["n_modelled"] = [int(p[f"modelled_{k}"].sum()) for k in SPEEDS]
    return b[["disturbance_id", "storm_id", "season", "track_source", "wind_speed_kt",
              "n_points", "n_modelled", "geometry"]]


def run_swio_history(
    mode: str = "dev",
    first_season: int = FIRST_SEASON,
    keys: list[str] | None = None,
    overwrite: bool = False,
    out_dir: str | None = None,
    stage_blob: str = "dev",
    ibtracs_stage: str = "prod",
) -> None:
    """Historical SWIO swaths + exposure, one track per system.

    ``out_dir``: write parquet files there instead of the database (reads
    the database, writes nothing to it). ``ibtracs_stage``: database read for
    the IBTrACS fallback (read only).
    """
    read_engine = stratus.get_engine(stage=mode)
    tracks = history_tracks(read_engine, stage_blob, first_season,
                            ibtracs_engine=stratus.get_engine(stage=ibtracs_stage))
    if keys:
        tracks = {k: v for k, v in tracks.items() if k in set(keys)}
    engine = None if out_dir else stratus.get_engine(stage=mode, write=True)
    if engine is not None and overwrite and not keys:
        # A system can change key (an IBTrACS fallback replaced by a
        # Météo-France track): a full refresh drops keys no longer produced.
        current = sorted(tracks)
        for table in ("swio_hist_wind_exposure", "swio_hist_wind_buffers"):
            with engine.begin() as conn:
                n = conn.execute(
                    text(f"DELETE FROM storms.{table} WHERE NOT (disturbance_id = ANY(:k)) "
                         "AND disturbance_id IN (SELECT disturbance_id FROM storms.swio_hist_wind_buffers "
                         "WHERE season >= :first)"),
                    {"k": current, "first": first_season},
                ).rowcount
            if n:
                logger.info(f"{table}: removed {n} rows of systems no longer in the history")
    if engine is not None and not overwrite:
        with engine.connect() as conn:
            done = {r[0] for r in conn.execute(text(
                "SELECT DISTINCT disturbance_id FROM storms.swio_hist_wind_buffers"))}
        tracks = {k: v for k, v in tracks.items() if k not in done}
    logger.info(f"{len(tracks)} SWIO systems to process (season >= {first_season})")
    ctx = ExposureContext(SWIO_ISO3S, stage=mode, admin_levels=ADMIN_LEVELS)
    buf_parts, exp_parts = [], []
    for i, (key, (pts, source, storm_id, season)) in enumerate(sorted(tracks.items())):
        buf = history_buffers(key, pts, source, storm_id, season)
        exp = compute_exposure(buf[["disturbance_id", "wind_speed_kt", "geometry"]], ctx)
        if engine is not None:
            # One transaction per system: a resumed run skips systems that
            # have buffers, so they must never be written without exposure.
            with engine.begin() as conn:
                replace_rows(engine, "swio_hist_wind_buffers", buf, {"disturbance_id": key}, conn=conn)
                replace_rows(engine, "swio_hist_wind_exposure", exp if not exp.empty else None,
                             {"disturbance_id": key}, conn=conn)
        else:
            buf_parts.append(buf)
            if not exp.empty:
                exp_parts.append(exp)
        if (i + 1) % 25 == 0:
            logger.info(f"history: {i + 1}/{len(tracks)} systems")
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
        gpd.GeoDataFrame(pd.concat(buf_parts, ignore_index=True), crs="EPSG:4326").to_parquet(
            os.path.join(out_dir, "swio_hist_wind_buffers.parquet"))
        if exp_parts:
            pd.concat(exp_parts, ignore_index=True).to_parquet(
                os.path.join(out_dir, "swio_hist_wind_exposure.parquet"))


def run_swio_admin_population(mode: str = "dev", out_dir: str | None = None) -> pd.DataFrame:
    ctx = ExposureContext(SWIO_ISO3S, stage=mode, admin_levels=ADMIN_LEVELS)
    pop = compute_unit_population(ctx)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
        pop.to_parquet(os.path.join(out_dir, "swio_admin_population.parquet"))
        return pop
    engine = stratus.get_engine(stage=mode, write=True)
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM storms.swio_admin_population WHERE iso3 IN :isos").bindparams(
                bindparam("isos", expanding=True)),
            {"isos": sorted(pop.iso3.unique())},
        )
        pop.to_sql("swio_admin_population", conn, schema="storms", if_exists="append", index=False)
    return pop


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _ts(value):
    return None if value is None else pd.Timestamp(value).tz_localize(None)


def main(argv=None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", force=True)
    logging.getLogger("azure").setLevel(logging.WARNING)
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--mode", choices=["dev", "prod"], default="dev")

    rt = sub.add_parser("realtime", parents=[common],
                        help="Swaths, exposure and scenarios for new Météo-France forecasts")
    rt.add_argument("--disturbance-id", metavar="ID", help="e.g. 10-20252026")
    rt.add_argument("--issued-time", metavar="YYYY-MM-DDTHH")
    rt.add_argument("--since", metavar="YYYY-MM-DD",
                    help=f"Forecasts issued since then (default: the last {REALTIME_LOOKBACK.days} days)")
    rt.add_argument("--overwrite", action="store_true")
    rt.add_argument("--no-scenarios", action="store_true")

    hi = sub.add_parser("history", parents=[common], help="Historical swaths and exposure")
    hi.add_argument("--first-season", type=int, default=FIRST_SEASON)
    hi.add_argument("--keys", nargs="+", metavar="ID", help="disturbance_ids (or IBTrACS sids)")
    hi.add_argument("--overwrite", action="store_true")
    hi.add_argument("--out-dir", help="Write parquet here instead of the database")
    hi.add_argument("--blob-stage", default="dev", choices=["dev", "prod"],
                    help="Blob stage holding the best-track copy")
    hi.add_argument("--ibtracs-stage", default="prod", choices=["dev", "prod"],
                    help="Database read for the IBTrACS fallback")

    po = sub.add_parser("admin-population", parents=[common],
                        help="Total population per (sea-extended) admin unit")
    po.add_argument("--out-dir")

    a = ap.parse_args(argv)
    if a.cmd == "realtime":
        run_swio_realtime(a.mode, a.disturbance_id, _ts(a.issued_time), _ts(a.since),
                          a.overwrite, not a.no_scenarios)
    elif a.cmd == "history":
        run_swio_history(a.mode, a.first_season, a.keys, a.overwrite, a.out_dir, a.blob_stage,
                         a.ibtracs_stage)
    elif a.cmd == "admin-population":
        run_swio_admin_population(a.mode, a.out_dir)


if __name__ == "__main__":
    main()
