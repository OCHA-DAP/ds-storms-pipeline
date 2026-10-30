"""South Pacific (RSMC Nadi / FMS) wind buffers and population exposure.

The South Pacific counterpart of the NHC buffer/exposure stages in nhc.py,
writing the tables in src/schemas/sql/sp_tables.sql. Two families:

Realtime, from FMS Official forecast tracks (storms.fms_tracks_geo, written by
src/pipelines/fms.py inside the pa-aa-fji-storms job):
  forecast swath (analysis + forecast points), cumulative observed swath
  (analysis points of every forecast received so far), forecast-only swath
  (forecast minus observed), and 36 uncertainty scenarios: every track point
  moved by its FMS position uncertainty towards one compass bearing, the
  Fiji framework's method, each scenario's forecast-only exposure stored.

Historical, from IBTrACS (season 2002 on, when JTWC started giving radii in
the basin): JTWC positions and radii, with a k-kt radius kept only where the
official 10-minute intensity reaches k kt. Storms with no JTWC radii at all
fall back to the Fiji framework regression on the official intensity.

Choices that differ from the NHC tables, on purpose:
  * FMS 10-minute winds. JTWC's radii are not shrunk to 10-minute: matched
    against FMS's own radii (6 post-event best tracks + forecast analysis
    points, 13 storms) and BoM's (84 storms), JTWC's radii already come out
    the same size, while converting the wind field shrinks them 12-30%.
  * Geodesic buffers (ocha_lens method="geodesic"): Mercator disks reach only
    cos(latitude) of the stated radius.
  * Admin units extended 500 m into the sea and made non-overlapping, so
    coastal and atoll populations on 1 km WorldPop cells are counted.
"""

import json
import logging
import os
import tempfile
from functools import lru_cache

import geopandas as gpd
import numpy as np
import ocha_stratus as stratus
import pandas as pd
import shapely
from ocha_lens.utils.storm import calculate_wind_buffers_gdf
from pyproj import Geod
from shapely.geometry import box
from shapely.validation import make_valid
from sqlalchemy import bindparam, text

from src.utils.exposure import calculate_exposure, load_adm_units, load_pop

logger = logging.getLogger(__name__)

# The 16 Pacific island countries and territories in or next to the FMS
# area of responsibility (equator to 25°S, 160°E to 120°W).
SP_ISO3S = [
    "FJI", "VUT", "NCL", "SLB", "TON", "WSM", "ASM", "WLF",
    "TUV", "TKL", "NIU", "COK", "PYF", "KIR", "NRU", "PCN",
]
SPEEDS = [34, 50, 64]
QUADS = ["ne", "se", "sw", "nw"]
ADMIN_LEVELS = [0, 1]

SEA_EXPANSION_M = 500
WORLDPOP_YEAR = 2026

# Historical gate: the official (RSMC/TCWC) 10-minute intensity in IBTrACS,
# else JTWC's 1-minute wind times 0.90 (the median FMS/JTWC ratio at the
# matched points; FMS and IBTrACS Nadi agree within 1 kt there).
HIST_FIRST_SEASON = 2002
OFFICIAL_AGENCIES = {"nadi", "wellington", "bom"}
JTWC_TO_10MIN = 0.90

# Fiji 2025 framework regression (pa-aa-fji-storms WIND_RADIUS_PARAMS_FMS):
# log(mean quadrant radius, nm) = const + a log(10-min wind) + b log|lat|.
FJI_RADIUS_REGRESSION = {
    34: (3.489578, 0.166424, 0.127185),
    50: (-2.438167, 0.921004, 0.747109),
    64: (-3.945538, 1.125596, 0.789324),
}

OFFICIAL_TRACK = "Official Forecast Track"
MAX_LEADTIME_H = 120
SCENARIO_STEP_DEG = 10

_GEOD = Geod(ellps="WGS84")
_LONWRAP180 = "+proj=longlat +datum=WGS84 +lon_wrap=180"
_QUAD_FMT = "q_{speed}_{quad}"
NM_TO_M = 1852.0


# ---------------------------------------------------------------------------
# Admin units extended into the sea
# ---------------------------------------------------------------------------


def _country_aeqd(gdf: gpd.GeoDataFrame) -> str:
    """Azimuthal equidistant CRS centred on a country, dateline-safe."""
    minx, miny, maxx, maxy = gdf.to_crs(_LONWRAP180).total_bounds
    lon0 = (minx + maxx) / 2
    lon0 = lon0 - 360 if lon0 > 180 else lon0
    lat0 = (miny + maxy) / 2
    return (
        f"+proj=aeqd +lat_0={lat0:.5f} +lon_0={lon0:.5f} "
        "+datum=WGS84 +units=m +no_defs"
    )


def _split_at_antimeridian(geom):
    """A polygon with continuous longitudes (0 to 360 here) back into
    [-180, 180]: parts beyond 180 are clipped off and shifted by 360.

    Admin units, unlike wind swaths, have vertices exactly on ±180 (FieldMaps
    cuts countries there), which a wrapped ring makes ambiguous: one of the
    sea-extended Fiji test units came back spanning -180 to 180 through
    ocha_lens.utils.storm.fix_antimeridian. Clipping continuous longitudes
    has no such ambiguity.
    """
    if geom is None or geom.is_empty:
        return geom
    if not geom.is_valid:
        geom = make_valid(geom)
    parts = []
    for k in (-1, 0, 1):
        piece = geom.intersection(box(-180.0 + 360.0 * k, -90.0, 180.0 + 360.0 * k, 90.0))
        if piece.is_empty:
            continue
        if k:
            piece = shapely.affinity.translate(piece, xoff=-360.0 * k)
        parts.extend(p for p in getattr(piece, "geoms", [piece]) if p.geom_type == "Polygon")
    out = shapely.union_all(parts) if parts else shapely.Polygon()
    return out if out.is_valid else make_valid(out)


def expand_units_into_sea(
    units: gpd.GeoDataFrame, metres: float = SEA_EXPANSION_M
) -> gpd.GeoDataFrame:
    """One country's admin units, each extended ``metres`` into the sea.

    Only sea is added (never a neighbour's land), and sea claimed by one unit
    is not given to the next, so the units still don't overlap and admin 1
    sums to admin 0. Buffering happens in a metric projection centred on the
    country; the result is split at ±180°.
    """
    crs = _country_aeqd(units)
    p = units.to_crs(crs)
    p["geometry"] = p.geometry.make_valid()
    land = shapely.union_all(p.geometry.values)
    claimed = None
    geoms = []
    for g in p.geometry:
        sea = g.buffer(metres, quad_segs=4).difference(land)
        if claimed is not None:
            sea = sea.difference(claimed)
        claimed = sea if claimed is None else claimed.union(sea)
        geoms.append(shapely.union(g, sea))
    p["geometry"] = geoms
    back = p.to_crs(_LONWRAP180)
    back["geometry"] = back.geometry.apply(_split_at_antimeridian)
    return back.set_crs("EPSG:4326", allow_override=True)


@lru_cache(maxsize=None)
def _sp_units(stage: str, countries: tuple) -> dict:
    adm1 = load_adm_units(list(countries), 1, stage=stage)
    adm1 = adm1.dissolve(by=["iso3", "pcode"], as_index=False)
    parts = [
        expand_units_into_sea(g.sort_values("pcode"))
        for _, g in adm1.groupby("iso3")
    ]
    u1 = gpd.GeoDataFrame(pd.concat(parts, ignore_index=True), crs="EPSG:4326")
    u1 = u1[["iso3", "pcode", "geometry"]].reset_index(drop=True)
    u0 = u1.dissolve(by="iso3", as_index=False)[["iso3", "geometry"]]
    u0["pcode"] = u0["iso3"]
    return {0: u0[["iso3", "pcode", "geometry"]], 1: u1}


def load_sp_units(stage: str = "dev", countries=None) -> dict:
    """{admin_level: GeoDataFrame[iso3, pcode, geometry]} on sea-extended units."""
    return _sp_units(stage, tuple(sorted(countries or SP_ISO3S)))


# ---------------------------------------------------------------------------
# Exposure
# ---------------------------------------------------------------------------


class _ExposureContext:
    """WorldPop + sea-extended units, with per-country rasters cached."""

    def __init__(self, stage: str, countries=None, admin_levels=None):
        self.units = load_sp_units(stage, countries)
        self.admin_levels = list(admin_levels or ADMIN_LEVELS)
        self.da = load_pop()
        self._country = {}

    def country(self, iso3: str):
        """(country geometry, country raster or None if WorldPop has no people there)."""
        if iso3 not in self._country:
            cu = self.units[0]
            geom = cu.loc[cu.iso3 == iso3, "geometry"].iloc[0]
            da_c = self.da.rio.clip([geom], all_touched=True).load()
            if not np.nansum(np.where(da_c.values > 0, da_c.values, 0)):
                # WorldPop 2026 (R2025A) has no data for Kiribati at all.
                logger.warning(f"WorldPop has no population for {iso3}; exposure skipped")
                da_c = None
            self._country[iso3] = (geom, da_c)
        return self._country[iso3]


def compute_exposure(buffers: gpd.GeoDataFrame, ctx: _ExposureContext, admin_levels=None) -> pd.DataFrame:
    """Population exposed per buffer row and admin unit.

    ``buffers``: key columns + ``wind_speed_kt`` + geometry (EPSG:4326, split
    at ±180°). Returns the key columns + wind_speed_kt + admin_level, iso3,
    pcode, pop_exposed, one row per unit a buffer touches.
    """
    keep = buffers.geometry.notna() & ~buffers.geometry.is_empty
    b = buffers[keep].reset_index(drop=True)
    out = []
    if b.empty:
        return pd.DataFrame()
    sidx = b.sindex
    for al in admin_levels or ctx.admin_levels:
        for iso3, cu in ctx.units[al].groupby("iso3"):
            cgeom, da_c = ctx.country(iso3)
            if da_c is None:
                continue
            cand = b.iloc[sorted(set(sidx.query(cgeom, predicate="intersects")))]
            if cand.empty:
                continue
            for _, unit in cu.iterrows():
                bu = cand[cand.intersects(unit.geometry)]
                if bu.empty:
                    continue
                e = calculate_exposure(bu, da_c, mask_geom=unit.geometry)
                e["admin_level"] = al
                e["iso3"] = iso3
                e["pcode"] = unit.pcode
                out.append(e)
    if not out:
        return pd.DataFrame()
    return pd.concat(out, ignore_index=True)


def compute_unit_population(ctx: _ExposureContext) -> pd.DataFrame:
    rows = []
    for al in ctx.admin_levels:
        for iso3, cu in ctx.units[al].groupby("iso3"):
            _, da_c = ctx.country(iso3)
            for _, unit in cu.iterrows():
                pop = 0
                if da_c is not None:
                    g = gpd.GeoDataFrame({"k": [0]}, geometry=[unit.geometry], crs=4326)
                    pop = int(calculate_exposure(g, da_c).pop_exposed.iloc[0])
                rows.append({"admin_level": al, "iso3": iso3, "pcode": unit.pcode,
                             "total_pop": pop, "worldpop_year": WORLDPOP_YEAR})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------


def _replace_rows(engine, table: str, df: pd.DataFrame, key: dict) -> None:
    """Delete every row of ``table`` matching ``key`` and insert ``df``, in one
    transaction (an upsert alone would leave stale units behind)."""
    where = " AND ".join(f"{c} = :{c}" for c in key)
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM storms.{table} WHERE {where}"), key)
        if df is not None and not df.empty:
            df = df.copy()
            if "geometry" in df.columns:
                df["geometry"] = df["geometry"].apply(
                    lambda g: None if g is None or g.is_empty else g.wkt
                )
            df.to_sql(table, conn, schema="storms", if_exists="append", index=False)


def _mark_done(engine, disturbance_id: str, issued_time, stage: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO storms.fms_processing_completion "
                "(disturbance_id, issued_time, stage) VALUES (:d, :t, :s) "
                "ON CONFLICT (disturbance_id, issued_time, stage) "
                "DO UPDATE SET completed_at = now() AT TIME ZONE 'utc'"
            ),
            {"d": disturbance_id, "t": issued_time, "s": stage},
        )


# ---------------------------------------------------------------------------
# Buffers from track points
# ---------------------------------------------------------------------------


def regression_radius(speed: int, wind, lat) -> np.ndarray:
    """Fiji framework radius (nm) at ``speed`` kt; 0 where the wind is below it."""
    c, a, b = FJI_RADIUS_REGRESSION[speed]
    wind = np.asarray(wind, float)
    lat = np.abs(np.asarray(lat, float))
    with np.errstate(divide="ignore", invalid="ignore"):
        r = np.exp(c + a * np.log(wind) + b * np.log(lat))
    return np.where(wind >= speed, r, 0.0)


def wind_buffers(points: gpd.GeoDataFrame, time_col: str = "valid_time") -> gpd.GeoDataFrame:
    """Geodesic 34/50/64 kt swaths of one track with ``q_{speed}_{quad}`` radii (nm)."""
    return calculate_wind_buffers_gdf(
        points, quad_cols_format=_QUAD_FMT, valid_time_col=time_col,
        method="geodesic", speeds=SPEEDS,
    )


def _fcastonly(fcast, obsv):
    if fcast is None or fcast.is_empty:
        return None
    if obsv is None or obsv.is_empty:
        return fcast
    diff = fcast.difference(obsv)
    return None if diff.is_empty else diff


# ---------------------------------------------------------------------------
# Realtime: FMS forecasts
# ---------------------------------------------------------------------------


def _quad_list(value) -> list:
    """[NE, SE, SW, NW] from a stored JSON array; four Nones if absent or malformed."""
    v = json.loads(value) if isinstance(value, str) else None
    return v if isinstance(v, list) and len(v) == 4 else [None] * 4


def fms_point_radii(df: pd.DataFrame) -> pd.DataFrame:
    """Add ``q_{speed}_{quad}`` radii to FMS track points.

    FMS gives quadrant radii, a symmetric radius, or neither. Per quadrant:
    the quadrant value; else the symmetric radius; else, where FMS gives no
    radius at that speed at all but its wind reaches it, the Fiji framework
    regression (what the live Fiji monitor does); else 0.
    """
    df = df.copy()
    lat = df.geometry.y.to_numpy()
    for k in SPEEDS:
        quads = df[f"quadrant_radius_{k}"].apply(_quad_list)
        sym = df[f"radius_{k}"].to_numpy(float)
        reg = regression_radius(k, df["wind_speed"], lat)
        given_any = quads.apply(lambda v: any(x is not None for x in v)).to_numpy() | np.isfinite(sym)
        for i, q in enumerate(QUADS):
            vals = quads.apply(lambda v, i=i: np.nan if v[i] is None else float(v[i])).to_numpy()
            vals = np.where(np.isfinite(vals), vals, sym)
            vals = np.where(np.isfinite(vals), vals, np.where(given_any, 0.0, reg))
            df[_QUAD_FMT.format(speed=k, quad=q)] = np.nan_to_num(vals, nan=0.0)
    return df


def _load_fms_points(engine, disturbance_id: str, issued_time=None, analysis_upto=None) -> gpd.GeoDataFrame:
    """Official track points of one storm: one issuance (``issued_time``), or
    the analysis points of every issuance up to ``analysis_upto``."""
    if issued_time is not None:
        cond = "issued_time = :t AND leadtime <= :maxlt"
        params = {"t": issued_time, "maxlt": MAX_LEADTIME_H}
    else:
        cond = "leadtime = 0 AND issued_time <= :t"
        params = {"t": analysis_upto}
    sql = f"""
        SELECT disturbance_id, issued_time, valid_time, leadtime, wind_speed,
               uncertainty, radius_34, radius_50, radius_64,
               quadrant_radius_34, quadrant_radius_50, quadrant_radius_64,
               geometry
        FROM storms.fms_tracks_geo
        WHERE track_type = :tt AND disturbance_id = :d AND {cond}
        ORDER BY issued_time, valid_time
    """
    with engine.connect() as conn:
        gdf = gpd.read_postgis(
            text(sql), conn, geom_col="geometry",
            params={"tt": OFFICIAL_TRACK, "d": disturbance_id, **params},
        )
    if gdf.empty:
        return gdf
    if issued_time is None:
        # one analysis point per time: a reissued forecast replaces it
        gdf = gdf.sort_values("issued_time").drop_duplicates("valid_time", keep="last")
    return fms_point_radii(gdf)


def shift_points(points: gpd.GeoDataFrame, bearing_deg: float) -> gpd.GeoDataFrame:
    """Move every point by its FMS position uncertainty (nm) towards a bearing."""
    out = points.copy()
    dist = np.nan_to_num(points["uncertainty"].to_numpy(float), nan=0.0) * NM_TO_M
    lon, lat, _ = _GEOD.fwd(
        points.geometry.x.to_numpy(), points.geometry.y.to_numpy(),
        np.full(len(points), float(bearing_deg)), dist,
    )
    lon = (np.asarray(lon) + 180.0) % 360.0 - 180.0
    out["geometry"] = gpd.points_from_xy(lon, lat)
    return out.set_crs("EPSG:4326", allow_override=True)


def _geoms_by_speed(gdf: gpd.GeoDataFrame) -> dict:
    return dict(zip(gdf["wind_speed_kt"], gdf.geometry))


def process_fms_issuance(
    engine, ctx: _ExposureContext, disturbance_id: str, issued_time, scenarios: bool = True
) -> None:
    """All buffers and exposure for one FMS Official forecast."""
    key_it = {"disturbance_id": disturbance_id, "issued_time": issued_time}
    key_vt = {"disturbance_id": disturbance_id, "valid_time": issued_time}
    fc_pts = _load_fms_points(engine, disturbance_id, issued_time=issued_time)
    if fc_pts.empty:
        logger.warning(f"No Official track points for {disturbance_id} {issued_time}")
        return
    an_pts = _load_fms_points(engine, disturbance_id, analysis_upto=issued_time)

    fcast = wind_buffers(fc_pts)
    obsv = wind_buffers(an_pts) if not an_pts.empty else None
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

    _replace_rows(engine, "fms_tracks_fcast_buffers", _frame(fcast, key_it), key_it)
    if obsv is not None:
        _replace_rows(engine, "fms_tracks_obsv_buffers", _frame(obsv, key_vt), key_vt)
    _replace_rows(engine, "fms_tracks_fcastonly_buffers", _frame(fco, key_it), key_it)
    _mark_done(engine, disturbance_id, issued_time, "buffers")

    for table, gdf, key in (
        ("fms_tracks_fcast_exposure", fcast, key_it),
        ("fms_tracks_obsv_exposure", obsv, key_vt),
        ("fms_tracks_fcastonly_exposure", fco, key_it),
    ):
        if gdf is None:
            continue
        e = compute_exposure(_frame(gdf, key), ctx)
        _replace_rows(engine, table, e, key)
    _mark_done(engine, disturbance_id, issued_time, "exposure")

    if not scenarios:
        return
    exps, envelope = [], {k: [] for k in SPEEDS}
    bearings = list(range(0, 360, SCENARIO_STEP_DEG))
    for brg in bearings:
        sb = wind_buffers(shift_points(fc_pts, brg))
        sg = _geoms_by_speed(sb)
        sco = gpd.GeoDataFrame(
            {"wind_speed_kt": SPEEDS},
            geometry=[_fcastonly(sg.get(k), obsv_g.get(k)) for k in SPEEDS],
            crs="EPSG:4326",
        )
        for k, g in zip(sco.wind_speed_kt, sco.geometry):
            if g is not None and not g.is_empty:
                envelope[k].append(g)
        sco["shift_deg"] = brg
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
    _replace_rows(engine, "fms_tracks_scenario_envelope", env, key_it)
    exp_all = pd.concat(exps, ignore_index=True) if exps else None
    _replace_rows(engine, "fms_tracks_scenario_exposure", exp_all, key_it)
    _mark_done(engine, disturbance_id, issued_time, "scenarios")


def _pending_issuances(
    engine, disturbance_id=None, issued_time=None, since=None, overwrite=False,
    last_stage="scenarios",
) -> pd.DataFrame:
    filters = ["f.track_type = :tt"]
    params = {"tt": OFFICIAL_TRACK}
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
            # A forecast re-ingested after it was processed (a corrected CSV
            # replaces its points) is processed again.
            "NOT EXISTS (SELECT 1 FROM storms.fms_processing_completion c "
            "WHERE c.disturbance_id = f.disturbance_id "
            "AND c.issued_time = f.issued_time AND c.stage = :last_stage "
            "AND c.completed_at >= f.ingested_at)"
        )
        params["last_stage"] = last_stage
    sql = f"""
        SELECT f.disturbance_id, f.issued_time FROM storms.fms_forecasts f
        WHERE {' AND '.join(filters)} ORDER BY f.issued_time, f.disturbance_id
    """
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def run_fms_realtime(
    mode: str = "dev",
    disturbance_id: str | None = None,
    issued_time=None,
    since=None,
    overwrite: bool = False,
    scenarios: bool = True,
) -> list[tuple]:
    """Process every FMS Official forecast not yet fully processed (or the one
    asked for). Returns the (disturbance_id, issued_time) pairs processed, in
    issue order, so the observed swath of each builds on the ones before."""
    engine = stratus.get_engine(stage=mode, write=True)
    todo = _pending_issuances(
        engine, disturbance_id, issued_time, since, overwrite,
        last_stage="scenarios" if scenarios else "exposure",
    )
    if todo.empty:
        logger.info("No FMS forecasts to process.")
        return []
    logger.info(f"{len(todo)} FMS forecast(s) to process")
    ctx = _ExposureContext(stage=mode)
    done = []
    for row in todo.itertuples():
        logger.info(f"FMS {row.disturbance_id} {row.issued_time}")
        process_fms_issuance(engine, ctx, row.disturbance_id, row.issued_time, scenarios=scenarios)
        done.append((row.disturbance_id, row.issued_time))
    return done


# ---------------------------------------------------------------------------
# Historical: IBTrACS
# ---------------------------------------------------------------------------


def load_ibtracs_sp(nc_path: str | None = None, first_season: int = HIST_FIRST_SEASON) -> pd.DataFrame:
    """IBTrACS South Pacific subset as one row per storm and time (spur tracks dropped)."""
    import xarray as xr

    tmp = None
    if nc_path is None:
        from ocha_lens.datasources.ibtracs import download_ibtracs

        tmp = tempfile.TemporaryDirectory(prefix="ibtracs_sp_")
        nc_path = str(download_ibtracs(dataset="SP", save_dir=tmp.name))
    try:
        ds = xr.open_dataset(nc_path).load()
    finally:
        if tmp is not None:
            tmp.cleanup()
    n_storm, n_time = ds.sizes["storm"], ds.sizes["date_time"]

    def _s(v):
        if v.dtype.kind == "S":
            return np.char.decode(v, "utf-8")
        if v.dtype.kind == "O":
            return np.vectorize(lambda x: x.decode() if isinstance(x, bytes) else str(x))(v)
        return v.astype(str)

    per_storm = pd.DataFrame({
        "sid": _s(ds.sid.values), "name": _s(ds.name.values),
        "season": ds.season.values, "track_type": _s(ds.track_type.values),
    })
    df = pd.DataFrame({
        "sid": np.repeat(per_storm.sid.values, n_time),
        "valid_time": pd.to_datetime(_s(ds.iso_time.values).ravel(), errors="coerce"),
        "lat": ds.lat.values.ravel(), "lon": ds.lon.values.ravel(),
        "usa_lat": ds.usa_lat.values.ravel(), "usa_lon": ds.usa_lon.values.ravel(),
        "usa_wind": ds.usa_wind.values.ravel(), "wmo_wind": ds.wmo_wind.values.ravel(),
        "wmo_agency": _s(ds.wmo_agency.values).ravel(),
    })
    for k in SPEEDS:
        arr = ds[f"usa_r{k}"].values.reshape(n_storm * n_time, 4)
        for i, q in enumerate(QUADS):
            df[f"usa_r{k}_{q}"] = arr[:, i]
    df = df.merge(per_storm, on="sid")
    # main + PROVISIONAL / US-PROVISIONAL (recent seasons); drop the spur-* fragments
    spur = df.track_type.str.lower().str.startswith("spur")
    df = df[(df.season >= first_season) & df.valid_time.notna() & ~spur]
    return df.reset_index(drop=True)


def official_10min_wind(df: pd.DataFrame) -> np.ndarray:
    wmo = df["wmo_wind"].to_numpy(float)
    ok = np.isfinite(wmo) & df["wmo_agency"].str.lower().isin(OFFICIAL_AGENCIES).to_numpy()
    return np.where(ok, wmo, JTWC_TO_10MIN * df["usa_wind"].to_numpy(float))


def hist_track_points(st: pd.DataFrame):
    """(GeoDataFrame with q_ radii, radii_source) for one storm, or (None, None)."""
    st = st.sort_values("valid_time")
    off = official_10min_wind(st)
    r34 = st[[f"usa_r34_{q}" for q in QUADS]].to_numpy(float)
    if np.isfinite(r34).any():
        source = "jtwc"
        has = np.isfinite(r34).any(axis=1) & np.isfinite(st["usa_lat"].to_numpy(float))
        pts = st[has].copy()
        offp = off[has]
        lon, lat = pts["usa_lon"].to_numpy(float), pts["usa_lat"].to_numpy(float)
        for k in SPEEDS:
            for q in QUADS:
                v = np.nan_to_num(pts[f"usa_r{k}_{q}"].to_numpy(float), nan=0.0)
                # no gate where the official intensity is unknown
                v = np.where(np.isfinite(offp) & (offp < k), 0.0, v)
                pts[_QUAD_FMT.format(speed=k, quad=q)] = v
    else:
        if not (np.nanmax(np.where(np.isfinite(off), off, -1)) >= SPEEDS[0]):
            return None, None
        source = "fji_regression"
        has = np.isfinite(off)
        pts = st[has].copy()
        lon = np.where(np.isfinite(pts["usa_lon"]), pts["usa_lon"], pts["lon"]).astype(float)
        lat = np.where(np.isfinite(pts["usa_lat"]), pts["usa_lat"], pts["lat"]).astype(float)
        for k in SPEEDS:
            r = regression_radius(k, off[has], lat)
            for q in QUADS:
                pts[_QUAD_FMT.format(speed=k, quad=q)] = r
    if pts.empty:
        return None, None
    lon = (lon + 180.0) % 360.0 - 180.0
    gdf = gpd.GeoDataFrame(pts, geometry=gpd.points_from_xy(lon, lat), crs="EPSG:4326")
    return gdf, source


def compute_history_buffers(df: pd.DataFrame) -> gpd.GeoDataFrame:
    rows = []
    for sid, st in df.groupby("sid"):
        pts, source = hist_track_points(st)
        if pts is None:
            continue
        b = wind_buffers(pts)
        b["sid"] = sid
        b["radii_source"] = source
        rows.append(b)
    if not rows:
        return gpd.GeoDataFrame(columns=["sid", "wind_speed_kt", "radii_source", "geometry"], crs="EPSG:4326")
    return gpd.GeoDataFrame(pd.concat(rows, ignore_index=True), crs="EPSG:4326")[
        ["sid", "wind_speed_kt", "radii_source", "geometry"]
    ]


def run_sp_history(
    mode: str = "dev",
    first_season: int = HIST_FIRST_SEASON,
    sids: list[str] | None = None,
    overwrite: bool = False,
    nc_path: str | None = None,
    out_dir: str | None = None,
    batch_size: int = 25,
) -> None:
    """Historical South Pacific buffers + exposure.

    ``out_dir``: write parquet files there instead of the database (read-only
    dry run for checks).
    """
    df = load_ibtracs_sp(nc_path, first_season)
    if sids:
        df = df[df.sid.isin(sids)]
    engine = None if out_dir else stratus.get_engine(stage=mode, write=True)
    if engine is not None and overwrite and not sids:
        # NCEI replaces provisional records (new sid, or a merged storm), so a
        # full refresh also drops storms that are no longer in IBTrACS;
        # otherwise a storm could be counted twice in the return periods.
        current = sorted(df.sid.unique())
        for table in ("ibtracs_sp_wind_exposure", "ibtracs_sp_wind_buffers"):
            with engine.begin() as conn:
                n = conn.execute(
                    text(f"DELETE FROM storms.{table} WHERE NOT (sid = ANY(:sids))"),
                    {"sids": current},
                ).rowcount
            if n:
                logger.info(f"{table}: removed {n} rows of storms no longer in IBTrACS")
    if engine is not None and not overwrite:
        with engine.connect() as conn:
            done = {r[0] for r in conn.execute(text("SELECT DISTINCT sid FROM storms.ibtracs_sp_wind_buffers"))}
        df = df[~df.sid.isin(done)]
    all_sids = sorted(df.sid.unique())
    logger.info(f"{len(all_sids)} South Pacific storms to process (season >= {first_season})")
    ctx = _ExposureContext(stage=mode if engine is not None else "dev")
    buf_parts, exp_parts = [], []
    for i in range(0, len(all_sids), batch_size):
        chunk = all_sids[i:i + batch_size]
        buf = compute_history_buffers(df[df.sid.isin(chunk)])
        exp = compute_exposure(buf.drop(columns="radii_source"), ctx)
        if engine is not None:
            for sid in chunk:
                key = {"sid": sid}
                _replace_rows(engine, "ibtracs_sp_wind_buffers", buf[buf.sid == sid], key)
                _replace_rows(engine, "ibtracs_sp_wind_exposure",
                              exp[exp.sid == sid] if not exp.empty else None, key)
        else:
            buf_parts.append(buf)
            exp_parts.append(exp)
        logger.info(f"history: {min(i + batch_size, len(all_sids))}/{len(all_sids)} storms")
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
        gpd.GeoDataFrame(pd.concat(buf_parts, ignore_index=True), crs="EPSG:4326").to_parquet(
            os.path.join(out_dir, "ibtracs_sp_wind_buffers.parquet"))
        pd.concat([e for e in exp_parts if not e.empty], ignore_index=True).to_parquet(
            os.path.join(out_dir, "ibtracs_sp_wind_exposure.parquet"))


def run_sp_admin_population(mode: str = "dev", out_dir: str | None = None) -> pd.DataFrame:
    ctx = _ExposureContext(stage=mode)
    pop = compute_unit_population(ctx)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
        pop.to_parquet(os.path.join(out_dir, "sp_admin_population.parquet"))
    else:
        engine = stratus.get_engine(stage=mode, write=True)
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM storms.sp_admin_population WHERE iso3 IN :isos").bindparams(
                    bindparam("isos", expanding=True)),
                {"isos": sorted(pop.iso3.unique())},
            )
            pop.to_sql("sp_admin_population", conn, schema="storms", if_exists="append", index=False)
    return pop
