"""Population exposure on sea-extended admin units, for the regional
(non-NHC) storm pipelines.

The same method as the South Pacific pipeline (src/pipelines/sp.py, PR #62),
kept here so the South-West Indian Ocean pipeline (src/pipelines/swio.py)
does not depend on that PR; sp.py can switch to this module once both are
merged.

  * Admin units are FieldMaps units extended 500 m into the sea and made
    non-overlapping, so coastal and island populations on 1 km WorldPop
    cells are counted, and admin 1 still sums to admin 0.
  * Exposure is the WorldPop population inside a swath and a unit
    (exactextract, partial cells weighted by area: calculate_exposure).
"""

import logging
from functools import lru_cache

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from shapely.geometry import box
from shapely.validation import make_valid
from sqlalchemy import text

from src.utils.exposure import calculate_exposure, load_adm_units, load_pop

logger = logging.getLogger(__name__)

SEA_EXPANSION_M = 500
WORLDPOP_YEAR = 2026
_LONWRAP180 = "+proj=longlat +datum=WGS84 +lon_wrap=180"


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


def split_at_antimeridian(geom):
    """A polygon with continuous longitudes (0 to 360) back into [-180, 180]:
    parts beyond 180 are clipped off and shifted by 360."""
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
    is not given to the next, so the units still don't overlap. Buffering
    happens in a metric projection centred on the country.
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
    back["geometry"] = back.geometry.apply(split_at_antimeridian)
    return back.set_crs("EPSG:4326", allow_override=True)


@lru_cache(maxsize=None)
def _sea_units(stage: str, countries: tuple) -> dict:
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


def load_sea_units(countries, stage: str = "dev") -> dict:
    """{admin_level: GeoDataFrame[iso3, pcode, geometry]} on sea-extended units."""
    return _sea_units(stage, tuple(sorted(countries)))


class ExposureContext:
    """WorldPop + sea-extended units, with per-country rasters cached."""

    def __init__(self, countries, stage: str = "dev", admin_levels=(0, 1)):
        self.units = load_sea_units(countries, stage)
        self.admin_levels = list(admin_levels)
        self.da = load_pop()
        self._country = {}

    def country(self, iso3: str):
        """(country geometry, country raster or None if WorldPop has no people there)."""
        if iso3 not in self._country:
            cu = self.units[0]
            geom = cu.loc[cu.iso3 == iso3, "geometry"].iloc[0]
            da_c = self.da.rio.clip([geom], all_touched=True).load()
            if not np.nansum(np.where(da_c.values > 0, da_c.values, 0)):
                logger.warning(f"WorldPop has no population for {iso3}; exposure skipped")
                da_c = None
            self._country[iso3] = (geom, da_c)
        return self._country[iso3]


def compute_exposure(buffers: gpd.GeoDataFrame, ctx: ExposureContext, admin_levels=None) -> pd.DataFrame:
    """Population exposed per buffer row and admin unit.

    ``buffers``: key columns + ``wind_speed_kt`` + geometry (EPSG:4326, split
    at ±180°). Returns the key columns + wind_speed_kt + admin_level, iso3,
    pcode, pop_exposed, one row per unit a buffer touches.
    """
    keep = buffers.geometry.apply(lambda g: g is not None and not g.is_empty)
    b = buffers[keep.to_numpy(bool)].reset_index(drop=True)
    if b.empty:
        return pd.DataFrame()
    out = []
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


def compute_unit_population(ctx: ExposureContext) -> pd.DataFrame:
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


def replace_rows(engine, table: str, df: pd.DataFrame, key: dict) -> None:
    """Delete every row of ``table`` matching ``key`` and insert ``df``, in one
    transaction (an upsert alone would leave stale units behind)."""
    where = " AND ".join(f"{c} = :{c}" for c in key)
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM storms.{table} WHERE {where}"), key)
        if df is not None and not df.empty:
            df = pd.DataFrame(df)  # plain frame: geometry goes in as WKT
            if "geometry" in df.columns:
                df["geometry"] = [None if g is None or g.is_empty else g.wkt for g in df["geometry"]]
            df.to_sql(table, conn, schema="storms", if_exists="append", index=False)
