"""
Calculate population exposure to NHC forecast wind buffer polygons at ADM0 level.

Reads wind buffer polygons from storms.nhc_wind_buffers (dev DB) and
calculates how many people in each country fall within each buffer using
the WorldPop 2026 1km raster. Results are written to storms.nhc_wind_exposure
(admin_level=0 rows).

Usage:
    python scripts/calc_nhc_wind_adm0_exp.py                       # all countries, all storms
    python scripts/calc_nhc_wind_adm0_exp.py --test                 # CUB, NA basin, since 2024
    python scripts/calc_nhc_wind_adm0_exp.py --countries CUB HTI MEX
    python scripts/calc_nhc_wind_adm0_exp.py --since 2023-01-01 --basin NA
    python scripts/calc_nhc_wind_adm0_exp.py --overwrite            # recalculate everything
"""

import argparse
import sys
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import fsspec
import geopandas as gpd
import ocha_stratus as stratus
import pandas as pd
from rasterio.errors import ShapeSkipWarning
from sqlalchemy import text

from src.utils.exposure import GEO_CRS_ANTIMERIDIAN, calculate_exposure

FIELDMAPS_URL = "https://data.fieldmaps.io/edge-matched/humanitarian/intl/adm1_polygons.parquet"
POP_BLOB = "worldpop/pop_count/global_pop_2026_CN_1km_R2025A_UA_v1.tif"
KEY_COLS = ["atcf_id", "issued_time", "wind_speed_kt", "admin_level", "pcode"]
ADMIN_LEVEL = 0
TEST_COUNTRIES = ["CUB"]
TEST_SINCE = "2024-01-01"
TEST_BASIN = "NA"


def load_buffers(engine, since: str | None = None, basin: str | None = None) -> gpd.GeoDataFrame:
    filters = []
    if since:
        filters.append(f"b.issued_time >= '{since}'")
    if basin:
        filters.append(f"s.genesis_basin = '{basin}'")

    desc = []
    if since:
        desc.append(f"since {since}")
    if basin:
        desc.append(f"basin={basin}")
    label = f" ({', '.join(desc)})" if desc else ""
    print(f"Loading NHC wind buffers from DB{label}...")

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

    with engine.connect() as con:
        gdf = gpd.read_postgis(query, con, geom_col="geometry")
    print(f"  {len(gdf)} wind buffer polygons")
    return gdf


def load_adm1(countries: list[str] | None) -> gpd.GeoDataFrame:
    print("Loading FieldMaps ADM1 boundaries (streaming)...")
    filters = [("iso_3", "in", countries)] if countries else None
    with fsspec.open(FIELDMAPS_URL, "rb") as f:
        gdf = gpd.read_parquet(f, columns=["iso_3", "geometry"], filters=filters)
    return gdf


def load_pop():
    print("Opening WorldPop raster (COG)...")
    da = stratus.open_blob_cog(POP_BLOB, container_name="raster").squeeze(drop=True)
    da_wrapped = da.assign_coords({"x": ((da.x + 360) % 360)}).sortby("x")
    return da, da_wrapped


def get_done(engine) -> pd.DataFrame:
    try:
        with engine.connect() as con:
            return pd.read_sql(
                text(
                    "SELECT atcf_id, issued_time, wind_speed_kt, admin_level, pcode"
                    " FROM storms.nhc_wind_exposure"
                    f" WHERE admin_level = {ADMIN_LEVEL}"
                ),
                con,
            )
    except Exception:
        return pd.DataFrame(columns=KEY_COLS)


def _filter_already_done(
    buffers: gpd.GeoDataFrame,
    done_country: pd.DataFrame,
) -> gpd.GeoDataFrame:
    merge_cols = ["atcf_id", "issued_time", "wind_speed_kt"]
    merged = buffers[merge_cols].merge(
        done_country[merge_cols].drop_duplicates().assign(_done=True),
        on=merge_cols,
        how="left",
    )
    return buffers[merged["_done"].isna().values].reset_index(drop=True)


def run(countries=None, since=None, basin=None, overwrite=False):
    warnings.filterwarnings("ignore", category=ShapeSkipWarning)

    engine = stratus.get_engine(stage="dev", write=True)

    gdf_buffers = load_buffers(engine, since=since, basin=basin)
    if gdf_buffers.empty:
        print("No wind buffers found for the given filters. Exiting.")
        return
    gdf_buffers_antimeridian = gdf_buffers.to_crs(GEO_CRS_ANTIMERIDIAN)

    gdf_adm1 = load_adm1(countries)
    country_list = sorted(gdf_adm1["iso_3"].unique())
    print(f"  {len(country_list)} countries to process")

    da_wp_global, da_wp_wrapped = load_pop()

    done_df = get_done(engine)
    if not done_df.empty:
        print(f"  {done_df['pcode'].nunique()} countries with existing data in DB")

    total = len(country_list)
    skipped_all_done = 0
    processed = 0

    print(f"\nStarting exposure calculation for {total} countries...\n")

    for i, iso3 in enumerate(country_list, 1):
        prefix = f"[{i}/{total}] {iso3}"
        pcode = iso3

        adm_geom = (
            gdf_adm1[gdf_adm1["iso_3"] == iso3][["geometry"]]
            .dissolve()
            .iloc[0]
            .geometry
        )

        minx, miny, maxx, maxy = adm_geom.bounds
        wrap = maxx > 160 or minx < -160

        if wrap:
            da_wp = da_wp_wrapped
            adm_geom = (
                gpd.GeoSeries([adm_geom], crs=4326)
                .to_crs(GEO_CRS_ANTIMERIDIAN)
                .iloc[0]
            )
            buffers = gdf_buffers_antimeridian
        else:
            da_wp = da_wp_global
            buffers = gdf_buffers

        intersects_mask = buffers.intersects(adm_geom)
        buf_in_country = buffers[intersects_mask]
        buf_no_overlap = buffers[~intersects_mask]

        if not overwrite and not done_df.empty:
            done_country = done_df[done_df["pcode"] == pcode]
            if not done_country.empty:
                buf_in_country = _filter_already_done(buf_in_country, done_country)
                buf_no_overlap = _filter_already_done(buf_no_overlap, done_country)
                if buf_in_country.empty and buf_no_overlap.empty:
                    skipped_all_done += 1
                    print(f"{prefix} — all done, skipping")
                    continue

        print(f"{prefix} — {len(buf_in_country)} intersecting, {len(buf_no_overlap)} zeros, calculating...", end="", flush=True)

        if not buf_in_country.empty:
            da_wp_country = da_wp.rio.clip([adm_geom], all_touched=True)
            df = calculate_exposure(buf_in_country, da_wp_country)
            df["iso3"] = iso3
            df["pcode"] = pcode
            df["admin_level"] = ADMIN_LEVEL
            del da_wp_country
        else:
            df = pd.DataFrame(columns=KEY_COLS + ["iso3", "pop_exposed"])

        if not buf_no_overlap.empty:
            df_zeros = buf_no_overlap.drop(columns=["geometry"], errors="ignore").copy()
            df_zeros["iso3"] = iso3
            df_zeros["pcode"] = pcode
            df_zeros["admin_level"] = ADMIN_LEVEL
            df_zeros["pop_exposed"] = 0
            df = pd.concat([df, df_zeros], ignore_index=True)

        out = df.drop_duplicates(subset=KEY_COLS, keep="last")
        with engine.connect() as con:
            out.to_sql(
                "nhc_wind_exposure",
                con,
                schema="storms",
                if_exists="append",
                index=False,
                method=stratus.postgres_upsert,
            )
            con.commit()

        n_exposed = int((df["pop_exposed"] > 0).sum())
        print(f" done ({n_exposed} with pop > 0)")
        processed += 1

    print(f"\nFinished. {processed} countries written, {skipped_all_done} skipped (already done).")
    engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate NHC wind buffer ADM0 population exposure")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--test", action="store_true",
                       help=f"Run for {', '.join(TEST_COUNTRIES)}, {TEST_BASIN} basin, since {TEST_SINCE}")
    group.add_argument("--countries", nargs="+", metavar="ISO3", help="Specific country ISO3 codes to process")
    parser.add_argument("--since", metavar="YYYY-MM-DD", help="Only include buffers with issued_time on or after this date")
    parser.add_argument("--basin", metavar="BASIN", help="Only include storms from this basin (e.g. NA, EP)")
    parser.add_argument("--overwrite", action="store_true", help="Recalculate even if results already exist in DB")
    args = parser.parse_args()

    if args.test:
        run(countries=TEST_COUNTRIES, since=TEST_SINCE, basin=TEST_BASIN, overwrite=args.overwrite)
    else:
        countries = [c.upper() for c in args.countries] if args.countries else None
        run(countries=countries, since=args.since, basin=args.basin, overwrite=args.overwrite)
