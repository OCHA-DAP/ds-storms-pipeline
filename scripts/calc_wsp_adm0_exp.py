"""
Calculate population exposure to NHC WSP polygons at ADM0 level.

Reads WSP polygons from storms.nhc_wsp_polygon (dev DB), matches each to an
ATCF storm ID using NHC track forecasts, then calculates how many people in
each country fall within each WSP polygon using the WorldPop 2026 1km raster.
Results are written to storms.nhc_wsp_adm0_exp.

Usage:
    python scripts/calc_wsp_adm0_exp.py                        # all countries, all storms
    python scripts/calc_wsp_adm0_exp.py --test                 # CUB, NA basin, since 2024
    python scripts/calc_wsp_adm0_exp.py --countries CUB HTI MEX
    python scripts/calc_wsp_adm0_exp.py --since 2023-01-01 --basin NA
    python scripts/calc_wsp_adm0_exp.py --overwrite            # recalculate everything
"""

import argparse
import warnings

import fsspec
import geopandas as gpd
import ocha_stratus as stratus
import pandas as pd
from ocha_lens.utils.storm import match_wsp_to_tracks
from rasterio.errors import ShapeSkipWarning
from rioxarray.exceptions import NoDataInBounds
from sqlalchemy import text

GEO_CRS_ANTIMERIDIAN = "+proj=longlat +datum=WGS84 +lon_wrap=180"
FIELDMAPS_URL = "https://data.fieldmaps.io/edge-matched/humanitarian/intl/adm1_polygons.parquet"
POP_BLOB = "worldpop/pop_count/global_pop_2026_CN_1km_R2025A_UA_v1.tif"
KEY_COLS = ["issued_time", "wind_threshold_kt", "percentage", "atcf_id", "adm0_pcode"]
TEST_COUNTRIES = ["CUB"]
TEST_SINCE = "2024-01-01"
TEST_BASIN = "NA"


def calculate_single_adm_exposure(gdf_buffers, da_wp):
    records = []
    for _, row in gdf_buffers.iterrows():
        row_data = row.drop(labels="geometry").to_dict()
        if not row.geometry or row.geometry.is_empty:
            pop_exposed = 0
        else:
            if row.geometry.bounds[0] < -160 or row.geometry.bounds[2] > 160:
                row_geometry_work = (
                    gpd.GeoSeries([row.geometry], crs=4326)
                    .to_crs(GEO_CRS_ANTIMERIDIAN)
                    .iloc[0]
                )
            else:
                row_geometry_work = row.geometry
            try:
                da_wp_clip = da_wp.rio.clip([row_geometry_work])
                pop_exposed = int(da_wp_clip.where(da_wp_clip > 0).sum())
            except NoDataInBounds:
                pop_exposed = 0
        row_data["pop_exposed"] = pop_exposed
        records.append(row_data)
    return pd.DataFrame(records)


def load_wsp(engine, since=None, basin=None):
    filters = []
    if since:
        filters.append(f"issued_time >= '{since}'")
    if basin:
        filters.append(f"basin = '{basin}'")

    track_where = ("WHERE " + " AND ".join(filters)) if filters else ""

    # WSP polygons filtered to issued_times that have matching tracks
    wsp_time_filter = (
        f"WHERE issued_time IN (SELECT DISTINCT issued_time FROM storms.nhc_tracks_geo {track_where})"
    )

    desc = []
    if since:
        desc.append(f"since {since}")
    if basin:
        desc.append(f"basin={basin}")
    label = f" ({', '.join(desc)})" if desc else ""
    print(f"Loading WSP polygons and NHC tracks from DB{label}...")

    with engine.connect() as con:
        gdf_wsp_raw = gpd.read_postgis(
            f"SELECT id, issued_time, wind_threshold_kt, percentage, geometry"
            f" FROM storms.nhc_wsp_polygon {wsp_time_filter}",
            con,
            geom_col="geometry",
        )
        gdf_tracks = gpd.read_postgis(
            f"SELECT atcf_id, issued_time, geometry FROM storms.nhc_tracks_geo"
            f" {track_where}"
            f" AND issued_time IN (SELECT DISTINCT issued_time FROM storms.nhc_wsp_polygon {wsp_time_filter})"
            if track_where else
            f"SELECT atcf_id, issued_time, geometry FROM storms.nhc_tracks_geo"
            f" WHERE issued_time IN (SELECT DISTINCT issued_time FROM storms.nhc_wsp_polygon)",
            con,
            geom_col="geometry",
        )
    print(f"  {len(gdf_wsp_raw)} WSP polygons, {len(gdf_tracks)} track points")
    print("Matching WSP polygons to ATCF storm IDs...")
    gdf_wsp = match_wsp_to_tracks(gdf_wsp_raw, gdf_tracks)
    n_matched = gdf_wsp["atcf_id"].notna().sum()
    print(f"  {n_matched}/{len(gdf_wsp)} polygons matched to an ATCF ID")
    return gdf_wsp


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
    """Load all existing (issued_time, wind_threshold_kt, percentage, atcf_id, adm0_pcode) from DB."""
    try:
        with engine.connect() as con:
            return pd.read_sql(
                text(
                    "SELECT issued_time, wind_threshold_kt, percentage, atcf_id, adm0_pcode"
                    " FROM storms.nhc_wsp_adm0_exp"
                ),
                con,
            )
    except Exception:
        return pd.DataFrame(columns=KEY_COLS)


def _filter_already_done(
    wsp: gpd.GeoDataFrame,
    done_country: pd.DataFrame,
) -> gpd.GeoDataFrame:
    """Return only WSP rows not yet calculated for this country."""
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


def run(countries=None, since=None, basin=None, overwrite=False):
    warnings.filterwarnings("ignore", category=ShapeSkipWarning)

    engine = stratus.get_engine(stage="dev", write=True)

    gdf_wsp = load_wsp(engine, since=since, basin=basin)
    if gdf_wsp.empty:
        print("No WSP polygons found for the given filters. Exiting.")
        return
    gdf_wsp_antimeridian = gdf_wsp.to_crs(GEO_CRS_ANTIMERIDIAN)

    gdf_adm1 = load_adm1(countries)
    country_list = sorted(gdf_adm1["iso_3"].unique())
    print(f"  {len(country_list)} countries to process")

    da_wp_global, da_wp_wrapped = load_pop()

    done_df = get_done(engine)
    if not done_df.empty:
        n_done_countries = done_df["adm0_pcode"].nunique()
        print(f"  {n_done_countries} countries with existing data in DB")

    total = len(country_list)
    skipped_all_done = 0
    processed = 0

    print(f"\nStarting exposure calculation for {total} countries...\n")

    for i, adm0_pcode in enumerate(country_list, 1):
        prefix = f"[{i}/{total}] {adm0_pcode}"

        adm_geom = (
            gdf_adm1[gdf_adm1["iso_3"] == adm0_pcode][["geometry"]]
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
            wsp = gdf_wsp_antimeridian
        else:
            da_wp = da_wp_global
            wsp = gdf_wsp

        intersects_mask = wsp.intersects(adm_geom)
        wsp_in_country = wsp[intersects_mask]
        wsp_no_overlap = wsp[~intersects_mask]

        if not overwrite and not done_df.empty:
            done_country = done_df[done_df["adm0_pcode"] == adm0_pcode]
            if not done_country.empty:
                wsp_in_country = _filter_already_done(wsp_in_country, done_country)
                wsp_no_overlap = _filter_already_done(wsp_no_overlap, done_country)
                if wsp_in_country.empty and wsp_no_overlap.empty:
                    skipped_all_done += 1
                    print(f"{prefix} — all done, skipping")
                    continue

        n_intersecting = len(wsp_in_country)
        n_zeros = len(wsp_no_overlap)
        print(f"{prefix} — {n_intersecting} intersecting, {n_zeros} zeros, calculating...", end="", flush=True)

        if not wsp_in_country.empty:
            da_wp_country = da_wp.rio.clip([adm_geom], all_touched=True)
            df = calculate_single_adm_exposure(wsp_in_country, da_wp_country)
            df["adm0_pcode"] = adm0_pcode
            del da_wp_country
        else:
            df = pd.DataFrame(columns=[c for c in KEY_COLS] + ["pop_exposed"])

        if not wsp_no_overlap.empty:
            df_zeros = wsp_no_overlap.drop(columns=["geometry", "id"], errors="ignore").copy()
            df_zeros["adm0_pcode"] = adm0_pcode
            df_zeros["pop_exposed"] = 0
            df = pd.concat([df, df_zeros], ignore_index=True)

        out = df.drop(columns=["id"], errors="ignore")
        out = out.drop_duplicates(subset=KEY_COLS, keep="last")
        with engine.connect() as con:
            out.to_sql(
                "nhc_wsp_adm0_exp",
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
    parser = argparse.ArgumentParser(description="Calculate WSP ADM0 population exposure")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--test", action="store_true",
                       help=f"Run for {', '.join(TEST_COUNTRIES)}, {TEST_BASIN} basin, since {TEST_SINCE}")
    group.add_argument("--countries", nargs="+", metavar="PCODE", help="Specific country p-codes to process")
    parser.add_argument("--since", metavar="YYYY-MM-DD", help="Only load WSP polygons issued on or after this date")
    parser.add_argument("--basin", metavar="BASIN", help="Only load WSP polygons for this basin (e.g. NA, EP)")
    parser.add_argument("--overwrite", action="store_true", help="Recalculate even if results already exist in DB")
    args = parser.parse_args()

    if args.test:
        run(countries=TEST_COUNTRIES, since=TEST_SINCE, basin=TEST_BASIN, overwrite=args.overwrite)
    else:
        countries = [c.upper() for c in args.countries] if args.countries else None
        run(countries=countries, since=args.since, basin=args.basin, overwrite=args.overwrite)
