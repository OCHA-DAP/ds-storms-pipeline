"""
One-off historical backfill for storms.nhc_wsp_polygon_raw.
Fetches one year at a time, converts geometries to WKT immediately to free
memory, then writes one issued_time at a time with a commit per issuance.
Not committed — run locally as needed.
"""
import gc
import logging
import warnings

import ocha_lens as lens
import ocha_stratus as stratus
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
)

YEARS = [2017, 2018, 2019, 2020, 2021, 2022, 2023, 2025]

engine = stratus.get_engine("dev", write=True)

for year in YEARS:
    logging.info(f"Loading {year}...")
    gdf = lens.nhc.get_wsp(start=f"{year}-01-01", end=f"{year}-12-31")
    if gdf is None or len(gdf) == 0:
        logging.info(f"{year}: no data")
        continue

    logging.info(f"{year}: {len(gdf)} rows, {gdf['issued_time'].nunique()} issuances — converting geometries")

    # Clip to world bounds (NHC raw shapefiles sometimes carry longitudes
    # wrapped past 180/-180), then convert to WKT to free shapely objects.
    from shapely.geometry import box as shapely_box
    _world = shapely_box(-180, -90, 180, 90)
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Geometry column does not contain geometry")
        gdf["geometry"] = gdf["geometry"].apply(
            lambda g: g.intersection(_world) if g is not None and not g.is_empty else g
        )
        gdf["geometry"] = gdf["geometry"].apply(lambda g: g.wkt if g is not None else None)

    gc.collect()

    issuances = sorted(gdf["issued_time"].unique())
    year_rows = 0

    for issued_time in tqdm(issuances, desc=str(year), unit="issuance", leave=True):
        df = gdf[gdf["issued_time"] == issued_time]
        with engine.connect() as conn:
            df.to_sql(
                name="nhc_wsp_polygon_raw",
                con=conn,
                schema="storms",
                if_exists="append",
                index=False,
                method=stratus.postgres_upsert,
                chunksize=500,
            )
            conn.commit()
        year_rows += len(df)

    del gdf
    gc.collect()
    logging.info(f"{year}: {year_rows} rows written")
