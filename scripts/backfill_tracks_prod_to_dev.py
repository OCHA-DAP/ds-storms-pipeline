"""One-off: copy nhc_storms + nhc_tracks_geo from PROD to DEV."""
import ocha_stratus as stratus
import pandas as pd
from sqlalchemy import text, MetaData
from sqlalchemy.dialects.postgresql import insert as pg_insert

prod_engine = stratus.get_engine("prod")
dev_engine = stratus.get_engine("dev", write=True)

# --- nhc_storms (small, all at once) ---
# stratus.postgres_upsert targets nhc_storms_unique (atcf_id, storm_id), but
# atcf_id is the PK — PK conflict fires first. Use explicit upsert on PK.
print("Copying nhc_storms...")
with prod_engine.connect() as conn:
    df = pd.read_sql(text("SELECT * FROM storms.nhc_storms"), conn)
print(f"  {len(df):,} storms")
meta = MetaData()
meta.reflect(dev_engine, schema="storms", only=["nhc_storms"])
nhc_storms_tbl = meta.tables["storms.nhc_storms"]
records = df.to_dict("records")
with dev_engine.connect() as conn:
    for i in range(0, len(records), 1000):
        chunk = records[i : i + 1000]
        stmt = pg_insert(nhc_storms_tbl).values(chunk)
        stmt = stmt.on_conflict_do_update(
            index_elements=["atcf_id"],
            set_={c: stmt.excluded[c] for c in df.columns if c != "atcf_id"},
        )
        conn.execute(stmt)
    conn.commit()
print("  Done.")

# --- nhc_tracks_geo (large, batched by atcf_id) ---
# No serial id column; paginate by atcf_id to avoid OFFSET scan cost
print("Copying nhc_tracks_geo...")
with prod_engine.connect() as conn:
    atcf_ids = pd.read_sql(
        text("SELECT DISTINCT atcf_id FROM storms.nhc_tracks_geo ORDER BY atcf_id"),
        conn,
    )["atcf_id"].tolist()
print(f"  {len(atcf_ids):,} storms to copy")

copied = 0
for i, atcf_id in enumerate(atcf_ids, 1):
    with prod_engine.connect() as conn:
        df = pd.read_sql(
            text("""
                SELECT atcf_id, provider, basin, issued_time, valid_time, leadtime,
                       wind_speed, pressure, max_wind_radius,
                       last_closed_isobar_radius, last_closed_isobar_pressure,
                       gust_speed, nature,
                       quadrant_radius_34, quadrant_radius_50, quadrant_radius_64,
                       number, storm_id, point_id,
                       ST_AsText(geometry) AS geometry
                FROM storms.nhc_tracks_geo
                WHERE atcf_id = :atcf_id
            """),
            conn,
            params={"atcf_id": atcf_id},
        )
    with dev_engine.connect() as conn:
        df.to_sql(
            "nhc_tracks_geo", conn, schema="storms", if_exists="append",
            index=False, method=stratus.postgres_upsert, chunksize=1000,
        )
        conn.commit()
    copied += len(df)
    if i % 50 == 0 or i == len(atcf_ids):
        print(f"  [{i}/{len(atcf_ids)}] {atcf_id} — {copied:,} rows total")

print("Done.")
