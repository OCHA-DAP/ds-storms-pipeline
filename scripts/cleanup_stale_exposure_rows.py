"""Delete stale exposure rows that the current corrected code wouldn't write.

An earlier buggy run (mostly antimeridian-related) wrote exposure rows for
(buffer_key, admin_level, pcode) tuples that the current code never produces.
The recent rerun with --overwrite upserted the valid rows but couldn't touch
the phantom ones, so they're still hanging around. This script identifies
and deletes them.

For each of the 3 exposure tables, the script:

  1. Loads the corresponding buffer table from PROD/DEV.
  2. Builds the "valid" set of (buffer_key, admin_level, pcode) tuples by
     redoing the spatial intersect-only check — same per-country / per-unit
     loop the exposure pipeline uses, minus exactextract. Much faster.
  3. Stages the valid set in a temp table.
  4. DELETEs exposure rows whose key tuple isn't in the valid set.

Usage:
    uv run python scripts/cleanup_stale_exposure_rows.py --mode dev --dry-run
    uv run python scripts/cleanup_stale_exposure_rows.py --mode dev
"""

import argparse
import logging
import os
import sys
from dataclasses import dataclass

# Make `src.*` importable when run as `uv run python scripts/...`.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import coloredlogs  # noqa: E402
import geopandas as gpd  # noqa: E402
import pandas as pd  # noqa: E402
from sqlalchemy import text  # noqa: E402
from tqdm import tqdm  # noqa: E402

from src.pipelines.nhc import _bbox_overlaps, build_exposure_session  # noqa: E402

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TablePair:
    """One exposure table + its corresponding buffer table."""
    name: str
    exposure_table: str
    buffer_table: str
    time_col: str  # "issued_time" or "valid_time"


PAIRS = [
    TablePair(
        name="fcast",
        exposure_table="storms.nhc_tracks_fcast_exposure",
        buffer_table="storms.nhc_tracks_fcast_buffers",
        time_col="issued_time",
    ),
    TablePair(
        name="obsv",
        exposure_table="storms.nhc_tracks_obsv_exposure",
        buffer_table="storms.nhc_tracks_obsv_buffers",
        time_col="valid_time",
    ),
    TablePair(
        name="fcastonly",
        exposure_table="storms.nhc_tracks_fcastonly_exposure",
        buffer_table="storms.nhc_tracks_fcastonly_buffers",
        time_col="issued_time",
    ),
]


def load_buffers(engine, table: str, time_col: str) -> gpd.GeoDataFrame:
    query = (
        f"SELECT atcf_id, {time_col}, wind_speed_kt, geometry "
        f"FROM {table} "
        f"WHERE geometry IS NOT NULL AND NOT ST_IsEmpty(geometry)"
    )
    with engine.connect() as conn:
        return gpd.read_postgis(query, conn, geom_col="geometry")


def collect_valid_keys(
    buffers_gdf: gpd.GeoDataFrame,
    time_col: str,
    session,
    admin_levels: list[int] | None = None,
) -> pd.DataFrame:
    """Return DataFrame of valid (atcf_id, time_col, wind_speed_kt,
    admin_level, pcode) tuples — same intersect logic as the exposure
    pipeline, minus exactextract.
    """
    buffers_sindex = buffers_gdf.sindex
    buffers_bbox = tuple(buffers_gdf.total_bounds)
    rows: list[tuple] = []

    levels = admin_levels if admin_levels is not None else session.admin_levels
    for admin_level in levels:
        country_groups = session.country_groups_by_level[admin_level]
        for iso3, country_units, cb in tqdm(
            country_groups, desc=f"adm{admin_level}", unit="country"
        ):
            if not _bbox_overlaps(cb, buffers_bbox):
                continue
            country_geom = country_units.geometry.union_all()
            cidx = list(buffers_sindex.intersection(country_geom.bounds))
            if not cidx:
                continue
            country_buffers = buffers_gdf.iloc[cidx]
            country_buffers = country_buffers[
                country_buffers.intersects(country_geom)
            ]
            if country_buffers.empty:
                continue
            unit_sindex = country_buffers.sindex if admin_level > 0 else None
            for _, unit in country_units.iterrows():
                pcode = unit["pcode"]
                if admin_level == 0:
                    hits = country_buffers
                else:
                    idx2 = list(unit_sindex.intersection(unit.geometry.bounds))
                    if not idx2:
                        continue
                    candidates = country_buffers.iloc[idx2]
                    hits = candidates[candidates.intersects(unit.geometry)]
                if hits.empty:
                    continue
                for atcf, t, kt in zip(
                    hits["atcf_id"], hits[time_col], hits["wind_speed_kt"],
                    strict=True,
                ):
                    rows.append((atcf, t, int(kt), int(admin_level), pcode))

    df = pd.DataFrame(
        rows, columns=["atcf_id", time_col, "wind_speed_kt", "admin_level", "pcode"]
    )
    df = df.drop_duplicates().reset_index(drop=True)
    return df


def cleanup_one(
    engine,
    pair: TablePair,
    session,
    dry_run: bool,
    admin_levels: list[int] | None = None,
) -> None:
    scope = (
        f"adm{'+'.join(str(a) for a in admin_levels)}"
        if admin_levels else "all levels"
    )
    logger.info(f"=== {pair.name} ({scope}): {pair.exposure_table} ===")

    logger.info(f"Loading buffers from {pair.buffer_table}...")
    buffers = load_buffers(engine, pair.buffer_table, pair.time_col)
    logger.info(f"  {len(buffers):,} buffers loaded.")
    if buffers.empty:
        logger.warning("No buffers found — refusing to delete everything.")
        return

    logger.info("Computing valid (buffer × admin_unit) intersections...")
    valid_df = collect_valid_keys(
        buffers, pair.time_col, session, admin_levels=admin_levels,
    )
    logger.info(f"  {len(valid_df):,} valid (key, admin_level, pcode) tuples.")

    # Stage valid keys in a temp table on the same connection used for
    # the DELETE. ON COMMIT DROP keeps the table out of subsequent steps.
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TEMP TABLE _valid_keys (
                atcf_id       VARCHAR,
                {pair.time_col} TIMESTAMP,
                wind_speed_kt SMALLINT,
                admin_level   SMALLINT,
                pcode         VARCHAR
            ) ON COMMIT DROP
        """))
        # Bulk-insert via pandas. Stays within the same connection (and
        # therefore the same transaction) as the DELETE, so the temp
        # table is visible to the DELETE.
        valid_df.to_sql(
            "_valid_keys",
            conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=10_000,
        )
        conn.execute(text(
            f"CREATE INDEX ON _valid_keys "
            f"(atcf_id, {pair.time_col}, wind_speed_kt, admin_level, pcode)"
        ))

        # Scope filter so we never touch admin_levels outside the
        # requested set (e.g. when only cleaning adm0, leave adm1 alone).
        if admin_levels:
            scope_clause = (
                "e.admin_level IN ("
                + ",".join(str(int(a)) for a in admin_levels)
                + ")"
            )
        else:
            scope_clause = "TRUE"

        # Count first so we can report (and abort on dry-run).
        count_q = text(f"""
            SELECT count(*) FROM {pair.exposure_table} e
            WHERE {scope_clause}
              AND NOT EXISTS (
                SELECT 1 FROM _valid_keys v
                WHERE v.atcf_id = e.atcf_id
                  AND v.{pair.time_col} = e.{pair.time_col}
                  AND v.wind_speed_kt = e.wind_speed_kt
                  AND v.admin_level = e.admin_level
                  AND v.pcode = e.pcode
              )
        """)
        n_stale = conn.execute(count_q).scalar()
        n_total = conn.execute(text(
            f"SELECT count(*) FROM {pair.exposure_table} e WHERE {scope_clause}"
        )).scalar()
        pct = (n_stale / n_total * 100) if n_total else 0.0
        logger.info(
            f"  Would delete {n_stale:,} / {n_total:,} in-scope rows ({pct:.2f}%)."
        )

        if dry_run:
            logger.info("  Dry run — rolling back.")
            conn.rollback()
            return

        if n_stale == 0:
            logger.info("  Nothing to delete.")
            return

        logger.info(f"  Deleting {n_stale:,} stale rows...")
        conn.execute(text(f"""
            DELETE FROM {pair.exposure_table} e
            WHERE {scope_clause}
              AND NOT EXISTS (
                SELECT 1 FROM _valid_keys v
                WHERE v.atcf_id = e.atcf_id
                  AND v.{pair.time_col} = e.{pair.time_col}
                  AND v.wind_speed_kt = e.wind_speed_kt
                  AND v.admin_level = e.admin_level
                  AND v.pcode = e.pcode
              )
        """))
        logger.info("  Deleted.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["dev", "prod"], default="dev")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Report counts but don't actually DELETE."
    )
    parser.add_argument(
        "--table", choices=[p.name for p in PAIRS], action="append",
        help="Restrict to one or more table pairs. Default: all 3.",
    )
    parser.add_argument(
        "--admin-level", type=int, choices=[0, 1], action="append",
        dest="admin_level",
        help=(
            "Restrict cleanup to one or more admin levels. Repeatable. "
            "Default: all levels present in the session."
        ),
    )
    args = parser.parse_args()
    coloredlogs.install(
        level="INFO",
        logger=logger,
        fmt="%(asctime)s %(levelname)s %(message)s",
    )

    pairs = (
        [p for p in PAIRS if p.name in args.table] if args.table else PAIRS
    )

    logger.info(f"Building exposure session (mode={args.mode})...")
    session = build_exposure_session(mode=args.mode)
    engine = session.engine

    try:
        for pair in pairs:
            cleanup_one(
                engine, pair, session,
                dry_run=args.dry_run,
                admin_levels=args.admin_level,
            )
    finally:
        engine.dispose()

    logger.info("Done.")


if __name__ == "__main__":
    main()
