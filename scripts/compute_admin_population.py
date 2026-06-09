"""Compute total WorldPop population per FieldMaps admin unit (adm0 + adm1).

Populates ``storms.admin_population`` — one row per (admin_level, iso3,
pcode) with the area-weighted exactextract sum of the global WorldPop COG
inside that polygon. Used as the denominator for storm exposure tables so
downstream consumers can render "% of adm1 population exposed" without
recomputing the same sum from the COG.

Static. Re-run when WorldPop is bumped; existing rows are upserted (same
key tuple).

Usage:
    uv run python scripts/compute_admin_population.py --mode dev
    uv run python scripts/compute_admin_population.py --mode dev --admin-level 1
    uv run python scripts/compute_admin_population.py --mode dev --countries HTI JAM
"""

import argparse
import gc
import logging
import os
import re
import sys
import time
from pathlib import Path

# Make `src.*` importable when run as `uv run python scripts/...`.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import coloredlogs  # noqa: E402
import ocha_stratus as stratus  # noqa: E402
import pandas as pd  # noqa: E402
from sqlalchemy import text  # noqa: E402
from tqdm import tqdm  # noqa: E402

from src.utils.exposure import (  # noqa: E402
    _POP_BLOB,
    calculate_exposure,
    load_adm_units,
    load_pop,
)

logger = logging.getLogger(__name__)

SCHEMA_FILE = (
    Path(__file__).parent.parent / "src/schemas/sql/admin_population.sql"
)
TABLE = "admin_population"
SCHEMA = "storms"


def bootstrap_table(engine) -> None:
    """Idempotent: applies the canonical schema file. CREATE TABLE IF NOT
    EXISTS + CREATE INDEX IF NOT EXISTS means re-running is a no-op once
    the table is in place."""
    sql = SCHEMA_FILE.read_text().replace("{owner}", "CURRENT_USER")
    with engine.begin() as conn:
        # Strip SQL comments and split on bare semicolons. The schema file
        # has no semicolons inside string literals or DO blocks, so this is
        # safe; revisit if that ever changes.
        cleaned = re.sub(r"--[^\n]*", "", sql)
        for stmt in cleaned.split(";"):
            if stmt.strip():
                conn.execute(text(stmt))
    logger.info(f"Table {SCHEMA}.{TABLE} ready.")


def worldpop_year_from_blob() -> int:
    """e.g. 'worldpop/pop_count/global_pop_2026_CN_1km_R2025A_UA_v1.tif' → 2026."""
    m = re.search(r"pop_(\d{4})_", _POP_BLOB)
    if not m:
        raise ValueError(f"Could not parse year from _POP_BLOB={_POP_BLOB!r}")
    return int(m.group(1))


def load_done_iso3s(engine, admin_level: int) -> set[str]:
    """ISO3s already present in the table for this admin_level — used to
    skip-if-present when resuming a partial run."""
    with engine.connect() as conn:
        rows = conn.execute(text(
            f"SELECT DISTINCT iso3 FROM {SCHEMA}.{TABLE} WHERE admin_level = :al"
        ), {"al": admin_level}).fetchall()
    return {r[0] for r in rows}


def _compute_country_with_retry(
    iso3, units, da_wp, admin_level, year, max_attempts=4,
):
    """Compute one country's df, retrying on transient blob-read errors.

    The WorldPop COG sits behind an Azure SAS URL and occasionally drops
    a tile read mid-stream (``TIFFFillTile … got 0 bytes``). Backoff and
    retry — the next attempt usually succeeds.
    """
    from rasterio.errors import RasterioIOError
    from rioxarray.exceptions import NoDataInBounds

    country_geom = units.geometry.union_all()
    # TODO: dateline-wraparound countries whose polygons don't touch
    # ±180° exactly come out zero. KIR is the only confirmed case in
    # this run — its Gilbert (169–177°E), Phoenix (-175 to -170°W), and
    # Line (-160 to -150°W) groups all sit *away* from the dateline, so
    # union_all().bounds = (-174.5°, …, 176.8°, …) — a 351° "long-way-
    # around" bbox that rio.clip uses as its clipping window, missing
    # the actual islands. FJI/RUS work because FieldMaps split their
    # polygons at ±180° so total_bounds = (-180°, …, 180°, …) — full
    # globe in longitude, which rio.clip handles correctly. Same pattern
    # would hit any future country with disjoint parts on opposite sides
    # of the dateline but not touching ±180°. Fix: detect this case and
    # rio.clip per multi-part instead of once for the union.
    for attempt in range(1, max_attempts + 1):
        try:
            try:
                da_wp_country = da_wp.rio.clip(
                    [country_geom], all_touched=True
                )
            except NoDataInBounds:
                df = units[["iso3", "pcode"]].copy()
                df["total_pop"] = 0
                df["admin_level"] = admin_level
                df["worldpop_year"] = year
                return df[
                    ["admin_level", "iso3", "pcode", "total_pop", "worldpop_year"]
                ], "no_cover"
            df = calculate_exposure(
                units, da_wp_country, result_col="total_pop"
            )
            df["admin_level"] = admin_level
            df["worldpop_year"] = year
            del da_wp_country
            return df[
                ["admin_level", "iso3", "pcode", "total_pop", "worldpop_year"]
            ], "ok"
        except RasterioIOError as e:
            if attempt == max_attempts:
                logger.error(f"[{iso3}] giving up after {attempt} attempts: {e}")
                raise
            backoff = 2 ** attempt
            logger.warning(
                f"[{iso3}] transient COG read error "
                f"(attempt {attempt}/{max_attempts}): {e!s} — "
                f"sleeping {backoff}s"
            )
            time.sleep(backoff)


def compute(
    mode: str,
    admin_levels: list[int],
    countries: list[str] | None,
    resume: bool,
) -> None:
    import warnings
    from rasterio.errors import ShapeSkipWarning
    warnings.filterwarnings("ignore", category=ShapeSkipWarning)

    logger.info(f"Loading WorldPop COG ({_POP_BLOB})...")
    da_wp = load_pop()
    year = worldpop_year_from_blob()
    logger.info(f"WorldPop year: {year}")

    engine = stratus.get_engine(stage=mode, write=True)
    bootstrap_table(engine)

    try:
        for admin_level in admin_levels:
            logger.info(f"--- admin_level={admin_level} ---")
            gdf = load_adm_units(countries, admin_level, stage=mode)
            logger.info(
                f"  {len(gdf):,} units across "
                f"{gdf['iso3'].nunique()} countries."
            )

            done = load_done_iso3s(engine, admin_level) if resume else set()
            if done:
                logger.info(
                    f"  Resume mode: skipping {len(done)} iso3s already in DB."
                )

            no_cover: list[str] = []
            n_written = n_skipped = 0
            for iso3, units in tqdm(
                gdf.groupby("iso3"), desc=f"adm{admin_level}", unit="country"
            ):
                if iso3 in done:
                    n_skipped += 1
                    continue
                df, status = _compute_country_with_retry(
                    iso3, units, da_wp, admin_level, year,
                )
                if status == "no_cover":
                    no_cover.append(iso3)
                # Per-country upsert — resumable, bounded memory.
                with engine.connect() as conn:
                    df.to_sql(
                        TABLE, conn, schema=SCHEMA,
                        if_exists="append", index=False,
                        method=stratus.postgres_upsert,
                    )
                    conn.commit()
                n_written += 1
                gc.collect()

            if no_cover:
                logger.info(
                    f"  {len(no_cover)} country/ies with no WorldPop coverage "
                    f"(set to 0): {', '.join(sorted(no_cover))}"
                )
            logger.info(
                f"  admin_level={admin_level} done — "
                f"{n_written} written, {n_skipped} skipped."
            )
    finally:
        engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["dev", "prod"], default="dev")
    parser.add_argument(
        "--admin-level", type=int, choices=[0, 1], action="append",
        dest="admin_level",
        help=(
            "Admin level to compute (repeatable). Default: both 0 and 1."
        ),
    )
    parser.add_argument(
        "--countries", nargs="+", metavar="ISO3",
        help="Limit to specific ISO3 codes. Default: all countries in the mirror.",
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Skip iso3s already present in the table at the requested admin_level.",
    )
    args = parser.parse_args()
    coloredlogs.install(
        level="INFO",
        logger=logger,
        fmt="%(asctime)s %(levelname)s %(message)s",
    )
    admin_levels = args.admin_level or [0, 1]
    compute(args.mode, admin_levels, args.countries, resume=args.resume)
    logger.info("Done.")


if __name__ == "__main__":
    main()
