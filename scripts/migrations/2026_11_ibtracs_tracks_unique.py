"""Dedupe storms.ibtracs_tracks_geo and re-key it on (sid, valid_time).

Why
---
The table's unique constraint was ``UNIQUE (sid, storm_id, valid_time)``.
``storm_id`` is NULL for unnamed storms, and Postgres treats NULLs as
distinct, so the loader's ``ON CONFLICT ON CONSTRAINT
ibtracs_tracks_geo_unique`` never fired for them: every IBTrACS run
appended another full copy of every unnamed storm. In prod on 2026-11-16
the table held 6,053,231 rows for 237,876 (sid, valid_time) keys; all
2,978 unnamed SIDs were affected (up to 124 copies each).

The same key also left stale rows when a storm's ``storm_id`` changed
between runs (named after first being loaded unnamed, or renamed), e.g.
Nolo / Fay 2026 kept their unnamed copies next to the named ones.

``sid`` already identifies the storm (``storm_id`` is derived from it via
storms.ibtracs_storms), so the new key is ``UNIQUE (sid, valid_time)``.
The constraint keeps its name, which is what ``stratus.postgres_upsert``
targets, so the loader needs no change.

What it does (one transaction)
------------------------------
1. Lock the table against writers (readers keep working until step 3).
2. Keep one row per (sid, valid_time) in a temp table, preferring a row
   whose ``storm_id`` matches storms.ibtracs_storms, then set every kept
   row's ``storm_id`` from storms.ibtracs_storms (it is derived from sid).
   Among the remaining copies the pick is arbitrary (point_id is a random
   uuid). Copies differ for 869 prod keys where IBTrACS revised the data;
   all 869 are still in the 2026-11-14 ALL file, so the IBTrACS ETL run
   straight after this migration overwrites them with current values.
3. Drop the old constraint, TRUNCATE, re-insert the kept rows, add
   ``UNIQUE (sid, valid_time)`` under the same name, and drop
   idx_ibtracs_tracks_geo_sid_valid, which the new constraint's index
   duplicates.
4. Check the row count equals the key count taken in step 1 and the SID
   count is unchanged; otherwise roll back.
5. Commit (or roll back with ``--dry-run``), then ANALYZE.

Writers are blocked for the whole run, readers from the DROP CONSTRAINT
on (needs ACCESS EXCLUSIVE). Check pg_stat_activity for sessions idle in
transaction on the table first: they hold the lock upgrade past the 60 s
lock_timeout and the run fails (safely) after the expensive sort.

There is no undo after commit short of a point-in-time restore. The
removed rows are copies of kept keys, re-derivable from IBTrACS.

TRUNCATE + re-insert rather than DELETE: ~96% of the prod heap is
duplicates, so DELETE would leave ~1.8 GB of dead tuples behind.

Not in scope: keys IBTrACS has since dropped stay in the table, as they
did before (prod, 2026-11-16: 3,548 keys over 162 sids not in the
2026-11-14 ALL file, mostly provisional storms re-issued under a new sid
and pre-2000 reanalysis). The ETL never touches them, so the table is
deduplicated, not a mirror of IBTrACS.

Usage
-----
    python scripts/migrations/2026_11_ibtracs_tracks_unique.py --mode dev --dry-run
    python scripts/migrations/2026_11_ibtracs_tracks_unique.py --mode dev

Prod runs as a one-off Databricks job (prod write credentials only exist
there): databricks/ibtracs_tracks_unique_prod_dryrun.json, then
databricks/ibtracs_tracks_unique_prod_execute.json.

Idempotent: if the constraint is already ``UNIQUE (sid, valid_time)`` it
reports the state and exits.
"""

import argparse
import logging

import ocha_stratus as stratus
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
# Databricks' py4j logs a line every second at INFO.
logging.getLogger("py4j").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

TABLE = "storms.ibtracs_tracks_geo"
CONSTRAINT = "ibtracs_tracks_geo_unique"
REDUNDANT_INDEX = "storms.idx_ibtracs_tracks_geo_sid_valid"
NEW_KEY = "UNIQUE (sid, valid_time)"


def constraint_def(conn) -> str | None:
    return conn.execute(
        text(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conrelid = CAST(:t AS regclass) AND conname = :c"
        ),
        {"t": TABLE, "c": CONSTRAINT},
    ).scalar()


def counts(conn) -> dict:
    row = conn.execute(
        text(
            f"SELECT COUNT(*) AS n_rows, "
            f"COUNT(DISTINCT (sid, valid_time)) AS n_keys, "
            f"COUNT(DISTINCT sid) AS n_sids FROM {TABLE}"
        )
    ).one()
    return dict(row._mapping)


def table_columns(conn) -> list[str]:
    rows = conn.execute(
        text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'storms' "
            "AND table_name = 'ibtracs_tracks_geo' ORDER BY ordinal_position"
        )
    )
    return [r[0] for r in rows]


def migrate(engine, dry_run: bool) -> None:
    with engine.connect() as conn:
        current = constraint_def(conn)
        logger.info(f"Current {CONSTRAINT}: {current}")
        if current == NEW_KEY:
            logger.info(f"Already migrated: {counts(conn)}")
            return

        # Fail fast instead of queueing behind a running IBTrACS load.
        conn.execute(text("SET LOCAL lock_timeout = '60s'"))
        conn.execute(text(f"LOCK TABLE {TABLE} IN EXCLUSIVE MODE"))

        before = counts(conn)
        logger.info(f"Before: {before}")

        cols = ", ".join(f'"{c}"' for c in table_columns(conn))
        conn.execute(
            text(
                f"""
                CREATE TEMP TABLE ibtracs_tracks_keep ON COMMIT DROP AS
                SELECT DISTINCT ON (t.sid, t.valid_time) t.*
                FROM {TABLE} t
                LEFT JOIN storms.ibtracs_storms s ON s.sid = t.sid
                ORDER BY t.sid, t.valid_time,
                         (t.storm_id IS NOT DISTINCT FROM s.storm_id) DESC,
                         t.point_id
                """
            )
        )
        n_keep = conn.execute(
            text("SELECT COUNT(*) FROM ibtracs_tracks_keep")
        ).scalar()
        logger.info(f"Keeping {n_keep} rows")
        n_relabelled = conn.execute(
            text(
                "UPDATE ibtracs_tracks_keep k SET storm_id = s.storm_id "
                "FROM storms.ibtracs_storms s WHERE s.sid = k.sid "
                "AND k.storm_id IS DISTINCT FROM s.storm_id"
            )
        ).rowcount
        logger.info(
            f"Set storm_id from ibtracs_storms on {n_relabelled} kept rows"
        )

        conn.execute(text(f"ALTER TABLE {TABLE} DROP CONSTRAINT {CONSTRAINT}"))
        conn.execute(text(f"TRUNCATE {TABLE}"))
        conn.execute(
            text(
                f"INSERT INTO {TABLE} ({cols}) "
                f"SELECT {cols} FROM ibtracs_tracks_keep"
            )
        )
        conn.execute(
            text(f"ALTER TABLE {TABLE} ADD CONSTRAINT {CONSTRAINT} {NEW_KEY}")
        )
        conn.execute(text(f"DROP INDEX IF EXISTS {REDUNDANT_INDEX}"))

        after = counts(conn)
        stale = conn.execute(
            text(
                f"SELECT COUNT(*) FROM {TABLE} t "
                "JOIN storms.ibtracs_storms s ON s.sid = t.sid "
                "WHERE t.storm_id IS DISTINCT FROM s.storm_id"
            )
        ).scalar()
        logger.info(f"After: {after}")
        logger.info(
            f"Rows whose storm_id differs from ibtracs_storms: {stale}"
        )

        if (
            after["n_rows"] != before["n_keys"]
            or after["n_keys"] != before["n_keys"]
            or after["n_sids"] != before["n_sids"]
            or stale != 0
        ):
            conn.rollback()
            raise RuntimeError(
                f"Count check failed, rolled back: before={before} "
                f"after={after}"
            )

        if dry_run:
            conn.rollback()
            logger.info("Dry run: rolled back, nothing changed.")
            return

        conn.commit()
        logger.info(
            f"Committed: removed {before['n_rows'] - after['n_rows']} rows; "
            f"{CONSTRAINT} is now {constraint_def(conn)}"
        )

    # Fresh statistics for the rebuilt table, after the commit.
    with engine.connect().execution_options(
        isolation_level="AUTOCOMMIT"
    ) as conn:
        conn.execute(text(f"ANALYZE {TABLE}"))
    logger.info("ANALYZE done.")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--mode", choices=["dev", "prod"], required=True)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run every step, then roll back.",
    )
    args = parser.parse_args()

    engine = stratus.get_engine(stage=args.mode, write=True)
    try:
        migrate(engine, dry_run=args.dry_run)
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
