"""Initialize the NHC realtime-chain tables (idempotent).

Applies every ``nhc_*.sql`` schema file in ``src/schemas/sql`` (all
``CREATE TABLE IF NOT EXISTS`` + ``CREATE INDEX IF NOT EXISTS``) and the
``storms.exposure_completion`` marker table that the exposure stages
otherwise create lazily on first write. Safe to re-run: existing tables
are left untouched (a legacy table with a differently-named unique
constraint is NOT fixed here — see ``scripts/migrations/``).

Sibling of ``init_db_gdacs_adam.py`` / ``init_db_ecmwf.py``; those never
covered the NHC files, which were only ever applied by hand.

Usage
-----
    python scripts/init_db_nhc.py --mode prod
"""

import argparse
import sys
from pathlib import Path

import ocha_stratus as stratus
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()

CONFIG = {
    "dev": {"owner": "dbwriter"},
    "prod": {"owner": "dbwriter"},
}

# Order matters only for the FK nhc_tracks_geo -> nhc_storms (nhc_tables.sql
# holds both) — everything else is independent.
SQL_FILES = [
    "nhc_tables.sql",
    "nhc_wsp_polygon_raw.sql",
    "nhc_wsp_polygon_matched.sql",
    "nhc_wsp_fcastonly_polygon.sql",
    "nhc_tracks_fcast_buffers.sql",
    "nhc_tracks_obsv_buffers.sql",
    "nhc_tracks_fcastonly_buffers.sql",
    "nhc_tracks_fcast_exposure.sql",
    "nhc_tracks_obsv_exposure.sql",
    "nhc_tracks_fcastonly_exposure.sql",
    "nhc_wsp_exp.sql",
    "nhc_wsp_fcastonly_exposure.sql",
]

# Mirrors _mark_exposure_complete() in src/pipelines/nhc.py. Pre-creating it
# avoids the first-run race between the parallel tracks_exposure and
# wsp_exposure tasks both issuing CREATE TABLE IF NOT EXISTS.
EXPOSURE_COMPLETION_SQL = """
CREATE TABLE IF NOT EXISTS storms.exposure_completion (
    out_table    TEXT      NOT NULL,
    key_val      TIMESTAMP NOT NULL,
    admin_level  INTEGER   NOT NULL,
    completed_at TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (out_table, key_val, admin_level)
);
ALTER TABLE IF EXISTS storms.exposure_completion OWNER TO {owner};
"""


def get_default_sql_dir() -> Path:
    try:
        project_root = Path(__file__).parent.parent
    except NameError:
        project_root = Path(sys.argv[0]).resolve().parent.parent
    return project_root / "src" / "schemas" / "sql"


def run_sql(conn, sql: str, label: str, mode: str) -> None:
    # str.replace, not str.format: several schema files carry literal
    # braces inside COMMENT strings (e.g. "{name}_{basin}_{season}").
    sql = sql.replace("{owner}", CONFIG[mode]["owner"])
    print(f"Executing {label}...")
    conn.execute(text(sql))


def main():
    parser = argparse.ArgumentParser(description="Initialize NHC storm tables")
    parser.add_argument("--mode", choices=["dev", "prod"], required=True)
    parser.add_argument("--sql-dir", type=Path, default=get_default_sql_dir())
    args = parser.parse_args()

    engine = stratus.get_engine(args.mode, write=True)
    with engine.connect() as conn:
        for sql_file in SQL_FILES:
            path = args.sql_dir / sql_file
            if not path.exists():
                raise FileNotFoundError(f"SQL file not found: {path}")
            run_sql(conn, path.read_text(), sql_file, args.mode)
        run_sql(conn, EXPOSURE_COMPLETION_SQL, "exposure_completion", args.mode)
        conn.commit()
        print(f"Successfully initialized NHC tables for mode: {args.mode}")


if __name__ == "__main__":
    main()
