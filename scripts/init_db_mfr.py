"""Initialize the MFR (RSMC La Réunion) forecast-track tables (idempotent).

Applies ``src/schemas/sql/mfr_tables.sql`` (``CREATE TABLE IF NOT EXISTS``
+ ``CREATE INDEX IF NOT EXISTS``): storms.mfr_storms, storms.mfr_forecasts,
storms.mfr_tracks_geo, storms.mfr_files. Safe to re-run: existing tables are
left untouched.

Usage
-----
    python scripts/init_db_mfr.py --mode prod
    python scripts/init_db_mfr.py --mode dev --drop-first   # rebuild in dev
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

SQL_FILES = ["mfr_tables.sql"]

DROP_SQL = """
DROP TABLE IF EXISTS storms.mfr_tracks_geo, storms.mfr_forecasts,
    storms.mfr_storms, storms.mfr_files
"""


def get_default_sql_dir() -> Path:
    try:
        project_root = Path(__file__).parent.parent
    except NameError:
        project_root = Path(sys.argv[0]).resolve().parent.parent
    return project_root / "src" / "schemas" / "sql"


def run_sql(conn, sql: str, label: str, mode: str) -> None:
    # str.replace, not str.format: the schema file carries literal braces
    # inside COMMENT strings (e.g. "{name}_{basin}_{season}").
    sql = sql.replace("{owner}", CONFIG[mode]["owner"])
    print(f"Executing {label}...")
    conn.execute(text(sql))


def main():
    parser = argparse.ArgumentParser(
        description="Initialize MFR (La Réunion) storm tables"
    )
    parser.add_argument("--mode", choices=["dev", "prod"], required=True)
    parser.add_argument("--sql-dir", type=Path, default=get_default_sql_dir())
    parser.add_argument(
        "--drop-first",
        action="store_true",
        help="Drop the mfr_* tables first (dev only; to rebuild after a "
        "schema change)",
    )
    args = parser.parse_args()
    if args.drop_first and args.mode != "dev":
        parser.error("--drop-first is for dev only")

    engine = stratus.get_engine(args.mode, write=True)
    with engine.connect() as conn:
        if args.drop_first:
            print("Dropping the mfr_* tables (dev)...")
            conn.execute(text(DROP_SQL))
        for sql_file in SQL_FILES:
            path = args.sql_dir / sql_file
            if not path.exists():
                raise FileNotFoundError(f"SQL file not found: {path}")
            run_sql(conn, path.read_text(), sql_file, args.mode)
        conn.commit()
    print("Done.")


if __name__ == "__main__":
    main()
