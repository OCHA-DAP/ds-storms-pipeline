"""Initialize GDACS / ADAM exposure tables.

Mirrors init_db_ecmwf.py — reads the .sql files in src/schemas/sql,
substitutes the {owner} template var, and creates the tables.

Usage
-----
    python scripts/init_db_gdacs_adam.py --mode dev
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

SQL_FILES = [
    "gdacs_exposure.sql",
    "adam_exposure.sql",
]


def get_default_sql_dir() -> Path:
    try:
        project_root = Path(__file__).parent.parent
    except NameError:
        project_root = Path(sys.argv[0]).resolve().parent.parent
    return project_root / "src" / "schemas" / "sql"


def run_sql_file(conn, sql_path: Path, mode: str) -> None:
    sql = sql_path.read_text().format(**CONFIG[mode])
    print(f"Executing {sql_path.name}...")
    conn.execute(text(sql))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--mode", choices=["dev", "prod"], required=True)
    parser.add_argument(
        "--sql-dir", type=Path, default=get_default_sql_dir()
    )
    args = parser.parse_args()

    engine = stratus.get_engine(args.mode, write=True)
    with engine.connect() as conn:
        for sql_file in SQL_FILES:
            path = args.sql_dir / sql_file
            if not path.exists():
                raise FileNotFoundError(f"SQL file not found: {path}")
            run_sql_file(conn, path, args.mode)
        conn.commit()
        print(f"Initialized GDACS/ADAM tables for mode: {args.mode}")


if __name__ == "__main__":
    main()
