import argparse
from pathlib import Path
import sys

from sqlalchemy import text
import ocha_stratus as stratus
from dotenv import load_dotenv

load_dotenv()

CONFIG = {
    "dev": {"owner": "dbwriter"},
    "prod": {"owner": "dbwriter"},
}

SQL_FILES = [
    "ecmwf_storms.sql",
    "ecmwf_tracks_geo.sql",
]


def get_default_sql_dir():
    """Get default SQL directory, handling both local and Databricks environments."""
    try:
        project_root = Path(__file__).parent.parent
    except NameError:
        # Running in Databricks - derive from the script path
        script_path = Path(sys.argv[0]).resolve()
        project_root = script_path.parent.parent

    return project_root / "src" / "schemas" / "sql"


def run_sql_file(conn, sql_path: Path, mode: str):
    """Read and execute a SQL file, substituting any template variables."""
    sql = sql_path.read_text()
    sql = sql.format(**CONFIG[mode])

    print(f"Executing {sql_path.name}...")
    conn.execute(text(sql))


def main():
    parser = argparse.ArgumentParser(
        description="Initialize storm database tables"
    )
    parser.add_argument(
        "--mode",
        choices=["dev", "prod"],
        required=True,
        help="Deployment mode (dev or prod)",
    )
    parser.add_argument(
        "--sql-dir",
        type=Path,
        default=get_default_sql_dir(),
        help="Directory containing SQL files",
    )
    args = parser.parse_args()

    engine = stratus.get_engine(args.mode, write=True)

    with engine.connect() as conn:
        for sql_file in SQL_FILES:
            sql_path = args.sql_dir / sql_file
            if not sql_path.exists():
                raise FileNotFoundError(f"SQL file not found: {sql_path}")
            run_sql_file(conn, sql_path, args.mode)

        conn.commit()
        print(f"Successfully initialized tables for mode: {args.mode}")


if __name__ == "__main__":
    main()
