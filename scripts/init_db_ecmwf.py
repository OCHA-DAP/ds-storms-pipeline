import argparse
from pathlib import Path

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

PROJECT_ROOT = Path(__file__).parent.parent


def run_sql_file(conn, sql_path: Path, stage: str):
    """Read and execute a SQL file, substituting any template variables."""
    sql = sql_path.read_text()
    sql = sql.format(**CONFIG[stage])

    print(f"Executing {sql_path.name}...")
    conn.execute(text(sql))


def main():
    parser = argparse.ArgumentParser(
        description="Initialize storm database tables"
    )
    parser.add_argument(
        "--stage",
        choices=["dev", "prod"],
        required=True,
        help="Deployment stage (dev or prod)",
    )
    parser.add_argument(
        "--sql-dir",
        type=Path,
        default=PROJECT_ROOT / "src" / "schemas" / "sql",
        help="Directory containing SQL files",
    )
    args = parser.parse_args()

    engine = stratus.get_engine(args.stage, write=True)

    with engine.connect() as conn:
        for sql_file in SQL_FILES:
            sql_path = args.sql_dir / sql_file
            if not sql_path.exists():
                raise FileNotFoundError(f"SQL file not found: {sql_path}")
            run_sql_file(conn, sql_path, args.stage)

        conn.commit()
        print(f"Successfully initialized tables for stage: {args.stage}")


if __name__ == "__main__":
    main()
