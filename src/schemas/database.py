from .base import Base

from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError
from dotenv import load_dotenv

load_dotenv()


def init_db(engine) -> None:
    """Initialize the database with schema and tables."""
    try:
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS storms;"))
    except ProgrammingError:
        pass

    Base.metadata.create_all(engine)


def drop_all(engine) -> None:
    """Drop all tables and schema."""
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text("DROP SCHEMA IF EXISTS storms CASCADE;"))
