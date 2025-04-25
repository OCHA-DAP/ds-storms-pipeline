from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    Index,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from .base import Base, handle_datetime_columns, handle_json_columns
import pandas as pd
import numpy as np
import json


class Storm(Base):
    __tablename__ = "storms"

    id = Column(Integer, primary_key=True)
    storm_id = Column(String(50), unique=True, nullable=False)
    sid = Column(String(50), unique=True, nullable=False)
    atcf_id = Column(String(20))
    season = Column(Integer, nullable=False)
    basin = Column(String(10), nullable=False)
    number = Column(Integer)
    name = Column(String(100))
    name_alternate = Column(String(100))
    other_ids = Column(JSONB)
    created_at = Column(DateTime, server_default="NOW()", nullable=False)

    __table_args__ = (
        Index("idx_storms_season_basin", "season", "basin"),
        Index("idx_storms_name", "name"),
        UniqueConstraint("storm_id", name="uq_storm_id"),
        {"schema": "storms"},
    )

    @classmethod
    def from_dataframe(
        cls, df: pd.DataFrame, engine, chunk_size: int = 1000
    ) -> None:
        if "other_ids" in df.columns:
            df["other_ids"] = df["other_ids"].apply(json.dumps)

        df = df.replace({np.nan: None})
        df = handle_datetime_columns(df, ["created_at"])
        df = handle_json_columns(df, ["other_ids"])

        with engine.connect() as conn:
            with conn.begin():
                df.to_sql(
                    cls.__tablename__,
                    conn,
                    if_exists="append",
                    schema="storms",
                    index=False,
                    method="multi",
                    chunksize=chunk_size,
                )
