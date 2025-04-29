from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    Index,
    UniqueConstraint,
    Boolean,
)
from .base import Base, handle_datetime_columns
import pandas as pd
import numpy as np


class Storm(Base):
    __tablename__ = "storms"

    storm_id = Column(String(50), primary_key=True)
    sid = Column(String(50), unique=True, nullable=False)
    atcf_id = Column(String(20))
    season = Column(Integer, nullable=False)
    number = Column(Integer)
    name = Column(String(100))
    provisional = Column(Boolean)
    created_at = Column(DateTime, server_default="NOW()", nullable=False)

    __table_args__ = (
        Index("idx_storms_name", "name"),
        UniqueConstraint("storm_id", name="uq_storm_id"),
        {"schema": "storms"},
    )

    @classmethod
    def from_dataframe(
        cls, df: pd.DataFrame, engine, chunk_size: int = 1000
    ) -> None:
        df = df.replace({np.nan: None})
        df = handle_datetime_columns(df, ["created_at"])

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
