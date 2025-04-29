from sqlalchemy import (
    Column,
    String,
    Float,
    DateTime,
    ForeignKey,
    Index,
    UniqueConstraint,
    ARRAY,
)
from .base import Base, handle_array_columns, handle_datetime_columns
import pandas as pd
import numpy as np


class ObservedTrack(Base):
    __tablename__ = "observed_tracks"

    point_id = Column(String(50), primary_key=True)
    sid = Column(
        String(50),
        ForeignKey("storms.storms.sid", ondelete="CASCADE"),
        nullable=False,
    )
    valid_time = Column(DateTime, nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)

    # Measurements
    wind_speed = Column(Float)
    gust_speed = Column(Float)
    pressure = Column(Float)
    max_wind_radius = Column(Float)
    last_closed_isobar_radius = Column(Float)
    last_closed_isobar_pressure = Column(Float)
    basin = Column(String(10))

    # Classification
    category = Column(String(20))
    nature = Column(String(20))
    provider = Column(String(20))

    # Store quadrant data as arrays
    quadrant_radius_34 = Column(ARRAY(Float))
    quadrant_radius_50 = Column(ARRAY(Float))
    quadrant_radius_64 = Column(ARRAY(Float))

    created_at = Column(DateTime, server_default="NOW()", nullable=False)

    __table_args__ = (
        Index("idx_observed_tracks_time", "valid_time"),
        Index("idx_observed_tracks_storm_time", "sid", "valid_time"),
        UniqueConstraint("sid", "valid_time", name="uq_observed_track"),
        {"schema": "storms"},
    )

    @classmethod
    def from_dataframe(
        cls, df: pd.DataFrame, engine, chunk_size: int = 1000
    ) -> None:
        df = df.replace({np.nan: None})
        df = handle_datetime_columns(df, ["valid_time", "created_at"])
        df = handle_array_columns(
            df,
            ["quadrant_radius_34", "quadrant_radius_50", "quadrant_radius_64"],
        )

        with engine.connect() as conn:
            with conn.begin():
                df.to_sql(
                    cls.__tablename__,
                    conn,
                    if_exists="append",
                    index=False,
                    schema="storms",
                    method="multi",
                    chunksize=chunk_size,
                )
