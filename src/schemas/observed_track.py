from sqlalchemy import (
    Column,
    Integer,
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

    id = Column(Integer, primary_key=True)
    storm_id = Column(
        String(50),
        ForeignKey("storms.storms.storm_id", ondelete="CASCADE"),
        nullable=False,
    )
    valid_time = Column(DateTime, nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)

    # Measurements
    wind_speed = Column(Float, nullable=False)
    gust_speed = Column(Float)
    pressure = Column(Float)
    max_wind_radius = Column(Float)
    last_closed_isobar_radius = Column(Float)
    last_closed_isobar_pressure = Column(Float)

    # Classification
    category = Column(String(20))
    nature = Column(String(20))
    provider = Column(String(20))

    # Store quadrant data as arrays
    wind_radii = Column(ARRAY(Float))  # [34kt, 50kt, 64kt]
    wind_radii_quadrants = Column(
        ARRAY(Float)
    )  # [NE_34, SE_34, SW_34, NW_34, ...]

    created_at = Column(DateTime, server_default="NOW()", nullable=False)

    __table_args__ = (
        Index("idx_observed_tracks_time", "valid_time"),
        Index("idx_observed_tracks_storm_time", "storm_id", "valid_time"),
        UniqueConstraint(
            "storm_id", "valid_time", "provider", name="uq_observed_track"
        ),
        {"schema": "storms"},
    )

    @classmethod
    def from_dataframe(
        cls, df: pd.DataFrame, engine, chunk_size: int = 1000
    ) -> None:
        df = df.replace({np.nan: None})
        df = handle_datetime_columns(df, ["valid_time", "created_at"])
        df = handle_array_columns(df, ["wind_radii", "wind_radii_quadrants"])

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
