from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    DateTime,
    ForeignKey,
    Index,
    UniqueConstraint,
)
from .base import Base
import pandas as pd
from datetime import datetime
from typing import Optional
import numpy as np


class ForecastTrack(Base):
    __tablename__ = "forecast_tracks"

    id = Column(Integer, primary_key=True)
    storm_id = Column(
        String(50),
        ForeignKey("storms.storms.storm_id", ondelete="CASCADE"),
        nullable=False,
    )
    issue_time = Column(DateTime, nullable=False)
    valid_time = Column(DateTime, nullable=False)

    # Position and intensity
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    wind_speed = Column(Float, nullable=False)
    gust_speed = Column(Float)
    pressure = Column(Float)
    basin = Column(String(10), nullable=False)

    # Forecast-specific fields
    uncertainty = Column(Float)

    # Classification
    category = Column(String(20))
    nature = Column(String(20))
    provider = Column(String(20))

    created_at = Column(DateTime, server_default="NOW()", nullable=False)

    __table_args__ = (
        Index("idx_forecast_tracks_times", "issue_time", "valid_time"),
        Index("idx_forecast_tracks_storm_issue", "storm_id", "issue_time"),
        UniqueConstraint(
            "storm_id",
            "issue_time",
            "valid_time",
            "provider",
            name="uq_forecast_track",
        ),
        {"schema": "storms"},
    )

    @classmethod
    def from_dataframe(
        cls, df: pd.DataFrame, engine, chunk_size: int = 1000
    ) -> None:
        """
        Insert forecast tracks from a DataFrame.

        Args:
            df: DataFrame with forecast track data
            engine: SQLAlchemy engine
            chunk_size: Number of records to insert at once
        """
        # Ensure datetime columns are in UTC
        for col in ["issue_time", "valid_time"]:
            if not pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = pd.to_datetime(df[col], utc=True)

        # Handle array data
        for col in ["wind_radii", "wind_radii_quadrants"]:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: x if isinstance(x, list) else []
                )

        df = df.replace({np.nan: None})

        with engine.connect() as conn:
            with conn.begin():
                df.to_sql(
                    cls.__tablename__,
                    conn,
                    if_exists="append",
                    index=False,
                    method="multi",
                    schema="storms",
                    chunksize=chunk_size,
                )

    @classmethod
    def to_dataframe(
        cls,
        engine,
        storm_id: Optional[str] = None,
        issue_time: Optional[datetime] = None,
        start_valid_time: Optional[datetime] = None,
        end_valid_time: Optional[datetime] = None,
        provider: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Retrieve forecast tracks as a DataFrame.
        """
        query = "SELECT * FROM forecast_tracks WHERE 1=1"
        params = {}

        if storm_id:
            query += " AND storm_id = %(storm_id)s"
            params["storm_id"] = storm_id

        if issue_time:
            query += " AND issue_time = %(issue_time)s"
            params["issue_time"] = issue_time

        if start_valid_time:
            query += " AND valid_time >= %(start_valid_time)s"
            params["start_valid_time"] = start_valid_time

        if end_valid_time:
            query += " AND valid_time <= %(end_valid_time)s"
            params["end_valid_time"] = end_valid_time

        if provider:
            query += " AND provider = %(provider)s"
            params["provider"] = provider

        query += " ORDER BY storm_id, issue_time, valid_time"

        return pd.read_sql_query(
            query,
            engine,
            params=params,
            parse_dates=["issue_time", "valid_time", "created_at"],
        )
