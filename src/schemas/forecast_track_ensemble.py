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
from .base import Base
import pandas as pd
from datetime import datetime
from typing import Optional
import numpy as np


class ForecastTrackEnsemble(Base):
    __tablename__ = "forecast_track_ensembles"

    id = Column(Integer, primary_key=True)
    storm_id = Column(
        String(50),
        ForeignKey("storms.storms.storm_id", ondelete="CASCADE"),
        nullable=False,
    )

    # Forecast identification
    ensemble_member = Column(Integer, nullable=False)
    issue_time = Column(DateTime, nullable=False)
    valid_time = Column(DateTime, nullable=False)

    # Position and intensity
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    wind_speed = Column(Float, nullable=False)
    pressure = Column(Float)

    # Ensemble-specific fields
    uncertainty = Column(Float)

    # Classification
    category = Column(String(20))
    nature = Column(String(20))
    provider = Column(String(20))

    # Store quadrant data as arrays
    wind_radii = Column(ARRAY(Float))
    wind_radii_quadrants = Column(ARRAY(Float))

    created_at = Column(DateTime, server_default="NOW()", nullable=False)

    __table_args__ = (
        Index("idx_ensemble_tracks_times", "issue_time", "valid_time"),
        Index("idx_ensemble_tracks_storm_issue", "storm_id", "issue_time"),
        UniqueConstraint(
            "storm_id",
            "issue_time",
            "valid_time",
            "ensemble_member",
            "provider",
            name="uq_ensemble_track",
        ),
        {"schema": "storms"},
    )

    @classmethod
    def from_dataframe(
        cls, df: pd.DataFrame, engine, chunk_size: int = 1000
    ) -> None:
        """
        Insert ensemble forecast tracks from a DataFrame.
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
                    schema="storms",
                    method="multi",
                    chunksize=chunk_size,
                )

    @classmethod
    def to_dataframe(
        cls,
        engine,
        storm_id: Optional[str] = None,
        issue_time: Optional[datetime] = None,
        ensemble_member: Optional[int] = None,
        provider: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Retrieve ensemble forecast tracks as a DataFrame.
        """
        query = "SELECT * FROM forecast_track_ensembles WHERE 1=1"
        params = {}

        if storm_id:
            query += " AND storm_id = %(storm_id)s"
            params["storm_id"] = storm_id

        if issue_time:
            query += " AND issue_time = %(issue_time)s"
            params["issue_time"] = issue_time

        if ensemble_member is not None:
            query += " AND ensemble_member = %(ensemble_member)s"
            params["ensemble_member"] = ensemble_member

        if provider:
            query += " AND provider = %(provider)s"
            params["provider"] = provider

        query += " ORDER BY storm_id, issue_time, ensemble_member, valid_time"

        return pd.read_sql_query(
            query,
            engine,
            params=params,
            parse_dates=["issue_time", "valid_time", "created_at"],
        )
