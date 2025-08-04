from sqlalchemy import (
    Column,
    BigInteger,
    Text,
    Index,
    SmallInteger,
    Boolean,
    MetaData,
    Table,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import TIMESTAMP
from geoalchemy2 import Geometry

Base = declarative_base()

# Create metadata object
metadata = MetaData(schema="storms")


IBTRACS_GEO = Table(
    "ibtracs_tracks_geo_isa",
    metadata,
    # Storm measurement data (bigint columns)
    Column("wind_speed", BigInteger, nullable=True),
    Column("pressure", BigInteger, nullable=True),
    Column("max_wind_radius", BigInteger, nullable=True),
    Column("last_closed_isobar_radius", BigInteger, nullable=True),
    Column("last_closed_isobar_pressure", BigInteger, nullable=True),
    Column("gust_speed", BigInteger, nullable=True),
    # Storm identification and metadata (text columns)
    Column("sid", Text, nullable=True),
    Column("provider", Text, nullable=True),
    Column("basin", Text, nullable=True),
    Column("nature", Text, nullable=True),
    # Temporal data
    Column("valid_time", TIMESTAMP(timezone=False), nullable=True),
    # Radius data for different wind speeds (text columns)
    Column("quadrant_radius_34", Text, nullable=True),
    Column("quadrant_radius_50", Text, nullable=True),
    Column("quadrant_radius_64", Text, nullable=True),
    # Additional identifiers (text columns)
    Column("point_id", Text, nullable=True),
    Column("storm_id", Text, nullable=True),
    # Geometric data (PostGIS Point geometry with SRID 4326)
    Column("geometry", Geometry("POINT", srid=4326), nullable=True),
    postgresql_tablespace="pg_default",
)

geometry_index = Index(
    "idx_ibtracs_tracks_geo_geometry",
    IBTRACS_GEO.c.geometry,
    postgresql_using="gist",
    postgresql_tablespace="pg_default",
)

IBTRACS_STORMS = Table(
    "ibtracs_storms_isa",
    metadata,
    Column("index", BigInteger, primary_key=True),
    Column("sid", Text),
    Column("atcf_id", Text),
    Column("number", SmallInteger),
    Column("season", BigInteger),
    Column("name", Text),
    Column("genesis_basin", Text),
    Column("provisional", Boolean),
    Column("storm_id", Text),
    # Index definition
    Index("ix_storms_ibtracs_storms_isa_index", "index"),
    schema="storms",
)

storms_index = Index(
    "ix_storms_ibtracs_storms_index",
    IBTRACS_STORMS.c.index.asc().nullslast(),  # ASC NULLS LAST
    postgresql_using="btree",
    postgresql_tablespace="pg_default",
)
