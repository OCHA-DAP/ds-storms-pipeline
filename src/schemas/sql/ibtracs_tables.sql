-- ============================================================================
-- IBTrACS (International Best Track Archive for Climate Stewardship) Database Schema
-- ============================================================================
-- This SQL script creates the PostgreSQL tables for storing IBTrACS tropical
-- cyclone data, including both storm metadata and track data.
--
-- Tables:
--   - storms.ibtracs_storms: Storm-level metadata (one row per storm)
--   - storms.ibtracs_tracks_geo: Track points with geometry (one row per observation point)
--
-- Compatible with ocha-lens IBTrACS module schemas
-- ============================================================================

-- ============================================================================
-- Table: storms.ibtracs_storms
-- ============================================================================
-- Storm metadata table containing one row per unique storm.
-- Primary key: sid (IBTrACS serial identifier)

-- DROP TABLE IF EXISTS storms.ibtracs_storms CASCADE;

CREATE TABLE IF NOT EXISTS storms.ibtracs_storms
(
    sid VARCHAR PRIMARY KEY,
    atcf_id VARCHAR,
    number SMALLINT NOT NULL,
    season BIGINT NOT NULL CHECK (season BETWEEN 1840 AND 2100),
    name VARCHAR,
    genesis_basin VARCHAR NOT NULL,
    provisional BOOLEAN NOT NULL,
    storm_id VARCHAR,
    CONSTRAINT ibtracs_storms_unique UNIQUE (sid)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.ibtracs_storms
    OWNER to {owner};

COMMENT ON TABLE storms.ibtracs_storms IS
    'IBTrACS storm metadata - one row per unique storm identified by sid';
COMMENT ON COLUMN storms.ibtracs_storms.sid IS
    'IBTrACS serial identifier (unique storm ID)';
COMMENT ON COLUMN storms.ibtracs_storms.atcf_id IS
    'ATCF storm identifier (e.g., AL012023, EP092023)';
COMMENT ON COLUMN storms.ibtracs_storms.number IS
    'Annual cyclone number within basin';
COMMENT ON COLUMN storms.ibtracs_storms.season IS
    'Storm season year';
COMMENT ON COLUMN storms.ibtracs_storms.name IS
    'Storm name (uppercase) or NULL for unnamed systems';
COMMENT ON COLUMN storms.ibtracs_storms.genesis_basin IS
    'Basin where storm originated';
COMMENT ON COLUMN storms.ibtracs_storms.provisional IS
    'Whether storm data is provisional (not yet finalized)';
COMMENT ON COLUMN storms.ibtracs_storms.storm_id IS
    'Standardized storm identifier linking to tracks';

-- ============================================================================
-- Table: storms.ibtracs_tracks_geo
-- ============================================================================
-- Track data with geometry - one row per observation point.
-- Contains all track observations with storm characteristics at each point.

-- DROP TABLE IF EXISTS storms.ibtracs_tracks_geo CASCADE;

CREATE TABLE IF NOT EXISTS storms.ibtracs_tracks_geo
(
    sid VARCHAR NOT NULL,
    point_id VARCHAR NOT NULL,
    storm_id VARCHAR,
    valid_time TIMESTAMP NOT NULL,
    provider VARCHAR NOT NULL,
    basin VARCHAR NOT NULL,
    nature VARCHAR,
    wind_speed INTEGER CHECK (wind_speed BETWEEN -1 AND 300),
    pressure INTEGER CHECK (pressure BETWEEN 800 AND 1100),
    max_wind_radius INTEGER CHECK (max_wind_radius >= 0),
    last_closed_isobar_radius INTEGER CHECK (last_closed_isobar_radius >= 0),
    last_closed_isobar_pressure INTEGER CHECK (last_closed_isobar_pressure BETWEEN 800 AND 1100),
    gust_speed INTEGER CHECK (gust_speed BETWEEN 0 AND 400),
    quadrant_radius_34 TEXT,
    quadrant_radius_50 TEXT,
    quadrant_radius_64 TEXT,
    usa_quadrant_radius_34 TEXT,
    usa_quadrant_radius_50 TEXT,
    usa_quadrant_radius_64 TEXT,
    geometry geometry(Point, 4326) NOT NULL,
    CONSTRAINT ibtracs_tracks_geo_unique UNIQUE (sid, storm_id, valid_time),
    CONSTRAINT foreign_key_sid FOREIGN KEY (sid)
        REFERENCES storms.ibtracs_storms(sid)
        ON DELETE CASCADE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.ibtracs_tracks_geo
    OWNER to {owner};

COMMENT ON TABLE storms.ibtracs_tracks_geo IS
    'IBTrACS track points with geometry - one row per observation point';
COMMENT ON COLUMN storms.ibtracs_tracks_geo.sid IS
    'IBTrACS serial identifier - foreign key to ibtracs_storms';
COMMENT ON COLUMN storms.ibtracs_tracks_geo.point_id IS
    'Unique identifier for this observation point';
COMMENT ON COLUMN storms.ibtracs_tracks_geo.storm_id IS
    'Standardized storm identifier';
COMMENT ON COLUMN storms.ibtracs_tracks_geo.valid_time IS
    'Observation time (UTC)';
COMMENT ON COLUMN storms.ibtracs_tracks_geo.provider IS
    'Agency providing the data (e.g., nhc, jtwc, tokyo, reunion, etc.)';
COMMENT ON COLUMN storms.ibtracs_tracks_geo.basin IS
    'Current basin location of the storm';
COMMENT ON COLUMN storms.ibtracs_tracks_geo.nature IS
    'Storm classification (tropical, extratropical, subtropical, etc.)';
COMMENT ON COLUMN storms.ibtracs_tracks_geo.wind_speed IS
    'Maximum sustained wind speed (knots)';
COMMENT ON COLUMN storms.ibtracs_tracks_geo.pressure IS
    'Minimum central pressure (hPa/mb)';
COMMENT ON COLUMN storms.ibtracs_tracks_geo.max_wind_radius IS
    'Radius of maximum winds (nautical miles)';
COMMENT ON COLUMN storms.ibtracs_tracks_geo.last_closed_isobar_radius IS
    'Radius of outermost closed isobar (nautical miles)';
COMMENT ON COLUMN storms.ibtracs_tracks_geo.last_closed_isobar_pressure IS
    'Pressure of outermost closed isobar (hPa/mb)';
COMMENT ON COLUMN storms.ibtracs_tracks_geo.gust_speed IS
    'Peak wind gust speed (knots)';
COMMENT ON COLUMN storms.ibtracs_tracks_geo.quadrant_radius_34 IS
    'JSON array: 34-knot wind radii by quadrant [NE, NW, SE, SW] in nautical miles';
COMMENT ON COLUMN storms.ibtracs_tracks_geo.quadrant_radius_50 IS
    'JSON array: 50-knot wind radii by quadrant [NE, NW, SE, SW] in nautical miles';
COMMENT ON COLUMN storms.ibtracs_tracks_geo.quadrant_radius_64 IS
    'JSON array: 64-knot wind radii by quadrant [NE, NW, SE, SW] in nautical miles';
COMMENT ON COLUMN storms.ibtracs_tracks_geo.usa_quadrant_radius_34 IS
    'JSON array: 34-knot wind radii by quadrant [NE, NW, SE, SW] in nautical miles (USA agency data)';
COMMENT ON COLUMN storms.ibtracs_tracks_geo.usa_quadrant_radius_50 IS
    'JSON array: 50-knot wind radii by quadrant [NE, NW, SE, SW] in nautical miles (USA agency data)';
COMMENT ON COLUMN storms.ibtracs_tracks_geo.usa_quadrant_radius_64 IS
    'JSON array: 64-knot wind radii by quadrant [NE, NW, SE, SW] in nautical miles (USA agency data)';
COMMENT ON COLUMN storms.ibtracs_tracks_geo.geometry IS
    'Point geometry in WGS84 (EPSG:4326)';

-- ============================================================================
-- Indexes
-- ============================================================================

-- Spatial index for geometry column
-- DROP INDEX IF EXISTS storms.idx_ibtracs_tracks_geo_geometry;

CREATE INDEX IF NOT EXISTS idx_ibtracs_tracks_geo_geometry
    ON storms.ibtracs_tracks_geo USING gist (geometry)
    TABLESPACE pg_default;

-- Index on valid_time for temporal queries
CREATE INDEX IF NOT EXISTS idx_ibtracs_tracks_geo_valid_time
    ON storms.ibtracs_tracks_geo (valid_time)
    TABLESPACE pg_default;

-- Composite index for common query patterns (storm + time)
CREATE INDEX IF NOT EXISTS idx_ibtracs_tracks_geo_sid_valid
    ON storms.ibtracs_tracks_geo (sid, valid_time)
    TABLESPACE pg_default;

-- Index on storm_id for joining with other datasets
CREATE INDEX IF NOT EXISTS idx_ibtracs_tracks_geo_storm_id
    ON storms.ibtracs_tracks_geo (storm_id)
    TABLESPACE pg_default;

-- Index on basin for filtering by geographic region
CREATE INDEX IF NOT EXISTS idx_ibtracs_tracks_geo_basin
    ON storms.ibtracs_tracks_geo (basin)
    TABLESPACE pg_default;

-- Index on provider for filtering by data source
CREATE INDEX IF NOT EXISTS idx_ibtracs_tracks_geo_provider
    ON storms.ibtracs_tracks_geo (provider)
    TABLESPACE pg_default;
