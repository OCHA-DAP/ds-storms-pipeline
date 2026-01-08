-- ============================================================================
-- NHC (National Hurricane Center) Database Schema
-- ============================================================================
-- This SQL script creates the PostgreSQL tables for storing NHC tropical
-- cyclone data, including both storm metadata and track data.
--
-- Tables:
--   - storms.nhc_storms: Storm-level metadata (one row per storm)
--   - storms.nhc_tracks_geo: Track points with geometry (one row per forecast point)
--
-- Compatible with ocha-lens NHC module schemas
-- ============================================================================

-- ============================================================================
-- Table: storms.nhc_storms
-- ============================================================================
-- Storm metadata table containing one row per unique storm.
-- Primary key: atcf_id (e.g., "AL012023", "EP092023", "CP012024")

-- DROP TABLE IF EXISTS storms.nhc_storms CASCADE;

CREATE TABLE IF NOT EXISTS storms.nhc_storms
(
    atcf_id VARCHAR NOT NULL PRIMARY KEY,
    name VARCHAR,
    number VARCHAR NOT NULL,
    season INTEGER NOT NULL CHECK (season BETWEEN 1840 AND 2050),
    genesis_basin VARCHAR NOT NULL CHECK (genesis_basin IN ('NA', 'EP')),
    provider VARCHAR CHECK (provider IN ('NHC', 'CPHC')),
    storm_id VARCHAR UNIQUE,
    CONSTRAINT nhc_storms_unique UNIQUE (atcf_id, storm_id)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.nhc_storms
    OWNER to {owner};

COMMENT ON TABLE storms.nhc_storms IS
    'NHC storm metadata - one row per unique storm identified by ATCF ID';
COMMENT ON COLUMN storms.nhc_storms.atcf_id IS
    'ATCF storm identifier (e.g., AL012023, EP092023, CP012024)';
COMMENT ON COLUMN storms.nhc_storms.name IS
    'Storm name (uppercase) or NULL for unnamed systems';
COMMENT ON COLUMN storms.nhc_storms.number IS
    'Annual cyclone number within basin (e.g., "01", "14")';
COMMENT ON COLUMN storms.nhc_storms.season IS
    'Storm season year (calendar year for Northern Hemisphere)';
COMMENT ON COLUMN storms.nhc_storms.genesis_basin IS
    'Standardized basin code: NA (North Atlantic) or EP (Eastern/Central Pacific)';
COMMENT ON COLUMN storms.nhc_storms.provider IS
    'NHC (National Hurricane Center) or CPHC (Central Pacific Hurricane Center)';
COMMENT ON COLUMN storms.nhc_storms.storm_id IS
    'Standardized identifier: {name}_{basin}_{season} (lowercase)';

-- ============================================================================
-- Table: storms.nhc_tracks_geo
-- ============================================================================
-- Track data with geometry - one row per forecast point.
-- Includes both observations (leadtime=0) and forecasts (leadtime>0).

-- DROP TABLE IF EXISTS storms.nhc_tracks_geo CASCADE;

CREATE TABLE IF NOT EXISTS storms.nhc_tracks_geo
(
    atcf_id VARCHAR NOT NULL,
    provider VARCHAR NOT NULL,
    basin VARCHAR NOT NULL CHECK (basin IN ('NA', 'EP')),
    issued_time TIMESTAMP NOT NULL,
    valid_time TIMESTAMP NOT NULL,
    leadtime INTEGER NOT NULL CHECK (leadtime >= 0),
    wind_speed REAL CHECK (wind_speed BETWEEN 0 AND 300),
    pressure REAL CHECK (pressure BETWEEN 800 AND 1100),
    max_wind_radius INTEGER CHECK (max_wind_radius >= 0),
    last_closed_isobar_radius INTEGER CHECK (last_closed_isobar_radius >= 0),
    last_closed_isobar_pressure INTEGER CHECK (last_closed_isobar_pressure BETWEEN 800 AND 1100),
    gust_speed INTEGER CHECK (gust_speed BETWEEN 0 AND 400),
    nature VARCHAR,
    quadrant_radius_34 TEXT,
    quadrant_radius_50 TEXT,
    quadrant_radius_64 TEXT,
    number VARCHAR,
    storm_id VARCHAR,
    point_id VARCHAR NOT NULL,
    geometry geometry(Point, 4326) NOT NULL,
    CONSTRAINT nhc_tracks_geo_unique UNIQUE (atcf_id, valid_time, leadtime, issued_time),
    CONSTRAINT foreign_key_atcf_id FOREIGN KEY (atcf_id)
        REFERENCES storms.nhc_storms(atcf_id)
        ON DELETE CASCADE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.nhc_tracks_geo
    OWNER to {owner};

COMMENT ON TABLE storms.nhc_tracks_geo IS
    'NHC track points with geometry - includes observations (leadtime=0) and forecasts (leadtime>0)';
COMMENT ON COLUMN storms.nhc_tracks_geo.atcf_id IS
    'ATCF storm identifier - foreign key to nhc_storms';
COMMENT ON COLUMN storms.nhc_tracks_geo.provider IS
    'NHC or CPHC';
COMMENT ON COLUMN storms.nhc_tracks_geo.basin IS
    'Standardized basin code (NA or EP)';
COMMENT ON COLUMN storms.nhc_tracks_geo.issued_time IS
    'When the forecast/observation was issued (UTC)';
COMMENT ON COLUMN storms.nhc_tracks_geo.valid_time IS
    'Time this position is valid for (UTC)';
COMMENT ON COLUMN storms.nhc_tracks_geo.leadtime IS
    'Hours ahead of issue time (0 for observations, >0 for forecasts)';
COMMENT ON COLUMN storms.nhc_tracks_geo.wind_speed IS
    'Maximum sustained wind speed (knots)';
COMMENT ON COLUMN storms.nhc_tracks_geo.pressure IS
    'Minimum central pressure (hPa/mb)';
COMMENT ON COLUMN storms.nhc_tracks_geo.max_wind_radius IS
    'Radius of maximum winds (nautical miles)';
COMMENT ON COLUMN storms.nhc_tracks_geo.last_closed_isobar_radius IS
    'Radius of outermost closed isobar (nautical miles)';
COMMENT ON COLUMN storms.nhc_tracks_geo.last_closed_isobar_pressure IS
    'Pressure of outermost closed isobar (hPa/mb)';
COMMENT ON COLUMN storms.nhc_tracks_geo.gust_speed IS
    'Peak wind gust speed (knots)';
COMMENT ON COLUMN storms.nhc_tracks_geo.nature IS
    'System type/nature (e.g., TS, HU, TD, EX, SS)';
COMMENT ON COLUMN storms.nhc_tracks_geo.quadrant_radius_34 IS
    'JSON array: 34-knot wind radii by quadrant [NE, SE, SW, NW] in nautical miles';
COMMENT ON COLUMN storms.nhc_tracks_geo.quadrant_radius_50 IS
    'JSON array: 50-knot wind radii by quadrant [NE, SE, SW, NW] in nautical miles';
COMMENT ON COLUMN storms.nhc_tracks_geo.quadrant_radius_64 IS
    'JSON array: 64-knot wind radii by quadrant [NE, SE, SW, NW] in nautical miles';
COMMENT ON COLUMN storms.nhc_tracks_geo.number IS
    'Annual cyclone number within basin';
COMMENT ON COLUMN storms.nhc_tracks_geo.storm_id IS
    'Standardized storm identifier';
COMMENT ON COLUMN storms.nhc_tracks_geo.point_id IS
    'Unique identifier (UUID) for this forecast point';
COMMENT ON COLUMN storms.nhc_tracks_geo.geometry IS
    'Point geometry in WGS84 (EPSG:4326)';

-- ============================================================================
-- Indexes
-- ============================================================================

-- Spatial index for geometry column
-- DROP INDEX IF EXISTS storms.idx_nhc_tracks_geo_geometry;

CREATE INDEX IF NOT EXISTS idx_nhc_tracks_geo_geometry
    ON storms.nhc_tracks_geo USING gist (geometry)
    TABLESPACE pg_default;

-- Index on valid_time for temporal queries
CREATE INDEX IF NOT EXISTS idx_nhc_tracks_geo_valid_time
    ON storms.nhc_tracks_geo (valid_time)
    TABLESPACE pg_default;

-- Index on issued_time for querying specific forecast runs
CREATE INDEX IF NOT EXISTS idx_nhc_tracks_geo_issued_time
    ON storms.nhc_tracks_geo (issued_time)
    TABLESPACE pg_default;

-- Index on leadtime for filtering observations vs forecasts
CREATE INDEX IF NOT EXISTS idx_nhc_tracks_geo_leadtime
    ON storms.nhc_tracks_geo (leadtime)
    TABLESPACE pg_default;

-- Composite index for common query patterns (storm + time)
CREATE INDEX IF NOT EXISTS idx_nhc_tracks_geo_atcf_valid
    ON storms.nhc_tracks_geo (atcf_id, valid_time)
    TABLESPACE pg_default;

-- Index on storm_id for joining with other datasets
CREATE INDEX IF NOT EXISTS idx_nhc_tracks_geo_storm_id
    ON storms.nhc_tracks_geo (storm_id)
    TABLESPACE pg_default;

-- Index on basin for filtering by geographic region
CREATE INDEX IF NOT EXISTS idx_nhc_tracks_geo_basin
    ON storms.nhc_tracks_geo (basin)
    TABLESPACE pg_default;

