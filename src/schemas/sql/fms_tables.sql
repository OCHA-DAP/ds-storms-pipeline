-- ============================================================================
-- FMS (Fiji Meteorological Service / RSMC Nadi) Database Schema
-- ============================================================================
-- Forecast tracks that RSMC Nadi emails as CSV exports of its cyclone
-- forecast software (one file per issuance). Shaped like the NHC tables
-- (nhc_tables.sql) so the wind-buffer code can read them, plus the columns
-- the Fiji AA monitor (OCHA-DAP/pa-aa-fji-storms) needs, so that monitor can
-- run from the database instead of the CSV.
--
-- Tables:
--   - storms.fms_storms:      one row per disturbance (e.g. 2526-11F)
--   - storms.fms_forecasts:   one row per issued forecast track (CSV file)
--   - storms.fms_tracks_geo:  one row per track point
--
-- Writes are per issuance: the fms_forecasts row is deleted (cascading to
-- its track points) and re-inserted in one transaction, so a reissued CSV
-- with fewer points can't leave stale rows behind.
--
-- Physical values carry no range CHECKs, unlike nhc_tracks_geo: this table
-- feeds a live alert path, and one odd value must not stop a forecast from
-- landing. The raw CSV is archived to blob (fms_forecasts.raw_blob).
-- ============================================================================

-- ============================================================================
-- Table: storms.fms_storms
-- ============================================================================

-- DROP TABLE IF EXISTS storms.fms_storms CASCADE;

CREATE TABLE IF NOT EXISTS storms.fms_storms
(
    disturbance_id VARCHAR NOT NULL PRIMARY KEY,
    name VARCHAR,
    number VARCHAR NOT NULL,
    season INTEGER NOT NULL CHECK (season BETWEEN 1840 AND 2050),
    genesis_basin VARCHAR NOT NULL CHECK (genesis_basin = 'SP'),
    provider VARCHAR NOT NULL CHECK (provider = 'fms'),
    storm_id VARCHAR,
    CONSTRAINT fms_storms_unique UNIQUE (disturbance_id, storm_id)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.fms_storms
    OWNER to {owner};

COMMENT ON TABLE storms.fms_storms IS
    'RSMC Nadi (FMS) disturbances - one row per DisturbanceId';
COMMENT ON COLUMN storms.fms_storms.disturbance_id IS
    'FMS disturbance identifier {season}-{number}, e.g. 2526-11F';
COMMENT ON COLUMN storms.fms_storms.name IS
    'Cyclone name (uppercase, pronunciation guide removed) from the latest named forecast, or NULL';
COMMENT ON COLUMN storms.fms_storms.number IS
    'Disturbance number within the season, e.g. 11F';
COMMENT ON COLUMN storms.fms_storms.season IS
    'Southern Hemisphere season, as the year it ends (2526 -> 2026)';
COMMENT ON COLUMN storms.fms_storms.genesis_basin IS
    'Standardized basin code: SP (South Pacific)';
COMMENT ON COLUMN storms.fms_storms.provider IS
    'fms (Fiji Meteorological Service, RSMC Nadi)';
COMMENT ON COLUMN storms.fms_storms.storm_id IS
    'Standardized identifier: {name}_{basin}_{season} (lowercase), NULL while unnamed';

-- ============================================================================
-- Table: storms.fms_forecasts
-- ============================================================================

-- DROP TABLE IF EXISTS storms.fms_forecasts CASCADE;

CREATE TABLE IF NOT EXISTS storms.fms_forecasts
(
    disturbance_id VARCHAR NOT NULL,
    track_type VARCHAR NOT NULL,
    issued_time TIMESTAMP NOT NULL,
    name_as_issued VARCHAR,
    data_source VARCHAR,
    source_filename VARCHAR,
    content_sha256 CHAR(64) NOT NULL,
    raw_blob VARCHAR,
    n_points INTEGER NOT NULL CHECK (n_points >= 0),
    ingested_at TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    CONSTRAINT fms_forecasts_pkey PRIMARY KEY (disturbance_id, track_type, issued_time),
    CONSTRAINT fms_forecasts_disturbance_id_fkey FOREIGN KEY (disturbance_id)
        REFERENCES storms.fms_storms(disturbance_id)
        ON DELETE CASCADE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.fms_forecasts
    OWNER to {owner};

COMMENT ON TABLE storms.fms_forecasts IS
    'RSMC Nadi (FMS) forecast issuances - one row per CSV file';
COMMENT ON COLUMN storms.fms_forecasts.track_type IS
    'trackType header, e.g. Official Forecast Track';
COMMENT ON COLUMN storms.fms_forecasts.issued_time IS
    'baseTime header (UTC)';
COMMENT ON COLUMN storms.fms_forecasts.name_as_issued IS
    'CycloneName header verbatim, e.g. VAIANU, RAE [RAY], noname';
COMMENT ON COLUMN storms.fms_forecasts.data_source IS
    'dataSource header, when present';
COMMENT ON COLUMN storms.fms_forecasts.source_filename IS
    'Attachment filename the CSV arrived with';
COMMENT ON COLUMN storms.fms_forecasts.content_sha256 IS
    'SHA-256 of the raw CSV bytes';
COMMENT ON COLUMN storms.fms_forecasts.raw_blob IS
    'Archived raw CSV: blob name in the storm container of the same stage';
COMMENT ON COLUMN storms.fms_forecasts.n_points IS
    'Number of track points written to fms_tracks_geo';
COMMENT ON COLUMN storms.fms_forecasts.ingested_at IS
    'When this issuance was last written (UTC)';

-- ============================================================================
-- Table: storms.fms_tracks_geo
-- ============================================================================
-- Radii are nautical miles. quadrant_radius_* are JSON arrays
-- [NE, SE, SW, NW] (same order as nhc_tracks_geo) with null for a missing
-- quadrant, or NULL when all four are missing. FMS gives either quadrant or
-- symmetric radii, sometimes both styles in one row, so both are kept.
-- 48 kt is FMS's "strong gale"; nhc_tracks_geo has no such column.

-- DROP TABLE IF EXISTS storms.fms_tracks_geo CASCADE;

CREATE TABLE IF NOT EXISTS storms.fms_tracks_geo
(
    disturbance_id VARCHAR NOT NULL,
    provider VARCHAR NOT NULL CHECK (provider = 'fms'),
    basin VARCHAR NOT NULL CHECK (basin = 'SP'),
    track_type VARCHAR NOT NULL,
    issued_time TIMESTAMP NOT NULL,
    valid_time TIMESTAMP NOT NULL,
    leadtime INTEGER NOT NULL CHECK (leadtime >= 0),
    wind_speed REAL,
    gust_speed REAL,
    pressure REAL,
    max_wind_radius REAL,
    last_closed_isobar_radius REAL,
    last_closed_isobar_pressure REAL,
    category REAL,
    uncertainty REAL,
    radius_34 REAL,
    radius_48 REAL,
    radius_50 REAL,
    radius_64 REAL,
    quadrant_radius_34 TEXT,
    quadrant_radius_48 TEXT,
    quadrant_radius_50 TEXT,
    quadrant_radius_64 TEXT,
    raw_fields JSONB,
    storm_id VARCHAR,
    point_id VARCHAR NOT NULL,
    geometry geometry(Point, 4326) NOT NULL,
    CONSTRAINT fms_tracks_geo_unique UNIQUE (disturbance_id, track_type, issued_time, valid_time),
    CONSTRAINT fms_tracks_geo_forecast_fkey FOREIGN KEY (disturbance_id, track_type, issued_time)
        REFERENCES storms.fms_forecasts(disturbance_id, track_type, issued_time)
        ON DELETE CASCADE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.fms_tracks_geo
    OWNER to {owner};

COMMENT ON TABLE storms.fms_tracks_geo IS
    'RSMC Nadi (FMS) forecast track points with geometry (leadtime 0 = analysis position)';
COMMENT ON COLUMN storms.fms_tracks_geo.leadtime IS
    'Hours after issued_time';
COMMENT ON COLUMN storms.fms_tracks_geo.wind_speed IS
    'MeanWind: 10-minute mean wind (knots)';
COMMENT ON COLUMN storms.fms_tracks_geo.gust_speed IS
    'WindGust (knots)';
COMMENT ON COLUMN storms.fms_tracks_geo.pressure IS
    'Pressure: central pressure (hPa)';
COMMENT ON COLUMN storms.fms_tracks_geo.max_wind_radius IS
    'RadiusMaxWinds (nautical miles)';
COMMENT ON COLUMN storms.fms_tracks_geo.last_closed_isobar_radius IS
    'RadiusOCI: radius of the outermost closed isobar (nautical miles)';
COMMENT ON COLUMN storms.fms_tracks_geo.last_closed_isobar_pressure IS
    'PressureOCI: pressure of the outermost closed isobar (hPa)';
COMMENT ON COLUMN storms.fms_tracks_geo.category IS
    'Category on the Australian 1-5 scale, NULL when FMS gives none';
COMMENT ON COLUMN storms.fms_tracks_geo.uncertainty IS
    'Uncertainty: position uncertainty radius (nautical miles)';
COMMENT ON COLUMN storms.fms_tracks_geo.radius_34 IS
    'GaleRadius: symmetric 34-knot radius (nautical miles)';
COMMENT ON COLUMN storms.fms_tracks_geo.radius_48 IS
    'StrongGaleRadius: symmetric 48-knot radius (nautical miles)';
COMMENT ON COLUMN storms.fms_tracks_geo.radius_50 IS
    'StormRadius: symmetric 50-knot radius (nautical miles)';
COMMENT ON COLUMN storms.fms_tracks_geo.radius_64 IS
    'HurricaneRadius: symmetric 64-knot radius (nautical miles)';
COMMENT ON COLUMN storms.fms_tracks_geo.quadrant_radius_34 IS
    'JSON array: {NE,SE,SW,NW}GaleRadius, 34-knot radii by quadrant (nautical miles)';
COMMENT ON COLUMN storms.fms_tracks_geo.quadrant_radius_48 IS
    'JSON array: {NE,SE,SW,NW}StrongGaleRadius, 48-knot radii by quadrant (nautical miles)';
COMMENT ON COLUMN storms.fms_tracks_geo.quadrant_radius_50 IS
    'JSON array: {NE,SE,SW,NW}StormRadius, 50-knot radii by quadrant (nautical miles)';
COMMENT ON COLUMN storms.fms_tracks_geo.quadrant_radius_64 IS
    'JSON array: {NE,SE,SW,NW}HurricaneRadius, 64-knot radii by quadrant (nautical miles)';
COMMENT ON COLUMN storms.fms_tracks_geo.raw_fields IS
    'Every other CSV column with a value, keyed by its FMS column heading';
COMMENT ON COLUMN storms.fms_tracks_geo.storm_id IS
    'storm_id implied by this forecast''s name, NULL while unnamed';
COMMENT ON COLUMN storms.fms_tracks_geo.point_id IS
    'Unique identifier (UUID) for this track point';
COMMENT ON COLUMN storms.fms_tracks_geo.geometry IS
    'Point geometry in WGS84 (EPSG:4326), longitude in [-180, 180)';

-- ============================================================================
-- Indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_fms_tracks_geo_geometry
    ON storms.fms_tracks_geo USING gist (geometry)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_fms_tracks_geo_issued_time
    ON storms.fms_tracks_geo (issued_time)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_fms_tracks_geo_valid_time
    ON storms.fms_tracks_geo (valid_time)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_fms_tracks_geo_storm_id
    ON storms.fms_tracks_geo (storm_id)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_fms_forecasts_issued_time
    ON storms.fms_forecasts (issued_time)
    TABLESPACE pg_default;
