-- ============================================================================
-- MFR (Météo-France La Réunion / RSMC La Réunion) Database Schema
-- ============================================================================
-- Tropical cyclone forecast tracks for the South-West Indian Ocean, from the
-- JSON files Météo-France La Réunion publishes for every system in its area
-- of responsibility (one file per system per forecast). Written by
-- src/pipelines/mfr.py. Shaped like the FMS tables (fms_tables.sql) so the
-- same downstream code can read both.
--
-- Tables:
--   - storms.mfr_storms:      one row per system (e.g. 10-20252026 = GEZANI)
--   - storms.mfr_forecasts:   one row per forecast (reference time)
--   - storms.mfr_tracks_geo:  one row per track point, analysis + forecast
--   - storms.mfr_files:       every source file read, whether or not it was
--                             written (the FTP poller's memory)
--
-- Each file carries the system's whole analysed track so far (data_type
-- 'analysis', leadtime <= 0) and the forecast (data_type 'forecast',
-- leadtime 0 to 120 h). Both are kept. The analysis at the reference time
-- appears twice, once of each type, as in the file.
--
-- Physical values are DOUBLE PRECISION without range CHECKs (same reasoning
-- as docs/decisions/0002-fms-tables-and-standalone-cli.md): one odd value
-- must not stop a forecast from landing. Every raw file is archived to blob
-- (mfr_files.raw_blob), parsed or not.
-- ============================================================================

-- ============================================================================
-- Table: storms.mfr_storms
-- ============================================================================

-- DROP TABLE IF EXISTS storms.mfr_storms CASCADE;

CREATE TABLE IF NOT EXISTS storms.mfr_storms
(
    disturbance_id VARCHAR NOT NULL PRIMARY KEY,
    cyclone_id VARCHAR NOT NULL UNIQUE,
    name VARCHAR,
    number SMALLINT NOT NULL,
    season INTEGER NOT NULL CHECK (season BETWEEN 1840 AND 2050),
    genesis_basin VARCHAR NOT NULL CHECK (genesis_basin = 'SI'),
    provider VARCHAR NOT NULL CHECK (provider = 'mfr'),
    storm_id VARCHAR
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.mfr_storms
    OWNER to {owner};

COMMENT ON TABLE storms.mfr_storms IS
    'RSMC La Réunion (Météo-France) systems - one row per cyclone_id';
COMMENT ON COLUMN storms.mfr_storms.disturbance_id IS
    '{number}-{season}, e.g. 10-20252026: how Météo-France names a system before it is named';
COMMENT ON COLUMN storms.mfr_storms.cyclone_id IS
    'cyclone_id as in the file, e.g. SWI$10/20252026';
COMMENT ON COLUMN storms.mfr_storms.name IS
    'Cyclone name from the latest named forecast, or NULL while unnamed';
COMMENT ON COLUMN storms.mfr_storms.number IS
    'System number within the season';
COMMENT ON COLUMN storms.mfr_storms.season IS
    'Southern Hemisphere season, as the year it ends (20252026 -> 2026)';
COMMENT ON COLUMN storms.mfr_storms.genesis_basin IS
    'Standardized basin code: SI (South Indian)';
COMMENT ON COLUMN storms.mfr_storms.provider IS
    'mfr (Météo-France La Réunion, RSMC La Réunion)';
COMMENT ON COLUMN storms.mfr_storms.storm_id IS
    'Standardized identifier {name}_si_{season} (as in storms.ibtracs_storms), NULL while unnamed';

-- ============================================================================
-- Table: storms.mfr_forecasts
-- ============================================================================

-- DROP TABLE IF EXISTS storms.mfr_forecasts CASCADE;

CREATE TABLE IF NOT EXISTS storms.mfr_forecasts
(
    disturbance_id VARCHAR NOT NULL,
    issued_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP,
    name_as_issued VARCHAR,
    product VARCHAR,
    production_center VARCHAR,
    data_source VARCHAR NOT NULL,
    source_filename VARCHAR,
    content_sha256 CHAR(64) NOT NULL,
    raw_blob VARCHAR,
    n_analysis INTEGER NOT NULL CHECK (n_analysis >= 0),
    n_forecast INTEGER NOT NULL CHECK (n_forecast >= 0),
    max_leadtime INTEGER,
    late_reissue BOOLEAN NOT NULL,
    uncertainty_cone geometry(Geometry, 4326),
    ingested_at TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    CONSTRAINT mfr_forecasts_pkey PRIMARY KEY (disturbance_id, issued_time),
    CONSTRAINT mfr_forecasts_disturbance_id_fkey FOREIGN KEY (disturbance_id)
        REFERENCES storms.mfr_storms(disturbance_id)
        ON DELETE CASCADE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.mfr_forecasts
    OWNER to {owner};

COMMENT ON TABLE storms.mfr_forecasts IS
    'RSMC La Réunion forecasts - one row per system and reference time';
COMMENT ON COLUMN storms.mfr_forecasts.issued_time IS
    'reference_time in the file (UTC): the analysis time the forecast starts from. The hour in the filename is the upload hour, not this';
COMMENT ON COLUMN storms.mfr_forecasts.update_time IS
    'update_time in the file (UTC): when Météo-France produced the file';
COMMENT ON COLUMN storms.mfr_forecasts.name_as_issued IS
    'cyclone_name as in the file, e.g. GEZANI, or 10-20252026 while unnamed';
COMMENT ON COLUMN storms.mfr_forecasts.data_source IS
    'Where the file came from: ftp (live poll) or backfill';
COMMENT ON COLUMN storms.mfr_forecasts.source_filename IS
    'Original filename, e.g. CMRSTRACK_SWI$10_20252026_GEZANI_2026_02_09_18Z.json';
COMMENT ON COLUMN storms.mfr_forecasts.content_sha256 IS
    'SHA-256 of the raw file bytes';
COMMENT ON COLUMN storms.mfr_forecasts.raw_blob IS
    'Archived raw file: blob name in the storm container of the same stage';
COMMENT ON COLUMN storms.mfr_forecasts.n_analysis IS
    'Number of analysis points written (the system''s track so far)';
COMMENT ON COLUMN storms.mfr_forecasts.n_forecast IS
    'Number of forecast points written (leadtime 0 included)';
COMMENT ON COLUMN storms.mfr_forecasts.max_leadtime IS
    'Longest forecast leadtime in hours, NULL without forecast points';
COMMENT ON COLUMN storms.mfr_forecasts.late_reissue IS
    'True when the file was produced more than 24 h after its reference time (Météo-France re-publishes some files after the season); never used to replace an operational forecast';
COMMENT ON COLUMN storms.mfr_forecasts.uncertainty_cone IS
    'Météo-France uncertainty cone polygon as published (WGS84). Its half-width matches position_accuracy';
COMMENT ON COLUMN storms.mfr_forecasts.ingested_at IS
    'When this forecast was last written (UTC)';

-- ============================================================================
-- Table: storms.mfr_tracks_geo
-- ============================================================================
-- Radii are nautical miles. quadrant_radius_* are JSON arrays
-- [NE, SE, SW, NW] (same order as nhc/fms_tracks_geo); a sector the file
-- leaves out is null (Météo-France omits sectors the wind does not reach),
-- and the column is NULL when the file gives no contour at that speed.
-- Météo-France contours are 28, 34, 48 and 64 kt (no 50 kt). Winds are
-- 10-minute means.

-- DROP TABLE IF EXISTS storms.mfr_tracks_geo CASCADE;

CREATE TABLE IF NOT EXISTS storms.mfr_tracks_geo
(
    disturbance_id VARCHAR NOT NULL,
    provider VARCHAR NOT NULL CHECK (provider = 'mfr'),
    basin VARCHAR NOT NULL CHECK (basin = 'SI'),
    issued_time TIMESTAMP NOT NULL,
    data_type VARCHAR NOT NULL CHECK (data_type IN ('analysis', 'forecast')),
    valid_time TIMESTAMP NOT NULL,
    leadtime INTEGER NOT NULL,
    development VARCHAR,
    wind_speed DOUBLE PRECISION,
    gust_speed DOUBLE PRECISION,
    pressure DOUBLE PRECISION,
    max_wind_radius DOUBLE PRECISION,
    last_closed_isobar_pressure DOUBLE PRECISION,
    last_closed_isobar_radius DOUBLE PRECISION,
    position_accuracy DOUBLE PRECISION,
    motion_speed DOUBLE PRECISION,
    motion_direction DOUBLE PRECISION,
    dvorak_final_t DOUBLE PRECISION,
    dvorak_ci DOUBLE PRECISION,
    quadrant_radius_28 TEXT,
    quadrant_radius_34 TEXT,
    quadrant_radius_48 TEXT,
    quadrant_radius_64 TEXT,
    raw_fields JSONB,
    storm_id VARCHAR,
    point_id VARCHAR NOT NULL,
    geometry geometry(Point, 4326) NOT NULL,
    CONSTRAINT mfr_tracks_geo_unique UNIQUE (disturbance_id, issued_time, data_type, valid_time),
    CONSTRAINT mfr_tracks_geo_forecast_fkey FOREIGN KEY (disturbance_id, issued_time)
        REFERENCES storms.mfr_forecasts(disturbance_id, issued_time)
        ON DELETE CASCADE
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.mfr_tracks_geo
    OWNER to {owner};

COMMENT ON TABLE storms.mfr_tracks_geo IS
    'RSMC La Réunion track points with geometry: analysis (past) and forecast points of each forecast';
COMMENT ON COLUMN storms.mfr_tracks_geo.data_type IS
    'analysis (observed track so far) or forecast (leadtime 0 = analysis at the reference time)';
COMMENT ON COLUMN storms.mfr_tracks_geo.leadtime IS
    'Hours after issued_time; negative for earlier analysis points';
COMMENT ON COLUMN storms.mfr_tracks_geo.development IS
    'Météo-France stage, e.g. disturbance, moderate tropical storm, intense tropical cyclone, inland';
COMMENT ON COLUMN storms.mfr_tracks_geo.wind_speed IS
    'maximum_wind.wind_speed_kt: 10-minute mean wind (knots)';
COMMENT ON COLUMN storms.mfr_tracks_geo.gust_speed IS
    'maximum_wind.wind_speed_gust_kt (knots)';
COMMENT ON COLUMN storms.mfr_tracks_geo.pressure IS
    'minimum_pressure: central pressure (hPa)';
COMMENT ON COLUMN storms.mfr_tracks_geo.max_wind_radius IS
    'maximum_wind.radius: radius of maximum winds (nautical miles)';
COMMENT ON COLUMN storms.mfr_tracks_geo.last_closed_isobar_pressure IS
    'last_closed_isobar.pressure (hPa)';
COMMENT ON COLUMN storms.mfr_tracks_geo.last_closed_isobar_radius IS
    'last_closed_isobar.radius (nautical miles)';
COMMENT ON COLUMN storms.mfr_tracks_geo.position_accuracy IS
    'position_accuracy: position uncertainty radius (nautical miles); equals the uncertainty cone half-width';
COMMENT ON COLUMN storms.mfr_tracks_geo.motion_speed IS
    'storm_motion.speed_kt (knots)';
COMMENT ON COLUMN storms.mfr_tracks_geo.motion_direction IS
    'storm_motion.direction_toward (degrees)';
COMMENT ON COLUMN storms.mfr_tracks_geo.dvorak_final_t IS
    'Dvorak.final_T_number';
COMMENT ON COLUMN storms.mfr_tracks_geo.dvorak_ci IS
    'Dvorak.current_intensity';
COMMENT ON COLUMN storms.mfr_tracks_geo.quadrant_radius_28 IS
    'JSON array [NE,SE,SW,NW]: 28-knot radii by quadrant (nautical miles)';
COMMENT ON COLUMN storms.mfr_tracks_geo.quadrant_radius_34 IS
    'JSON array [NE,SE,SW,NW]: 34-knot radii by quadrant (nautical miles)';
COMMENT ON COLUMN storms.mfr_tracks_geo.quadrant_radius_48 IS
    'JSON array [NE,SE,SW,NW]: 48-knot radii by quadrant (nautical miles)';
COMMENT ON COLUMN storms.mfr_tracks_geo.quadrant_radius_64 IS
    'JSON array [NE,SE,SW,NW]: 64-knot radii by quadrant (nautical miles)';
COMMENT ON COLUMN storms.mfr_tracks_geo.raw_fields IS
    'Every other point property with a value, keyed by its path in the file';
COMMENT ON COLUMN storms.mfr_tracks_geo.storm_id IS
    'storm_id implied by this forecast''s name, NULL while unnamed';
COMMENT ON COLUMN storms.mfr_tracks_geo.point_id IS
    'Unique identifier (UUID) for this track point';
COMMENT ON COLUMN storms.mfr_tracks_geo.geometry IS
    'Point geometry in WGS84 (EPSG:4326), longitude in [-180, 180)';

-- ============================================================================
-- Table: storms.mfr_files
-- ============================================================================
-- One row per distinct file read (name + content). The FTP poller compares
-- each listing against it, so a file is downloaded once; last_seen shows how
-- long files stay on the FTP.

-- DROP TABLE IF EXISTS storms.mfr_files CASCADE;

CREATE TABLE IF NOT EXISTS storms.mfr_files
(
    source_filename VARCHAR NOT NULL,
    content_sha256 CHAR(64) NOT NULL,
    source VARCHAR NOT NULL,
    size_bytes BIGINT,
    ftp_modified TIMESTAMP,
    first_seen TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    last_seen TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    raw_blob VARCHAR,
    status VARCHAR NOT NULL CHECK (status IN ('written', 'unchanged', 'kept_existing', 'failed')),
    message TEXT,
    disturbance_id VARCHAR,
    issued_time TIMESTAMP,
    CONSTRAINT mfr_files_pkey PRIMARY KEY (source_filename, content_sha256)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.mfr_files
    OWNER to {owner};

COMMENT ON TABLE storms.mfr_files IS
    'Every RSMC La Réunion source file read, with what happened to it';
COMMENT ON COLUMN storms.mfr_files.source IS
    'ftp (live poll) or backfill';
COMMENT ON COLUMN storms.mfr_files.size_bytes IS
    'Size in bytes (from the FTP listing when it gives one)';
COMMENT ON COLUMN storms.mfr_files.ftp_modified IS
    'Modification time from the FTP listing (UTC), when it gives one';
COMMENT ON COLUMN storms.mfr_files.first_seen IS
    'First time this file was read (UTC)';
COMMENT ON COLUMN storms.mfr_files.last_seen IS
    'Last FTP listing this file was in (UTC)';
COMMENT ON COLUMN storms.mfr_files.status IS
    'written: became the stored forecast; unchanged: same content already stored; kept_existing: a different version of a stored forecast that did not replace it (older or a late re-issue); failed: could not be read';
COMMENT ON COLUMN storms.mfr_files.message IS
    'Why, for kept_existing and failed';

-- ============================================================================
-- Indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_mfr_tracks_geo_geometry
    ON storms.mfr_tracks_geo USING gist (geometry)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_mfr_tracks_geo_issued_time
    ON storms.mfr_tracks_geo (issued_time)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_mfr_tracks_geo_valid_time
    ON storms.mfr_tracks_geo (valid_time)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_mfr_tracks_geo_storm_id
    ON storms.mfr_tracks_geo (storm_id)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_mfr_forecasts_issued_time
    ON storms.mfr_forecasts (issued_time)
    TABLESPACE pg_default;
