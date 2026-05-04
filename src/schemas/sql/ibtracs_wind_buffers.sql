-- ============================================================================
-- IBTrACS wind buffer polygons schema
-- ============================================================================
-- One row per (sid, wind_speed_kt): the union of all wind buffer discs along
-- the storm track at that wind speed threshold.
--
-- Populated by running: python run_pipeline.py wind-buffers
-- Reads tracks from PROD storms.ibtracs_tracks_geo, writes here (DEV).
-- ============================================================================

-- DROP TABLE IF EXISTS storms.ibtracs_wind_buffers CASCADE;

CREATE TABLE IF NOT EXISTS storms.ibtracs_wind_buffers
(
    sid           VARCHAR  NOT NULL,
    wind_speed_kt SMALLINT NOT NULL CHECK (wind_speed_kt IN (34, 50, 64)),
    geometry      geometry(Geometry, 4326),
    CONSTRAINT ibtracs_wind_buffers_unique UNIQUE (sid, wind_speed_kt)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.ibtracs_wind_buffers
    OWNER to {owner};

COMMENT ON TABLE storms.ibtracs_wind_buffers IS
    'Per-storm wind buffer polygons derived from IBTrACS USA quadrant wind radii';
COMMENT ON COLUMN storms.ibtracs_wind_buffers.sid IS
    'IBTrACS serial identifier - foreign key to ibtracs_storms';
COMMENT ON COLUMN storms.ibtracs_wind_buffers.wind_speed_kt IS
    'Wind speed threshold in knots (34, 50, or 64)';
COMMENT ON COLUMN storms.ibtracs_wind_buffers.geometry IS
    'Union of all wind buffer discs along the storm track in WGS84 (EPSG:4326). '
    'May be Polygon or MultiPolygon (e.g. for antimeridian-crossing storms).';

-- ============================================================================
-- Indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_ibtracs_wind_buffers_geometry
    ON storms.ibtracs_wind_buffers USING gist (geometry)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_ibtracs_wind_buffers_sid
    ON storms.ibtracs_wind_buffers (sid)
    TABLESPACE pg_default;
