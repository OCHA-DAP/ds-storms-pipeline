-- ============================================================================
-- NHC observed track cumulative wind buffer polygons schema
-- ============================================================================
-- One row per (atcf_id, valid_time, wind_speed_kt): the cumulative union of
-- all observed (leadtime=0) wind buffer discs up to and including that
-- advisory. Each row is a growing swath of where the storm has been.
--
-- Populated by running: python run_pipeline.py nhc-tracks-obsv-buffers
-- Reads from PROD storms.nhc_tracks_geo, writes here (DEV).
-- ============================================================================

-- DROP TABLE IF EXISTS storms.nhc_tracks_obsv_buffers CASCADE;

CREATE TABLE IF NOT EXISTS storms.nhc_tracks_obsv_buffers
(
    atcf_id       VARCHAR   NOT NULL,
    valid_time    TIMESTAMP NOT NULL,
    wind_speed_kt SMALLINT  NOT NULL CHECK (wind_speed_kt IN (34, 50, 64)),
    geometry      geometry(Geometry, 4326),
    CONSTRAINT nhc_tracks_obsv_buffers_unique UNIQUE (atcf_id, valid_time, wind_speed_kt)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.nhc_tracks_obsv_buffers
    OWNER to {owner};

COMMENT ON TABLE storms.nhc_tracks_obsv_buffers IS
    'Cumulative observed-track wind buffer polygons: union of all leadtime=0 wind buffers up to each advisory';
COMMENT ON COLUMN storms.nhc_tracks_obsv_buffers.atcf_id IS
    'ATCF storm identifier (e.g. AL092023) - links to nhc_storms';
COMMENT ON COLUMN storms.nhc_tracks_obsv_buffers.valid_time IS
    'Most recent advisory issued_time included in this cumulative buffer (UTC)';
COMMENT ON COLUMN storms.nhc_tracks_obsv_buffers.wind_speed_kt IS
    'Wind speed threshold in knots (34, 50, or 64)';
COMMENT ON COLUMN storms.nhc_tracks_obsv_buffers.geometry IS
    'Cumulative union of wind buffer discs from all observed positions up to valid_time in WGS84 (EPSG:4326). '
    'May be Polygon or MultiPolygon.';

-- ============================================================================
-- Indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_nhc_tracks_obsv_buffers_geometry
    ON storms.nhc_tracks_obsv_buffers USING gist (geometry)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_nhc_tracks_obsv_buffers_atcf_id
    ON storms.nhc_tracks_obsv_buffers (atcf_id)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_nhc_tracks_obsv_buffers_valid_time
    ON storms.nhc_tracks_obsv_buffers (valid_time)
    TABLESPACE pg_default;
