-- ============================================================================
-- NHC forecast wind buffer polygons schema
-- ============================================================================
-- One row per (atcf_id, issued_time, wind_speed_kt): the union of all wind
-- buffer discs across the forecast track for that issuance.
--
-- Populated by running: python run_pipeline.py nhc-tracks-fcast-buffers
-- Reads from PROD storms.nhc_tracks_geo, writes here (DEV).
-- ============================================================================

-- DROP TABLE IF EXISTS storms.nhc_tracks_fcast_buffers CASCADE;

CREATE TABLE IF NOT EXISTS storms.nhc_tracks_fcast_buffers
(
    atcf_id       VARCHAR   NOT NULL,
    issued_time   TIMESTAMP NOT NULL,
    wind_speed_kt SMALLINT  NOT NULL CHECK (wind_speed_kt IN (34, 50, 64)),
    geometry      geometry(Geometry, 4326),
    CONSTRAINT nhc_tracks_fcast_buffers_unique UNIQUE (atcf_id, issued_time, wind_speed_kt)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.nhc_tracks_fcast_buffers
    OWNER to {owner};

COMMENT ON TABLE storms.nhc_tracks_fcast_buffers IS
    'Per-forecast wind buffer polygons derived from NHC quadrant wind radii';
COMMENT ON COLUMN storms.nhc_tracks_fcast_buffers.atcf_id IS
    'ATCF storm identifier (e.g. AL092023) - links to nhc_storms';
COMMENT ON COLUMN storms.nhc_tracks_fcast_buffers.issued_time IS
    'Forecast issuance time (UTC)';
COMMENT ON COLUMN storms.nhc_tracks_fcast_buffers.wind_speed_kt IS
    'Wind speed threshold in knots (34, 50, or 64)';
COMMENT ON COLUMN storms.nhc_tracks_fcast_buffers.geometry IS
    'Union of wind buffer discs across the full forecast track in WGS84 (EPSG:4326). '
    'May be Polygon or MultiPolygon.';

-- ============================================================================
-- Indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_nhc_tracks_fcast_buffers_geometry
    ON storms.nhc_tracks_fcast_buffers USING gist (geometry)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_nhc_tracks_fcast_buffers_atcf_id
    ON storms.nhc_tracks_fcast_buffers (atcf_id)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_nhc_tracks_fcast_buffers_issued_time
    ON storms.nhc_tracks_fcast_buffers (issued_time)
    TABLESPACE pg_default;
