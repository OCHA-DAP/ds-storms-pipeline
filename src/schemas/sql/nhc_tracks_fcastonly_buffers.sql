-- ============================================================================
-- NHC forecast-only wind buffer polygons schema
-- ============================================================================
-- One row per (atcf_id, issued_time, wind_speed_kt): the part of the forecast
-- buffer that does NOT overlap with the cumulative observed track buffer at
-- that issued_time. I.e. fcast_geometry.difference(obsv_geometry).
--
-- Populated by running: python run_pipeline.py nhc-tracks-fcastonly-buffers
-- Reads from storms.nhc_tracks_fcast_buffers and storms.nhc_tracks_obsv_buffers.
-- ============================================================================

-- DROP TABLE IF EXISTS storms.nhc_tracks_fcastonly_buffers CASCADE;

CREATE TABLE IF NOT EXISTS storms.nhc_tracks_fcastonly_buffers
(
    atcf_id       VARCHAR   NOT NULL,
    issued_time   TIMESTAMP NOT NULL,
    wind_speed_kt SMALLINT  NOT NULL CHECK (wind_speed_kt IN (34, 50, 64)),
    geometry      geometry(Geometry, 4326),
    CONSTRAINT nhc_tracks_fcastonly_buffers_unique UNIQUE (atcf_id, issued_time, wind_speed_kt)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.nhc_tracks_fcastonly_buffers
    OWNER to {owner};

COMMENT ON TABLE storms.nhc_tracks_fcastonly_buffers IS
    'Forecast-only wind buffer polygons: forecast buffer minus the cumulative observed track swath at that issued_time';
COMMENT ON COLUMN storms.nhc_tracks_fcastonly_buffers.atcf_id IS
    'ATCF storm identifier (e.g. AL092023) - links to nhc_storms';
COMMENT ON COLUMN storms.nhc_tracks_fcastonly_buffers.issued_time IS
    'Forecast issuance time (UTC)';
COMMENT ON COLUMN storms.nhc_tracks_fcastonly_buffers.wind_speed_kt IS
    'Wind speed threshold in knots (34, 50, or 64)';
COMMENT ON COLUMN storms.nhc_tracks_fcastonly_buffers.geometry IS
    'Forecast buffer minus observed track swath in WGS84 (EPSG:4326). NULL if forecast is fully covered by observed track.';

-- ============================================================================
-- Indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_nhc_tracks_fcastonly_buffers_geometry
    ON storms.nhc_tracks_fcastonly_buffers USING gist (geometry)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_nhc_tracks_fcastonly_buffers_atcf_id
    ON storms.nhc_tracks_fcastonly_buffers (atcf_id)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_nhc_tracks_fcastonly_buffers_issued_time
    ON storms.nhc_tracks_fcastonly_buffers (issued_time)
    TABLESPACE pg_default;
