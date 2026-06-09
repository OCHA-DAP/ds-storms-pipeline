-- ============================================================================
-- NHC forecast-only track buffer population exposure
-- ============================================================================
-- One row per (atcf_id, issued_time, wind_speed_kt, admin_level, pcode):
-- population exposed within that admin unit to the forecast-only wind buffer
-- (forecast buffer minus cumulative observed track swath).
--
-- Designed to hold exposure at any admin level. Current data is admin_level=0
-- (country). Subnational rows (admin_level >= 1) use the same table.
--
-- Populated by running: python run_pipeline.py nhc-fcastonly-exp
-- Source: storms.nhc_tracks_fcastonly_buffers (geometry) x WorldPop 1km raster x
--         FieldMaps ADM boundaries
-- ============================================================================

-- DROP TABLE IF EXISTS storms.nhc_tracks_fcastonly_exposure CASCADE;

CREATE TABLE IF NOT EXISTS storms.nhc_tracks_fcastonly_exposure
(
    atcf_id       VARCHAR     NOT NULL,
    issued_time   TIMESTAMP   NOT NULL,
    wind_speed_kt SMALLINT    NOT NULL CHECK (wind_speed_kt IN (34, 50, 64)),
    admin_level   SMALLINT    NOT NULL,
    iso3          VARCHAR(3)  NOT NULL,
    pcode         VARCHAR(20) NOT NULL,
    pop_exposed   INTEGER     NOT NULL,
    CONSTRAINT nhc_tracks_fcastonly_exposure_unique
        UNIQUE (atcf_id, issued_time, wind_speed_kt, admin_level, pcode)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.nhc_tracks_fcastonly_exposure
    OWNER to {owner};

COMMENT ON TABLE storms.nhc_tracks_fcastonly_exposure IS
    'Population exposure to NHC forecast-only wind buffer polygons (forecast minus observed swath), by admin unit. admin_level=0 is country level.';
COMMENT ON COLUMN storms.nhc_tracks_fcastonly_exposure.atcf_id IS
    'ATCF storm identifier (e.g. AL092023)';
COMMENT ON COLUMN storms.nhc_tracks_fcastonly_exposure.issued_time IS
    'Forecast issuance timestamp (UTC)';
COMMENT ON COLUMN storms.nhc_tracks_fcastonly_exposure.wind_speed_kt IS
    'Wind speed threshold in knots (34, 50, or 64)';
COMMENT ON COLUMN storms.nhc_tracks_fcastonly_exposure.admin_level IS
    'Administrative level (0 = country, 1 = first subnational, etc.)';
COMMENT ON COLUMN storms.nhc_tracks_fcastonly_exposure.iso3 IS
    'ISO 3166-1 alpha-3 country code';
COMMENT ON COLUMN storms.nhc_tracks_fcastonly_exposure.pcode IS
    'Admin unit p-code (ISO3 at admin_level=0, country-prefixed pcode for subnational)';
COMMENT ON COLUMN storms.nhc_tracks_fcastonly_exposure.pop_exposed IS
    'Population count exposed within this admin unit to this wind buffer (WorldPop 2026 1km) — 0 means checked but no overlap or no population';

-- ============================================================================
-- Indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_nhc_tracks_fcastonly_exposure_atcf_id
    ON storms.nhc_tracks_fcastonly_exposure (atcf_id)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_nhc_tracks_fcastonly_exposure_issued_time
    ON storms.nhc_tracks_fcastonly_exposure (issued_time)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_nhc_tracks_fcastonly_exposure_pcode
    ON storms.nhc_tracks_fcastonly_exposure (pcode)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_nhc_tracks_fcastonly_exposure_iso3
    ON storms.nhc_tracks_fcastonly_exposure (iso3)
    TABLESPACE pg_default;
