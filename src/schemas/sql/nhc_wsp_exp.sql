-- ============================================================================
-- NHC Wind Speed Probability (WSP) population exposure
-- ============================================================================
-- One row per (issued_time, wind_threshold_kt, percentage, atcf_id,
-- admin_level, pcode): the population exposed within that admin unit to the
-- given WSP polygon.
--
-- Designed to hold exposure at any admin level. Current data is admin_level=0
-- (country). Subnational rows (admin_level >= 1) use the same table.
--
-- Populated by running: scripts/calc_wsp_adm0_exp.py
-- Source: storms.nhc_wsp_polygon (geometry) x WorldPop 1km raster x
--         FieldMaps ADM boundaries
-- ============================================================================

-- DROP TABLE IF EXISTS storms.nhc_wsp_exposure CASCADE;

CREATE TABLE IF NOT EXISTS storms.nhc_wsp_exposure
(
    issued_time       TIMESTAMP   NOT NULL,
    wind_threshold_kt SMALLINT    NOT NULL CHECK (wind_threshold_kt IN (34, 50, 64)),
    percentage        SMALLINT    NOT NULL,
    atcf_id           VARCHAR,
    admin_level       SMALLINT    NOT NULL,
    iso3              VARCHAR(3)  NOT NULL,
    pcode             VARCHAR(20) NOT NULL,
    pop_exposed       INTEGER     NOT NULL,
    CONSTRAINT nhc_wsp_exposure_unique
        UNIQUE NULLS NOT DISTINCT (issued_time, wind_threshold_kt, percentage, atcf_id, admin_level, pcode)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.nhc_wsp_exposure
    OWNER to {owner};

COMMENT ON TABLE storms.nhc_wsp_exposure IS
    'Population exposure to NHC wind speed probability polygons, by admin unit. admin_level=0 is country level.';
COMMENT ON COLUMN storms.nhc_wsp_exposure.issued_time IS
    'WSP forecast issuance timestamp (UTC)';
COMMENT ON COLUMN storms.nhc_wsp_exposure.wind_threshold_kt IS
    'Wind speed threshold in knots (34, 50, or 64)';
COMMENT ON COLUMN storms.nhc_wsp_exposure.percentage IS
    'WSP probability band as integer percentage (e.g. 5, 10, 20, 30)';
COMMENT ON COLUMN storms.nhc_wsp_exposure.atcf_id IS
    'ATCF storm identifier matched from storms.nhc_tracks_geo (NULL if no track found)';
COMMENT ON COLUMN storms.nhc_wsp_exposure.admin_level IS
    'Administrative level (0 = country, 1 = first subnational, etc.)';
COMMENT ON COLUMN storms.nhc_wsp_exposure.iso3 IS
    'ISO 3166-1 alpha-3 country code';
COMMENT ON COLUMN storms.nhc_wsp_exposure.pcode IS
    'Admin unit p-code (ISO3 at admin_level=0, country-prefixed pcode for subnational)';
COMMENT ON COLUMN storms.nhc_wsp_exposure.pop_exposed IS
    'Population count exposed within this admin unit to this WSP polygon (WorldPop 2026 1km) — 0 means checked but no overlap or no population';

-- ============================================================================
-- Indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_nhc_wsp_exposure_issued_time
    ON storms.nhc_wsp_exposure (issued_time)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_nhc_wsp_exposure_atcf_id
    ON storms.nhc_wsp_exposure (atcf_id)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_nhc_wsp_exposure_pcode
    ON storms.nhc_wsp_exposure (pcode)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_nhc_wsp_exposure_iso3
    ON storms.nhc_wsp_exposure (iso3)
    TABLESPACE pg_default;
