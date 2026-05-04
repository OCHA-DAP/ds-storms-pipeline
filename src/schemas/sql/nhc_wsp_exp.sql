-- ============================================================================
-- NHC Wind Speed Probability (WSP) population exposure at ADM0 level
-- ============================================================================
-- One row per (issued_time, wind_threshold_kt, percentage, atcf_id, adm0_pcode):
-- the population exposed within that country to the given WSP polygon.
--
-- Populated by running: examples/adm0_wsp_exp.py (marimo)
-- Source: storms.nhc_wsp_polygon (geometry) x WorldPop 1km raster x
--         FieldMaps ADM1 boundaries dissolved to ADM0
-- ============================================================================

-- DROP TABLE IF EXISTS storms.nhc_wsp_adm0_exp CASCADE;

CREATE TABLE IF NOT EXISTS storms.nhc_wsp_adm0_exp
(
    issued_time       TIMESTAMP   NOT NULL,
    wind_threshold_kt SMALLINT    NOT NULL CHECK (wind_threshold_kt IN (34, 50, 64)),
    percentage        SMALLINT    NOT NULL,
    atcf_id           VARCHAR,
    adm0_pcode        VARCHAR(10) NOT NULL,
    pop_exposed       INTEGER     NOT NULL,
    CONSTRAINT nhc_wsp_adm0_exp_unique
        UNIQUE NULLS NOT DISTINCT (issued_time, wind_threshold_kt, percentage, atcf_id, adm0_pcode)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.nhc_wsp_adm0_exp
    OWNER to {owner};

COMMENT ON TABLE storms.nhc_wsp_adm0_exp IS
    'Population exposure to NHC wind speed probability polygons, aggregated at ADM0 level';
COMMENT ON COLUMN storms.nhc_wsp_adm0_exp.issued_time IS
    'WSP forecast issuance timestamp (UTC)';
COMMENT ON COLUMN storms.nhc_wsp_adm0_exp.wind_threshold_kt IS
    'Wind speed threshold in knots (34, 50, or 64)';
COMMENT ON COLUMN storms.nhc_wsp_adm0_exp.percentage IS
    'WSP probability band as integer percentage (e.g. 5, 10, 20, 30)';
COMMENT ON COLUMN storms.nhc_wsp_adm0_exp.atcf_id IS
    'ATCF storm identifier matched from storms.nhc_tracks_geo; NULL if no track found';
COMMENT ON COLUMN storms.nhc_wsp_adm0_exp.adm0_pcode IS
    'Country p-code from FieldMaps humanitarian boundaries';
COMMENT ON COLUMN storms.nhc_wsp_adm0_exp.pop_exposed IS
    'Population count exposed within this country to this WSP polygon (WorldPop 2026 1km)';

-- ============================================================================
-- Indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_nhc_wsp_adm0_exp_issued_time
    ON storms.nhc_wsp_adm0_exp (issued_time)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_nhc_wsp_adm0_exp_atcf_id
    ON storms.nhc_wsp_adm0_exp (atcf_id)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_nhc_wsp_adm0_exp_adm0_pcode
    ON storms.nhc_wsp_adm0_exp (adm0_pcode)
    TABLESPACE pg_default;
