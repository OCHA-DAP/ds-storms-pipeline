-- ============================================================================
-- IBTrACS wind buffer population exposure
-- ============================================================================
-- One row per (sid, wind_speed_kt, admin_level, pcode): population exposed
-- within that admin unit to the given storm's wind buffer polygon.
--
-- Designed to hold exposure at any admin level. Current data is admin_level=0
-- (country). Subnational rows (admin_level >= 1) use the same table.
--
-- Populated by running: python run_pipeline.py ibtracs-track-exp
-- Source: storms.ibtracs_wind_buffers (geometry) x WorldPop 1km raster x
--         FieldMaps ADM boundaries
-- ============================================================================

-- DROP TABLE IF EXISTS storms.ibtracs_wind_exposure CASCADE;

CREATE TABLE IF NOT EXISTS storms.ibtracs_wind_exposure
(
    sid           VARCHAR     NOT NULL,
    wind_speed_kt SMALLINT    NOT NULL CHECK (wind_speed_kt IN (34, 50, 64)),
    admin_level   SMALLINT    NOT NULL,
    iso3          VARCHAR(3)  NOT NULL,
    pcode         VARCHAR(20) NOT NULL,
    pop_exposed   INTEGER     NOT NULL,
    CONSTRAINT ibtracs_wind_exposure_unique
        UNIQUE (sid, wind_speed_kt, admin_level, pcode)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.ibtracs_wind_exposure
    OWNER to {owner};

COMMENT ON TABLE storms.ibtracs_wind_exposure IS
    'Population exposure to IBTrACS wind buffer polygons, by admin unit. admin_level=0 is country level.';
COMMENT ON COLUMN storms.ibtracs_wind_exposure.sid IS
    'IBTrACS serial identifier (foreign key to storms.ibtracs_storms)';
COMMENT ON COLUMN storms.ibtracs_wind_exposure.wind_speed_kt IS
    'Wind speed threshold in knots (34, 50, or 64)';
COMMENT ON COLUMN storms.ibtracs_wind_exposure.admin_level IS
    'Administrative level (0 = country, 1 = first subnational, etc.)';
COMMENT ON COLUMN storms.ibtracs_wind_exposure.iso3 IS
    'ISO 3166-1 alpha-3 country code';
COMMENT ON COLUMN storms.ibtracs_wind_exposure.pcode IS
    'Admin unit p-code (ISO3 at admin_level=0, country-prefixed pcode for subnational)';
COMMENT ON COLUMN storms.ibtracs_wind_exposure.pop_exposed IS
    'Population count exposed within this admin unit to this wind buffer (WorldPop 2026 1km) — 0 means checked but no overlap or no population';

-- ============================================================================
-- Indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_ibtracs_wind_exposure_sid
    ON storms.ibtracs_wind_exposure (sid)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_ibtracs_wind_exposure_pcode
    ON storms.ibtracs_wind_exposure (pcode)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_ibtracs_wind_exposure_iso3
    ON storms.ibtracs_wind_exposure (iso3)
    TABLESPACE pg_default;
