-- ============================================================================
-- GDACS cumulative wind buffer population exposure
-- ============================================================================
-- One row per (gdacs_eventid, gdacs_episodeid, wind_speed_kt, admin_level,
-- pcode): population exposed within that admin unit to the GDACS cumulative
-- wind buffer at that episode (advisory) snapshot.
--
-- Designed to hold exposure at any admin level. admin_level=0 is country
-- level (from the GDACS impact-buffer alias='country' datums). admin_level=1
-- subnational rows (from alias='alert' datums) use the same table.
--
-- Populated by running: python run_pipeline.py gdacs-exp
-- Source: GDACS /api/events/getepisodedata + per-buffer impact JSONs
--         (read via ocha_lens.datasources.gdacs.get_exposure_adm0/adm1)
-- ============================================================================

-- DROP TABLE IF EXISTS storms.gdacs_exposure CASCADE;

CREATE TABLE IF NOT EXISTS storms.gdacs_exposure
(
    gdacs_eventid    INTEGER     NOT NULL,
    gdacs_episodeid  INTEGER     NOT NULL,
    valid_time       TIMESTAMP   NOT NULL,
    wind_speed_kt    SMALLINT    NOT NULL CHECK (wind_speed_kt IN (34, 50, 64)),
    admin_level      SMALLINT    NOT NULL,
    iso3             VARCHAR(3)  NOT NULL,
    admin_name       VARCHAR     NOT NULL,
    gdacs_admin_code VARCHAR     NOT NULL,
    pcode            VARCHAR(20),
    pop_exposed      INTEGER,
    CONSTRAINT gdacs_exposure_unique
        UNIQUE (gdacs_eventid, gdacs_episodeid, wind_speed_kt, admin_level, iso3, admin_name)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.gdacs_exposure
    OWNER to {owner};

COMMENT ON TABLE storms.gdacs_exposure IS
    'Population exposure to GDACS cumulative wind buffer polygons, by admin unit. admin_level=0 is country level.';
COMMENT ON COLUMN storms.gdacs_exposure.gdacs_eventid IS
    'GDACS native event identifier (e.g. 1001067 for BERYL-24)';
COMMENT ON COLUMN storms.gdacs_exposure.gdacs_episodeid IS
    'GDACS episode (advisory) identifier — one episode per ~6h model run';
COMMENT ON COLUMN storms.gdacs_exposure.valid_time IS
    'Timestamp of the episode (most recent advisory included in the cumulative buffer, UTC)';
COMMENT ON COLUMN storms.gdacs_exposure.wind_speed_kt IS
    'Wind speed threshold in knots (34, 50, or 64). GDACS buffer39 maps to 34 kt, buffer74 maps to 64 kt; 50 kt is not produced by GDACS.';
COMMENT ON COLUMN storms.gdacs_exposure.admin_level IS
    'Administrative level (0 = country, 1 = first subnational, etc.)';
COMMENT ON COLUMN storms.gdacs_exposure.iso3 IS
    'ISO 3166-1 alpha-3 country code (parent country at admin_level=1). Standardized via ocha_lens.datasources.gdacs.to_iso3() — GDACS proprietary codes like XJE (Jersey) are remapped to the official ISO3 (JEY).';
COMMENT ON COLUMN storms.gdacs_exposure.admin_name IS
    'Admin unit name as reported by GDACS — CNTRY_NAME at admin_level=0, ADMIN_NAME at admin_level=1. Always populated.';
COMMENT ON COLUMN storms.gdacs_exposure.gdacs_admin_code IS
    'Native GDACS admin code preserved as-is. At admin_level=0: GMI_CNTRY (= iso3 except for territories with X-prefixed proprietary codes). At admin_level=1: GMI_ADMIN (e.g., USA-ARK for Arkansas, XJE-JE for Jersey-as-region) — country-prefixed proprietary, not a standard humanitarian pcode.';
COMMENT ON COLUMN storms.gdacs_exposure.pcode IS
    'Admin unit p-code. Nullable: populated as iso3 at admin_level=0; null at admin_level=1 until backfilled by a downstream enrichment step (GDACS only emits proprietary FIPS_ADMIN/GMI_ADMIN codes, not standard pcodes).';
COMMENT ON COLUMN storms.gdacs_exposure.pop_exposed IS
    'Population count exposed within this admin unit to this wind buffer, as reported by GDACS. NULL when GDACS legitimately omits POP_AFFECTED (typically: country intersected the wind footprint geometrically but no population is exposed at that wind threshold).';

-- ============================================================================
-- Indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_gdacs_exposure_gdacs_eventid
    ON storms.gdacs_exposure (gdacs_eventid)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_gdacs_exposure_valid_time
    ON storms.gdacs_exposure (valid_time)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_gdacs_exposure_pcode
    ON storms.gdacs_exposure (pcode)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_gdacs_exposure_iso3
    ON storms.gdacs_exposure (iso3)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_gdacs_exposure_gdacs_admin_code
    ON storms.gdacs_exposure (gdacs_admin_code)
    TABLESPACE pg_default;
