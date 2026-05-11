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
    atcf_id          VARCHAR,
    valid_time       TIMESTAMP   NOT NULL,
    wind_speed_kt    SMALLINT    NOT NULL CHECK (wind_speed_kt IN (34, 50, 64)),
    admin_level      SMALLINT    NOT NULL,
    iso3             VARCHAR(3)  NOT NULL,
    pcode            VARCHAR(20) NOT NULL,
    pop_exposed      INTEGER     NOT NULL,
    CONSTRAINT gdacs_exposure_unique
        UNIQUE (gdacs_eventid, gdacs_episodeid, wind_speed_kt, admin_level, pcode)
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
COMMENT ON COLUMN storms.gdacs_exposure.atcf_id IS
    'ATCF storm identifier (e.g. AL092023); nullable, populated by the GDACS->ATCF matching function once available';
COMMENT ON COLUMN storms.gdacs_exposure.valid_time IS
    'Timestamp of the episode (most recent advisory included in the cumulative buffer, UTC)';
COMMENT ON COLUMN storms.gdacs_exposure.wind_speed_kt IS
    'Wind speed threshold in knots (34, 50, or 64). GDACS buffer39 maps to 34 kt, buffer74 maps to 64 kt; 50 kt is not produced by GDACS.';
COMMENT ON COLUMN storms.gdacs_exposure.admin_level IS
    'Administrative level (0 = country, 1 = first subnational, etc.)';
COMMENT ON COLUMN storms.gdacs_exposure.iso3 IS
    'ISO 3166-1 alpha-3 country code';
COMMENT ON COLUMN storms.gdacs_exposure.pcode IS
    'Admin unit p-code (ISO3 at admin_level=0, country-prefixed pcode for subnational)';
COMMENT ON COLUMN storms.gdacs_exposure.pop_exposed IS
    'Population count exposed within this admin unit to this wind buffer, as reported by GDACS';

-- ============================================================================
-- Indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_gdacs_exposure_gdacs_eventid
    ON storms.gdacs_exposure (gdacs_eventid)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_gdacs_exposure_atcf_id
    ON storms.gdacs_exposure (atcf_id)
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
