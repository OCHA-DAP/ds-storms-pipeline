-- ============================================================================
-- WFP ADAM cumulative wind exposure
-- ============================================================================
-- One row per (adam_eventid, adam_episodeid, wind_speed_kt, admin_level,
-- pcode): population exposed within that admin unit to the ADAM cumulative
-- wind footprint at that episode snapshot.
--
-- Designed to hold exposure at any admin level. admin_level=0 is country
-- level (aggregated from ADM0_NAME in ADAM's population CSV). admin_level=1
-- subnational rows (from ADM1_NAME) use the same table.
--
-- Populated by running: python run_pipeline.py adam-exp
-- Source: WFP ADAM OGC API (adam.adam_ts_events collection) + per-event
--         population_csv_url downloads
-- ============================================================================

-- DROP TABLE IF EXISTS storms.adam_exposure CASCADE;

CREATE TABLE IF NOT EXISTS storms.adam_exposure
(
    adam_eventid     INTEGER     NOT NULL,
    adam_episodeid   INTEGER     NOT NULL,
    gdacs_eventid    INTEGER,
    atcf_id          VARCHAR,
    valid_time       TIMESTAMP   NOT NULL,
    wind_speed_kt    SMALLINT    NOT NULL CHECK (wind_speed_kt IN (34, 50, 64)),
    admin_level      SMALLINT    NOT NULL,
    iso3             VARCHAR(3)  NOT NULL,
    pcode            VARCHAR(20) NOT NULL,
    pop_exposed      INTEGER     NOT NULL,
    CONSTRAINT adam_exposure_unique
        UNIQUE (adam_eventid, adam_episodeid, wind_speed_kt, admin_level, pcode)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.adam_exposure
    OWNER to {owner};

COMMENT ON TABLE storms.adam_exposure IS
    'Population exposure to WFP ADAM cumulative wind footprint, by admin unit. admin_level=0 is country level.';
COMMENT ON COLUMN storms.adam_exposure.adam_eventid IS
    'ADAM native event identifier (integer; ADAM also exposes a string ``uid`` field which is not stored here)';
COMMENT ON COLUMN storms.adam_exposure.adam_episodeid IS
    'ADAM episode identifier — one episode per advisory; latest-per-event used in the historical compile';
COMMENT ON COLUMN storms.adam_exposure.gdacs_eventid IS
    'GDACS event identifier when linkable (ADAM ingests GDACS feeds for some sources); nullable';
COMMENT ON COLUMN storms.adam_exposure.atcf_id IS
    'ATCF storm identifier (e.g. AL092023); nullable, populated by the ADAM->ATCF matching function once available';
COMMENT ON COLUMN storms.adam_exposure.valid_time IS
    'Timestamp of the episode (most recent advisory included in the cumulative footprint, UTC)';
COMMENT ON COLUMN storms.adam_exposure.wind_speed_kt IS
    'Wind speed threshold in knots (34, 50, or 64). ADAM 60/90/120 km/h thresholds are mapped to 34/50/64 kt for cross-source comparability.';
COMMENT ON COLUMN storms.adam_exposure.admin_level IS
    'Administrative level (0 = country, 1 = first subnational, etc.)';
COMMENT ON COLUMN storms.adam_exposure.iso3 IS
    'ISO 3166-1 alpha-3 country code';
COMMENT ON COLUMN storms.adam_exposure.pcode IS
    'Admin unit p-code (ISO3 at admin_level=0, country-prefixed pcode for subnational)';
COMMENT ON COLUMN storms.adam_exposure.pop_exposed IS
    'Population count exposed within this admin unit to this wind threshold, as reported by ADAM';

-- ============================================================================
-- Indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_adam_exposure_adam_eventid
    ON storms.adam_exposure (adam_eventid)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_adam_exposure_gdacs_eventid
    ON storms.adam_exposure (gdacs_eventid)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_adam_exposure_atcf_id
    ON storms.adam_exposure (atcf_id)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_adam_exposure_valid_time
    ON storms.adam_exposure (valid_time)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_adam_exposure_pcode
    ON storms.adam_exposure (pcode)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_adam_exposure_iso3
    ON storms.adam_exposure (iso3)
    TABLESPACE pg_default;
