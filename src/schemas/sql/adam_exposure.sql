-- ============================================================================
-- WFP ADAM cumulative wind exposure
-- ============================================================================
-- One row per (adam_eventid, adam_episodeid, wind_speed_kt, admin_level,
-- admin_name, parent_admin_name): population exposed within that admin unit
-- to the ADAM cumulative wind footprint at that episode snapshot.
--
-- Holds exposure at three admin levels in the same table:
--   admin_level=0 — country (aggregated by pipeline from ADM0_NAME)
--   admin_level=1 — first subnational (aggregated by pipeline from ADM1_NAME)
--   admin_level=2 — second subnational (native granularity in ADAM's CSV)
--
-- Populated by: python run_pipeline.py adam
-- Source:       WFP ADAM OGC API (adam.adam_ts_events collection) +
--               per-event population_csv_url downloads
--
-- NOTE: the UNIQUE constraint uses `NULLS NOT DISTINCT` (PostgreSQL 15+).
-- iso3 and parent_admin_name are nullable but must still participate in
-- uniqueness — without this clause two rows with NULL iso3 would not
-- collide under default NULL-is-distinct semantics.
-- ============================================================================

-- DROP TABLE IF EXISTS storms.adam_exposure CASCADE;

CREATE TABLE IF NOT EXISTS storms.adam_exposure
(
    adam_eventid       INTEGER     NOT NULL,
    adam_episodeid     INTEGER     NOT NULL,
    valid_time         TIMESTAMP   NOT NULL,
    wind_speed_kt      SMALLINT    NOT NULL CHECK (wind_speed_kt IN (34, 50, 64)),
    admin_level        SMALLINT    NOT NULL,
    iso3               VARCHAR(3),
    admin_name         VARCHAR     NOT NULL,
    parent_admin_name  VARCHAR,
    pcode              VARCHAR(20),
    pop_exposed        INTEGER,
    CONSTRAINT adam_exposure_unique
        UNIQUE NULLS NOT DISTINCT
        (adam_eventid, adam_episodeid, wind_speed_kt, admin_level, iso3, admin_name, parent_admin_name)
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
COMMENT ON COLUMN storms.adam_exposure.valid_time IS
    'Timestamp of the episode (most recent advisory included in the cumulative footprint, UTC)';
COMMENT ON COLUMN storms.adam_exposure.wind_speed_kt IS
    'Wind speed threshold in knots (34, 50, or 64). ADAM 60/90/120 km/h thresholds are mapped to 34/50/64 kt for cross-source comparability.';
COMMENT ON COLUMN storms.adam_exposure.admin_level IS
    'Administrative level (0 = country, 1 = first subnational, 2 = second subnational). ADAM CSV is at admin_level=2; admin_level=0 and =1 rows are aggregated by the pipeline.';
COMMENT ON COLUMN storms.adam_exposure.iso3 IS
    'ISO 3166-1 alpha-3 country code (parent country at admin_level=1,2). Nullable when ADAM''s ADM0_NAME cannot be mapped via pycountry + overrides (rare: disputed territories).';
COMMENT ON COLUMN storms.adam_exposure.admin_name IS
    'Admin unit name as reported by ADAM — ADM0_NAME at admin_level=0, ADM1_NAME at admin_level=1, ADM2_NAME at admin_level=2. Always populated.';
COMMENT ON COLUMN storms.adam_exposure.parent_admin_name IS
    'Name of the parent admin unit — null at admin_level=0, ADM0_NAME at admin_level=1, ADM1_NAME at admin_level=2. Lets consumers walk the hierarchy without joins.';
COMMENT ON COLUMN storms.adam_exposure.pcode IS
    'Admin unit p-code. Nullable: populated as iso3 at admin_level=0; null at admin_level=1 until backfilled by a downstream enrichment step (ADAM only emits an admin name at ADM1, no code).';
COMMENT ON COLUMN storms.adam_exposure.pop_exposed IS
    'Population count exposed within this admin unit to this wind threshold, as reported by ADAM';

-- ============================================================================
-- Indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_adam_exposure_adam_eventid
    ON storms.adam_exposure (adam_eventid)
    TABLESPACE pg_default;

-- Supports the pipeline's idempotency lookup
--   SELECT DISTINCT adam_eventid, adam_episodeid FROM storms.adam_exposure
-- which is run on every pipeline invocation. Without this, that DISTINCT
-- degrades to a full table scan as the row count grows (~700 rows/event).
CREATE INDEX IF NOT EXISTS idx_adam_exposure_event_episode
    ON storms.adam_exposure (adam_eventid, adam_episodeid)
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
