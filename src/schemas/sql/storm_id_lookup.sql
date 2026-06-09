-- ============================================================================
-- Storm ID lookup — cross-source identifier registry
-- ============================================================================
-- Maps a single storm's identifiers across data sources. The matching
-- pipeline (run_pipeline.py match) writes (gdacs_eventid, atcf_id) pairs
-- here; future enrichment steps will fill in sid (IBTrACS) and adam_eventid.
--
-- Downstream tools query this table to discover which data sources have
-- already ingested + linked a given storm. Joining
-- e.g. storms.gdacs_exposure to storms.nhc_tracks_obsv_exposure for the same
-- storm is: JOIN through storm_id_lookup on the matching id pair.
--
-- ----------------------------------------------------------------------------
-- MVP NOTE on uniqueness model:
--
-- Right now `gdacs_eventid` is the PRIMARY KEY because the only writer
-- (the GDACS->ATCF matching pipeline) always has a gdacs_eventid in
-- hand. This means we cannot yet have rows for an NHC storm with no
-- GDACS counterpart, or an IBTrACS storm with neither.
--
-- When that becomes necessary (e.g., an NHC-side or IBTrACS-side
-- registration pipeline lands), we'll need to:
--   1. Drop the PK on gdacs_eventid
--   2. Add a synthetic SERIAL primary key
--   3. Add partial unique indexes on each source-native column where
--      not null (atcf_id, sid, gdacs_eventid, adam_eventid)
--
-- That migration will be a single, explicit, reviewable change.
-- ============================================================================

-- DROP TABLE IF EXISTS storms.storm_id_lookup CASCADE;

CREATE TABLE IF NOT EXISTS storms.storm_id_lookup
(
    gdacs_eventid INTEGER     NOT NULL,
    atcf_id       VARCHAR,
    sid           VARCHAR,
    adam_eventid  INTEGER,
    last_updated  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT storm_id_lookup_pkey PRIMARY KEY (gdacs_eventid)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.storm_id_lookup
    OWNER to {owner};

COMMENT ON TABLE storms.storm_id_lookup IS
    'Cross-source storm identifier registry. One row per storm (indexed by gdacs_eventid in the MVP shape).';
COMMENT ON COLUMN storms.storm_id_lookup.gdacs_eventid IS
    'GDACS native event id. Primary key in the MVP — every row originates from a GDACS event ingest.';
COMMENT ON COLUMN storms.storm_id_lookup.atcf_id IS
    'NHC ATCF storm id (e.g. AL142024); nullable. Populated by run_pipeline.py match.';
COMMENT ON COLUMN storms.storm_id_lookup.sid IS
    'IBTrACS serial id; nullable. Populated by a future IBTrACS-side enrichment step.';
COMMENT ON COLUMN storms.storm_id_lookup.adam_eventid IS
    'WFP ADAM event id; nullable. Often equal to gdacs_eventid (ADAM ingests GDACS upstream), confirmed by a future ADAM-side enrichment step.';
COMMENT ON COLUMN storms.storm_id_lookup.last_updated IS
    'Auto-set on insert; bump on update via the matching pipeline if you want to track linkage churn.';

-- ============================================================================
-- Indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_storm_id_lookup_atcf_id
    ON storms.storm_id_lookup (atcf_id)
    TABLESPACE pg_default
    WHERE atcf_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_storm_id_lookup_sid
    ON storms.storm_id_lookup (sid)
    TABLESPACE pg_default
    WHERE sid IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_storm_id_lookup_adam_eventid
    ON storms.storm_id_lookup (adam_eventid)
    TABLESPACE pg_default
    WHERE adam_eventid IS NOT NULL;
