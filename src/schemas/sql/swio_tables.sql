-- ============================================================================
-- South-West Indian Ocean (RSMC La Réunion / Météo-France) wind swaths and
-- population exposure
-- ============================================================================
-- Written by src/pipelines/swio.py, from the tables in mfr_tables.sql. Two
-- families:
--
--   Realtime, from Météo-France forecasts (storms.mfr_tracks_geo):
--     storms.mfr_tracks_{fcast,obsv,fcastonly}_buffers  + _exposure
--     storms.mfr_tracks_scenario_exposure    (36 uncertainty-shifted tracks)
--     storms.mfr_tracks_scenario_envelope    (union of the shifted swaths)
--     storms.mfr_processing_completion       (per storm + issuance markers)
--
--   Historical, one track per system from Réunion's best track (1985/86 to
--   2022/23), else its final track, else its forecasts' analysis points
--   (topped up from IBTrACS where IBTrACS runs on past them), else IBTrACS
--   (Réunion's columns):
--     storms.swio_hist_wind_buffers + storms.swio_hist_wind_exposure
--
-- Plus storms.swio_admin_population: total population of each admin unit as
-- used here.
--
-- Conventions:
--   * Wind thresholds are Réunion's 10-minute mean winds, at Réunion's own
--     radii: 34, 48 and 64 kt (not 50: the older product's "50 kt" radius is
--     the same 48 kt radius and is read as 48).
--   * Where Réunion's wind reaches a threshold but no radius is given (all
--     radii before 2003/04; 34 and 64 kt before 2010/11; gaps after), the
--     radius is modelled from wind, latitude and central pressure, and from
--     Réunion's 48 kt radius where it exists (src/pipelines/swio_radii.py).
--   * Buffers are geodesic (true ground distances).
--   * Admin units are FieldMaps units extended 500 m into the sea and made
--     non-overlapping (src/utils/regional_exposure.py).
--   * Storms are keyed by Météo-France disturbance_id ('NN-SSSSSSSS').
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Realtime buffers
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS storms.mfr_tracks_fcast_buffers
(
    disturbance_id VARCHAR   NOT NULL,
    issued_time    TIMESTAMP NOT NULL,
    wind_speed_kt  SMALLINT  NOT NULL CHECK (wind_speed_kt IN (34, 48, 64)),
    geometry       geometry(Geometry, 4326),
    CONSTRAINT mfr_tracks_fcast_buffers_unique
        UNIQUE (disturbance_id, issued_time, wind_speed_kt)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS storms.mfr_tracks_fcast_buffers OWNER to {owner};
COMMENT ON TABLE storms.mfr_tracks_fcast_buffers IS
    'Wind swath of one Météo-France forecast track (leadtime 0 to 120 h), geodesic, 10-minute winds';

CREATE TABLE IF NOT EXISTS storms.mfr_tracks_obsv_buffers
(
    disturbance_id VARCHAR   NOT NULL,
    valid_time     TIMESTAMP NOT NULL,
    wind_speed_kt  SMALLINT  NOT NULL CHECK (wind_speed_kt IN (34, 48, 64)),
    geometry       geometry(Geometry, 4326),
    CONSTRAINT mfr_tracks_obsv_buffers_unique
        UNIQUE (disturbance_id, valid_time, wind_speed_kt)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS storms.mfr_tracks_obsv_buffers OWNER to {owner};
COMMENT ON TABLE storms.mfr_tracks_obsv_buffers IS
    'Observed swath up to valid_time: the analysis points of every forecast issued up to then (a later forecast''s analysis of a time replaces an earlier one)';

CREATE TABLE IF NOT EXISTS storms.mfr_tracks_fcastonly_buffers
(
    disturbance_id VARCHAR   NOT NULL,
    issued_time    TIMESTAMP NOT NULL,
    wind_speed_kt  SMALLINT  NOT NULL CHECK (wind_speed_kt IN (34, 48, 64)),
    geometry       geometry(Geometry, 4326),
    CONSTRAINT mfr_tracks_fcastonly_buffers_unique
        UNIQUE (disturbance_id, issued_time, wind_speed_kt)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS storms.mfr_tracks_fcastonly_buffers OWNER to {owner};
COMMENT ON TABLE storms.mfr_tracks_fcastonly_buffers IS
    'Forecast swath minus the observed swath at issued_time: the area still to come';

CREATE TABLE IF NOT EXISTS storms.mfr_tracks_scenario_envelope
(
    disturbance_id VARCHAR   NOT NULL,
    issued_time    TIMESTAMP NOT NULL,
    wind_speed_kt  SMALLINT  NOT NULL CHECK (wind_speed_kt IN (34, 48, 64)),
    n_scenarios    SMALLINT  NOT NULL,
    geometry       geometry(Geometry, 4326),
    CONSTRAINT mfr_tracks_scenario_envelope_unique
        UNIQUE (disturbance_id, issued_time, wind_speed_kt)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS storms.mfr_tracks_scenario_envelope OWNER to {owner};
COMMENT ON TABLE storms.mfr_tracks_scenario_envelope IS
    'Union of the forecast-only swath and the n_scenarios swaths of the track moved by each point''s position_accuracy towards one compass bearing';

-- ---------------------------------------------------------------------------
-- Realtime exposure
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS storms.mfr_tracks_fcast_exposure
(
    disturbance_id VARCHAR     NOT NULL,
    issued_time    TIMESTAMP   NOT NULL,
    wind_speed_kt  SMALLINT    NOT NULL CHECK (wind_speed_kt IN (34, 48, 64)),
    admin_level    SMALLINT    NOT NULL,
    iso3           VARCHAR(3)  NOT NULL,
    pcode          VARCHAR(20) NOT NULL,
    pop_exposed    INTEGER     NOT NULL,
    CONSTRAINT mfr_tracks_fcast_exposure_unique
        UNIQUE (disturbance_id, issued_time, wind_speed_kt, admin_level, pcode)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS storms.mfr_tracks_fcast_exposure OWNER to {owner};

CREATE TABLE IF NOT EXISTS storms.mfr_tracks_obsv_exposure
(
    disturbance_id VARCHAR     NOT NULL,
    valid_time     TIMESTAMP   NOT NULL,
    wind_speed_kt  SMALLINT    NOT NULL CHECK (wind_speed_kt IN (34, 48, 64)),
    admin_level    SMALLINT    NOT NULL,
    iso3           VARCHAR(3)  NOT NULL,
    pcode          VARCHAR(20) NOT NULL,
    pop_exposed    INTEGER     NOT NULL,
    CONSTRAINT mfr_tracks_obsv_exposure_unique
        UNIQUE (disturbance_id, valid_time, wind_speed_kt, admin_level, pcode)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS storms.mfr_tracks_obsv_exposure OWNER to {owner};

CREATE TABLE IF NOT EXISTS storms.mfr_tracks_fcastonly_exposure
(
    disturbance_id VARCHAR     NOT NULL,
    issued_time    TIMESTAMP   NOT NULL,
    wind_speed_kt  SMALLINT    NOT NULL CHECK (wind_speed_kt IN (34, 48, 64)),
    admin_level    SMALLINT    NOT NULL,
    iso3           VARCHAR(3)  NOT NULL,
    pcode          VARCHAR(20) NOT NULL,
    pop_exposed    INTEGER     NOT NULL,
    CONSTRAINT mfr_tracks_fcastonly_exposure_unique
        UNIQUE (disturbance_id, issued_time, wind_speed_kt, admin_level, pcode)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS storms.mfr_tracks_fcastonly_exposure OWNER to {owner};

CREATE TABLE IF NOT EXISTS storms.mfr_tracks_scenario_exposure
(
    disturbance_id VARCHAR     NOT NULL,
    issued_time    TIMESTAMP   NOT NULL,
    shift_deg      SMALLINT    NOT NULL CHECK (shift_deg >= 0 AND shift_deg < 360),
    wind_speed_kt  SMALLINT    NOT NULL CHECK (wind_speed_kt IN (34, 48, 64)),
    admin_level    SMALLINT    NOT NULL,
    iso3           VARCHAR(3)  NOT NULL,
    pcode          VARCHAR(20) NOT NULL,
    pop_exposed    INTEGER     NOT NULL,
    CONSTRAINT mfr_tracks_scenario_exposure_unique
        UNIQUE (disturbance_id, issued_time, shift_deg, wind_speed_kt, admin_level, pcode)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS storms.mfr_tracks_scenario_exposure OWNER to {owner};
COMMENT ON TABLE storms.mfr_tracks_scenario_exposure IS
    'Forecast-only exposure (admin 0) of the track moved by each point''s position_accuracy towards bearing shift_deg';

CREATE TABLE IF NOT EXISTS storms.mfr_processing_completion
(
    disturbance_id VARCHAR   NOT NULL,
    issued_time    TIMESTAMP NOT NULL,
    stage          VARCHAR   NOT NULL,
    completed_at   TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    CONSTRAINT mfr_processing_completion_pkey
        PRIMARY KEY (disturbance_id, issued_time, stage)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS storms.mfr_processing_completion OWNER to {owner};
COMMENT ON TABLE storms.mfr_processing_completion IS
    'Stages done per forecast (buffers, exposure, scenarios); a forecast ingested again after completed_at is processed again';

-- ---------------------------------------------------------------------------
-- Historical
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS storms.swio_hist_wind_buffers
(
    disturbance_id VARCHAR  NOT NULL,
    storm_id       VARCHAR,
    season         SMALLINT NOT NULL,
    track_source   VARCHAR  NOT NULL
        CHECK (track_source IN ('besttrack', 'mfr_track', 'mfr_forecasts',
                                'mfr_forecasts_ibtracs', 'ibtracs')),
    wind_speed_kt  SMALLINT NOT NULL CHECK (wind_speed_kt IN (34, 48, 64)),
    n_points       SMALLINT NOT NULL,
    n_modelled     SMALLINT NOT NULL,
    geometry       geometry(Geometry, 4326),
    CONSTRAINT swio_hist_wind_buffers_unique UNIQUE (disturbance_id, wind_speed_kt)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS storms.swio_hist_wind_buffers OWNER to {owner};
COMMENT ON TABLE storms.swio_hist_wind_buffers IS
    'Historical wind swath per system and threshold (geodesic, Réunion 10-minute winds)';
COMMENT ON COLUMN storms.swio_hist_wind_buffers.season IS
    'Season start year (2018 = 2018/19)';
COMMENT ON COLUMN storms.swio_hist_wind_buffers.n_points IS
    'Track points at or above wind_speed_kt';
COMMENT ON COLUMN storms.swio_hist_wind_buffers.n_modelled IS
    'Of those, points whose radius at wind_speed_kt is modelled (Réunion gave none)';

CREATE TABLE IF NOT EXISTS storms.swio_hist_wind_exposure
(
    disturbance_id VARCHAR     NOT NULL,
    wind_speed_kt  SMALLINT    NOT NULL CHECK (wind_speed_kt IN (34, 48, 64)),
    admin_level    SMALLINT    NOT NULL,
    iso3           VARCHAR(3)  NOT NULL,
    pcode          VARCHAR(20) NOT NULL,
    pop_exposed    INTEGER     NOT NULL,
    CONSTRAINT swio_hist_wind_exposure_unique
        UNIQUE (disturbance_id, wind_speed_kt, admin_level, pcode)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS storms.swio_hist_wind_exposure OWNER to {owner};

CREATE TABLE IF NOT EXISTS storms.swio_admin_population
(
    admin_level   SMALLINT    NOT NULL,
    iso3          VARCHAR(3)  NOT NULL,
    pcode         VARCHAR(20) NOT NULL,
    total_pop     BIGINT      NOT NULL,
    worldpop_year SMALLINT    NOT NULL,
    CONSTRAINT swio_admin_population_unique UNIQUE (admin_level, iso3, pcode)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS storms.swio_admin_population OWNER to {owner};
COMMENT ON TABLE storms.swio_admin_population IS
    'Total WorldPop population of each sea-extended admin unit used in the SWIO exposure tables';

-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_mfr_tracks_fcast_buffers_it ON storms.mfr_tracks_fcast_buffers (issued_time);
CREATE INDEX IF NOT EXISTS idx_mfr_tracks_obsv_buffers_vt ON storms.mfr_tracks_obsv_buffers (valid_time);
CREATE INDEX IF NOT EXISTS idx_mfr_tracks_fcastonly_buffers_it ON storms.mfr_tracks_fcastonly_buffers (issued_time);
CREATE INDEX IF NOT EXISTS idx_mfr_tracks_fcast_exposure_it ON storms.mfr_tracks_fcast_exposure (issued_time);
CREATE INDEX IF NOT EXISTS idx_mfr_tracks_obsv_exposure_vt ON storms.mfr_tracks_obsv_exposure (valid_time);
CREATE INDEX IF NOT EXISTS idx_mfr_tracks_fcastonly_exposure_it ON storms.mfr_tracks_fcastonly_exposure (issued_time);
CREATE INDEX IF NOT EXISTS idx_mfr_tracks_scenario_exposure_it ON storms.mfr_tracks_scenario_exposure (issued_time);
CREATE INDEX IF NOT EXISTS idx_swio_hist_wind_exposure_iso3 ON storms.swio_hist_wind_exposure (iso3);
