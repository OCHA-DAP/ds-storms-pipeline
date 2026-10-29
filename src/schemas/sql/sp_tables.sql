-- ============================================================================
-- South Pacific (RSMC Nadi / FMS) wind buffers and population exposure
-- ============================================================================
-- The South Pacific counterpart of the NHC buffer/exposure tables, written by
-- src/pipelines/sp.py. Two families:
--
--   Realtime, from FMS forecast tracks (storms.fms_tracks_geo, Official only):
--     storms.fms_tracks_{fcast,obsv,fcastonly}_buffers  + _exposure
--     storms.fms_tracks_scenario_exposure    (36 uncertainty-shifted tracks)
--     storms.fms_tracks_scenario_envelope    (union of the shifted swaths)
--     storms.fms_processing_completion       (per storm + issuance markers)
--
--   Historical, from IBTrACS (JTWC positions and radii, a k-kt radius kept only
--   where the official 10-minute intensity reaches k kt):
--     storms.ibtracs_sp_wind_buffers + storms.ibtracs_sp_wind_exposure
--
-- Plus storms.sp_admin_population: total population of each admin unit as
-- used here.
--
-- Conventions that differ from the NHC tables, on purpose:
--   * Wind thresholds are FMS's 10-minute mean winds (NHC: 1-minute).
--   * Buffers are geodesic: radii are true ground distances (the NHC tables'
--     Mercator disks reach cos(latitude) of the stated radius).
--   * Admin units are FieldMaps units extended 500 m into the sea and made
--     non-overlapping, so coastal and atoll populations on 1 km WorldPop
--     cells are counted (plain polygons count Tuvalu as 3,340 people).
--   * Storms are keyed by FMS disturbance_id (e.g. 2526-11F) in the realtime
--     tables and by IBTrACS sid in the historical ones.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Realtime buffers
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS storms.fms_tracks_fcast_buffers
(
    disturbance_id VARCHAR   NOT NULL,
    issued_time    TIMESTAMP NOT NULL,
    wind_speed_kt  SMALLINT  NOT NULL CHECK (wind_speed_kt IN (34, 50, 64)),
    geometry       geometry(Geometry, 4326),
    CONSTRAINT fms_tracks_fcast_buffers_unique
        UNIQUE (disturbance_id, issued_time, wind_speed_kt)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS storms.fms_tracks_fcast_buffers OWNER to {owner};
COMMENT ON TABLE storms.fms_tracks_fcast_buffers IS
    'Wind swath of one FMS Official forecast track (analysis point + forecast points), geodesic, 10-minute winds';

CREATE TABLE IF NOT EXISTS storms.fms_tracks_obsv_buffers
(
    disturbance_id VARCHAR   NOT NULL,
    valid_time     TIMESTAMP NOT NULL,
    wind_speed_kt  SMALLINT  NOT NULL CHECK (wind_speed_kt IN (34, 50, 64)),
    geometry       geometry(Geometry, 4326),
    CONSTRAINT fms_tracks_obsv_buffers_unique
        UNIQUE (disturbance_id, valid_time, wind_speed_kt)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS storms.fms_tracks_obsv_buffers OWNER to {owner};
COMMENT ON TABLE storms.fms_tracks_obsv_buffers IS
    'Cumulative observed swath: union of the analysis (leadtime 0) points of every Official forecast received up to valid_time';

CREATE TABLE IF NOT EXISTS storms.fms_tracks_fcastonly_buffers
(
    disturbance_id VARCHAR   NOT NULL,
    issued_time    TIMESTAMP NOT NULL,
    wind_speed_kt  SMALLINT  NOT NULL CHECK (wind_speed_kt IN (34, 50, 64)),
    geometry       geometry(Geometry, 4326),
    CONSTRAINT fms_tracks_fcastonly_buffers_unique
        UNIQUE (disturbance_id, issued_time, wind_speed_kt)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS storms.fms_tracks_fcastonly_buffers OWNER to {owner};
COMMENT ON TABLE storms.fms_tracks_fcastonly_buffers IS
    'Forecast swath minus the observed swath at the same time (where the storm has not been yet); NULL geometry = nothing left';

CREATE TABLE IF NOT EXISTS storms.fms_tracks_scenario_envelope
(
    disturbance_id VARCHAR   NOT NULL,
    issued_time    TIMESTAMP NOT NULL,
    wind_speed_kt  SMALLINT  NOT NULL CHECK (wind_speed_kt IN (34, 50, 64)),
    n_scenarios    SMALLINT  NOT NULL,
    geometry       geometry(Geometry, 4326),
    CONSTRAINT fms_tracks_scenario_envelope_unique
        UNIQUE (disturbance_id, issued_time, wind_speed_kt)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS storms.fms_tracks_scenario_envelope OWNER to {owner};
COMMENT ON TABLE storms.fms_tracks_scenario_envelope IS
    'Union of the forecast-only swaths of the uncertainty scenarios: every track point moved by its FMS position uncertainty in one compass direction, n_scenarios directions';

-- ---------------------------------------------------------------------------
-- Realtime exposure
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS storms.fms_tracks_fcast_exposure
(
    disturbance_id VARCHAR     NOT NULL,
    issued_time    TIMESTAMP   NOT NULL,
    wind_speed_kt  SMALLINT    NOT NULL CHECK (wind_speed_kt IN (34, 50, 64)),
    admin_level    SMALLINT    NOT NULL,
    iso3           VARCHAR(3)  NOT NULL,
    pcode          VARCHAR(20) NOT NULL,
    pop_exposed    INTEGER     NOT NULL,
    CONSTRAINT fms_tracks_fcast_exposure_unique
        UNIQUE (disturbance_id, issued_time, wind_speed_kt, admin_level, pcode)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS storms.fms_tracks_fcast_exposure OWNER to {owner};

CREATE TABLE IF NOT EXISTS storms.fms_tracks_obsv_exposure
(
    disturbance_id VARCHAR     NOT NULL,
    valid_time     TIMESTAMP   NOT NULL,
    wind_speed_kt  SMALLINT    NOT NULL CHECK (wind_speed_kt IN (34, 50, 64)),
    admin_level    SMALLINT    NOT NULL,
    iso3           VARCHAR(3)  NOT NULL,
    pcode          VARCHAR(20) NOT NULL,
    pop_exposed    INTEGER     NOT NULL,
    CONSTRAINT fms_tracks_obsv_exposure_unique
        UNIQUE (disturbance_id, valid_time, wind_speed_kt, admin_level, pcode)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS storms.fms_tracks_obsv_exposure OWNER to {owner};

CREATE TABLE IF NOT EXISTS storms.fms_tracks_fcastonly_exposure
(
    disturbance_id VARCHAR     NOT NULL,
    issued_time    TIMESTAMP   NOT NULL,
    wind_speed_kt  SMALLINT    NOT NULL CHECK (wind_speed_kt IN (34, 50, 64)),
    admin_level    SMALLINT    NOT NULL,
    iso3           VARCHAR(3)  NOT NULL,
    pcode          VARCHAR(20) NOT NULL,
    pop_exposed    INTEGER     NOT NULL,
    CONSTRAINT fms_tracks_fcastonly_exposure_unique
        UNIQUE (disturbance_id, issued_time, wind_speed_kt, admin_level, pcode)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS storms.fms_tracks_fcastonly_exposure OWNER to {owner};

CREATE TABLE IF NOT EXISTS storms.fms_tracks_scenario_exposure
(
    disturbance_id VARCHAR     NOT NULL,
    issued_time    TIMESTAMP   NOT NULL,
    shift_deg      SMALLINT    NOT NULL CHECK (shift_deg >= 0 AND shift_deg < 360),
    wind_speed_kt  SMALLINT    NOT NULL CHECK (wind_speed_kt IN (34, 50, 64)),
    admin_level    SMALLINT    NOT NULL,
    iso3           VARCHAR(3)  NOT NULL,
    pcode          VARCHAR(20) NOT NULL,
    pop_exposed    INTEGER     NOT NULL,
    CONSTRAINT fms_tracks_scenario_exposure_unique
        UNIQUE (disturbance_id, issued_time, shift_deg, wind_speed_kt, admin_level, pcode)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS storms.fms_tracks_scenario_exposure OWNER to {owner};
COMMENT ON TABLE storms.fms_tracks_scenario_exposure IS
    'Forecast-only exposure if the whole track moves by its FMS position uncertainty towards compass bearing shift_deg (Fiji framework method, geodesic)';

CREATE TABLE IF NOT EXISTS storms.fms_processing_completion
(
    disturbance_id VARCHAR   NOT NULL,
    issued_time    TIMESTAMP NOT NULL,
    stage          VARCHAR   NOT NULL,
    completed_at   TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    CONSTRAINT fms_processing_completion_pkey
        PRIMARY KEY (disturbance_id, issued_time, stage)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS storms.fms_processing_completion OWNER to {owner};
COMMENT ON TABLE storms.fms_processing_completion IS
    'One row per (storm, issuance, stage) once that stage finished end to end; a stage without a row is redone on the next run';

-- ---------------------------------------------------------------------------
-- Historical (IBTrACS)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS storms.ibtracs_sp_wind_buffers
(
    sid           VARCHAR  NOT NULL,
    wind_speed_kt SMALLINT NOT NULL CHECK (wind_speed_kt IN (34, 50, 64)),
    radii_source  VARCHAR  NOT NULL CHECK (radii_source IN ('jtwc', 'fji_regression')),
    geometry      geometry(Geometry, 4326),
    CONSTRAINT ibtracs_sp_wind_buffers_unique UNIQUE (sid, wind_speed_kt)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS storms.ibtracs_sp_wind_buffers OWNER to {owner};
COMMENT ON TABLE storms.ibtracs_sp_wind_buffers IS
    'Whole-track wind swath per South Pacific storm (season 2002+): JTWC positions and radii, a k-kt radius kept only where the official 10-min intensity (wmo_wind, else 0.90 x JTWC 1-min) reaches k; storms with no JTWC radii use the Fiji framework regression (radii_source)';

CREATE TABLE IF NOT EXISTS storms.ibtracs_sp_wind_exposure
(
    sid           VARCHAR     NOT NULL,
    wind_speed_kt SMALLINT    NOT NULL CHECK (wind_speed_kt IN (34, 50, 64)),
    admin_level   SMALLINT    NOT NULL,
    iso3          VARCHAR(3)  NOT NULL,
    pcode         VARCHAR(20) NOT NULL,
    pop_exposed   INTEGER     NOT NULL,
    CONSTRAINT ibtracs_sp_wind_exposure_unique
        UNIQUE (sid, wind_speed_kt, admin_level, pcode)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS storms.ibtracs_sp_wind_exposure OWNER to {owner};

CREATE TABLE IF NOT EXISTS storms.sp_admin_population
(
    admin_level   SMALLINT    NOT NULL,
    iso3          VARCHAR(3)  NOT NULL,
    pcode         VARCHAR(20) NOT NULL,
    total_pop     BIGINT      NOT NULL,
    worldpop_year SMALLINT    NOT NULL,
    CONSTRAINT sp_admin_population_unique UNIQUE (admin_level, iso3, pcode)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS storms.sp_admin_population OWNER to {owner};
COMMENT ON TABLE storms.sp_admin_population IS
    'WorldPop total per South Pacific admin unit, on the same sea-extended units the SP exposure uses';

-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_fms_tracks_fcast_buffers_it ON storms.fms_tracks_fcast_buffers (issued_time);
CREATE INDEX IF NOT EXISTS idx_fms_tracks_obsv_buffers_vt ON storms.fms_tracks_obsv_buffers (valid_time);
CREATE INDEX IF NOT EXISTS idx_fms_tracks_fcastonly_buffers_it ON storms.fms_tracks_fcastonly_buffers (issued_time);
CREATE INDEX IF NOT EXISTS idx_fms_tracks_fcast_exposure_it ON storms.fms_tracks_fcast_exposure (issued_time);
CREATE INDEX IF NOT EXISTS idx_fms_tracks_obsv_exposure_vt ON storms.fms_tracks_obsv_exposure (valid_time);
CREATE INDEX IF NOT EXISTS idx_fms_tracks_fcastonly_exposure_it ON storms.fms_tracks_fcastonly_exposure (issued_time);
CREATE INDEX IF NOT EXISTS idx_fms_tracks_scenario_exposure_it ON storms.fms_tracks_scenario_exposure (issued_time);
CREATE INDEX IF NOT EXISTS idx_ibtracs_sp_wind_exposure_iso3 ON storms.ibtracs_sp_wind_exposure (iso3);
