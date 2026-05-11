-- ============================================================================
-- NHC WSP forecast-only polygon schema
-- ============================================================================
-- One row per (issued_time, wind_threshold_kt, percentage, atcf_id): the WSP
-- polygon with the cumulative observed track swath cut out, leaving only the
-- area still ahead of the storm.
--
-- WSP polygons are published ~3h after their nominal issued_time, so the
-- obsv buffer at issued_time + 3h is used for the cut-out when available.
-- obsv_valid_time records which obsv buffer was actually used:
--   = issued_time + 3h  (normal case: 3h offset applied)
--   = issued_time       (fallback: no +3h obsv available)
--   = NULL              (no obsv buffer found; full WSP geometry stored)
--
-- Unlike nhc_wsp_polygon, this table stores atcf_id directly (matched via
-- match_wsp_to_tracks). Multi-storm issuances produce one row per storm.
--
-- Populated by running: python run_pipeline.py nhc-wsp-fcastonly-polygons
-- ============================================================================

-- DROP TABLE IF EXISTS storms.nhc_wsp_fcastonly_polygon CASCADE;

CREATE TABLE IF NOT EXISTS storms.nhc_wsp_fcastonly_polygon
(
    id                BIGSERIAL   NOT NULL,
    issued_time       TIMESTAMP   NOT NULL,
    wind_threshold_kt SMALLINT    NOT NULL CHECK (wind_threshold_kt IN (34, 50, 64)),
    percentage        SMALLINT    NOT NULL,
    atcf_id           VARCHAR,
    obsv_valid_time   TIMESTAMP,
    geometry          geometry(Geometry, 4326),
    CONSTRAINT nhc_wsp_fcastonly_polygon_pkey PRIMARY KEY (id),
    CONSTRAINT nhc_wsp_fcastonly_polygon_unique
        UNIQUE NULLS NOT DISTINCT (issued_time, wind_threshold_kt, percentage, atcf_id)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.nhc_wsp_fcastonly_polygon
    OWNER to {owner};

COMMENT ON TABLE storms.nhc_wsp_fcastonly_polygon IS
    'WSP polygons with the cumulative observed track swath cut out. obsv_valid_time records which obsv buffer was used (issued_time + 3h in the normal case).';
COMMENT ON COLUMN storms.nhc_wsp_fcastonly_polygon.issued_time IS
    'WSP nominal issuance timestamp (UTC) — actual publication is ~3h later';
COMMENT ON COLUMN storms.nhc_wsp_fcastonly_polygon.wind_threshold_kt IS
    'Wind speed threshold in knots (34, 50, or 64)';
COMMENT ON COLUMN storms.nhc_wsp_fcastonly_polygon.percentage IS
    'WSP probability band lower bound as integer percentage (e.g. 5, 10, 20, 30)';
COMMENT ON COLUMN storms.nhc_wsp_fcastonly_polygon.atcf_id IS
    'ATCF storm identifier matched from storms.nhc_tracks_geo (NULL if no track found)';
COMMENT ON COLUMN storms.nhc_wsp_fcastonly_polygon.obsv_valid_time IS
    'valid_time of the obsv buffer used for the cut-out. issued_time + 3h = 3h offset applied; issued_time = exact match fallback; NULL = no obsv buffer found (full WSP geometry stored).';
COMMENT ON COLUMN storms.nhc_wsp_fcastonly_polygon.geometry IS
    'WSP polygon minus the cumulative observed track swath in WGS84 (EPSG:4326). NULL if WSP is fully covered by the observed swath.';

-- ============================================================================
-- Indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_nhc_wsp_fcastonly_polygon_geometry
    ON storms.nhc_wsp_fcastonly_polygon USING gist (geometry)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_nhc_wsp_fcastonly_polygon_issued_time
    ON storms.nhc_wsp_fcastonly_polygon (issued_time)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_nhc_wsp_fcastonly_polygon_atcf_id
    ON storms.nhc_wsp_fcastonly_polygon (atcf_id)
    TABLESPACE pg_default;
