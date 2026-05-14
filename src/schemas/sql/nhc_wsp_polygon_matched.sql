-- ============================================================================
-- Table: storms.nhc_wsp_polygon_matched
-- ============================================================================
-- One row per (issued_time, wind_threshold_kt, percentage, atcf_id). Derived
-- from storms.nhc_wsp_polygon_raw + storms.nhc_tracks_geo by matching each
-- raw MultiPolygon part to its owning storm (via ocha_lens
-- match_wsp_to_tracks), then dissolving all parts belonging to the same
-- storm/band into a single MultiPolygon.
--
-- Invariants:
--   - Exactly one row per (issued_time, wind_threshold_kt, percentage,
--     atcf_id). Multi-part regions for a single storm are stored as one
--     MultiPolygon.
--   - atcf_id IS NULL when no track was found at the same issued_time
--     (treated as distinct via NULLS NOT DISTINCT on the unique constraint).
--
-- Populated by running: python run_pipeline.py nhc-wsp-polygon-matched

-- DROP TABLE IF EXISTS storms.nhc_wsp_polygon_matched CASCADE;

CREATE TABLE IF NOT EXISTS storms.nhc_wsp_polygon_matched
(
    id                BIGSERIAL PRIMARY KEY,
    issued_time       TIMESTAMP   NOT NULL,
    wind_threshold_kt SMALLINT    NOT NULL CHECK (wind_threshold_kt IN (34, 50, 64)),
    percentage        SMALLINT    NOT NULL,
    atcf_id           VARCHAR,
    geometry          geometry(MultiPolygon, 4326),
    CONSTRAINT nhc_wsp_polygon_matched_unique
        UNIQUE NULLS NOT DISTINCT (issued_time, wind_threshold_kt, percentage, atcf_id)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.nhc_wsp_polygon_matched
    OWNER to {owner};

COMMENT ON TABLE storms.nhc_wsp_polygon_matched IS
    'NHC WSP polygons matched to their owning storm. Exactly one MultiPolygon per (issued_time, wind_threshold_kt, percentage, atcf_id), so multi-part regions for a single storm are preserved.';
COMMENT ON COLUMN storms.nhc_wsp_polygon_matched.issued_time IS
    'Forecast issued time (UTC)';
COMMENT ON COLUMN storms.nhc_wsp_polygon_matched.wind_threshold_kt IS
    'Wind speed threshold in knots (34, 50, or 64)';
COMMENT ON COLUMN storms.nhc_wsp_polygon_matched.percentage IS
    'Lower bound of probability band in percent (0, 5, 10, ..., 90).';
COMMENT ON COLUMN storms.nhc_wsp_polygon_matched.atcf_id IS
    'ATCF storm identifier matched from storms.nhc_tracks_geo (NULL if no track found).';
COMMENT ON COLUMN storms.nhc_wsp_polygon_matched.geometry IS
    'MultiPolygon covering this storm''s portion of the probability band, in WGS84 (EPSG:4326).';

-- ============================================================================
-- Indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_nhc_wsp_polygon_matched_geometry
    ON storms.nhc_wsp_polygon_matched USING gist (geometry)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_nhc_wsp_polygon_matched_issued_time
    ON storms.nhc_wsp_polygon_matched (issued_time)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_nhc_wsp_polygon_matched_atcf_id
    ON storms.nhc_wsp_polygon_matched (atcf_id)
    TABLESPACE pg_default;
