-- ============================================================================
-- Table: storms.nhc_wsp_polygon_raw
-- ============================================================================
-- One row per (issued_time, wind_threshold_kt, percentage). Equivalent schema
-- to the official NHC 5km GIS shapefiles. Multi-storm issuances produce a
-- single MultiPolygon row covering every active storm's cone for that band.
-- The matched-per-storm view lives in storms.nhc_wsp_polygon_matched.

-- DROP TABLE IF EXISTS storms.nhc_wsp_polygon_raw CASCADE;

CREATE TABLE IF NOT EXISTS storms.nhc_wsp_polygon_raw
(
    id                BIGSERIAL PRIMARY KEY,
    issued_time       TIMESTAMP   NOT NULL,
    wind_threshold_kt SMALLINT    NOT NULL CHECK (wind_threshold_kt IN (34, 50, 64)),
    percentage        SMALLINT    NOT NULL,
    geometry          geometry(MultiPolygon, 4326),
    CONSTRAINT nhc_wsp_polygon_raw_unique UNIQUE (issued_time, wind_threshold_kt, percentage)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.nhc_wsp_polygon_raw
    OWNER to {owner};

COMMENT ON TABLE storms.nhc_wsp_polygon_raw IS
    'NHC basin-wide wind speed probability polygons - raw NHC output, one row per issued_time/threshold/probability band. No atcf_id; multi-storm issuances appear as a single MultiPolygon covering every active storm. See storms.nhc_wsp_polygon_matched for per-storm geometries.';
COMMENT ON COLUMN storms.nhc_wsp_polygon_raw.issued_time IS
    'Forecast issued time (UTC)';
COMMENT ON COLUMN storms.nhc_wsp_polygon_raw.wind_threshold_kt IS
    'Wind speed threshold in knots (34, 50, or 64)';
COMMENT ON COLUMN storms.nhc_wsp_polygon_raw.percentage IS
    'Lower bound of probability band in percent (0, 5, 10, ..., 90). 0 means <5%, 90 means >90%.';
COMMENT ON COLUMN storms.nhc_wsp_polygon_raw.geometry IS
    'MultiPolygon covering the probability band area in WGS84 (EPSG:4326).';

-- ============================================================================
-- Indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_nhc_wsp_polygon_raw_geometry
    ON storms.nhc_wsp_polygon_raw USING gist (geometry)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_nhc_wsp_polygon_raw_issued_time
    ON storms.nhc_wsp_polygon_raw (issued_time)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_nhc_wsp_polygon_raw_threshold
    ON storms.nhc_wsp_polygon_raw (wind_threshold_kt)
    TABLESPACE pg_default;
