-- ============================================================================
-- Migration: rename nhc_wsp_polygon -> nhc_wsp_polygon_raw + create
--            nhc_wsp_polygon_matched (and tighten nhc_wsp_fcastonly_polygon
--            geometry column to MultiPolygon).
-- Date: 2026-05
-- ============================================================================
-- Apply once, in order. Pair with:
--   uv run python run_pipeline.py nhc-wsp-polygon-matched --since 2017-01-01 \
--       --mode prod --overwrite
-- followed by truncate + regenerate of nhc_wsp_fcastonly_polygon,
-- nhc_wsp_fcastonly_exposure and nhc_wsp_exposure. See the plan for the full
-- backfill recipe.

BEGIN;

-- 1. Rename the existing raw table and its constraint/indexes.
ALTER TABLE storms.nhc_wsp_polygon RENAME TO nhc_wsp_polygon_raw;

ALTER TABLE storms.nhc_wsp_polygon_raw
    RENAME CONSTRAINT nhc_wsp_polygon_unique
                   TO nhc_wsp_polygon_raw_unique;

ALTER INDEX storms.idx_nhc_wsp_polygon_geometry
    RENAME TO idx_nhc_wsp_polygon_raw_geometry;
ALTER INDEX storms.idx_nhc_wsp_polygon_issued_time
    RENAME TO idx_nhc_wsp_polygon_raw_issued_time;
ALTER INDEX storms.idx_nhc_wsp_polygon_threshold
    RENAME TO idx_nhc_wsp_polygon_raw_threshold;

-- 2. Create the new matched table (idempotent).
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
);

CREATE INDEX IF NOT EXISTS idx_nhc_wsp_polygon_matched_geometry
    ON storms.nhc_wsp_polygon_matched USING gist (geometry);
CREATE INDEX IF NOT EXISTS idx_nhc_wsp_polygon_matched_issued_time
    ON storms.nhc_wsp_polygon_matched (issued_time);
CREATE INDEX IF NOT EXISTS idx_nhc_wsp_polygon_matched_atcf_id
    ON storms.nhc_wsp_polygon_matched (atcf_id);

-- 3. Tighten fcastonly geometry column from Geometry -> MultiPolygon.
--    Run TRUNCATE before this so the existing rows (which include single
--    Polygon results) don't fail the column-type cast.
TRUNCATE storms.nhc_wsp_fcastonly_polygon RESTART IDENTITY;
ALTER TABLE storms.nhc_wsp_fcastonly_polygon
    ALTER COLUMN geometry TYPE geometry(MultiPolygon, 4326)
    USING geometry::geometry(MultiPolygon, 4326);

COMMIT;
