-- ============================================================================
-- Migration: rename legacy nhc_wind_* unique constraints on the fcast
--            buffer / exposure tables to match the current schema (and
--            what stratus.postgres_upsert auto-targets).
-- Date: 2026-05
-- ============================================================================
-- Two tables were renamed in an earlier change but their unique constraints
-- kept the old names:
--   nhc_tracks_fcast_buffers  : nhc_wind_buffers_unique   → nhc_tracks_fcast_buffers_unique
--   nhc_tracks_fcast_exposure : nhc_wind_exposure_unique  → nhc_tracks_fcast_exposure_unique
-- Pandas' to_sql(... method=stratus.postgres_upsert ...) defaults the
-- ON CONFLICT target to f"{table_name}_unique", so both upserts currently
-- fail with `UndefinedObject: constraint ... does not exist`.
--
-- Idempotent: each rename only fires if the old name exists and the new
-- name doesn't.

BEGIN;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'storms.nhc_tracks_fcast_buffers'::regclass
          AND conname = 'nhc_wind_buffers_unique'
    ) AND NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'storms.nhc_tracks_fcast_buffers'::regclass
          AND conname = 'nhc_tracks_fcast_buffers_unique'
    ) THEN
        ALTER TABLE storms.nhc_tracks_fcast_buffers
            RENAME CONSTRAINT nhc_wind_buffers_unique
                           TO nhc_tracks_fcast_buffers_unique;
    END IF;

    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'storms.nhc_tracks_fcast_exposure'::regclass
          AND conname = 'nhc_wind_exposure_unique'
    ) AND NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'storms.nhc_tracks_fcast_exposure'::regclass
          AND conname = 'nhc_tracks_fcast_exposure_unique'
    ) THEN
        ALTER TABLE storms.nhc_tracks_fcast_exposure
            RENAME CONSTRAINT nhc_wind_exposure_unique
                           TO nhc_tracks_fcast_exposure_unique;
    END IF;
END $$;

COMMIT;
