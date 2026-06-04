-- ============================================================================
-- ADAM → FieldMaps admin crosswalk
-- ============================================================================
-- Static reference table mapping each WFP ADAM admin unit to its canonical
-- FieldMaps (FM) p-code, so ADAM exposure can be reported against
-- standardized FM admin boundaries.
--
-- Built offline, NOT by the runtime pipelines: scripts/build_adam_fm_lookup_v2.py
-- runs the spatial matcher in src/static/adam/matcher.py. It uses the
-- PER-ADAM-admin iteration (match_per_adam_admin): one row per ADAM admin
-- polygon, each picking its best-IoU FM polygon. That direction is deliberate
-- — ADAM exposure joins by admin NAME, and per-ADAM iteration guarantees each
-- adam_admin_id appears at most once, so the downstream name join stays 1:1
-- and can't fan out / double-count.
--
-- Downstream join: storms.adam_exposure.admin_name = adam_fm_lookup.adam_admin_name
-- to attach (fm_pcode, fm_name) → adam_exposure.pcode.
--
-- ----------------------------------------------------------------------------
-- NOTE on shape:
--
-- Created by a pandas to_sql write, so no constraints/indexes and every
-- column is nullable. The CREATE below faithfully transcribes that live
-- shape. A recommended natural key + index are included COMMENTED OUT.
-- ============================================================================

-- DROP TABLE IF EXISTS storms.adam_fm_lookup CASCADE;

CREATE TABLE IF NOT EXISTS storms.adam_fm_lookup
(
    iso3            TEXT,
    admin_level     BIGINT,
    fm_pcode        TEXT,
    fm_name         TEXT,
    adam_admin_id   DOUBLE PRECISION,
    adam_admin_name TEXT,
    iou             DOUBLE PRECISION,
    caveat_kind     TEXT,
    caveat_note     TEXT
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.adam_fm_lookup
    OWNER to {owner};

COMMENT ON TABLE storms.adam_fm_lookup IS
    'Static ADAM-admin → FieldMaps-pcode crosswalk. Built offline by scripts/build_adam_fm_lookup_v2.py via per-ADAM-admin IoU matching; join to storms.adam_exposure on admin_name.';
COMMENT ON COLUMN storms.adam_fm_lookup.iso3 IS
    'ISO3 country code the admin unit belongs to.';
COMMENT ON COLUMN storms.adam_fm_lookup.admin_level IS
    'FieldMaps admin level the row maps at (0=country, 1=adm1, ...). The ADAM side is fixed at adm1.';
COMMENT ON COLUMN storms.adam_fm_lookup.fm_pcode IS
    'Canonical FieldMaps p-code (the match target). Nullable when no FM unit was matched (see caveat_kind).';
COMMENT ON COLUMN storms.adam_fm_lookup.fm_name IS
    'FieldMaps admin name for fm_pcode.';
COMMENT ON COLUMN storms.adam_fm_lookup.adam_admin_id IS
    'ADAM-native admin id (stored as double precision as written). Unique per row by construction (per-ADAM iteration).';
COMMENT ON COLUMN storms.adam_fm_lookup.adam_admin_name IS
    'ADAM admin name — the join key against storms.adam_exposure.admin_name.';
COMMENT ON COLUMN storms.adam_fm_lookup.iou IS
    'Intersection-over-union of the chosen ADAM↔FM polygon pair (0..1); match confidence. 0 when no overlap.';
COMMENT ON COLUMN storms.adam_fm_lookup.caveat_kind IS
    'Match-quality / admin-level caveat. NULL means a clean 1:1 match. Observed values: aggregating_from_adam, fm_adm1_only, no_adam_at_adm1, country_only, needs_manual_mapping, aggregated_in_adam, no_adam_source.';
COMMENT ON COLUMN storms.adam_fm_lookup.caveat_note IS
    'Free-text detail accompanying caveat_kind.';

-- ============================================================================
-- Recommended (not yet applied in the live DB) — uncomment in a migration
-- ============================================================================
-- ALTER TABLE storms.adam_fm_lookup
--     ADD CONSTRAINT adam_fm_lookup_unique UNIQUE (iso3, admin_level, adam_admin_id);
-- CREATE INDEX IF NOT EXISTS idx_adam_fm_lookup_admin_name
--     ON storms.adam_fm_lookup (adam_admin_name) WHERE adam_admin_name IS NOT NULL;
