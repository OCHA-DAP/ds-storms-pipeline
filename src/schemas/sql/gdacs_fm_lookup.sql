-- ============================================================================
-- GDACS → FieldMaps admin crosswalk
-- ============================================================================
-- Static reference table mapping each GDACS admin unit to its canonical
-- FieldMaps (FM) p-code, so GDACS exposure (keyed on GDACS-native admin
-- codes) can be reported against standardized FM admin boundaries.
--
-- Built offline, NOT by the runtime pipelines: scripts/build_gdacs_fm_lookup_v2.py
-- runs the spatial matcher in src/static/gdacs/matcher.py (per-FM-unit IoU
-- against GDACS admin polygons) and writes the result here. It is rebuilt
-- only when FieldMaps or GDACS change their admin schema.
--
-- Downstream join: storms.gdacs_exposure.gmi_admin = gdacs_fm_lookup.gmi_admin
-- to attach (fm_pcode, fm_name).
--
-- ----------------------------------------------------------------------------
-- NOTE on shape:
--
-- The table is currently created by a pandas to_sql write, so it carries no
-- constraints or indexes and every column is nullable. The CREATE below is a
-- faithful transcription of that live shape. A recommended natural key and
-- index are included COMMENTED OUT — apply them in a deliberate migration if
-- this table starts being joined at scale.
-- ============================================================================

-- DROP TABLE IF EXISTS storms.gdacs_fm_lookup CASCADE;

CREATE TABLE IF NOT EXISTS storms.gdacs_fm_lookup
(
    iso3             TEXT,
    admin_level      BIGINT,
    fm_pcode         TEXT,
    fm_name          TEXT,
    gmi_admin        TEXT,
    gdacs_admin_name TEXT,
    caveat_kind      TEXT,
    caveat_note      TEXT
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.gdacs_fm_lookup
    OWNER to {owner};

COMMENT ON TABLE storms.gdacs_fm_lookup IS
    'Static GDACS-admin → FieldMaps-pcode crosswalk. Built offline by scripts/build_gdacs_fm_lookup_v2.py via spatial (IoU) matching; join to storms.gdacs_exposure on gmi_admin.';
COMMENT ON COLUMN storms.gdacs_fm_lookup.iso3 IS
    'ISO3 country code the admin unit belongs to.';
COMMENT ON COLUMN storms.gdacs_fm_lookup.admin_level IS
    'FieldMaps admin level the row maps at (0=country, 1=adm1, ...).';
COMMENT ON COLUMN storms.gdacs_fm_lookup.fm_pcode IS
    'Canonical FieldMaps p-code (the match target). Nullable when no FM unit was matched (see caveat_kind).';
COMMENT ON COLUMN storms.gdacs_fm_lookup.fm_name IS
    'FieldMaps admin name for fm_pcode.';
COMMENT ON COLUMN storms.gdacs_fm_lookup.gmi_admin IS
    'GDACS-native GMI admin code — the join key against storms.gdacs_exposure.';
COMMENT ON COLUMN storms.gdacs_fm_lookup.gdacs_admin_name IS
    'GDACS admin name for gmi_admin.';
COMMENT ON COLUMN storms.gdacs_fm_lookup.caveat_kind IS
    'Match-quality / admin-level caveat. NULL means a clean 1:1 match. Observed values: fm_adm1_only, no_gdacs_at_adm1, aggregating_from_gdacs, country_only, aggregated_in_gdacs, needs_manual_mapping, no_fm_source.';
COMMENT ON COLUMN storms.gdacs_fm_lookup.caveat_note IS
    'Free-text detail accompanying caveat_kind (e.g. which units were aggregated).';

-- ============================================================================
-- Recommended (not yet applied in the live DB) — uncomment in a migration
-- ============================================================================
-- ALTER TABLE storms.gdacs_fm_lookup
--     ADD CONSTRAINT gdacs_fm_lookup_unique UNIQUE (iso3, admin_level, gmi_admin);
-- CREATE INDEX IF NOT EXISTS idx_gdacs_fm_lookup_gmi_admin
--     ON storms.gdacs_fm_lookup (gmi_admin) WHERE gmi_admin IS NOT NULL;
