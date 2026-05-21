-- ============================================================================
-- Total WorldPop population per FieldMaps admin unit
-- ============================================================================
-- One row per (admin_level, iso3, pcode): the area-weighted sum of WorldPop
-- pixels inside that admin polygon. Used as the denominator for storm
-- exposure tables (e.g. "X% of adm1 population exposed" = pop_exposed /
-- total_pop) so each consumer doesn't have to recompute the same expensive
-- exactextract sum from the global COG.
--
-- Static. Recompute when WorldPop is bumped. Populated by:
--   scripts/compute_admin_population.py
--
-- Source: WorldPop 1km global pop_count COG (raster container) x FieldMaps
--         per-country adm0/adm1 parquet mirrors (global container).
-- ============================================================================

-- DROP TABLE IF EXISTS storms.admin_population CASCADE;

CREATE TABLE IF NOT EXISTS storms.admin_population
(
    admin_level   SMALLINT    NOT NULL,
    iso3          VARCHAR(3)  NOT NULL,
    pcode         VARCHAR(20) NOT NULL,
    total_pop     BIGINT      NOT NULL,
    worldpop_year SMALLINT    NOT NULL,
    CONSTRAINT admin_population_unique
        UNIQUE (admin_level, iso3, pcode)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS storms.admin_population
    OWNER to {owner};

COMMENT ON TABLE storms.admin_population IS
    'Total WorldPop population per FieldMaps admin unit. admin_level=0 is country level. Denominator for exposure tables.';
COMMENT ON COLUMN storms.admin_population.admin_level IS
    'Administrative level (0 = country, 1 = first subnational).';
COMMENT ON COLUMN storms.admin_population.iso3 IS
    'ISO 3166-1 alpha-3 country code.';
COMMENT ON COLUMN storms.admin_population.pcode IS
    'Admin unit p-code (ISO3 at admin_level=0, country-prefixed pcode for subnational).';
COMMENT ON COLUMN storms.admin_population.total_pop IS
    'Population count inside this admin unit (WorldPop 1km, area-weighted exactextract sum).';
COMMENT ON COLUMN storms.admin_population.worldpop_year IS
    'Year of the WorldPop raster used (parsed from the COG filename, e.g. 2026).';

-- ============================================================================
-- Indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_admin_population_iso3
    ON storms.admin_population (iso3)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_admin_population_admin_level
    ON storms.admin_population (admin_level)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_admin_population_worldpop_year
    ON storms.admin_population (worldpop_year)
    TABLESPACE pg_default;
