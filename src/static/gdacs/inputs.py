"""Inputs needed to build the canonical FM↔GDACS admin lookup.

This module lives under ``src/static/`` because the lookup table is
*static reference data*: built once, verified, then frozen until
FieldMaps or GDACS changes its admin schema. It is **not** an
ETL pipeline — recurring data ingestion lives in ``src/pipelines/``.

Three pieces of input:

1. ``ATLANTIC_ISO3`` — scope of the lookup (NHC Atlantic basin
   countries plus W. European extratropical-transition recipients).
2. ``load_level_config`` — parses ``config/adm_level_config.toml``,
   the per-country policy (accept / country_only / aggregate / etc.)
   plus per-row caveat notes for pre-split boundary cases.
3. ``load_fieldmaps_adm`` — pulls FieldMaps admin polygons per
   country from the OCHA-DAP ``global`` blob mirror (10× faster than
   the upstream ``data.fieldmaps.io`` global parquet).

GDACS-side inputs (the admin shapefile) live in
:mod:`src.static.gdacs.admin`. ``scripts/build_canonical_lookup.py``
pulls from both modules.
"""

from __future__ import annotations

import io
import tomllib
from pathlib import Path

import geopandas as gpd
import ocha_stratus as stratus
from azure.core.exceptions import ResourceNotFoundError


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONFIG = REPO_ROOT / "config" / "adm_level_config.toml"

DEFAULT_FM_LEVEL = 1
# Kept for TOML-schema compatibility — early policy rows have
# ``[overrides.*]`` entries with ``gadm_level``. The FM↔GDACS bridge
# doesn't use it, but parsing-then-ignoring keeps the load function
# tolerant of legacy keys.
DEFAULT_GADM_LEVEL = 1

# Narrow allow-list of iso3s where FieldMaps puts the actual
# subdivision name in ``adm0_name`` and leaves ``adm1_name`` NULL
# (e.g. UMI "Baker Island (USA)", SJM "Svalbard Islands (Nor.)").
# Anywhere else, NULL adm1_name is treated as a real data gap and
# left as NaN downstream. Strict opt-in by iso3 prevents the fallback
# from masking unrelated FM data-quality issues.
FM_ADM1_NAME_FALLBACK_ISOS = {"SJM", "UMI"}


def fallback_fm_name_from_adm0(adm0_name: str | None) -> str | None:
    """Strip trailing ' (XYZ)' country annotation from an adm0_name
    so it reads like a real adm1 name. UMI's ``adm0_name`` looks like
    'Baker Island (USA)' → returns 'Baker Island'. Only call this for
    iso3s in :data:`FM_ADM1_NAME_FALLBACK_ISOS`."""
    import re
    if adm0_name is None:
        return None
    return re.sub(r"\s*\([^)]+\)\s*$", "", str(adm0_name)).strip() or None


# Build scope: every ISO3 that ever appears in storms.gdacs_exposure
# AND has FieldMaps adm0 data available, so the lookup has at least an
# adm0 row for every country GDACS reports on. Name is historical
# (``ATLANTIC_ISO3``) — initially the NHC Atlantic basin — but the set
# has grown to include extratropical-transition recipients (W. Europe,
# Iceland, Faroes, etc.) plus a handful of edge-case countries that
# leak in from cross-basin events or upstream GDACS routing quirks
# (RUS-Kamchatka, UMI/Johnston Atoll, SLV Pacific coast, etc.). The
# Atlantic-only origin shows in the structure of the groups below.
ATLANTIC_ISO3 = sorted({
    # ── Core NHC Atlantic basin ─────────────────────────────────────
    # North America (Atlantic / Gulf coasts)
    "USA", "CAN", "MEX",
    # Central America (Caribbean coasts)
    "BLZ", "GTM", "HND", "NIC", "CRI", "PAN",
    # Greater Antilles + Caymans
    "CUB", "DOM", "HTI", "JAM", "PRI", "CYM",
    # Bahamas / Turks & Caicos
    "BHS", "TCA",
    # Lesser Antilles (north)
    "VGB", "VIR", "AIA", "ATG", "MSR", "KNA", "GLP", "MAF", "SXM", "BLM",
    # Lesser Antilles (south)
    "DMA", "MTQ", "LCA", "VCT", "GRD", "BRB", "TTO",
    # ABC islands + Sint Eustatius/Saba/Bonaire
    "ABW", "CUW", "BES",
    # Bermuda
    "BMU",
    # South America (Caribbean coast)
    "VEN", "COL", "GUY", "SUR", "GUF",
    # Atlantic islands
    "CPV",
    # ── Extratropical transition recipients ─────────────────────────
    "IRL", "GBR", "PRT", "ESP", "FRA", "BEL", "NLD", "LUX", "DEU", "JEY",
    # ── Added to absorb out-of-scope GDACS reports ──────────────────
    # FieldMaps data confirmed available for each of these. GDACS
    # reports on them via extratropical transitions, polar-bridge
    # events, or basin-crossover quirks. Policy decisions for each
    # are still in the country_only default until per-country review.
    "FRO",  # Faroe Islands (extratropical)
    "GIB",  # Gibraltar (Atlantic edge)
    "GRL",  # Greenland (high-latitude extratropical)
    "ISL",  # Iceland (extratropical)
    "MAR",  # Morocco (Atlantic Africa, rare hits)
    "NOR",  # Norway (extratropical)
    "RUS",  # Russia (Kamchatka shows up; data-edge quirk)
    "SJM",  # Svalbard / Jan Mayen (Arctic edge)
    "SLV",  # El Salvador (Pacific Central America; basin crossover)
    "SPM",  # St. Pierre & Miquelon (NW Atlantic)
    "SWE",  # Sweden (extratropical)
    "UMI",  # US Minor Outlying Islands (Pacific atolls; data quirk)
    # ── In scope but deliberately skipped (action="no_fm_source") ────
    # GDACS reports these but FM has no parquet — included here so the
    # policy is visible in the qmd. The build script skips them on
    # encountering action=no_fm_source.
    "ANT",  # Netherlands Antilles (dissolved 2010-10-10)
    "XIM",  # Isle of Man (no FM parquet)
})


def load_level_config(path: Path | None = None) -> dict:
    """Read the per-country admin-level + policy TOML.

    Defaults to :data:`DEFAULT_CONFIG` (``config/adm_level_config.toml``)
    when no path is given. Missing file → defaults-only dict (no
    overrides, no policy entries, no caveats).

    Returned dict shape::

        {
          "defaults":          {"fm_level": int, "gadm_level": int},
          "overrides":         {ISO3: {fm_level, gadm_level?, note?}},
          "gdacs_overrides":   {ISO3: {fm_level, note?}},
          "gdacs_policy":      {ISO3: {action: str, note?}},
          "per_row_notes":     [{iso3, fm_pcode, note}, ...],
          "data_quality":      {ISO3: {action, note?}},
          "adam_policy":       {ISO3: {action: str, note?}},
          "adam_per_row_notes": [{iso3, ge_adm1_id, note}, ...],
          "adam_row_overrides": [{iso3, ge_adm1_id, action,
                                  fm_pcode_override?, ...}, ...],
          "gdacs_shared_source": [{iso3, gmi_admin, fm_pcodes:[],
                                   caveat_kind, note}, ...],
          "adam_shared_source":  [{iso3, adam_admin_id, fm_pcodes:[],
                                   caveat_kind, note}, ...],
        }

    The two ``*_shared_source`` arrays express "this one source polygon
    covers these N FM units" — one entry per shared group, and the
    build script writes the same caveat_kind+note onto every listed FM
    row. Used for boundary-reform cases (CUB Artemisa/Mayabeque ↔ pre-
    2011 La Habana; NIC RACN+RACS ↔ pre-1987 Zelaya) where the source's
    one number represents the combined area of multiple modern FM units.

    ``[overrides]`` historically carried the FM↔GADM bridge config.
    ``[gdacs_overrides]`` takes precedence for the FM↔GDACS bridge.
    ``[adam_policy]`` / ``[[adam_per_row_notes]]`` are the FM↔ADAM
    equivalents of ``gdacs_policy`` / ``per_row_notes``; the two
    bridges keep separate per-iso3 policy because the upstream admin
    hierarchies disagree on some countries (e.g. ISL: ADAM uses 75
    municipalities, GDACS uses 1 whole-country polygon).
    """
    cfg = {
        "defaults": {
            "fm_level": DEFAULT_FM_LEVEL,
            "gadm_level": DEFAULT_GADM_LEVEL,
        },
        "overrides": {},
        "gdacs_overrides": {},
        "gdacs_policy": {},
        "per_row_notes": [],
        "data_quality": {},
        "adam_policy": {},
        "adam_per_row_notes": [],
        "adam_row_overrides": [],
        "gdacs_shared_source": [],
        "adam_shared_source": [],
    }
    if path is None:
        path = DEFAULT_CONFIG
    if not path.exists():
        return cfg
    with open(path, "rb") as f:
        loaded = tomllib.load(f)
    if "defaults" in loaded:
        cfg["defaults"].update(loaded["defaults"])
    for key in ("overrides", "gdacs_overrides", "gdacs_policy",
                "data_quality", "adam_policy"):
        if key in loaded:
            cfg[key] = loaded[key]
    for key in ("per_row_notes", "adam_per_row_notes",
                "adam_row_overrides",
                "gdacs_shared_source", "adam_shared_source"):
        if key in loaded:
            cfg[key] = loaded[key]
    return cfg


def fm_id_and_name_cols(
    fm: gpd.GeoDataFrame, level: int,
) -> tuple[str | None, str | None]:
    """Resolve FieldMaps id and name columns for an admin level.

    Different FieldMaps releases use different conventions —
    ``adm{N}_pcode`` / ``adm{N}_id`` / ``adm{N}_src`` for IDs;
    ``adm{N}_name`` / ``name_{N}`` for names. Pick the first present so
    the matcher survives a schema bump.
    """
    pcode_candidates = (
        f"adm{level}_pcode", f"adm{level}_id", f"adm{level}_src",
    )
    name_candidates = (f"adm{level}_name", f"name_{level}")
    pcode = next((c for c in pcode_candidates if c in fm.columns), None)
    name = next((c for c in name_candidates if c in fm.columns), None)
    return pcode, name


def resolve_gdacs_fm_level(iso3: str, cfg: dict) -> int:
    """Look up the FM admin level to use for the FM↔GDACS bridge.

    GDACS's admin layer is fixed at admin-1 (no hierarchy on its side),
    so only the FM-side level is configurable. Resolution order:

    1. ``[gdacs_overrides.<ISO3>].fm_level`` (GDACS-specific override)
    2. ``[overrides.<ISO3>].fm_level``       (shared FM↔GADM override
                                              kept for compat)
    3. ``[defaults].fm_level``               (typically 1)
    """
    ov = cfg.get("gdacs_overrides", {}).get(iso3)
    if ov and "fm_level" in ov:
        return ov["fm_level"]
    ov = cfg.get("overrides", {}).get(iso3)
    if ov and "fm_level" in ov:
        return ov["fm_level"]
    return cfg["defaults"]["fm_level"]


def load_fieldmaps_adm(
    iso3: str, admin_level: int, stage: str = "dev",
) -> "gpd.GeoDataFrame | None":
    """Load FieldMaps polygons for one (ISO3, admin level).

    Fast path: the OCHA-DAP ``global`` container mirrors
    data.fieldmaps.io as a tree of per-country parquets at
    ``fieldmaps/edge-matched/humanitarian/intl/adm{N}/{ISO3}.parquet``.
    Each file is small (KB–few MB), so a single blob read replaces the
    metadata-scan + filter-pushdown that data.fieldmaps.io's global
    parquet requires — per-call latency drops ~10× (8 s → 0.8 s on
    HTI adm1).

    Slow-path fallback: the blob currently only carries adm0 and adm1.
    For ``admin_level >= 2`` we fall back to
    ``ocha_stratus.codab.load_codab_from_fieldmaps``, which hits
    data.fieldmaps.io directly. Same return shape either way.

    Returns ``None`` if the country has no entry at this admin level.
    """
    iso3 = iso3.upper()
    if admin_level <= 1:
        blob_name = (
            f"fieldmaps/edge-matched/humanitarian/intl/"
            f"adm{admin_level}/{iso3}.parquet"
        )
        container = stratus.get_container_client(
            stage=stage, container_name="global",
        )
        blob_client = container.get_blob_client(blob_name)
        try:
            data = blob_client.download_blob().readall()
            return gpd.read_parquet(io.BytesIO(data))
        except ResourceNotFoundError:
            # Country missing from blob — fall through to HTTP path
            pass
    return stratus.codab.load_codab_from_fieldmaps(
        iso3, admin_level=admin_level,
    )
