"""Build the canonical FieldMaps↔GDACS lookup CSV and upload to blob.

The runtime pipeline doesn't do any spatial work — it just joins this CSV
against its FieldMaps-driven exposure analysis to attach GDACS exposure
numbers per FM admin unit. This script is the *only* place spatial /
algorithmic work happens; it runs offline whenever policy or source data
changes.

Inputs
------
- ``config/adm_level_config.toml``               policy + per-row notes
- FM adm0/adm1 parquets on blob
  (``global/fieldmaps/edge-matched/humanitarian/intl/adm{0,1}/<ISO3>.parquet``)
- GDACS admin shapefile on blob
  (``global/gdacs/admin/W_ADM_ADMIN2010_V2021.zip``) — streamed
  directly each build via ``load_gdacs_admin``; no local cache

The expensive FM↔GDACS IoU overlay is run inline per country here via
:func:`src.static.gdacs.matcher.match_country` — no pre-step required.

Output
------
- Local CSV at ``--out`` (default ``data/gdacs_fm_lookup.csv``,
  gitignored) — for diff/inspection between builds
- Postgres table ``storms.gdacs_fm_lookup`` (REPLACE on each build) so
  the lookup sits alongside the other inputs the runtime reads
  (``storms.gdacs_exposure``, ``storms.nhc_tracks_fcastonly_exposure``,
  ``storms.storm_id_lookup``). Skipped with ``--dry-run``.

Schema
------
``iso3, admin_level, fm_pcode, fm_name, gmi_admin, gdacs_admin_name,
caveat_note``

PK = ``(iso3, admin_level, fm_pcode, gmi_admin)``. Multiple rows per
``(iso3, admin_level, fm_pcode)`` are expected for
``aggregate_gdacs_to_fm`` countries (BEL, DOM, FRA). Multiple rows per
``(iso3, gmi_admin)`` are expected for ``needs_manual_mapping`` countries
with pre-split boundaries (CUB, NIC) — the ``caveat_note`` carries the
"don't sum across these rows" warning.

How each policy translates to rows
----------------------------------
==================================  ==========  ===========================
action                              adm0 rows   adm1 rows
==================================  ==========  ===========================
accept                              1 per ctry  1 per FM adm1 (1:1 matching)
country_only                        1 per ctry  none
aggregate_gdacs_to_fm               1 per ctry  N per FM adm1 (one per
                                                  GDACS poly inside it,
                                                  reverse spatial join)
needs_manual_mapping                1 per ctry  1 per FM adm1, with
                                                  caveat_note populated
fm_adm1_only                        1 per ctry  1 per FM adm1 with
                                                  gmi_admin=NULL — surfaces
                                                  FM units in the lookup so
                                                  other sources (NHC, ADAM)
                                                  can attach via fm_pcode,
                                                  while GDACS adm1 stays
                                                  deliberately unmatched
                                                  (1 GDACS whole-country
                                                  poly would otherwise be
                                                  fanned out across N FM
                                                  units and replicated)
no_fm_source                        none        none — iso3 is GDACS-known
                                                  but FM has no parquet
                                                  (defunct, e.g. ANT post-
                                                  2010; or just unavailable,
                                                  e.g. XIM). Build skips
                                                  entirely; downstream
                                                  consumers read the policy
                                                  to filter exposure rows.
==================================  ==========  ===========================

Single-polygon FM countries (ABW, AIA, BLM, CUW, JEY, MAF, PRI) normally
get only the adm0 row — FM adm1 is degenerate for crosswalk purposes.
Exception: `aggregate_gdacs_to_fm` still runs because the reverse spatial
join (N GDACS polys → 1 FM poly) is meaningful (e.g. PRI: 8 GDACS
Senatorial Districts → the single FM Puerto Rico polygon).

Usage
-----
::

    uv run python scripts/build_canonical_lookup.py
        [--mode dev|prod]
        [--dry-run]                  # write local CSV, skip blob upload
        [--out PATH]                 # local CSV destination
"""

import argparse
import logging
import sys
from pathlib import Path

import coloredlogs
import geopandas as gpd
import ocha_stratus as stratus
import pandas as pd

# Pull in the lookup-build helpers from src/static/gdacs/. The script
# is invoked from repo root (e.g. `uv run python scripts/build_...py`);
# the REPO_ROOT-on-sys.path shim keeps that working without requiring
# the package to be pip-installed.
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.static.gdacs.admin import (  # noqa: E402
    filter_gdacs_country,
    load_gdacs_admin,
)
from src.static.gdacs.inputs import (  # noqa: E402
    ATLANTIC_ISO3,
    DEFAULT_CONFIG,
    FM_ADM1_NAME_FALLBACK_ISOS,
    fallback_fm_name_from_adm0,
    load_fieldmaps_adm,
    load_level_config,
    resolve_gdacs_fm_level,
)
from src.static.gdacs.matcher import match_country  # noqa: E402

DEFAULT_LOCAL_OUT = REPO_ROOT / "data" / "gdacs_fm_lookup.csv"
DB_SCHEMA = "storms"
DB_TABLE = "gdacs_fm_lookup"

LOOKUP_COLUMNS = [
    "iso3", "admin_level", "fm_pcode", "fm_name",
    "gmi_admin", "gdacs_admin_name", "caveat_kind", "caveat_note",
]

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
# Row builders
# ─────────────────────────────────────────────────────────────────────

def build_adm0_row(
    iso3: str, fm_adm0: gpd.GeoDataFrame | pd.DataFrame | None,
    gmi_cntry: str, gdacs_cntry_name: str,
) -> dict:
    """Emit the single adm0 row for one country.

    `fm_pcode`: ISO3 universally. FM exposes a richer adm0_id for some
    countries (e.g. ``PRI-20250729``) but not all — many adm0 parquets
    contain only ``iso_3`` + ``geometry``. Using ISO3 keeps the schema
    uniform across the lookup and matches how the runtime joins.

    `fm_name`: FM's ``adm0_name`` when present, else the GDACS
    shapefile's ``CNTRY_NAME``. ISO3 only as last resort.

    `gmi_admin`: raw ``GMI_CNTRY`` from the GDACS shapefile — usually
    matches ISO3 but X-prefixed for some non-sovereign territories
    (e.g. ``XJE`` for JEY). Matters because
    ``storms.gdacs_exposure.gdacs_admin_code`` preserves the raw value
    at admin_level=0; the consumer joins on it.
    """
    fm_name = None
    if fm_adm0 is not None and len(fm_adm0):
        row = fm_adm0.iloc[0]
        if "adm0_name" in fm_adm0.columns and pd.notna(row.get("adm0_name")):
            fm_name = row["adm0_name"]
    if not fm_name:
        fm_name = gdacs_cntry_name or iso3
    return {
        "iso3": iso3,
        "admin_level": 0,
        "fm_pcode": iso3,
        "fm_name": fm_name,
        "gmi_admin": gmi_cntry,
        "gdacs_admin_name": gdacs_cntry_name or iso3,
        "caveat_kind": None,
        "caveat_note": None,
    }


def build_accept_adm1_rows(
    iso3: str,
    fm_level: int,
    gdacs_admin: gpd.GeoDataFrame,
    per_row_notes: list[dict],
    low_iou: float = 0.5,
) -> list[dict]:
    """Run the FM↔GDACS matcher and project results into lookup rows.

    Used for both `accept` and `needs_manual_mapping` actions — same
    data shape; only difference is that `needs_manual_mapping`
    countries have `[[per_row_notes]]` entries we attach as
    `caveat_note`.
    """
    match_rows = match_country(
        iso3, gdacs_admin, low_iou=low_iou, fm_level=fm_level,
    )
    # Drop unmatched / placeholder rows (no_overlap orphans like JAM
    # offshore cays, fm_load_error, gdacs_empty, etc.).
    match_rows = [
        r for r in match_rows
        if r.get("fm_pcode") and r.get("gmi_admin")
    ]
    if not match_rows:
        return []

    notes_by_pcode = {
        n["fm_pcode"]: n.get("note")
        for n in per_row_notes
        if n.get("iso3") == iso3
    }
    return [
        {
            "iso3": iso3,
            "admin_level": 1,
            "fm_pcode": r["fm_pcode"],
            "fm_name": r["fm_name"],
            "gmi_admin": r["gmi_admin"],
            "gdacs_admin_name": r["gdacs_admin_name"],
            "caveat_kind": None,
            "caveat_note": notes_by_pcode.get(r["fm_pcode"]),
        }
        for r in match_rows
    ]


def build_aggregate_adm1_rows(
    iso3: str, gdacs_admin: gpd.GeoDataFrame,
) -> list[dict]:
    """For aggregate_gdacs_to_fm countries: reverse spatial join.

    Each GDACS adm1 polygon for this country gets mapped to the FM adm1
    polygon containing its representative point. Falls back to maximum
    intersection-area when no FM polygon strictly contains the point
    (handles edge cases where GDACS and FM coastlines disagree slightly).
    """
    fm = load_fieldmaps_adm(iso3, 1)
    g = filter_gdacs_country(gdacs_admin, iso3)
    if fm is None or len(fm) == 0 or len(g) == 0:
        logger.warning(
            "Aggregate build for %s missing inputs (fm=%d, g=%d)",
            iso3, 0 if fm is None else len(fm), len(g),
        )
        return []

    pcode_col = next(
        (c for c in ("adm1_pcode", "adm1_id", "adm1_src") if c in fm.columns),
        None,
    )
    name_col = next(
        (c for c in ("adm1_name", "name_1") if c in fm.columns), None,
    )
    if pcode_col is None or name_col is None:
        logger.error(
            "Can't resolve FM adm1 columns for %s — got %s",
            iso3, list(fm.columns),
        )
        return []

    if fm.crs != g.crs:
        g = g.to_crs(fm.crs)

    rows = []
    for _, gp in g.iterrows():
        pt = gp.geometry.representative_point()
        contains_mask = fm.geometry.contains(pt)
        if contains_mask.any():
            container = fm[contains_mask].iloc[0]
        else:
            # Fallback: pick the FM polygon with the largest intersection
            inter = fm.geometry.intersection(gp.geometry).area
            if inter.max() == 0:
                logger.warning(
                    "GDACS %s in %s overlaps no FM adm1 polygon — skipped",
                    gp["GMI_ADMIN"], iso3,
                )
                continue
            container = fm.loc[inter.idxmax()]
        rows.append({
            "iso3": iso3,
            "admin_level": 1,
            "fm_pcode": container[pcode_col],
            "fm_name": container[name_col],
            "gmi_admin": gp["GMI_ADMIN"],
            "gdacs_admin_name": gp["ADMIN_NAME"],
            "caveat_kind": None,
            "caveat_note": None,
        })
    return rows


def build_fm_only_adm1_rows(
    iso3: str, fm_level: int = 1,
) -> list[dict]:
    """For ``fm_adm1_only`` countries: emit FM adm1 rows with
    ``gmi_admin = NULL`` so GDACS adm1 exposure deliberately does NOT
    attach at runtime.

    Use case: GDACS treats the country as a single whole-country
    polygon (no adm1 subdivision) but FM has real adm1 units AND a
    different source (NHC, ADAM) provides per-unit numbers we DO
    want to surface. Fanning the GDACS whole-country number out to
    each FM unit would be misleading (each unit would inherit the
    full country total). Leaving ``gmi_admin = NULL`` keeps the FM
    rows present in the lookup so NHC's per-unit data can attach
    via ``fm_pcode``, while the GDACS adm1 row lands as a
    by-design-unmatched orphan downstream.

    Example: BMU. GDACS has 1 whole-island polygon; FM has 9
    parishes; NHC publishes per-parish exposure.
    """
    fm = load_fieldmaps_adm(iso3, fm_level)
    if fm is None or len(fm) == 0:
        logger.warning("fm_adm1_only %s: FM empty at adm%d", iso3, fm_level)
        return []
    pcode_col, name_col = (
        next((c for c in (f"adm{fm_level}_pcode",
                          f"adm{fm_level}_id",
                          f"adm{fm_level}_src") if c in fm.columns), None),
        next((c for c in (f"adm{fm_level}_name",
                          f"name_{fm_level}") if c in fm.columns), None),
    )
    if pcode_col is None or name_col is None:
        logger.error(
            "Can't resolve FM adm%d columns for %s — got %s",
            fm_level, iso3, list(fm.columns),
        )
        return []

    # Narrow fallback for the two iso3s where FM has NULL adm1_name
    # and the real subdivision name sits in adm0_name. See
    # FM_ADM1_NAME_FALLBACK_ISOS in src/static/gdacs/inputs.py.
    def _name(row) -> str | None:
        v = row[name_col]
        if v is None or (isinstance(v, float) and v != v):
            if (iso3 in FM_ADM1_NAME_FALLBACK_ISOS
                    and "adm0_name" in row):
                return fallback_fm_name_from_adm0(row["adm0_name"])
            return None
        return v

    return [
        {
            "iso3": iso3,
            "admin_level": 1,
            "fm_pcode": r[pcode_col],
            "fm_name": _name(r),
            "gmi_admin": None,
            "gdacs_admin_name": None,
            "caveat_kind": "fm_only_policy",
            "caveat_note": None,
        }
        for _, r in fm.iterrows()
    ]


# ─────────────────────────────────────────────────────────────────────
# Driver
# ─────────────────────────────────────────────────────────────────────

def build_lookup(cfg: dict, gdacs_admin: gpd.GeoDataFrame) -> pd.DataFrame:
    """Produce the full lookup DataFrame in one pass."""
    per_row_notes = cfg.get("per_row_notes", [])
    policies = cfg.get("gdacs_policy", {})

    # Map iso3 → (GMI_CNTRY, CNTRY_NAME) for the adm0 rows
    g_country_meta = (
        gdacs_admin[["_iso3", "GMI_CNTRY", "CNTRY_NAME"]]
        .dropna(subset=["_iso3"]).drop_duplicates(subset=["_iso3"])
        .set_index("_iso3")
    )
    iso3_to_gmi_cntry = g_country_meta["GMI_CNTRY"].to_dict()
    iso3_to_cntry_name = g_country_meta["CNTRY_NAME"].to_dict()

    rows: list[dict] = []
    for iso3 in ATLANTIC_ISO3:
        # Skip iso3s flagged no_fm_source — these are documented as
        # GDACS-reported but FM-unavailable; the lookup intentionally
        # has no row for them, and downstream consumers (alert
        # pipeline, matching demo) read the policy to filter them out.
        if policies.get(iso3, {}).get("action") == "no_fm_source":
            logger.info("Skipping %s — policy=no_fm_source", iso3)
            continue

        # ── adm0 ──
        fm_adm0 = load_fieldmaps_adm(iso3, 0)
        gmi_cntry = iso3_to_gmi_cntry.get(iso3, iso3)
        cntry_name = iso3_to_cntry_name.get(iso3, iso3)
        rows.append(build_adm0_row(iso3, fm_adm0, gmi_cntry, cntry_name))

        # ── adm1 ──
        action = policies.get(iso3, {}).get("action")
        if action is None:
            logger.warning("%s has no [gdacs_policy] entry — skipping adm1", iso3)
            continue
        if action == "country_only":
            continue

        # `aggregate_gdacs_to_fm` and `fm_adm1_only` are meaningful
        # even when FM has a single adm1 polygon:
        #   aggregate_gdacs_to_fm — 1 FM ← N GDACS via reverse spatial
        #     join (e.g. PRI: 1 FM ← 8 senatorial districts)
        #   fm_adm1_only          — emits FM rows with no GDACS attach;
        #     1 FM unit is still a valid (if uninteresting) edge case
        # For accept/needs_manual_mapping, a single FM polygon means
        # nothing to crosswalk; skip.
        fm_adm1 = load_fieldmaps_adm(iso3, 1)
        if fm_adm1 is None or len(fm_adm1) == 0:
            continue
        if (
            len(fm_adm1) == 1
            and action not in ("aggregate_gdacs_to_fm", "fm_adm1_only")
        ):
            continue

        if action in ("accept", "needs_manual_mapping"):
            fm_lvl = resolve_gdacs_fm_level(iso3, cfg)
            rows.extend(build_accept_adm1_rows(
                iso3, fm_lvl, gdacs_admin, per_row_notes,
            ))
        elif action == "aggregate_gdacs_to_fm":
            rows.extend(build_aggregate_adm1_rows(iso3, gdacs_admin))
        elif action == "fm_adm1_only":
            fm_lvl = resolve_gdacs_fm_level(iso3, cfg)
            rows.extend(build_fm_only_adm1_rows(iso3, fm_lvl))
        else:
            logger.warning(
                "Unknown action '%s' for %s — skipping adm1", action, iso3,
            )

    df = pd.DataFrame(rows, columns=LOOKUP_COLUMNS)

    # ── Final pass: apply [[gdacs_shared_source]] group caveats ──
    # Each entry names a coarse GDACS polygon (gmi_admin) + the FM
    # units it covers + a shared caveat. We overwrite caveat_kind and
    # caveat_note on every matching row so the alert text downstream
    # can render the same footnote for any FM in the group. Used for
    # boundary-reform cases (CUB Artemisa/Mayabeque ↔ pre-2011 La
    # Habana; NIC RACN+RACS ↔ pre-1987 Zelaya; etc.). Warns when a
    # listed fm_pcode doesn't appear in the lookup.
    shared = cfg.get("gdacs_shared_source", [])
    for entry in shared:
        iso3 = entry.get("iso3")
        fm_set = set(entry.get("fm_pcodes", []))
        note = entry.get("note")
        kind = entry.get("caveat_kind", "source_coarser_than_fm")
        if not iso3 or not fm_set or not note:
            logger.warning("Skipping malformed gdacs_shared_source: %s", entry)
            continue
        mask = (
            (df["iso3"] == iso3)
            & (df["admin_level"] == 1)
            & (df["fm_pcode"].isin(fm_set))
        )
        n = int(mask.sum())
        if n != len(fm_set):
            present = set(df.loc[mask, "fm_pcode"])
            missing = sorted(fm_set - present)
            logger.warning(
                "gdacs_shared_source for %s gmi_admin=%s: applied %d of %d "
                "fm_pcodes; missing from lookup: %s",
                iso3, entry.get("gmi_admin"), n, len(fm_set), missing,
            )
        df.loc[mask, "caveat_kind"] = kind
        df.loc[mask, "caveat_note"] = note

    df.sort_values(
        ["iso3", "admin_level", "fm_pcode", "gmi_admin"],
        inplace=True, kind="stable",
    )
    df.reset_index(drop=True, inplace=True)
    return df


# ─────────────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────────────

def validate(df: pd.DataFrame, cfg: dict) -> None:
    """Sanity checks. Raise on failure so a bad build never ships.

    Each assertion encodes an invariant the prod pipeline relies on. If
    one trips, fix the upstream cause (policy TOML, algorithm output)
    rather than relaxing the check.
    """
    policies = cfg.get("gdacs_policy", {})

    # 1) Exactly one adm0 row per Atlantic ISO3 (excluding ones we
    #    deliberately skip via no_fm_source).
    no_fm = {
        i for i, p in policies.items()
        if p.get("action") == "no_fm_source"
    }
    expected_iso3 = set(ATLANTIC_ISO3) - no_fm
    adm0 = df[df["admin_level"] == 0]
    missing = expected_iso3 - set(adm0["iso3"])
    if missing:
        raise AssertionError(f"adm0 rows missing for: {sorted(missing)}")
    dupes = adm0["iso3"].value_counts()
    if (dupes > 1).any():
        raise AssertionError(
            f"duplicate adm0 rows: {dupes[dupes > 1].to_dict()}"
        )

    # 2) Action-specific adm1 row counts
    adm1_counts = df[df["admin_level"] == 1].groupby("iso3").size()
    for iso3, pol in policies.items():
        n = int(adm1_counts.get(iso3, 0))
        action = pol["action"]
        if action == "country_only" and n != 0:
            raise AssertionError(
                f"country_only {iso3} has {n} adm1 rows (expected 0)"
            )
        if action == "accept" and n == 0:
            # Tolerate degenerate-FM accept cases (none currently, but
            # the schema permits it) by checking FM adm1 count
            fm = load_fieldmaps_adm(iso3, 1)
            if fm is not None and len(fm) > 1:
                raise AssertionError(
                    f"accept {iso3} has 0 adm1 rows but FM has {len(fm)} polygons"
                )
        if action == "fm_adm1_only":
            sub = df[(df["iso3"] == iso3) & (df["admin_level"] == 1)]
            if sub["gmi_admin"].notna().any():
                bad = sub[sub["gmi_admin"].notna()]["fm_pcode"].tolist()
                raise AssertionError(
                    f"fm_adm1_only {iso3} has adm1 rows with non-NULL "
                    f"gmi_admin: {bad[:5]}"
                )
            # Expect at least one adm1 row (otherwise should be country_only)
            if n == 0:
                fm = load_fieldmaps_adm(iso3, 1)
                if fm is not None and len(fm) > 0:
                    raise AssertionError(
                        f"fm_adm1_only {iso3} has 0 adm1 rows but FM has "
                        f"{len(fm)} polygons"
                    )

    # 3) PK uniqueness on (iso3, admin_level, fm_pcode, gmi_admin)
    pk_dupes = df.duplicated(
        subset=["iso3", "admin_level", "fm_pcode", "gmi_admin"]
    )
    if pk_dupes.any():
        sample = df[pk_dupes].head(5).to_dict("records")
        raise AssertionError(
            f"duplicate PK rows ({pk_dupes.sum()}); first few: {sample}"
        )

    # 4) Every gmi_admin in adm1 rows should exist in the GDACS admin
    #    shapefile. Defer this check to the caller (avoids re-loading).
    logger.info(
        "Validation OK: %d adm0 rows + %d adm1 rows across %d countries",
        len(adm0), (df["admin_level"] == 1).sum(),
        df["iso3"].nunique(),
    )


def validate_gmi_admins(
    df: pd.DataFrame, gdacs_admin: gpd.GeoDataFrame,
) -> None:
    """Every non-NULL adm1 gmi_admin must reference a real GDACS admin
    polygon. NULL gmi_admin is allowed and intentional — it's the
    fingerprint of the ``fm_adm1_only`` policy (FM unit present in the
    lookup so other sources can attach via fm_pcode; GDACS adm1
    deliberately doesn't attach)."""
    valid_gmis = set(gdacs_admin["GMI_ADMIN"].dropna().unique())
    adm1 = df[df["admin_level"] == 1]
    referenced = set(adm1["gmi_admin"].dropna().unique())
    unknown = referenced - valid_gmis
    if unknown:
        raise AssertionError(
            f"adm1 references gmi_admin codes not in GDACS layer: "
            f"{sorted(unknown)[:10]}"
        )


# ─────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--mode", choices=["dev", "prod"], default="dev")
    p.add_argument(
        "--dry-run", action="store_true",
        help="write local CSV but skip blob upload",
    )
    p.add_argument(
        "--out", type=Path, default=DEFAULT_LOCAL_OUT,
        help="local CSV destination",
    )
    p.add_argument(
        "--config", type=Path, default=DEFAULT_CONFIG,
        help="policy TOML",
    )
    return p.parse_args()


def main() -> int:
    coloredlogs.install(
        level="INFO",
        fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger(
        "azure.core.pipeline.policies.http_logging_policy"
    ).setLevel(logging.WARNING)
    args = parse_args()

    logger.info("Loading policy from %s", args.config)
    cfg = load_level_config(args.config)
    logger.info(
        "  %d gdacs_policy entries, %d per_row_notes, %d gdacs_overrides",
        len(cfg.get("gdacs_policy", {})),
        len(cfg.get("per_row_notes", [])),
        len(cfg.get("gdacs_overrides", {})),
    )

    logger.info("Loading GDACS admin layer from blob (stage=%s)", args.mode)
    gdacs_admin = load_gdacs_admin(stage=args.mode)
    logger.info(
        "  %d GDACS admin polygons across %d distinct ISO3 codes",
        len(gdacs_admin), gdacs_admin["_iso3"].nunique(),
    )

    logger.info("Building lookup for %d Atlantic countries", len(ATLANTIC_ISO3))
    df = build_lookup(cfg, gdacs_admin)

    validate(df, cfg)
    validate_gmi_admins(df, gdacs_admin)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)
    logger.info("Wrote %d rows to %s (local CSV for inspection)", len(df), args.out)

    if args.dry_run:
        logger.info("--dry-run: skipping DB write")
    else:
        logger.info(
            "Writing %d rows to %s.%s (stage=%s, REPLACE)",
            len(df), DB_SCHEMA, DB_TABLE, args.mode,
        )
        engine = stratus.get_engine(stage=args.mode, write=True)
        df.to_sql(
            DB_TABLE,
            engine,
            schema=DB_SCHEMA,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=500,
        )
        logger.info("DB write complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
