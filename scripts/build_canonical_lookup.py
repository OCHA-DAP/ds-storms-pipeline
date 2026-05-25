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
  directly each build via ``load_full_gdacs``; no local cache

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
    load_full_gdacs,
)
from src.static.gdacs.inputs import (  # noqa: E402
    ATLANTIC_ISO3,
    DEFAULT_CONFIG,
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
    "gmi_admin", "gdacs_admin_name", "caveat_note",
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
        "caveat_note": None,
    }


def build_accept_adm1_rows(
    iso3: str,
    fm_level: int,
    gdacs_full: gpd.GeoDataFrame,
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
        iso3, gdacs_full, low_iou=low_iou, fm_level=fm_level,
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
            "caveat_note": notes_by_pcode.get(r["fm_pcode"]),
        }
        for r in match_rows
    ]


def build_aggregate_adm1_rows(
    iso3: str, gdacs_full: gpd.GeoDataFrame,
) -> list[dict]:
    """For aggregate_gdacs_to_fm countries: reverse spatial join.

    Each GDACS adm1 polygon for this country gets mapped to the FM adm1
    polygon containing its representative point. Falls back to maximum
    intersection-area when no FM polygon strictly contains the point
    (handles edge cases where GDACS and FM coastlines disagree slightly).
    """
    fm = load_fieldmaps_adm(iso3, 1)
    g = filter_gdacs_country(gdacs_full, iso3)
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
            "caveat_note": None,
        })
    return rows


# ─────────────────────────────────────────────────────────────────────
# Driver
# ─────────────────────────────────────────────────────────────────────

def build_lookup(cfg: dict, gdacs_full: gpd.GeoDataFrame) -> pd.DataFrame:
    """Produce the full lookup DataFrame in one pass."""
    per_row_notes = cfg.get("per_row_notes", [])
    policies = cfg.get("gdacs_policy", {})

    # Map iso3 → (GMI_CNTRY, CNTRY_NAME) for the adm0 rows
    g_country_meta = (
        gdacs_full[["_iso3", "GMI_CNTRY", "CNTRY_NAME"]]
        .dropna(subset=["_iso3"]).drop_duplicates(subset=["_iso3"])
        .set_index("_iso3")
    )
    iso3_to_gmi_cntry = g_country_meta["GMI_CNTRY"].to_dict()
    iso3_to_cntry_name = g_country_meta["CNTRY_NAME"].to_dict()

    rows: list[dict] = []
    for iso3 in ATLANTIC_ISO3:
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

        # `aggregate_gdacs_to_fm` is the one action that's meaningful
        # even when FM has a single adm1 polygon (e.g. PRI: 1 FM
        # polygon ← 8 GDACS senatorial districts via reverse spatial
        # join). For accept/needs_manual_mapping, a single FM polygon
        # means there's nothing to crosswalk; skip.
        fm_adm1 = load_fieldmaps_adm(iso3, 1)
        if fm_adm1 is None or len(fm_adm1) == 0:
            continue
        if len(fm_adm1) == 1 and action != "aggregate_gdacs_to_fm":
            continue

        if action in ("accept", "needs_manual_mapping"):
            fm_lvl = resolve_gdacs_fm_level(iso3, cfg)
            rows.extend(build_accept_adm1_rows(
                iso3, fm_lvl, gdacs_full, per_row_notes,
            ))
        elif action == "aggregate_gdacs_to_fm":
            rows.extend(build_aggregate_adm1_rows(iso3, gdacs_full))
        else:
            logger.warning(
                "Unknown action '%s' for %s — skipping adm1", action, iso3,
            )

    df = pd.DataFrame(rows, columns=LOOKUP_COLUMNS)
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

    # 1) Exactly one adm0 row per Atlantic ISO3
    adm0 = df[df["admin_level"] == 0]
    missing = set(ATLANTIC_ISO3) - set(adm0["iso3"])
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
    df: pd.DataFrame, gdacs_full: gpd.GeoDataFrame,
) -> None:
    """Every adm1 gmi_admin must reference a real GDACS admin polygon."""
    valid_gmis = set(gdacs_full["GMI_ADMIN"].dropna().unique())
    adm1 = df[df["admin_level"] == 1]
    unknown = set(adm1["gmi_admin"]) - valid_gmis
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
    gdacs_full = load_full_gdacs(stage=args.mode)
    logger.info(
        "  %d GDACS admin polygons across %d distinct ISO3 codes",
        len(gdacs_full), gdacs_full["_iso3"].nunique(),
    )

    logger.info("Building lookup for %d Atlantic countries", len(ATLANTIC_ISO3))
    df = build_lookup(cfg, gdacs_full)

    validate(df, cfg)
    validate_gmi_admins(df, gdacs_full)

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
