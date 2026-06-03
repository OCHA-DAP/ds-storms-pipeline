"""Build the full FM × GDACS spatial crosswalk CSV.

Produces `data/gdacs_fm_crosswalk.csv` — one row per EVERY spatial
relationship between FieldMaps adm1 and GDACS admin polygons, in the
scope of ATLANTIC_ISO3. Three kinds of rows:

  * overlap        — FM polygon and GDACS polygon spatially intersect
                     (any nonzero overlap). Carries iou + areas.
  * fm_only        — FM adm1 polygon with no GDACS overlap in this iso3.
                     gdacs_* columns NULL.
  * gdacs_only     — GDACS admin polygon with no FM overlap in this iso3.
                     fm_* columns NULL.

For every row a default `status` is set, based on the *topological*
relationship between the FM and GDACS polygons (counts of partners
above the noise threshold):

  match        — 1 FM ↔ 1 GDACS. Clean correspondence.
  gdacs_in_fm  — N GDACS polygons nested inside 1 FM polygon. Each row
                 is an equal participant (no ranking). Classic
                 aggregate-up case (e.g. BEL: 11 GDACS provinces → 3
                 FM regions).
  fm_in_gdacs  — N FM polygons nested inside 1 GDACS polygon. Each row
                 is an equal participant. GDACS is coarser than FM.
  fragmented   — both sides have multiplicity (many-to-many). Needs
                 reviewer judgment.
  noise        — iou < 0.05 (boundary digitization, ignored for counts)
  fm_only      — FM with no GDACS partner above noise
  gdacs_only   — GDACS with no FM partner above noise

Policy-driven overrides (set in `[gdacs_policy.X]`) take precedence
over the topological label:

  country_only          → all overlap rows → `drop`
  fm_adm1_only          → all overlap rows → `drop`
  no_fm_source          → all overlap rows → `drop`
  needs_manual_mapping  → all overlap rows → `needs_review`

Reviewer can override any row by setting `status` + `classification_type=human`.

Schema
------
iso3,
policy, policy_note, policy_fm_level,
fm_pcode, fm_name, fm_area_m2,
gmi_admin, gdacs_admin_name, gdacs_area_m2,
intersection_m2, iou,
row_kind, status, classification_type, note

`policy` / `policy_note` / `policy_fm_level` are read-only context
lifted from `[gdacs_policy.X]` (and `[gdacs_overrides.X]`) in the
config TOML so the reviewer can see which curation decision currently
governs the row. Editing those columns has no effect — drive behavior
with `status`.

`classification_type` records who decided the `status`:
  spatial — the default from spatial overlay heuristics
  llm     — Claude (or the migration step) refined the spatial default
  human   — final-pass reviewer override

Run from repo root::

    uv run python scripts/build_gdacs_fm_crosswalk.py [--out PATH]
"""

import argparse
import logging
import sys
from pathlib import Path

import coloredlogs
import geopandas as gpd
import pandas as pd
from tqdm import tqdm


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
    fm_id_and_name_cols,
    load_fieldmaps_adm,
    load_level_config,
    resolve_gdacs_fm_level,
)


DEFAULT_OUT = REPO_ROOT / "data" / "gdacs_fm_crosswalk.csv"
AREA_CRS = 6933  # World Cylindrical Equal Area (m²)
NOISE_IOU = 0.05  # below this, default status = "noise"

# Policies whose overlap rows are all unused by the lookup. The spatial
# overlay still enumerates the rows for visibility, but `status` is
# pre-set so the reviewer doesn't have to walk them one by one.
POLICY_OVERLAP_STATUS = {
    "country_only": "drop",
    "fm_adm1_only": "drop",
    "no_fm_source": "drop",
    # NOTE: GDACS does NOT override needs_manual_mapping → needs_review
    # (unlike ADAM). The existing storms.gdacs_fm_lookup already emits
    # clean-match rows for needs_manual_mapping countries (CAN, NIC,
    # CUB, PAN, ISL, GRL); the policy was a "this country needs
    # review" flag, not a "suppress all rows" directive. The migration
    # script preserves those decisions and the topology-derived status
    # speaks for itself. Reviewers can filter on the `policy` column
    # to find countries that still warrant attention.
}

logger = logging.getLogger(__name__)


def _resolve_fm_cols(fm: gpd.GeoDataFrame, fm_level: int, iso3: str):
    """Return (pcode_col, name_col) and apply the FM_ADM1_NAME_FALLBACK
    where needed. Returns (None, None) if columns can't be resolved."""
    if fm_level == 0:
        fm["adm0_pcode"] = iso3
        if "adm0_name" not in fm.columns:
            fm["adm0_name"] = iso3
        return "adm0_pcode", "adm0_name"
    pcode_col, name_col = fm_id_and_name_cols(fm, fm_level)
    if pcode_col is None or name_col is None:
        return None, None
    if (iso3 in FM_ADM1_NAME_FALLBACK_ISOS
            and "adm0_name" in fm.columns
            and fm[name_col].isna().any()):
        fm[name_col] = fm[name_col].fillna(
            fm["adm0_name"].map(fallback_fm_name_from_adm0)
        )
    return pcode_col, name_col


def country_crosswalk(
    iso3: str,
    gdacs_admin: gpd.GeoDataFrame,
    cfg: dict,
) -> list[dict]:
    """Emit all crosswalk rows for one country.

    Returns a list of dicts with the schema documented at the top of
    this module. May be empty only if both FM and GDACS are unavailable.
    """
    fm_level = resolve_gdacs_fm_level(iso3, cfg)
    policy_entry = (cfg.get("gdacs_policy") or {}).get(iso3, {}) or {}
    policy = policy_entry.get("action")
    policy_note = policy_entry.get("note", "")
    overlap_status_override = POLICY_OVERLAP_STATUS.get(policy)

    def policy_cols(extra: dict) -> dict:
        out = {
            "policy": policy,
            "policy_note": policy_note,
            "policy_fm_level": fm_level,
            **extra,
        }
        out.setdefault("classification_type", "spatial")
        return out

    gd = filter_gdacs_country(gdacs_admin, iso3).copy()
    fm = None
    try:
        fm = load_fieldmaps_adm(iso3, fm_level)
    except Exception as e:
        logger.warning("FM load failed for %s adm%d: %s", iso3, fm_level, e)
        fm = None

    if fm is None or len(fm) == 0:
        # Still surface gdacs_only rows so the country doesn't vanish.
        rows: list[dict] = []
        if gd.geometry.name != "geometry":
            gd = gd.rename_geometry("geometry")
        if len(gd) == 0:
            return rows
        gd = gd.to_crs(AREA_CRS)
        for _, grow in gd.iterrows():
            rows.append(policy_cols({
                "iso3": iso3,
                "fm_pcode": None,
                "fm_name": None,
                "fm_area_m2": None,
                "gmi_admin": grow.get("GMI_ADMIN"),
                "gdacs_admin_name": grow.get("ADMIN_NAME"),
                "gdacs_area_m2": float(grow.geometry.area),
                "intersection_m2": None,
                "iou": None,
                "row_kind": "gdacs_only",
                "status": "gdacs_only",
                "note": "FM unavailable for this iso3",
            }))
        return rows

    fm = fm.copy()
    pcode_col, name_col = _resolve_fm_cols(fm, fm_level, iso3)
    if pcode_col is None:
        logger.warning("Can't resolve FM cols for %s — got %s",
                       iso3, list(fm.columns))
        return []
    if gd.geometry.name != "geometry":
        gd = gd.rename_geometry("geometry")
    if fm.geometry.name != "geometry":
        fm = fm.rename_geometry("geometry")

    # Reproject both to an equal-area CRS for honest m² math. IoU is
    # CRS-invariant but the area columns we surface aren't.
    if gd.crs != fm.crs:
        gd = gd.to_crs(fm.crs)
    fm = fm.to_crs(AREA_CRS)
    gd = gd.to_crs(AREA_CRS)

    fm = fm[[pcode_col, name_col, "geometry"]].copy()
    fm.rename(
        columns={pcode_col: "_fm_pcode", name_col: "_fm_name"},
        inplace=True,
    )
    fm["_fm_idx"] = range(len(fm))
    fm["_fm_area"] = fm.geometry.area

    gd = gd[["GMI_ADMIN", "ADMIN_NAME", "geometry"]].copy()
    gd.rename(
        columns={"GMI_ADMIN": "_gmi_admin", "ADMIN_NAME": "_gdacs_name"},
        inplace=True,
    )
    gd["_gd_idx"] = range(len(gd))
    gd["_gd_area"] = gd.geometry.area

    rows: list[dict] = []

    # Full overlay — every overlapping FM × GDACS pair with nonzero area.
    if len(gd) > 0:
        overlay = gpd.overlay(fm, gd, how="intersection",
                              keep_geom_type=True)
    else:
        overlay = fm.iloc[0:0].copy()

    if len(overlay):
        overlay["_inter"] = overlay.geometry.area
        overlay = overlay[overlay["_inter"] > 0]

    if len(overlay):
        overlay["_iou"] = overlay["_inter"] / (
            overlay["_fm_area"] + overlay["_gd_area"] - overlay["_inter"]
        )
        # Topological labels from multiplicity counts. Only above-noise
        # overlaps contribute — a tiny spillover doesn't make an FM
        # look "fragmented" by accident.
        above_noise = overlay[overlay["_iou"] >= NOISE_IOU]
        fm_gd_count = above_noise.groupby("_fm_idx").size().to_dict()
        gd_fm_count = above_noise.groupby("_gd_idx").size().to_dict()

        for _, r in overlay.iterrows():
            iou = float(r["_iou"])
            if iou < NOISE_IOU:
                status = "noise"
            else:
                n_gdacs_in_this_fm = fm_gd_count.get(r["_fm_idx"], 0)
                n_fms_in_this_gdacs = gd_fm_count.get(r["_gd_idx"], 0)
                if n_gdacs_in_this_fm == 1 and n_fms_in_this_gdacs == 1:
                    status = "match"
                elif n_gdacs_in_this_fm > 1 and n_fms_in_this_gdacs == 1:
                    # FM contains multiple GDACS → this GDACS is one
                    # of N nested inside the FM
                    status = "gdacs_in_fm"
                elif n_gdacs_in_this_fm == 1 and n_fms_in_this_gdacs > 1:
                    # GDACS contains multiple FMs → this FM is one of
                    # N nested inside the GDACS
                    status = "fm_in_gdacs"
                else:
                    status = "fragmented"
            # Policy-driven overrides take precedence over topology,
            # but only for above-noise rows. Below-noise is boundary
            # digitization fuzz regardless of policy — labeling it
            # `noise` is the honest default.
            if iou >= NOISE_IOU and overlap_status_override is not None:
                status = overlap_status_override
            rows.append(policy_cols({
                "iso3": iso3,
                "fm_pcode": r["_fm_pcode"],
                "fm_name": r["_fm_name"],
                "fm_area_m2": float(r["_fm_area"]),
                "gmi_admin": r["_gmi_admin"],
                "gdacs_admin_name": r["_gdacs_name"],
                "gdacs_area_m2": float(r["_gd_area"]),
                "intersection_m2": float(r["_inter"]),
                "iou": iou,
                "row_kind": "overlap",
                "status": status,
                "note": "",
            }))

    # FM-only rows: every FM polygon with no overlap above.
    covered_fm_idx = set(overlay["_fm_idx"]) if len(overlay) else set()
    for _, frow in fm.iterrows():
        if frow["_fm_idx"] in covered_fm_idx:
            continue
        rows.append(policy_cols({
            "iso3": iso3,
            "fm_pcode": frow["_fm_pcode"],
            "fm_name": frow["_fm_name"],
            "fm_area_m2": float(frow["_fm_area"]),
            "gmi_admin": None,
            "gdacs_admin_name": None,
            "gdacs_area_m2": None,
            "intersection_m2": None,
            "iou": None,
            "row_kind": "fm_only",
            "status": "fm_only",
            "note": "",
        }))

    # GDACS-only rows: every GDACS polygon with no overlap above.
    covered_gd_idx = set(overlay["_gd_idx"]) if len(overlay) else set()
    for _, grow in gd.iterrows():
        if grow["_gd_idx"] in covered_gd_idx:
            continue
        rows.append(policy_cols({
            "iso3": iso3,
            "fm_pcode": None,
            "fm_name": None,
            "fm_area_m2": None,
            "gmi_admin": grow["_gmi_admin"],
            "gdacs_admin_name": grow["_gdacs_name"],
            "gdacs_area_m2": float(grow["_gd_area"]),
            "intersection_m2": None,
            "iou": None,
            "row_kind": "gdacs_only",
            "status": "gdacs_only",
            "note": "",
        }))

    return rows


def main() -> int:
    coloredlogs.install(
        level="INFO",
        fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger(
        "azure.core.pipeline.policies.http_logging_policy"
    ).setLevel(logging.WARNING)

    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT,
                    help="output CSV path")
    ap.add_argument("--config", type=Path, default=DEFAULT_CONFIG,
                    help="policy TOML (read for resolve_gdacs_fm_level)")
    args = ap.parse_args()

    cfg = load_level_config(args.config)
    logger.info("Loaded policy from %s", args.config)

    gdacs = load_gdacs_admin()
    logger.info(
        "Loaded GDACS admin layer: %d polygons across %d iso3s",
        len(gdacs), gdacs["_iso3"].nunique(),
    )

    all_rows: list[dict] = []
    for iso3 in tqdm(ATLANTIC_ISO3, desc="crosswalk"):
        all_rows.extend(country_crosswalk(iso3, gdacs, cfg))

    df = pd.DataFrame(all_rows, columns=[
        "iso3",
        "policy", "policy_note", "policy_fm_level",
        "fm_pcode", "fm_name", "fm_area_m2",
        "gmi_admin", "gdacs_admin_name", "gdacs_area_m2",
        "intersection_m2", "iou",
        "row_kind", "status", "classification_type", "note",
    ])
    df.sort_values(
        ["iso3", "row_kind", "fm_pcode", "gmi_admin"],
        inplace=True, kind="stable", na_position="last",
    )
    df.reset_index(drop=True, inplace=True)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    # utf-8-sig: BOM so Excel-on-Mac doesn't misread the file
    df.to_csv(args.out, index=False, encoding="utf-8-sig")
    logger.info("Wrote %d rows to %s", len(df), args.out)

    # Per-iso3 + per-status summary so the reviewer knows what's there
    summary = (
        df.groupby(["iso3", "status"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )
    logger.info("\n%s", summary.to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
