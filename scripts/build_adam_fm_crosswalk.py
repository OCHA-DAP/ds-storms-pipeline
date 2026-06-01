"""Build the full FM × ADAM spatial crosswalk CSV.

Produces `data/adam_fm_crosswalk.csv` — one row per EVERY spatial
relationship between FieldMaps adm1 and ADAM admin polygons, in the
scope of ATLANTIC_ISO3. Three kinds of rows:

  * overlap        — FM polygon and ADAM polygon spatially intersect
                     (any nonzero overlap). Carries iou + areas.
  * fm_only        — FM adm1 polygon with no ADAM overlap in this iso3.
                     adam_* columns NULL.
  * adam_only      — ADAM admin polygon with no FM overlap in this iso3.
                     fm_* columns NULL.

For every row a default `status` is set:

  primary    — the ADAM polygon's best-IoU FM partner (one per ADAM)
  secondary  — overlap exists but the ADAM polygon's primary FM is
               a different unit (spillover from a neighboring ADAM)
  noise      — overlap exists but iou < 0.05 (boundary digitization)
  fm_only    — FM with no ADAM partner
  adam_only  — ADAM with no FM partner

The reviewer edits the `status` column where they disagree.
`build_adam_fm_lookup.py` reads this CSV and emits one lookup row per
status="primary" (or "secondary" if you want them in the lookup too).

Schema
------
iso3, fm_pcode, fm_name, fm_area_m2,
adam_admin_id, adam_admin_name, adam_area_m2,
intersection_m2, iou,
row_kind, status, note

Run from repo root::

    uv run python scripts/build_adam_fm_crosswalk.py [--out PATH]
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

from src.static.adam.admin import filter_adam_country, load_adam_admin  # noqa: E402
from src.static.adam.inputs import (  # noqa: E402
    ATLANTIC_ISO3,
    DEFAULT_CONFIG,
    FM_ADM1_NAME_FALLBACK_ISOS,
    fallback_fm_name_from_adm0,
    fm_id_and_name_cols,
    load_fieldmaps_adm,
    load_level_config,
    resolve_adam_fm_level,
)


DEFAULT_OUT = REPO_ROOT / "data" / "adam_fm_crosswalk.csv"
AREA_CRS = 6933  # World Cylindrical Equal Area (m²)
NOISE_IOU = 0.05  # below this, default status = "noise"

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
    adam_admin: gpd.GeoDataFrame,
    cfg: dict,
) -> list[dict]:
    """Emit all crosswalk rows for one country.

    Returns a list of dicts with the schema documented at the top of
    this module. May be empty if FM is unavailable for the iso3.
    """
    fm_level = resolve_adam_fm_level(iso3, cfg)
    try:
        fm = load_fieldmaps_adm(iso3, fm_level)
    except Exception as e:
        logger.warning("FM load failed for %s adm%d: %s", iso3, fm_level, e)
        return []
    if fm is None or len(fm) == 0:
        return []

    fm = fm.copy()
    pcode_col, name_col = _resolve_fm_cols(fm, fm_level, iso3)
    if pcode_col is None:
        logger.warning("Can't resolve FM cols for %s — got %s",
                       iso3, list(fm.columns))
        return []

    aa = filter_adam_country(adam_admin, iso3).copy()
    if aa.geometry.name != "geometry":
        aa = aa.rename_geometry("geometry")
    if fm.geometry.name != "geometry":
        fm = fm.rename_geometry("geometry")

    # Reproject both to an equal-area CRS for honest m² math. IoU is
    # CRS-invariant but the area columns we surface aren't.
    if aa.crs != fm.crs:
        aa = aa.to_crs(fm.crs)
    fm = fm.to_crs(AREA_CRS)
    aa = aa.to_crs(AREA_CRS)

    fm = fm[[pcode_col, name_col, "geometry"]].copy()
    fm.rename(
        columns={pcode_col: "_fm_pcode", name_col: "_fm_name"},
        inplace=True,
    )
    fm["_fm_idx"] = range(len(fm))
    fm["_fm_area"] = fm.geometry.area

    aa = aa[["adm1_id", "adm1_name", "geometry"]].copy()
    aa.rename(
        columns={"adm1_id": "_aa_id", "adm1_name": "_aa_name"},
        inplace=True,
    )
    aa["_aa_idx"] = range(len(aa))
    aa["_aa_area"] = aa.geometry.area

    rows: list[dict] = []

    # Full overlay — every overlapping FM × ADAM pair with nonzero area.
    if len(aa) > 0:
        overlay = gpd.overlay(fm, aa, how="intersection",
                              keep_geom_type=True)
    else:
        overlay = fm.iloc[0:0].copy()

    if len(overlay):
        overlay["_inter"] = overlay.geometry.area
        overlay = overlay[overlay["_inter"] > 0]

    if len(overlay):
        overlay["_iou"] = overlay["_inter"] / (
            overlay["_fm_area"] + overlay["_aa_area"] - overlay["_inter"]
        )
        # For each ADAM polygon, mark its best-IoU FM partner as primary.
        # All other overlaps for that ADAM polygon become "secondary".
        best_fm_per_aa = overlay.sort_values(
            "_iou", ascending=False,
        ).drop_duplicates("_aa_idx", keep="first")[
            ["_aa_idx", "_fm_idx"]
        ].rename(columns={"_fm_idx": "_primary_fm_idx"})
        overlay = overlay.merge(best_fm_per_aa, on="_aa_idx", how="left")

        for _, r in overlay.iterrows():
            iou = float(r["_iou"])
            is_primary = (r["_fm_idx"] == r["_primary_fm_idx"])
            if iou < NOISE_IOU and not is_primary:
                # Below-noise secondary spillover — flag as noise so the
                # reviewer can keep/drop without scrolling
                status = "noise"
            elif is_primary:
                status = "primary"
            else:
                status = "secondary"
            rows.append({
                "iso3": iso3,
                "fm_pcode": r["_fm_pcode"],
                "fm_name": r["_fm_name"],
                "fm_area_m2": float(r["_fm_area"]),
                "adam_admin_id": int(r["_aa_id"]),
                "adam_admin_name": r["_aa_name"],
                "adam_area_m2": float(r["_aa_area"]),
                "intersection_m2": float(r["_inter"]),
                "iou": iou,
                "row_kind": "overlap",
                "status": status,
                "note": "",
            })

    # FM-only rows: every FM polygon with no overlap above.
    covered_fm_idx = set(overlay["_fm_idx"]) if len(overlay) else set()
    for _, frow in fm.iterrows():
        if frow["_fm_idx"] in covered_fm_idx:
            continue
        rows.append({
            "iso3": iso3,
            "fm_pcode": frow["_fm_pcode"],
            "fm_name": frow["_fm_name"],
            "fm_area_m2": float(frow["_fm_area"]),
            "adam_admin_id": None,
            "adam_admin_name": None,
            "adam_area_m2": None,
            "intersection_m2": None,
            "iou": None,
            "row_kind": "fm_only",
            "status": "fm_only",
            "note": "",
        })

    # ADAM-only rows: every ADAM polygon with no overlap above.
    covered_aa_idx = set(overlay["_aa_idx"]) if len(overlay) else set()
    for _, arow in aa.iterrows():
        if arow["_aa_idx"] in covered_aa_idx:
            continue
        rows.append({
            "iso3": iso3,
            "fm_pcode": None,
            "fm_name": None,
            "fm_area_m2": None,
            "adam_admin_id": int(arow["_aa_id"]),
            "adam_admin_name": arow["_aa_name"],
            "adam_area_m2": float(arow["_aa_area"]),
            "intersection_m2": None,
            "iou": None,
            "row_kind": "adam_only",
            "status": "adam_only",
            "note": "",
        })

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
                    help="policy TOML (read for resolve_adam_fm_level)")
    args = ap.parse_args()

    cfg = load_level_config(args.config)
    logger.info("Loaded policy from %s", args.config)

    adam = load_adam_admin()
    logger.info(
        "Loaded ADAM admin layer: %d polygons across %d iso3s",
        len(adam), adam["iso3"].nunique(),
    )

    all_rows: list[dict] = []
    for iso3 in tqdm(ATLANTIC_ISO3, desc="crosswalk"):
        all_rows.extend(country_crosswalk(iso3, adam, cfg))

    df = pd.DataFrame(all_rows, columns=[
        "iso3", "fm_pcode", "fm_name", "fm_area_m2",
        "adam_admin_id", "adam_admin_name", "adam_area_m2",
        "intersection_m2", "iou",
        "row_kind", "status", "note",
    ])
    df.sort_values(
        ["iso3", "row_kind", "fm_pcode", "adam_admin_id"],
        inplace=True, kind="stable", na_position="last",
    )
    df.reset_index(drop=True, inplace=True)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)
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
