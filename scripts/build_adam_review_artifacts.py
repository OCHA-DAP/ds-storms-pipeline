"""Regenerate the per-country review artifacts the ADAM review qmd reads.

⚠️  EXPLORATORY / REVIEW-ONLY — NOT live production monitoring, not run by
the Databricks bundle or CI. Manual aid for producing/checking the
human-reviewed crosswalk. Calls the review-only matcher in
`src/static/adam/matcher.py` (the production lookup does not).


Mirror of :mod:`scripts.build_review_artifacts` but for the FM↔ADAM
bridge. Produces under ``data/review/`` (gitignored):

  - adam_fm_summary.csv        per-country match stats
  - adam_review_countries.txt  ISOs needing per-country review
  - adam_clean_countries.txt   ISOs that matched cleanly
  - adam_review_geoms.gpkg     FM + ge_adm1 polygons for review ISOs
  - adam_per_country/{ISO}.parquet  per-country match parquet cache

The review qmd lives at ``exploration/review_report_adam.qmd`` and
reads these files from ``../data/review/``.

Delete ``data/review/adam_per_country/`` to force a re-match.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import coloredlogs
import geopandas as gpd
import pandas as pd
from tqdm import tqdm


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.static.adam.admin import (  # noqa: E402
    filter_adam_country, load_adam_admin,
)
from src.static.adam.inputs import (  # noqa: E402
    ATLANTIC_ISO3,
    DEFAULT_CONFIG,
    fm_id_and_name_cols,
    load_fieldmaps_adm,
    load_level_config,
    resolve_adam_fm_level,
)
from src.static.adam.matcher import match_country  # noqa: E402


OUT_DIR = REPO_ROOT / "data" / "review"
PER_COUNTRY_DIR = OUT_DIR / "adam_per_country"
GEOMS_PATH = OUT_DIR / "adam_review_geoms.gpkg"
SUMMARY_PATH = OUT_DIR / "adam_fm_summary.csv"
CLEAN_PATH = OUT_DIR / "adam_clean_countries.txt"
REVIEW_PATH = OUT_DIR / "adam_review_countries.txt"

LOW_IOU = 0.5
DEFAULT_FM_LEVEL = 1

logger = logging.getLogger(__name__)


def match_with_cache(
    iso3: str, fm_level: int, ge: gpd.GeoDataFrame, force: bool,
) -> pd.DataFrame:
    """Run :func:`match_country`, caching to per-country parquet."""
    out = PER_COUNTRY_DIR / f"{iso3}.parquet"
    if out.exists() and not force:
        return pd.read_parquet(out)
    try:
        rows = match_country(
            iso3, ge, low_iou=LOW_IOU, fm_level=fm_level,
        )
    except Exception as e:
        logger.exception("match failed for %s", iso3)
        rows = [{"iso3": iso3, "fm_level": fm_level,
                 "issue": "exception", "detail": repr(e)}]
    df = pd.DataFrame(rows)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".parquet.tmp")
    df.to_parquet(tmp, index=False)
    tmp.rename(out)
    return df


def per_country_summary_row(iso3: str, df: pd.DataFrame) -> dict:
    """One row with the ADAM-side column set (parallel to the GDACS
    summary but ``ge_count`` instead of ``gdacs_count``)."""
    fm_level = (
        int(df["fm_level"].iloc[0])
        if "fm_level" in df.columns and len(df) else DEFAULT_FM_LEVEL
    )
    has_fm = "fm_pcode" in df.columns and df["fm_pcode"].notna().any()
    if not has_fm:
        issue = (
            df["issue"].iloc[0]
            if "issue" in df.columns and len(df) else "unknown"
        )
        return {
            "iso3": iso3, "fm_level": fm_level,
            "fm_count": 0, "adam_admin_count": 0,
            "n_clean": 0, "n_low_iou": 0,
            "n_multi": 0, "n_no_overlap": 0,
            "mean_iou": None, "min_iou": None,
            "status": issue,
        }
    matched = df[df["fm_pcode"].notna()]
    n = len(matched)
    n_clean = int(matched["issue"].isna().sum())
    n_low = int((matched["issue"] == "low_iou").sum())
    n_multi = int((matched["issue"] == "multiple_candidates").sum())
    n_none = int((matched["issue"] == "no_overlap").sum())
    with_iou = matched[matched["issue"] != "no_overlap"]["iou"]
    ge_count = (
        int(matched["adam_admin_count"].iloc[0])
        if "adam_admin_count" in matched.columns else 0
    )
    return {
        "iso3": iso3,
        "fm_level": fm_level,
        "fm_count": n,
        "adam_admin_count": ge_count,
        "n_clean": n_clean, "n_low_iou": n_low,
        "n_multi": n_multi, "n_no_overlap": n_none,
        "mean_iou": float(with_iou.mean()) if len(with_iou) else None,
        "min_iou": float(with_iou.min()) if len(with_iou) else None,
        "status": "clean" if n_clean == n else "review",
    }


def write_review_geoms(review_iso3: list[str], ge: gpd.GeoDataFrame,
                       cfg: dict) -> None:
    """Stack FM + ge_adm1 polygons for review ISOs into one GeoPackage
    with ``fm`` and ``ge`` layers — schema the qmd's per-country maps
    expect."""
    fm_frames, g_frames = [], []
    for iso3 in tqdm(review_iso3, desc="geoms"):
        lvl = resolve_adam_fm_level(iso3, cfg)
        try:
            fm = load_fieldmaps_adm(iso3, lvl)
        except Exception:
            fm = None
        if fm is not None and len(fm) > 0:
            pcode_col, name_col = fm_id_and_name_cols(fm, lvl)
            fm = fm.assign(iso3=iso3)
            if pcode_col is not None:
                fm = fm.rename(columns={pcode_col: "fm_pcode"})
            else:
                fm["fm_pcode"] = None
            if name_col is not None:
                fm = fm.rename(columns={name_col: "fm_name"})
            else:
                fm["fm_name"] = None
            fm_frames.append(fm[["iso3", "fm_pcode", "fm_name", "geometry"]])
        g = filter_adam_country(ge, iso3)
        if len(g) > 0:
            # ge_adm1's geometry col is `shape`; normalize so the
            # GeoPackage layer is uniform with the FM side.
            if g.geometry.name != "geometry":
                g = g.rename_geometry("geometry")
            g = g.assign(iso3=iso3)
            g_frames.append(g[["iso3", "adm1_name", "geometry"]])

    if fm_frames:
        fm_all = gpd.GeoDataFrame(
            pd.concat(fm_frames, ignore_index=True), crs=fm_frames[0].crs,
        )
        fm_all.to_file(GEOMS_PATH, layer="fm", driver="GPKG")
    if g_frames:
        g_all = gpd.GeoDataFrame(
            pd.concat(g_frames, ignore_index=True), crs=g_frames[0].crs,
        )
        g_all.to_file(GEOMS_PATH, layer="ge", driver="GPKG")


def main() -> int:
    coloredlogs.install(
        level="INFO",
        fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger(
        "azure.core.pipeline.policies.http_logging_policy"
    ).setLevel(logging.WARNING)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    PER_COUNTRY_DIR.mkdir(parents=True, exist_ok=True)

    cfg = load_level_config(DEFAULT_CONFIG)
    logger.info("Loaded policy: %d adam_policy entries",
                len(cfg.get("adam_policy", {})))

    ge = load_adam_admin(stage="dev")
    logger.info("Loaded %d ge_adm1 polys (%d distinct ISO3)",
                len(ge), ge["iso3"].nunique())

    force = "--force" in sys.argv
    summary_rows = []
    for iso3 in tqdm(ATLANTIC_ISO3, desc="match"):
        fm_lvl = resolve_adam_fm_level(iso3, cfg)
        df = match_with_cache(iso3, fm_lvl, ge, force=force)
        summary_rows.append(per_country_summary_row(iso3, df))

    summary = pd.DataFrame(summary_rows)
    summary.to_csv(SUMMARY_PATH, index=False)
    logger.info("Wrote %s (%d rows)", SUMMARY_PATH, len(summary))

    clean = summary[summary["status"] == "clean"]["iso3"].tolist()
    review = summary[summary["status"] == "review"]["iso3"].tolist()
    CLEAN_PATH.write_text("\n".join(clean) + "\n")
    REVIEW_PATH.write_text("\n".join(review) + "\n")
    logger.info("Clean: %d, review: %d", len(clean), len(review))

    # ADAM diverges from the GDACS qmd here: the qmd's per-country
    # detail section iterates over every policy-seeded iso3 (not just
    # the review-status ones), so write geoms for clean+review together.
    # The handful of fm_empty iso3s (ANT, XIM) genuinely have no
    # geometry, so the qmd will gracefully skip them.
    geom_iso3 = sorted(set(clean) | set(review))
    write_review_geoms(geom_iso3, ge, cfg)
    logger.info("Wrote %s (iso3s with geoms: %d)", GEOMS_PATH, len(geom_iso3))
    return 0


if __name__ == "__main__":
    sys.exit(main())
