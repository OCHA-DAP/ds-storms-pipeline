"""Regenerate the per-country review artifacts the review qmd consumes.

⚠️  EXPLORATORY / REVIEW-ONLY — NOT live production monitoring, not run by
the Databricks bundle or CI. Manual aid for producing/checking the
human-reviewed crosswalk. Calls the review-only matcher in
`src/static/gdacs/matcher.py` (the production lookup does not).


Produces (under ``data/review/`` — gitignored, alongside the
canonical lookup CSV the production builder writes):
  - gdacs_fm_summary.csv         per-country match stats
  - gdacs_review_countries.txt   ISOs needing per-country review
  - gdacs_clean_countries.txt    ISOs that matched cleanly
  - gdacs_review_geoms.gpkg      FM + GDACS polys for review ISOs
  - gdacs_per_country/{ISO}.parquet  per-country match parquets
                                     (cache; reruns reuse them)

The review qmd lives at ``exploration/review_report_gdacs.qmd`` and
reads these files from ``../data/review/``.

This is a parity rewrite of the historical aggregation that
``artefacts/match_gdacs_fieldmaps.py main()`` did. It now uses the
graduated helpers under ``src/static/gdacs/`` and the canonical
ATLANTIC_ISO3 scope, so the qmd renders against the same source of
truth the production lookup builder uses.

Delete ``data/review/gdacs_per_country/`` to force a re-match.
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

from src.static.gdacs.admin import (  # noqa: E402
    filter_gdacs_country, load_gdacs_admin,
)
from src.static.gdacs.inputs import (  # noqa: E402
    ATLANTIC_ISO3,
    DEFAULT_CONFIG,
    fm_id_and_name_cols,
    load_level_config,
    resolve_gdacs_fm_level,
    load_fieldmaps_adm,
)
from src.static.gdacs.matcher import match_country  # noqa: E402


OUT_DIR = REPO_ROOT / "data" / "review"
PER_COUNTRY_DIR = OUT_DIR / "gdacs_per_country"
GEOMS_PATH = OUT_DIR / "gdacs_review_geoms.gpkg"
SUMMARY_PATH = OUT_DIR / "gdacs_fm_summary.csv"
CLEAN_PATH = OUT_DIR / "gdacs_clean_countries.txt"
REVIEW_PATH = OUT_DIR / "gdacs_review_countries.txt"

LOW_IOU = 0.5
DEFAULT_FM_LEVEL = 1

logger = logging.getLogger(__name__)


def match_with_cache(
    iso3: str, fm_level: int, gdacs: gpd.GeoDataFrame, force: bool,
) -> pd.DataFrame:
    """Run :func:`match_country`, caching to per-country parquet."""
    out = PER_COUNTRY_DIR / f"{iso3}.parquet"
    if out.exists() and not force:
        return pd.read_parquet(out)
    try:
        rows = match_country(
            iso3, gdacs, low_iou=LOW_IOU, fm_level=fm_level,
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
    """One row matching the legacy ``gdacs_fm_summary.csv`` schema."""
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
            "fm_count": 0, "gdacs_count": 0,
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
    gdacs_count = (
        int(matched["gdacs_country_count"].iloc[0])
        if "gdacs_country_count" in matched.columns else 0
    )
    return {
        "iso3": iso3,
        "fm_level": fm_level,
        "fm_count": n,
        "gdacs_count": gdacs_count,
        "n_clean": n_clean, "n_low_iou": n_low,
        "n_multi": n_multi, "n_no_overlap": n_none,
        "mean_iou": float(with_iou.mean()) if len(with_iou) else None,
        "min_iou": float(with_iou.min()) if len(with_iou) else None,
        "status": "clean" if n_clean == n else "review",
    }


def write_review_geoms(review_iso3: list[str], gdacs: gpd.GeoDataFrame,
                       cfg: dict) -> None:
    """Stack FM polygons + GDACS polygons for the review ISOs into a
    single GeoPackage with ``fm`` and ``gdacs`` layers — matches the
    schema the qmd's per-country maps consume."""
    fm_frames, g_frames = [], []
    for iso3 in tqdm(review_iso3, desc="geoms"):
        lvl = resolve_gdacs_fm_level(iso3, cfg)
        try:
            fm = load_fieldmaps_adm(iso3, lvl)
        except Exception:
            fm = None
        if fm is not None and len(fm) > 0:
            # Resolve FM pcode/name cols (vary by release: adm1_id vs
            # adm1_pcode etc.) and rename to fm_pcode / fm_name so the
            # qmd can merge against the per-country match results.
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
        g = filter_gdacs_country(gdacs, iso3)
        if len(g) > 0:
            g = g.assign(iso3=iso3)
            g_frames.append(g[["iso3", "ADMIN_NAME", "geometry"]])

    if fm_frames:
        fm_all = gpd.GeoDataFrame(
            pd.concat(fm_frames, ignore_index=True), crs=fm_frames[0].crs,
        )
        fm_all.to_file(GEOMS_PATH, layer="fm", driver="GPKG")
    if g_frames:
        g_all = gpd.GeoDataFrame(
            pd.concat(g_frames, ignore_index=True), crs=g_frames[0].crs,
        )
        g_all.to_file(GEOMS_PATH, layer="gdacs", driver="GPKG")


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
    logger.info("Loaded policy: %d gdacs_policy entries",
                len(cfg.get("gdacs_policy", {})))

    gdacs = load_gdacs_admin(stage="dev")
    logger.info("Loaded %d GDACS admin polys (%d distinct ISO3)",
                len(gdacs), gdacs["_iso3"].nunique())

    force = "--force" in sys.argv
    summary_rows = []
    for iso3 in tqdm(ATLANTIC_ISO3, desc="match"):
        fm_lvl = resolve_gdacs_fm_level(iso3, cfg)
        df = match_with_cache(iso3, fm_lvl, gdacs, force=force)
        summary_rows.append(per_country_summary_row(iso3, df))

    summary = pd.DataFrame(summary_rows)
    summary.to_csv(SUMMARY_PATH, index=False)
    logger.info("Wrote %s (%d rows)", SUMMARY_PATH, len(summary))

    clean = summary[summary["status"] == "clean"]["iso3"].tolist()
    review = summary[summary["status"] == "review"]["iso3"].tolist()
    CLEAN_PATH.write_text("\n".join(clean) + "\n")
    REVIEW_PATH.write_text("\n".join(review) + "\n")
    logger.info("Clean: %d, review: %d", len(clean), len(review))

    write_review_geoms(review, gdacs, cfg)
    logger.info("Wrote %s (review iso3s: %d)", GEOMS_PATH, len(review))
    return 0


if __name__ == "__main__":
    sys.exit(main())
