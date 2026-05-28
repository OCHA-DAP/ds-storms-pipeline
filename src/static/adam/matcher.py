"""Spatial matching: FieldMaps adm-1 polygons ↔ ge_adm1 polygons.

Mirror of :mod:`src.static.gdacs.matcher` for the FM↔ADAM bridge.
Pairs each FM unit with the ge_adm1 unit that maximises IoU inside
the country, returns one row per FM polygon (orphans included so the
caller can flag them).

The downstream consumer joins ADAM exposure to this lookup by
``(iso3, lower(admin_name)) ↔ (iso3, lower(adam_admin_name))``. We
store the raw ge_adm1 ``adm1_name`` as ``adam_admin_name`` because
that IS the string ADAM emits (verified for ~12 clean countries; see
exploration/review_report_adam.qmd when it exists).

ge_adm1 is fixed at admin-1 (no hierarchy), so only the FM-side level
is configurable via :func:`src.static.adam.inputs.resolve_adam_fm_level`.
"""

from __future__ import annotations

import logging

import geopandas as gpd

from src.static.adam.admin import filter_ge_country
from src.static.adam.inputs import (
    FM_ADM1_NAME_FALLBACK_ISOS,
    fallback_fm_name_from_adm0,
    fm_id_and_name_cols,
    load_fieldmaps_adm,
)


logger = logging.getLogger(__name__)


def match_country(
    iso3: str,
    ge: gpd.GeoDataFrame,
    low_iou: float = 0.5,
    fm_level: int = 1,
) -> list[dict]:
    """One row per FieldMaps unit-at-``fm_level`` for ``iso3``.

    Returned dict shape::

        {
          iso3, fm_level, fm_pcode, fm_name, fm_area_m2,
          fm_country_count, ge_country_count,
          ge_adm1_id, adam_admin_name, adam_admin_altname,
          iou, ge_area_m2, n_candidates,
          issue: None | "no_overlap" | "low_iou" | "multiple_candidates"
                 | "fm_load_error" | "fm_empty" | "fm_unknown_schema"
                 | "ge_empty",
        }

    Country-level failures (FM load error, empty FM, empty ge_adm1,
    unknown schema) return a single placeholder row carrying just
    ``iso3``, ``fm_level``, ``issue`` (+ optional ``detail``).
    """
    def placeholder(issue: str, **extra) -> list[dict]:
        return [{
            "iso3": iso3, "fm_level": fm_level, "issue": issue, **extra,
        }]

    try:
        fm = load_fieldmaps_adm(iso3, fm_level)
    except Exception as e:
        logger.warning(
            "FieldMaps load failed for %s adm%d: %s", iso3, fm_level, e,
        )
        return placeholder("fm_load_error", detail=str(e))

    if fm is None or len(fm) == 0:
        return placeholder("fm_empty")

    if fm_level == 0:
        fm = fm.copy()
        fm["adm0_pcode"] = iso3
        if "adm0_name" not in fm.columns:
            fm["adm0_name"] = iso3
        fm_pcode_col, fm_name_col = "adm0_pcode", "adm0_name"
    else:
        fm_pcode_col, fm_name_col = fm_id_and_name_cols(fm, fm_level)
        if fm_pcode_col is None or fm_name_col is None:
            return placeholder(
                "fm_unknown_schema",
                detail=f"columns={list(fm.columns)}",
            )
        if (iso3 in FM_ADM1_NAME_FALLBACK_ISOS
                and "adm0_name" in fm.columns
                and fm[fm_name_col].isna().any()):
            fm = fm.copy()
            fm[fm_name_col] = fm[fm_name_col].fillna(
                fm["adm0_name"].map(fallback_fm_name_from_adm0)
            )

    g = filter_ge_country(ge, iso3)
    if len(g) == 0:
        return placeholder(
            "ge_empty",
            detail=f"FM has {len(fm)} adm{fm_level} but ge_adm1 has 0",
        )

    fm = fm[[fm_pcode_col, fm_name_col, fm.geometry.name]].copy()
    # ge_adm1's active geometry col is named `shape` (not `geometry`),
    # AND both layers use lowercase `adm1_id`/`adm1_name` so overlay()
    # would collide. Rename ge cols to `_ge_*` and the geom to
    # `geometry` so the overlay output is unambiguous. We map them
    # back to the public names at row-emission time.
    g = g[[
        "adm1_id", "adm1_name", "adm1_altnm", g.geometry.name,
    ]].copy()
    if g.geometry.name != "geometry":
        g = g.rename_geometry("geometry")
    if fm.geometry.name != "geometry":
        fm = fm.rename_geometry("geometry")
    g = g.rename(columns={
        "adm1_id": "_ge_id",
        "adm1_name": "_ge_name",
        "adm1_altnm": "_ge_altnm",
    })

    # Equal-area projection before area math — same reasoning as the
    # GDACS matcher. EPSG:6933 World Cylindrical Equal Area; units m².
    # IoU ratios are CRS-independent so the reprojection only matters
    # for the absolute area columns we surface to the qmd.
    AREA_CRS = 6933
    if g.crs != fm.crs:
        g = g.to_crs(fm.crs)
    fm = fm.to_crs(AREA_CRS)
    g = g.to_crs(AREA_CRS)

    fm["_fm_idx"] = range(len(fm))
    fm["_fm_area"] = fm.geometry.area
    g["_g_idx"] = range(len(g))
    g["_g_area"] = g.geometry.area

    overlay = gpd.overlay(fm, g, how="intersection", keep_geom_type=True)
    overlay["_inter_area"] = overlay.geometry.area
    overlay["_iou"] = overlay["_inter_area"] / (
        overlay["_fm_area"] + overlay["_g_area"] - overlay["_inter_area"]
    )

    candidate_counts = (
        overlay[overlay["_iou"] > 0.01]
        .groupby("_fm_idx").size().to_dict()
    )
    best_rows = overlay.sort_values(
        "_iou", ascending=False,
    ).drop_duplicates("_fm_idx", keep="first")
    best_by_fm = best_rows.set_index("_fm_idx")

    rows: list[dict] = []
    n_g = len(g)
    n_fm = len(fm)
    for _, frow in fm.iterrows():
        fm_idx = frow["_fm_idx"]
        row = {
            "iso3": iso3,
            "fm_level": fm_level,
            "fm_pcode": frow[fm_pcode_col],
            "fm_name": frow[fm_name_col],
            "fm_area_m2": frow["_fm_area"],
            "fm_country_count": n_fm,
            "adam_admin_count": n_g,
        }
        if fm_idx not in best_by_fm.index:
            row.update({
                "adam_admin_id": None, "adam_admin_name": None,
                "adam_admin_altname": None,
                "iou": 0.0, "adam_area_m2": None,
                "n_candidates": 0, "issue": "no_overlap",
            })
            rows.append(row)
            continue

        best = best_by_fm.loc[fm_idx]
        n_cand = int(candidate_counts.get(fm_idx, 0))
        row.update({
            "adam_admin_id": best["_ge_id"],
            "adam_admin_name": best["_ge_name"],
            "adam_admin_altname": best["_ge_altnm"],
            "iou": float(best["_iou"]),
            "adam_area_m2": float(best["_g_area"]),
            "n_candidates": n_cand,
        })
        if row["iou"] < low_iou:
            row["issue"] = "low_iou"
        elif n_cand > 1:
            row["issue"] = "multiple_candidates"
        else:
            row["issue"] = None
        rows.append(row)

    return rows


def match_per_ge_country(
    iso3: str,
    ge: gpd.GeoDataFrame,
    low_iou: float = 0.5,
    fm_level: int = 1,
) -> list[dict]:
    """Per-ge variant of :func:`match_country` — iterates the ge_adm1
    side, emits one row per ge polygon with the best-IoU FM polygon.

    Why this exists: the per-FM iteration in :func:`match_country` is
    safe for code-keyed sources (GDACS' ``gmi_admin``) because each
    source code is unique in the source layer, so even when IoU picks
    a bad-but-best FM partner each source row still joins 1:1
    downstream. ADAM joins by NAME and the same name can appear on
    multiple per-FM lookup rows (FM finer than ge for BHS-like
    territories), which fans out the join and double-counts. The
    per-ge iteration eliminates the fanout structurally: each
    ``ge_adm1_id`` appears at most once in the output, so each ADAM
    name resolves to exactly one ``fm_pcode``.

    Returned dict shape (one row per ge polygon)::

        {
          iso3, fm_level,
          ge_adm1_id, adam_admin_name, adam_admin_altname,
          ge_area_m2,
          fm_pcode, fm_name, fm_area_m2,
          iou, n_fm_candidates, ge_country_count, fm_country_count,
          issue: None | "low_iou" | "multiple_candidates"
                 | "no_overlap" | "fm_load_error" | "fm_empty"
                 | "ge_empty" | "fm_unknown_schema",
        }
    """
    def placeholder(issue: str, **extra) -> list[dict]:
        return [{
            "iso3": iso3, "fm_level": fm_level, "issue": issue, **extra,
        }]

    try:
        fm = load_fieldmaps_adm(iso3, fm_level)
    except Exception as e:
        logger.warning(
            "FieldMaps load failed for %s adm%d: %s", iso3, fm_level, e,
        )
        return placeholder("fm_load_error", detail=str(e))

    if fm is None or len(fm) == 0:
        return placeholder("fm_empty")

    if fm_level == 0:
        fm = fm.copy()
        fm["adm0_pcode"] = iso3
        if "adm0_name" not in fm.columns:
            fm["adm0_name"] = iso3
        fm_pcode_col, fm_name_col = "adm0_pcode", "adm0_name"
    else:
        fm_pcode_col, fm_name_col = fm_id_and_name_cols(fm, fm_level)
        if fm_pcode_col is None or fm_name_col is None:
            return placeholder(
                "fm_unknown_schema",
                detail=f"columns={list(fm.columns)}",
            )
        if (iso3 in FM_ADM1_NAME_FALLBACK_ISOS
                and "adm0_name" in fm.columns
                and fm[fm_name_col].isna().any()):
            fm = fm.copy()
            fm[fm_name_col] = fm[fm_name_col].fillna(
                fm["adm0_name"].map(fallback_fm_name_from_adm0)
            )

    g = filter_ge_country(ge, iso3)
    if len(g) == 0:
        return placeholder(
            "ge_empty",
            detail=f"FM has {len(fm)} adm{fm_level} but ge_adm1 has 0",
        )

    # Identical preparation as match_country — same rename + reproject
    # so the IoU math is consistent across the two iteration modes.
    fm = fm[[fm_pcode_col, fm_name_col, fm.geometry.name]].copy()
    g = g[[
        "adm1_id", "adm1_name", "adm1_altnm", g.geometry.name,
    ]].copy()
    if g.geometry.name != "geometry":
        g = g.rename_geometry("geometry")
    if fm.geometry.name != "geometry":
        fm = fm.rename_geometry("geometry")
    g = g.rename(columns={
        "adm1_id": "_ge_id",
        "adm1_name": "_ge_name",
        "adm1_altnm": "_ge_altnm",
    })

    AREA_CRS = 6933
    if g.crs != fm.crs:
        g = g.to_crs(fm.crs)
    fm = fm.to_crs(AREA_CRS)
    g = g.to_crs(AREA_CRS)

    fm["_fm_idx"] = range(len(fm))
    fm["_fm_area"] = fm.geometry.area
    g["_g_idx"] = range(len(g))
    g["_g_area"] = g.geometry.area

    overlay = gpd.overlay(fm, g, how="intersection", keep_geom_type=True)
    overlay["_inter_area"] = overlay.geometry.area
    overlay["_iou"] = overlay["_inter_area"] / (
        overlay["_fm_area"] + overlay["_g_area"] - overlay["_inter_area"]
    )

    # n_fm_candidates: how many FM polygons substantially overlap this
    # ge polygon — surfaces ge units that straddle a recent FM
    # admin-reform boundary (analogue of n_candidates on the per-FM
    # side).
    candidate_counts = (
        overlay[overlay["_iou"] > 0.01]
        .groupby("_g_idx").size().to_dict()
    )
    best_rows = overlay.sort_values(
        "_iou", ascending=False,
    ).drop_duplicates("_g_idx", keep="first")
    best_by_ge = best_rows.set_index("_g_idx")

    rows: list[dict] = []
    n_g = len(g)
    n_fm = len(fm)
    for _, grow in g.iterrows():
        g_idx = grow["_g_idx"]
        row = {
            "iso3": iso3,
            "fm_level": fm_level,
            "adam_admin_id": grow["_ge_id"],
            "adam_admin_name": grow["_ge_name"],
            "adam_admin_altname": grow["_ge_altnm"],
            "adam_area_m2": float(grow["_g_area"]),
            "fm_country_count": n_fm,
            "adam_admin_count": n_g,
        }
        if g_idx not in best_by_ge.index:
            row.update({
                "fm_pcode": None, "fm_name": None, "fm_area_m2": None,
                "iou": 0.0, "n_fm_candidates": 0, "issue": "no_overlap",
            })
            rows.append(row)
            continue

        best = best_by_ge.loc[g_idx]
        n_cand = int(candidate_counts.get(g_idx, 0))
        row.update({
            "fm_pcode": best[fm_pcode_col],
            "fm_name": best[fm_name_col],
            "fm_area_m2": float(best["_fm_area"]),
            "iou": float(best["_iou"]),
            "n_fm_candidates": n_cand,
        })
        if row["iou"] < low_iou:
            row["issue"] = "low_iou"
        elif n_cand > 1:
            row["issue"] = "multiple_candidates"
        else:
            row["issue"] = None
        rows.append(row)

    return rows
