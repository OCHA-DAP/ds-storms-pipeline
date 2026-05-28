"""Build the canonical FieldMaps↔ADAM lookup CSV and upload to blob.

Mirror of :mod:`scripts.build_canonical_lookup` for the FM↔ADAM
bridge. The runtime pipeline does no spatial work — it joins this
CSV against its FieldMaps-driven exposure analysis to attach ADAM
exposure numbers per FM admin unit. This script is the *only* place
the spatial algorithm runs; it executes offline whenever policy or
source data changes.

Inputs
------
- ``config/adm_level_config.toml``              policy + per-row notes
  (the new ``[adam_policy]`` + ``[[adam_per_row_notes]]`` sections;
  the existing ``[gdacs_policy]`` is left alone)
- FM adm0/adm1 parquets on blob
  (``global/fieldmaps/edge-matched/humanitarian/intl/adm{0,1}/<ISO3>.parquet``)
- ge_adm1.parquet (WFP's published global adm1 layer, ~278 MB)
  – DEV: local at ``data/static/ge_adm1.parquet`` (gitignored)
  – PROD (planned): mirrored to ``global/adam/boundaries/ge_adm1.parquet``

Expensive FM↔ge_adm1 IoU overlay runs inline per country here via
:func:`src.static.adam.matcher.match_country` — no pre-step required.

Output
------
- Local CSV at ``--out`` (default ``data/adam_fm_lookup.csv``)
- Postgres table ``storms.adam_fm_lookup`` (REPLACE on each build)
  unless ``--dry-run``.

Schema
------
``iso3, admin_level, fm_pcode, fm_name, ge_adm1_id, adam_admin_name,
caveat_note``

PK = ``(iso3, admin_level, fm_pcode, ge_adm1_id)``. Multiple rows per
``(iso3, fm_pcode)`` expected for ``aggregate_adam_to_fm`` countries
(ISL: 8 FM regions ← 75 ge_adm1 municipalities; DOM: 10 ← 33 provinces).

How each policy translates to rows
----------------------------------
======================  ==========  ==============================
action                  adm0 rows   adm1 rows
======================  ==========  ==============================
accept                  1 per ctry  1 per FM adm1 (1:1 IoU matching)
country_only            1 per ctry  none
aggregate_adam_to_fm    1 per ctry  N per FM adm1 (one per ge_adm1
                                    polygon inside it, reverse
                                    spatial join)
needs_manual_mapping    1 per ctry  1 per FM adm1, caveat_note set
fm_adm1_only            1 per ctry  1 per FM adm1, ge_adm1_id=NULL
no_adam_source          none        none — iso3 has no ge_adm1
                                    rows (build skips)
======================  ==========  ==============================

Downstream consumer joins ADAM exposure rows to this lookup by
``(iso3, lower(admin_name))`` ↔ ``(iso3, lower(adam_admin_name))``.

Usage
-----
::

    uv run python scripts/build_adam_fm_lookup.py
        [--mode dev|prod]
        [--dry-run]                  # local CSV only, skip DB write
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

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.static.adam.admin import (  # noqa: E402
    filter_ge_country,
    load_ge_adm1,
)
from src.static.adam.inputs import (  # noqa: E402
    ATLANTIC_ISO3,
    DEFAULT_CONFIG,
    FM_ADM1_NAME_FALLBACK_ISOS,
    fallback_fm_name_from_adm0,
    load_fieldmaps_adm,
    load_level_config,
    resolve_adam_fm_level,
)
from src.static.adam.matcher import match_country  # noqa: E402

DEFAULT_LOCAL_OUT = REPO_ROOT / "data" / "adam_fm_lookup.csv"
DB_SCHEMA = "storms"
DB_TABLE = "adam_fm_lookup"

LOOKUP_COLUMNS = [
    "iso3", "admin_level", "fm_pcode", "fm_name",
    "ge_adm1_id", "adam_admin_name", "caveat_note",
]

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
# Row builders
# ─────────────────────────────────────────────────────────────────────

def build_adm0_row(
    iso3: str,
    fm_adm0: gpd.GeoDataFrame | pd.DataFrame | None,
    ge_country: gpd.GeoDataFrame,
) -> dict:
    """Emit the single adm0 row for one country.

    ``fm_pcode`` = ISO3 (uniform across the lookup, matches runtime
    joins). ``fm_name`` prefers FM's ``adm0_name``, falls back to
    ge_adm1's ``adm0_name``. ``adam_admin_name`` = ge_adm1's
    ``adm0_name`` because that IS what ADAM emits at adm_level=0.
    ``ge_adm1_id`` is NULL — ge_adm1 has no adm0 records.
    """
    fm_name = None
    if fm_adm0 is not None and len(fm_adm0):
        row = fm_adm0.iloc[0]
        if "adm0_name" in fm_adm0.columns and pd.notna(row.get("adm0_name")):
            fm_name = row["adm0_name"]
    ge_adm0_name = None
    if len(ge_country) and "adm0_name" in ge_country.columns:
        v = ge_country.iloc[0]["adm0_name"]
        if pd.notna(v):
            ge_adm0_name = v
    if not fm_name:
        fm_name = ge_adm0_name or iso3
    return {
        "iso3": iso3,
        "admin_level": 0,
        "fm_pcode": iso3,
        "fm_name": fm_name,
        "ge_adm1_id": None,
        "adam_admin_name": ge_adm0_name or iso3,
        "caveat_note": None,
    }


def build_accept_adm1_rows(
    iso3: str,
    fm_level: int,
    ge_admin: gpd.GeoDataFrame,
    per_row_notes: list[dict],
    low_iou: float = 0.5,
) -> list[dict]:
    """Run the FM↔ge_adm1 matcher and project results into lookup rows.

    Used for both ``accept`` and ``needs_manual_mapping`` — same row
    shape; only difference is that ``needs_manual_mapping`` countries
    have ``[[adam_per_row_notes]]`` entries we attach as
    ``caveat_note``.
    """
    match_rows = match_country(
        iso3, ge_admin, low_iou=low_iou, fm_level=fm_level,
    )
    # Drop placeholder rows (no_overlap, fm_empty, ge_empty, etc.) —
    # we only emit rows where both sides exist.
    match_rows = [
        r for r in match_rows
        if r.get("fm_pcode") and r.get("ge_adm1_id")
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
            "ge_adm1_id": r["ge_adm1_id"],
            "adam_admin_name": r["adam_admin_name"],
            "caveat_note": notes_by_pcode.get(r["fm_pcode"]),
        }
        for r in match_rows
    ]


def build_aggregate_adm1_rows(
    iso3: str, ge_admin: gpd.GeoDataFrame,
) -> list[dict]:
    """For aggregate_adam_to_fm countries: reverse spatial join.

    Each ge_adm1 polygon maps to the FM adm1 polygon containing its
    representative point. Falls back to max intersection-area when no
    FM polygon strictly contains the point. ISL (75 municipalities →
    8 regions) and DOM (33 provinces → 10 regions) are the main cases.
    """
    fm = load_fieldmaps_adm(iso3, 1)
    g = filter_ge_country(ge_admin, iso3)
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

    # Normalize geom col name on the ge side and align CRS to FM.
    if g.geometry.name != "geometry":
        g = g.rename_geometry("geometry")
    if g.crs != fm.crs:
        g = g.to_crs(fm.crs)

    rows = []
    for _, gp in g.iterrows():
        pt = gp.geometry.representative_point()
        contains_mask = fm.geometry.contains(pt)
        if contains_mask.any():
            container = fm[contains_mask].iloc[0]
        else:
            inter = fm.geometry.intersection(gp.geometry).area
            if inter.max() == 0:
                logger.warning(
                    "ge_adm1 %s in %s overlaps no FM adm1 — skipped",
                    gp["adm1_id"], iso3,
                )
                continue
            container = fm.loc[inter.idxmax()]
        rows.append({
            "iso3": iso3,
            "admin_level": 1,
            "fm_pcode": container[pcode_col],
            "fm_name": container[name_col],
            "ge_adm1_id": gp["adm1_id"],
            "adam_admin_name": gp["adm1_name"],
            "caveat_note": None,
        })
    return rows


def build_fm_only_adm1_rows(
    iso3: str, fm_level: int = 1,
) -> list[dict]:
    """For ``fm_adm1_only`` countries: emit FM adm1 rows with
    ``ge_adm1_id = NULL``.

    Same semantics as the GDACS-side ``fm_adm1_only``: ge_adm1 is
    coarser than FM (or absent), so we surface the FM units in the
    lookup with no ge attachment. Downstream sources (NHC, etc.) can
    still attach via fm_pcode; ADAM adm1 rows for this iso3 land as
    by-design unmatched.
    """
    fm = load_fieldmaps_adm(iso3, fm_level)
    if fm is None or len(fm) == 0:
        logger.warning(
            "fm_adm1_only %s: FM empty at adm%d", iso3, fm_level,
        )
        return []
    pcode_col = next(
        (c for c in (f"adm{fm_level}_pcode", f"adm{fm_level}_id",
                     f"adm{fm_level}_src") if c in fm.columns),
        None,
    )
    name_col = next(
        (c for c in (f"adm{fm_level}_name", f"name_{fm_level}")
         if c in fm.columns),
        None,
    )
    if pcode_col is None or name_col is None:
        logger.error(
            "Can't resolve FM adm%d columns for %s — got %s",
            fm_level, iso3, list(fm.columns),
        )
        return []

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
            "ge_adm1_id": None,
            "adam_admin_name": None,
            "caveat_note": None,
        }
        for _, r in fm.iterrows()
    ]


# ─────────────────────────────────────────────────────────────────────
# Driver
# ─────────────────────────────────────────────────────────────────────

def build_lookup(cfg: dict, ge_admin: gpd.GeoDataFrame) -> pd.DataFrame:
    """Produce the full lookup DataFrame in one pass."""
    per_row_notes = cfg.get("adam_per_row_notes", [])
    policies = cfg.get("adam_policy", {})

    rows: list[dict] = []
    for iso3 in ATLANTIC_ISO3:
        # Skip iso3s flagged no_adam_source.
        if policies.get(iso3, {}).get("action") == "no_adam_source":
            logger.info("Skipping %s — policy=no_adam_source", iso3)
            continue

        # ── adm0 ──
        fm_adm0 = load_fieldmaps_adm(iso3, 0)
        ge_country = filter_ge_country(ge_admin, iso3)
        rows.append(build_adm0_row(iso3, fm_adm0, ge_country))

        # ── adm1 ──
        action = policies.get(iso3, {}).get("action")
        if action is None:
            # Same first-pass behavior as the GDACS build: countries
            # without an explicit policy entry are silently
            # country_only. Once policy is seeded, missing entries
            # will be loud (warning).
            continue
        if action == "country_only":
            continue

        fm_adm1 = load_fieldmaps_adm(iso3, 1)
        if fm_adm1 is None or len(fm_adm1) == 0:
            continue
        if (
            len(fm_adm1) == 1
            and action not in ("aggregate_adam_to_fm", "fm_adm1_only")
        ):
            # Same single-polygon FM short-circuit as GDACS build —
            # `accept`/`needs_manual_mapping` on a 1-poly FM is a no-op.
            continue

        if action in ("accept", "needs_manual_mapping"):
            fm_lvl = resolve_adam_fm_level(iso3, cfg)
            rows.extend(build_accept_adm1_rows(
                iso3, fm_lvl, ge_admin, per_row_notes,
            ))
        elif action == "aggregate_adam_to_fm":
            rows.extend(build_aggregate_adm1_rows(iso3, ge_admin))
        elif action == "fm_adm1_only":
            fm_lvl = resolve_adam_fm_level(iso3, cfg)
            rows.extend(build_fm_only_adm1_rows(iso3, fm_lvl))
        else:
            logger.warning(
                "Unknown action '%s' for %s — skipping adm1",
                action, iso3,
            )

    df = pd.DataFrame(rows, columns=LOOKUP_COLUMNS)
    df.sort_values(
        ["iso3", "admin_level", "fm_pcode", "ge_adm1_id"],
        inplace=True, kind="stable",
    )
    df.reset_index(drop=True, inplace=True)
    return df


# ─────────────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────────────

def validate(df: pd.DataFrame, cfg: dict) -> None:
    """Sanity checks. Raise on failure so a bad build never ships.

    Same invariants as the GDACS lookup's ``validate``, retuned for
    the FM↔ADAM column set.
    """
    policies = cfg.get("adam_policy", {})

    # 1) Exactly one adm0 row per Atlantic ISO3 (minus no_adam_source).
    no_adam = {
        i for i, p in policies.items()
        if p.get("action") == "no_adam_source"
    }
    expected_iso3 = set(ATLANTIC_ISO3) - no_adam
    adm0 = df[df["admin_level"] == 0]
    missing = expected_iso3 - set(adm0["iso3"])
    if missing:
        raise AssertionError(
            f"adm0 rows missing for: {sorted(missing)}"
        )
    dupes = adm0["iso3"].value_counts()
    if (dupes > 1).any():
        raise AssertionError(
            f"duplicate adm0 rows: {dupes[dupes > 1].to_dict()}"
        )

    # 2) Action-specific adm1 row counts.
    adm1_counts = df[df["admin_level"] == 1].groupby("iso3").size()
    for iso3, pol in policies.items():
        n = int(adm1_counts.get(iso3, 0))
        action = pol["action"]
        if action == "country_only" and n != 0:
            raise AssertionError(
                f"country_only {iso3} has {n} adm1 rows (expected 0)"
            )
        if action == "accept" and n == 0:
            fm = load_fieldmaps_adm(iso3, 1)
            if fm is not None and len(fm) > 1:
                raise AssertionError(
                    f"accept {iso3} has 0 adm1 rows but FM has "
                    f"{len(fm)} polygons"
                )
        if action == "fm_adm1_only":
            sub = df[(df["iso3"] == iso3) & (df["admin_level"] == 1)]
            if sub["ge_adm1_id"].notna().any():
                bad = sub[sub["ge_adm1_id"].notna()]["fm_pcode"].tolist()
                raise AssertionError(
                    f"fm_adm1_only {iso3} has adm1 rows with non-NULL "
                    f"ge_adm1_id: {bad[:5]}"
                )

    # 3) PK uniqueness on (iso3, admin_level, fm_pcode, ge_adm1_id).
    pk_dupes = df.duplicated(
        subset=["iso3", "admin_level", "fm_pcode", "ge_adm1_id"]
    )
    if pk_dupes.any():
        sample = df[pk_dupes].head(5).to_dict("records")
        raise AssertionError(
            f"duplicate PK rows ({pk_dupes.sum()}); first few: {sample}"
        )

    logger.info(
        "Validation OK: %d adm0 + %d adm1 rows across %d countries",
        len(adm0), (df["admin_level"] == 1).sum(),
        df["iso3"].nunique(),
    )


def validate_ge_adm1_ids(
    df: pd.DataFrame, ge_admin: gpd.GeoDataFrame,
) -> None:
    """Every non-NULL adm1 ge_adm1_id must reference a real ge_adm1
    polygon. NULL is allowed (the ``fm_adm1_only`` fingerprint)."""
    valid_ids = set(ge_admin["adm1_id"].dropna().unique())
    adm1 = df[df["admin_level"] == 1]
    referenced = set(adm1["ge_adm1_id"].dropna().unique())
    unknown = referenced - valid_ids
    if unknown:
        raise AssertionError(
            f"adm1 references ge_adm1_id values not in ge_adm1 layer: "
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
        help="write local CSV but skip DB write",
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
        "  %d adam_policy entries, %d adam_per_row_notes",
        len(cfg.get("adam_policy", {})),
        len(cfg.get("adam_per_row_notes", [])),
    )

    logger.info(
        "Loading ge_adm1 layer (stage=%s, local-first)", args.mode,
    )
    ge_admin = load_ge_adm1(stage=args.mode)
    logger.info(
        "  %d ge_adm1 polygons across %d distinct ISO3 codes",
        len(ge_admin), ge_admin["iso3"].nunique(),
    )

    logger.info(
        "Building lookup for %d Atlantic countries", len(ATLANTIC_ISO3),
    )
    df = build_lookup(cfg, ge_admin)

    validate(df, cfg)
    validate_ge_adm1_ids(df, ge_admin)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)
    logger.info(
        "Wrote %d rows to %s (local CSV for inspection)",
        len(df), args.out,
    )

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
