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
``iso3, admin_level, fm_pcode, fm_name, adam_admin_id, adam_admin_name,
caveat_note``

PK = ``(iso3, admin_level, fm_pcode, adam_admin_id)``. Multiple rows per
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
fm_adm1_only            1 per ctry  1 per FM adm1, adam_admin_id=NULL
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
    filter_adam_country,
    load_adam_admin,
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
from src.static.adam.matcher import (  # noqa: E402
    match_country,
    match_per_adam_admin,
)

DEFAULT_LOCAL_OUT = REPO_ROOT / "data" / "adam_fm_lookup.csv"
DB_SCHEMA = "storms"
DB_TABLE = "adam_fm_lookup"

LOOKUP_COLUMNS = [
    "iso3", "admin_level", "fm_pcode", "fm_name",
    "adam_admin_id", "adam_admin_name",
    "iou", "caveat_kind", "caveat_note",
]

# caveat_kind controlled vocabulary — categorical labels the alert
# text uses to pick a footnote template. NULL = clean row, no caveat.
CAVEAT_NO_ADAM_AT_ADM1 = "no_adam_at_adm1"
CAVEAT_SOURCE_COARSER_THAN_FM = "source_coarser_than_fm"
CAVEAT_PRE_SPLIT_BOUNDARY = "pre_split_boundary"
CAVEAT_MANUAL_REMAP = "manual_remap"
CAVEAT_FM_ONLY_POLICY = "fm_only_policy"

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
# Row builders
# ─────────────────────────────────────────────────────────────────────

def build_adm0_row(
    iso3: str,
    fm_adm0: gpd.GeoDataFrame | pd.DataFrame | None,
    adam_country: gpd.GeoDataFrame,
) -> dict:
    """Emit the single adm0 row for one country.

    ``fm_pcode`` = ISO3 (uniform across the lookup, matches runtime
    joins). ``fm_name`` prefers FM's ``adm0_name``, falls back to
    ADAM admin layer's ``adm0_name``. ``adam_admin_name`` = ADAM
    admin layer's ``adm0_name`` because that IS what ADAM emits at
    adm_level=0. ``adam_admin_id`` is NULL — the ADAM admin layer
    has no adm0 records.
    """
    fm_name = None
    if fm_adm0 is not None and len(fm_adm0):
        row = fm_adm0.iloc[0]
        if "adm0_name" in fm_adm0.columns and pd.notna(row.get("adm0_name")):
            fm_name = row["adm0_name"]
    adam_adm0_name = None
    if len(adam_country) and "adm0_name" in adam_country.columns:
        v = adam_country.iloc[0]["adm0_name"]
        if pd.notna(v):
            adam_adm0_name = v
    if not fm_name:
        fm_name = adam_adm0_name or iso3
    return {
        "iso3": iso3,
        "admin_level": 0,
        "fm_pcode": iso3,
        "fm_name": fm_name,
        "adam_admin_id": None,
        "adam_admin_name": adam_adm0_name or iso3,
        "iou": None,
        "caveat_kind": None,
        "caveat_note": None,
    }


def _resolve_fm_units(
    iso3: str, fm_level: int,
) -> list[tuple[str, str | None]]:
    """Return the full list of (fm_pcode, fm_name) for an iso3's FM
    units at ``fm_level``. Used by the FM-centric emit pass to find
    which FM admin1 units didn't receive a source attach.
    """
    fm = load_fieldmaps_adm(iso3, fm_level)
    if fm is None or len(fm) == 0:
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
    if pcode_col is None:
        logger.error(
            "Can't resolve FM adm%d pcode for %s — got %s",
            fm_level, iso3, list(fm.columns),
        )
        return []
    units: list[tuple[str, str | None]] = []
    for _, r in fm.iterrows():
        pcode = r[pcode_col]
        if pd.isna(pcode):
            continue
        nm = r[name_col] if name_col else None
        if nm is None or (isinstance(nm, float) and nm != nm):
            if (iso3 in FM_ADM1_NAME_FALLBACK_ISOS
                    and "adm0_name" in r):
                nm = fallback_fm_name_from_adm0(r["adm0_name"])
            else:
                nm = None
        units.append((pcode, nm))
    return units


def build_accept_adm1_rows(
    iso3: str,
    fm_level: int,
    adam_admin: gpd.GeoDataFrame,
    per_row_notes: list[dict],
    overrides: list[dict],
    low_iou: float = 0.5,
) -> list[dict]:
    """Per-ADAM-admin spatial crosswalk PLUS FM-centric NULL-source rows.

    Shared build path for ``accept``, ``aggregate_adam_to_fm`` and
    ``needs_manual_mapping``. Two emit passes:

      Pass 1 — Per-ADAM-admin (one row per ADAM polygon)
        match_per_adam_admin gives best-IoU FM per ADAM polygon.
        Overrides apply on top:
          action = "drop"   — emit no row for this adam_admin_id
          action = "remap"  — change fm_pcode/fm_name to override
          action = "caveat" — keep IoU pick, attach caveat metadata

      Pass 2 — FM-centric NULL-source (one row per uncovered FM unit)
        After pass 1, find FM admin1 units that didn't receive an
        attached source (either no IoU match, or every match was
        dropped/remapped away). Emit a row per uncovered FM with
        adam_admin_id=NULL, iou=NULL, caveat_kind="no_adam_at_adm1".
        FM-keyed overrides apply here too:
          action = "mark_no_source" — force NULL-source row even if
                                       a weak IoU pick exists; allows
                                       custom caveat_note
          action = "caveat"         — attach caveat to the NULL-source
                                       row that auto-emitted

    Result: every FM admin1 unit gets at least one row. FM units with
    one source attached get one row. FM units with multiple sources
    (aggregate case) get N rows. FM units with no source get one
    NULL-source row with a caveat explaining why.
    """
    # Per-source matcher output.
    match_rows = match_per_adam_admin(
        iso3, adam_admin, low_iou=low_iou, fm_level=fm_level,
    )
    # Drop placeholder rows (no_overlap, fm_empty, etc.) — only emit
    # rows where both sides exist.
    match_rows = [
        r for r in match_rows
        if r.get("fm_pcode") and r.get("adam_admin_id")
    ]

    # Index overrides + per_row_notes for fast lookup. adam_admin_id
    # is bigint upstream; coerce both sides to str so TOML
    # representations compare cleanly. fm_pcode is already str.
    src_overrides = {
        str(o["adam_admin_id"]): o
        for o in overrides
        if o.get("iso3") == iso3 and o.get("adam_admin_id") is not None
        and o.get("action") in ("drop", "remap")
    }
    src_caveats = {
        str(o["adam_admin_id"]): o
        for o in overrides
        if o.get("iso3") == iso3 and o.get("adam_admin_id") is not None
        and o.get("action") == "caveat"
    }
    fm_force_null = {
        o["fm_pcode"]: o
        for o in overrides
        if o.get("iso3") == iso3 and o.get("fm_pcode") is not None
        and o.get("action") == "mark_no_source"
    }
    fm_caveats = {
        o["fm_pcode"]: o
        for o in overrides
        if o.get("iso3") == iso3 and o.get("fm_pcode") is not None
        and o.get("action") == "caveat"
    }
    src_notes = {
        str(n["adam_admin_id"]): n.get("note")
        for n in per_row_notes
        if n.get("iso3") == iso3 and n.get("adam_admin_id") is not None
    }

    out: list[dict] = []
    covered_fm_pcodes: set[str] = set()

    # ── Pass 1: per-source emit ──
    for r in match_rows:
        src_key = str(r["adam_admin_id"])
        ov = src_overrides.get(src_key)
        if ov and ov.get("action") == "drop":
            # Source-side drop — record nothing in lookup. TOML is the
            # audit trail.
            continue
        # Decide target FM (IoU pick or remap override).
        if ov and ov.get("action") == "remap":
            fm_pcode = ov.get("fm_pcode_override") or r["fm_pcode"]
            fm_name = ov.get("fm_name_override") or r["fm_name"]
            caveat_kind = ov.get("caveat_kind") or CAVEAT_MANUAL_REMAP
            caveat_note = ov.get("note")
        else:
            fm_pcode = r["fm_pcode"]
            fm_name = r["fm_name"]
            caveat_kind = None
            caveat_note = None
        # Force-null on FM side trumps a per-source attach.
        if fm_pcode in fm_force_null:
            continue
        # Apply source-keyed caveat if no override took it.
        src_cv = src_caveats.get(src_key)
        if src_cv and not caveat_note:
            caveat_kind = src_cv.get("caveat_kind") or caveat_kind
            caveat_note = src_cv.get("note") or caveat_note
        # Fallback to per_row_notes (legacy mechanism).
        if not caveat_note and src_key in src_notes:
            caveat_note = src_notes[src_key]
        out.append({
            "iso3": iso3,
            "admin_level": 1,
            "fm_pcode": fm_pcode,
            "fm_name": fm_name,
            "adam_admin_id": r["adam_admin_id"],
            "adam_admin_name": r["adam_admin_name"],
            "iou": r.get("iou"),
            "caveat_kind": caveat_kind,
            "caveat_note": caveat_note,
        })
        covered_fm_pcodes.add(fm_pcode)

    # ── Pass 2: FM-centric NULL-source rows ──
    # Every FM admin1 unit that didn't receive a source attach (or was
    # explicitly mark_no_source'd) gets a row with adam_admin_id=NULL.
    fm_units = _resolve_fm_units(iso3, fm_level)
    for fm_pcode, fm_name in fm_units:
        if fm_pcode in covered_fm_pcodes:
            continue
        force = fm_force_null.get(fm_pcode)
        cv = fm_caveats.get(fm_pcode)
        if force:
            kind = force.get("caveat_kind") or CAVEAT_NO_ADAM_AT_ADM1
            note = force.get("note")
        elif cv:
            kind = cv.get("caveat_kind") or CAVEAT_NO_ADAM_AT_ADM1
            note = cv.get("note")
        else:
            kind = CAVEAT_NO_ADAM_AT_ADM1
            note = None
        out.append({
            "iso3": iso3,
            "admin_level": 1,
            "fm_pcode": fm_pcode,
            "fm_name": fm_name,
            "adam_admin_id": None,
            "adam_admin_name": None,
            "iou": None,
            "caveat_kind": kind,
            "caveat_note": note,
        })

    return out


def build_fm_only_adm1_rows(
    iso3: str, fm_level: int = 1,
    policy_note: str | None = None,
) -> list[dict]:
    """For ``fm_adm1_only`` countries: emit FM adm1 rows with
    ``adam_admin_id = NULL`` for every FM unit.

    The ADAM admin layer is coarser than FM (or has no useful adm1
    breakdown), so we surface the FM units in the lookup with no
    source attached. ``caveat_kind`` = ``fm_only_policy`` so the
    alert text knows this is policy-driven, not a per-row gap.
    """
    fm_units = _resolve_fm_units(iso3, fm_level)
    if not fm_units:
        logger.warning(
            "fm_adm1_only %s: FM empty at adm%d", iso3, fm_level,
        )
        return []
    return [
        {
            "iso3": iso3,
            "admin_level": 1,
            "fm_pcode": fm_pcode,
            "fm_name": fm_name,
            "adam_admin_id": None,
            "adam_admin_name": None,
            "iou": None,
            "caveat_kind": CAVEAT_FM_ONLY_POLICY,
            "caveat_note": policy_note,
        }
        for fm_pcode, fm_name in fm_units
    ]


# ─────────────────────────────────────────────────────────────────────
# Driver
# ─────────────────────────────────────────────────────────────────────

def build_lookup(cfg: dict, adam_admin: gpd.GeoDataFrame) -> pd.DataFrame:
    """Produce the full lookup DataFrame in one pass."""
    per_row_notes = cfg.get("adam_per_row_notes", [])
    policies = cfg.get("adam_policy", {})
    overrides = cfg.get("adam_row_overrides", [])

    rows: list[dict] = []
    for iso3 in ATLANTIC_ISO3:
        # Skip iso3s flagged no_adam_source.
        if policies.get(iso3, {}).get("action") == "no_adam_source":
            logger.info("Skipping %s — policy=no_adam_source", iso3)
            continue

        # ── adm0 ──
        fm_adm0 = load_fieldmaps_adm(iso3, 0)
        adam_country = filter_adam_country(adam_admin, iso3)
        rows.append(build_adm0_row(iso3, fm_adm0, adam_country))

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
            and action not in ("accept", "aggregate_adam_to_fm",
                               "needs_manual_mapping", "fm_adm1_only")
        ):
            continue

        if action in ("accept", "aggregate_adam_to_fm",
                      "needs_manual_mapping"):
            # All three actions share the same per-ge emit logic.
            # Policy names stay distinct because they carry different
            # review-status info (clean / ge-finer / boundary-reform),
            # but the lookup-build code path is one.
            fm_lvl = resolve_adam_fm_level(iso3, cfg)
            rows.extend(build_accept_adm1_rows(
                iso3, fm_lvl, adam_admin, per_row_notes, overrides,
            ))
        elif action == "fm_adm1_only":
            fm_lvl = resolve_adam_fm_level(iso3, cfg)
            rows.extend(build_fm_only_adm1_rows(
                iso3, fm_lvl,
                policy_note=policies.get(iso3, {}).get("note"),
            ))
        else:
            logger.warning(
                "Unknown action '%s' for %s — skipping adm1",
                action, iso3,
            )

    df = pd.DataFrame(rows, columns=LOOKUP_COLUMNS)

    # ── Final pass: apply [[adam_shared_source]] group caveats ──
    # Each entry names a coarse ADAM polygon + the FM units it covers,
    # plus a shared caveat note. We overwrite caveat_kind/caveat_note
    # on every matching row so the alert text downstream can render the
    # same footnote for any FM in the group. Validation: warn when a
    # listed fm_pcode doesn't appear in the lookup (typo or scope gap).
    shared = cfg.get("adam_shared_source", [])
    for entry in shared:
        iso3 = entry.get("iso3")
        fm_set = set(entry.get("fm_pcodes", []))
        note = entry.get("note")
        kind = entry.get("caveat_kind", "source_coarser_than_fm")
        if not iso3 or not fm_set or not note:
            logger.warning("Skipping malformed adam_shared_source: %s", entry)
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
                "adam_shared_source for %s adam_admin_id=%s: applied %d of %d "
                "fm_pcodes; missing from lookup: %s",
                iso3, entry.get("adam_admin_id"), n, len(fm_set), missing,
            )
        df.loc[mask, "caveat_kind"] = kind
        df.loc[mask, "caveat_note"] = note

    df.sort_values(
        ["iso3", "admin_level", "fm_pcode", "adam_admin_id"],
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
        if action in ("accept", "aggregate_adam_to_fm",
                      "needs_manual_mapping"):
            # FM-centric emit guarantees at least one row per FM unit;
            # zero rows means FM was empty.
            fm = load_fieldmaps_adm(iso3, 1)
            if fm is not None and len(fm) > 0 and n == 0:
                raise AssertionError(
                    f"{action} {iso3} has 0 adm1 rows but FM has "
                    f"{len(fm)} polygons"
                )
        if action == "fm_adm1_only":
            sub = df[(df["iso3"] == iso3) & (df["admin_level"] == 1)]
            if sub["adam_admin_id"].notna().any():
                bad = sub[sub["adam_admin_id"].notna()]["fm_pcode"].tolist()
                raise AssertionError(
                    f"fm_adm1_only {iso3} has adm1 rows with non-NULL "
                    f"adam_admin_id: {bad[:5]}"
                )

    # 3) PK uniqueness on (iso3, admin_level, fm_pcode, adam_admin_id).
    pk_dupes = df.duplicated(
        subset=["iso3", "admin_level", "fm_pcode", "adam_admin_id"]
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


def validate_adam_admin_ids(
    df: pd.DataFrame, adam_admin: gpd.GeoDataFrame,
) -> None:
    """Every non-NULL adm1 adam_admin_id must reference a real ge_adm1
    polygon. NULL is allowed (the ``fm_adm1_only`` fingerprint)."""
    valid_ids = set(adam_admin["adm1_id"].dropna().unique())
    adm1 = df[df["admin_level"] == 1]
    referenced = set(adm1["adam_admin_id"].dropna().unique())
    unknown = referenced - valid_ids
    if unknown:
        raise AssertionError(
            f"adm1 references adam_admin_id values not in ge_adm1 layer: "
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
    adam_admin = load_adam_admin(stage=args.mode)
    logger.info(
        "  %d ge_adm1 polygons across %d distinct ISO3 codes",
        len(adam_admin), adam_admin["iso3"].nunique(),
    )

    logger.info(
        "Building lookup for %d Atlantic countries", len(ATLANTIC_ISO3),
    )
    df = build_lookup(cfg, adam_admin)

    validate(df, cfg)
    validate_adam_admin_ids(df, adam_admin)

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
