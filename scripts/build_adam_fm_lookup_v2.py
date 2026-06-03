"""Build storms.adam_fm_lookup from the human-reviewed crosswalk CSV.

Reads `data/review/adam_fm_crosswalk_humanreview.csv` (the
authoritative post-review crosswalk; every row is one FM↔ADAM spatial
relationship with the reviewer's decision attached) plus
`config/adm_level_config.toml` (for country-level policy and caveats)
plus FM adm0 polygons. Emits an FM-centric lookup with one adm0 row
per country plus zero-to-many adm1 rows per FM polygon.

Replaces the original `build_adam_fm_lookup.py` which drove the lookup
from spatial + TOML alone. The new path is human-review-first: the
row-level decisions in the humanreview crosswalk are the source of
truth for adm1.

Schema (matches existing storms.adam_fm_lookup PK):

    iso3, admin_level, fm_pcode, fm_name,
    adam_admin_id, adam_admin_name,
    iou, caveat_kind, caveat_note

PK = (iso3, admin_level, fm_pcode, adam_admin_id). adam_admin_id is
NULL for adm0 rows and for adm1 rows where FM has no ADAM partner.

Caveat priority per row:
    1. Per-row `caveat` from humanreview (human-written, authoritative)
    2. Country-level `policy_note` from TOML (for policy-driven rows)
    3. NULL (clean match, no caveat needed)

Usage::

    uv run python scripts/build_adam_fm_lookup_v2.py
        [--humrev PATH]   # default data/review/adam_fm_crosswalk_humanreview.csv
        [--out PATH]      # default data/adam_fm_lookup.csv
        [--mode dev|prod] # which DB stage if --write-db
        [--write-db]      # write to storms.adam_fm_lookup
"""

import argparse
import logging
import sys
from pathlib import Path

import coloredlogs
import geopandas as gpd
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
)

DEFAULT_HUMREV = (
    REPO_ROOT / "data" / "review" / "adam_fm_crosswalk_humanreview.csv"
)
DEFAULT_OUT = REPO_ROOT / "data" / "adam_fm_lookup.csv"
DB_SCHEMA = "storms"
DB_TABLE = "adam_fm_lookup"

LOOKUP_COLUMNS = [
    "iso3", "admin_level", "fm_pcode", "fm_name",
    "adam_admin_id", "adam_admin_name",
    "iou", "caveat_kind", "caveat_note",
]

# Status values that produce a lookup row (others are excluded).
EMIT_STATUS = {"match", "adam_in_fm", "fm_in_adam", "fm_only"}

# Country-level policies that suppress adm1 emit entirely.
SUPPRESS_ADM1 = {"country_only", "no_adam_source"}

# Status → caveat_kind controlled vocabulary mapping.
STATUS_TO_KIND = {
    "match": None,                         # clean 1:1, no caveat
    "adam_in_fm": "aggregating_from_adam",  # FM aggregates N ADAMs
    "fm_in_adam": "aggregated_in_adam",    # ADAM aggregates N FMs
    "fm_only": "no_adam_at_adm1",          # no ADAM partner
}
POLICY_TO_KIND = {
    "country_only": "country_only",
    "fm_adm1_only": "fm_adm1_only",
    "no_adam_source": "no_adam_source",
    "needs_manual_mapping": "needs_manual_mapping",
}

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
# adm0 builder
# ─────────────────────────────────────────────────────────────────────

def build_adm0_row(
    iso3: str,
    fm_adm0: "gpd.GeoDataFrame | pd.DataFrame | None",
    adam_country: gpd.GeoDataFrame,
    policy_action: str | None,
    policy_note: str | None,
) -> dict:
    """One adm0 row per country."""
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
    # If country has no ADAM at all, adam_adm0_name is the country name
    # alias which we still surface for downstream joins.
    return {
        "iso3": iso3,
        "admin_level": 0,
        "fm_pcode": iso3,
        "fm_name": fm_name,
        "adam_admin_id": None,
        "adam_admin_name": adam_adm0_name or iso3,
        "iou": None,
        "caveat_kind": POLICY_TO_KIND.get(policy_action),
        "caveat_note": policy_note or None,
    }


# ─────────────────────────────────────────────────────────────────────
# adm1 builder
# ─────────────────────────────────────────────────────────────────────

def emit_adm1_rows_for_country(
    iso3: str,
    country_xw: pd.DataFrame,
    policy_action: str | None,
    policy_note: str | None,
) -> list[dict]:
    """Emit adm1 lookup rows for one country from its humrev crosswalk
    rows."""
    if policy_action in SUPPRESS_ADM1:
        return []

    rows: list[dict] = []
    # Group by FM polygon — each group decides its own emit behavior
    for fm_pcode, g in country_xw.groupby("fm_pcode"):
        if pd.isna(fm_pcode):
            continue  # adam_only orphan rows have no FM
        emit = g[g["status"].isin(EMIT_STATUS)]
        fm_name = g.iloc[0]["fm_name"]

        if len(emit) == 0:
            # No definitive row. If country has fm_adm1_only policy
            # (or humrev policy column says so for this country),
            # emit a single FM-only row with NULL ADAM. Caveat
            # priority: any humrev `caveat` on this FM's rows wins,
            # else fall back to the country-level policy_note.
            row_policy = (
                g.iloc[0]["policy"] if "policy" in g.columns else None
            )
            if (
                policy_action == "fm_adm1_only"
                or row_policy == "fm_adm1_only"
            ):
                humrev_caveat = next(
                    (str(c).strip() for c in g["caveat"]
                     if pd.notna(c) and str(c).strip()),
                    None,
                )
                rows.append(_build_adm1_row(
                    iso3, fm_pcode, fm_name,
                    adam_id=None, adam_name=None, iou=None,
                    caveat_kind="fm_adm1_only",
                    caveat_note=humrev_caveat or policy_note,
                ))
            # else: FM silently disappears. The validator's coverage
            # check should have caught this; log a warning here too.
            else:
                logger.warning(
                    "FM %s/%s has no definitive row and no fm-only "
                    "policy — skipping (would silently disappear)",
                    iso3, fm_pcode,
                )
            continue

        # One or more definitive rows. Emit one lookup row per
        # definitive row. For status=fm_only, dedupe to a single row
        # (humrev might have multiple fm_only rows per FM).
        statuses = set(emit["status"])
        if "fm_only" in statuses and len(statuses) == 1:
            # All definitive rows are fm_only — dedupe to one.
            chosen = emit.iloc[0]
            rows.append(_build_adm1_row(
                iso3, fm_pcode, fm_name,
                adam_id=None, adam_name=None, iou=None,
                caveat_kind=STATUS_TO_KIND["fm_only"],
                caveat_note=_caveat(chosen),
            ))
            continue

        # Otherwise emit one lookup row per emit-status row. For
        # adam_in_fm this is many rows; for match/fm_in_adam it's one.
        for _, r in emit.iterrows():
            rows.append(_build_adm1_row(
                iso3, fm_pcode, fm_name,
                adam_id=r["adam_admin_id"],
                adam_name=r["adam_admin_name"],
                iou=r["iou"],
                caveat_kind=STATUS_TO_KIND.get(r["status"]),
                caveat_note=_caveat(r),
            ))

    return rows


def _build_adm1_row(
    iso3, fm_pcode, fm_name,
    adam_id, adam_name, iou,
    caveat_kind, caveat_note,
) -> dict:
    """Construct an adm1 lookup row dict."""
    # Coerce types — adam_admin_id is float in the crosswalk because
    # pandas reads NaN-containing int columns as float. We want
    # nullable int in the DB.
    if adam_id is not None and pd.notna(adam_id):
        adam_id = int(adam_id)
    else:
        adam_id = None
    if iou is not None and pd.notna(iou):
        iou = float(iou)
    else:
        iou = None
    return {
        "iso3": iso3,
        "admin_level": 1,
        "fm_pcode": fm_pcode,
        "fm_name": fm_name,
        "adam_admin_id": adam_id,
        "adam_admin_name": adam_name if pd.notna(adam_name) else None,
        "iou": iou,
        "caveat_kind": caveat_kind,
        "caveat_note": caveat_note,
    }


def _caveat(row: pd.Series) -> str | None:
    """Pick the right caveat text for a per-row emit.

    Only the humrev `caveat` column is honored. We deliberately do
    NOT fall back to the country-level policy_note for per-row emits
    — when the row exists in the crosswalk, the human/LLM has already
    made the call. An empty caveat means "no caveat needed" (e.g. a
    clean match). policy_note is only the right fallback for the
    *policy-driven* FM-only emit path (no humrev row exists).

    The row's `note` column is internal LLM/build reasoning and is
    NOT used as a consumer-facing caveat.
    """
    if "caveat" in row and pd.notna(row["caveat"]) and str(row["caveat"]).strip():
        return str(row["caveat"]).strip()
    return None


# ─────────────────────────────────────────────────────────────────────
# Driver
# ─────────────────────────────────────────────────────────────────────

def build_lookup(
    humrev: pd.DataFrame,
    cfg: dict,
    adam_admin: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """Produce the full lookup DataFrame in one pass."""
    rows: list[dict] = []
    policies = cfg.get("adam_policy", {})

    for iso3 in ATLANTIC_ISO3:
        policy = policies.get(iso3, {})
        policy_action = policy.get("action")
        policy_note = policy.get("note")

        # adm0 always emitted
        try:
            fm_adm0 = load_fieldmaps_adm(iso3, 0)
        except Exception as e:
            logger.warning("FM adm0 load failed for %s: %s", iso3, e)
            fm_adm0 = None
        adam_country = filter_adam_country(adam_admin, iso3)
        rows.append(build_adm0_row(
            iso3, fm_adm0, adam_country, policy_action, policy_note,
        ))

        # adm1 from humrev
        country_xw = humrev[humrev["iso3"] == iso3]
        if len(country_xw) == 0:
            continue
        rows.extend(emit_adm1_rows_for_country(
            iso3, country_xw, policy_action, policy_note,
        ))

    df = pd.DataFrame(rows, columns=LOOKUP_COLUMNS)
    df.sort_values(
        ["iso3", "admin_level", "fm_pcode", "adam_admin_id"],
        inplace=True, kind="stable", na_position="last",
    )
    df.reset_index(drop=True, inplace=True)
    return df


def main() -> int:
    coloredlogs.install(
        level="INFO",
        fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger(
        "azure.core.pipeline.policies.http_logging_policy"
    ).setLevel(logging.WARNING)

    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--humrev", type=Path, default=DEFAULT_HUMREV,
                    help="human-reviewed crosswalk CSV (the source of "
                         "truth for reviewer decisions)")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    ap.add_argument("--mode", choices=["dev", "prod"], default="dev")
    ap.add_argument("--write-db", action="store_true",
                    help="write the lookup to storms.adam_fm_lookup")
    args = ap.parse_args()

    logger.info("Loading humanreview crosswalk from %s", args.humrev)
    humrev = pd.read_csv(args.humrev)
    logger.info("  %d rows across %d iso3s",
                len(humrev), humrev["iso3"].nunique())

    cfg = load_level_config(args.config)
    n_policies = len(cfg.get("adam_policy", {}))
    logger.info("Loaded %d adam_policy entries from %s",
                n_policies, args.config)

    adam = load_adam_admin()
    logger.info("Loaded ADAM admin layer: %d polygons", len(adam))

    df = build_lookup(humrev, cfg, adam)
    logger.info("Built lookup: %d rows (%d adm0 + %d adm1)",
                len(df), (df["admin_level"] == 0).sum(),
                (df["admin_level"] == 1).sum())

    args.out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)
    logger.info("Wrote CSV to %s", args.out)

    if args.write_db:
        import ocha_stratus as stratus  # noqa: E402
        engine = stratus.get_engine(stage=args.mode, write=True)
        df.to_sql(
            DB_TABLE, engine,
            schema=DB_SCHEMA,
            if_exists="replace",
            index=False,
        )
        logger.info("Wrote %d rows to %s.%s (stage=%s)",
                    len(df), DB_SCHEMA, DB_TABLE, args.mode)

    return 0


if __name__ == "__main__":
    sys.exit(main())
