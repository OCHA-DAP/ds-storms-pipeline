"""Build storms.gdacs_fm_lookup from the human-reviewed crosswalk CSV.

Sibling of `build_adam_fm_lookup_v2.py`. Reads
`data/review/gdacs_fm_crosswalk_humanreview.csv` (the authoritative
post-review crosswalk; every row is one FM↔GDACS spatial relationship
with the reviewer's decision attached) plus
`config/adm_level_config.toml` (for country-level policy and caveats)
plus FM adm0 polygons. Emits an FM-centric lookup with one adm0 row
per country plus zero-to-many adm1 rows per FM polygon.

Replaces the original `build_canonical_lookup.py` which drove the
lookup from spatial + TOML alone. The new path is human-review-first:
the row-level decisions in the humanreview crosswalk are the source
of truth for adm1.

Schema (matches existing storms.gdacs_fm_lookup PK):

    iso3, admin_level, fm_pcode, fm_name,
    gmi_admin, gdacs_admin_name,
    caveat_kind, caveat_note

PK = (iso3, admin_level, fm_pcode, gmi_admin). gmi_admin is NULL for
adm0 rows and for adm1 rows where FM has no GDACS partner.

Caveat priority per row:
    1. Per-row `caveat` from humanreview (human-written, authoritative)
    2. Country-level `policy_note` from TOML (for policy-driven rows)
    3. NULL (clean match, no caveat needed)

Usage::

    uv run python scripts/build_gdacs_fm_lookup_v2.py
        [--humrev PATH]   # default data/review/gdacs_fm_crosswalk_humanreview.csv
        [--out PATH]      # default data/gdacs_fm_lookup.csv
        [--mode dev|prod] # which DB stage if --write-db
        [--write-db]      # write to storms.gdacs_fm_lookup
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

from src.static.gdacs.admin import (  # noqa: E402
    filter_gdacs_country,
    load_gdacs_admin,
)
from src.static.gdacs.inputs import (  # noqa: E402
    ATLANTIC_ISO3,
    DEFAULT_CONFIG,
    load_fieldmaps_adm,
    load_level_config,
)

DEFAULT_HUMREV = (
    REPO_ROOT / "data" / "review"
    / "gdacs_fm_crosswalk_humanreview.csv"
)
DEFAULT_OUT = REPO_ROOT / "data" / "gdacs_fm_lookup.csv"
DB_SCHEMA = "storms"
DB_TABLE = "gdacs_fm_lookup"

LOOKUP_COLUMNS = [
    "iso3", "admin_level", "fm_pcode", "fm_name",
    "gmi_admin", "gdacs_admin_name",
    "caveat_kind", "caveat_note",
]

# Status values that produce a lookup row (others are excluded).
EMIT_STATUS = {"match", "gdacs_in_fm", "fm_in_gdacs", "fm_only"}

# Country-level policies that suppress adm1 emit entirely.
SUPPRESS_ADM1 = {"country_only", "no_fm_source"}

# Status → caveat_kind controlled vocabulary mapping.
STATUS_TO_KIND = {
    "match": None,                          # clean 1:1, no caveat
    "gdacs_in_fm": "aggregating_from_gdacs",  # FM aggregates N GDACS
    "fm_in_gdacs": "aggregated_in_gdacs",   # GDACS aggregates N FMs
    "fm_only": "no_gdacs_at_adm1",          # no GDACS partner
}
POLICY_TO_KIND = {
    "country_only": "country_only",
    "fm_adm1_only": "fm_adm1_only",
    "no_fm_source": "no_fm_source",
    "needs_manual_mapping": "needs_manual_mapping",
}

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
# adm0 builder
# ─────────────────────────────────────────────────────────────────────

def build_adm0_row(
    iso3: str,
    fm_adm0: "gpd.GeoDataFrame | pd.DataFrame | None",
    gdacs_country: gpd.GeoDataFrame,
    policy_action: str | None,
    policy_note: str | None,
) -> dict:
    """One adm0 row per country."""
    fm_name = None
    if fm_adm0 is not None and len(fm_adm0):
        row = fm_adm0.iloc[0]
        if "adm0_name" in fm_adm0.columns and pd.notna(row.get("adm0_name")):
            fm_name = row["adm0_name"]
    gdacs_country_name = None
    gmi_country = None
    if len(gdacs_country):
        first = gdacs_country.iloc[0]
        if "CNTRY_NAME" in gdacs_country.columns:
            gdacs_country_name = first.get("CNTRY_NAME")
        if "GMI_CNTRY" in gdacs_country.columns:
            gmi_country = first.get("GMI_CNTRY")
    if not fm_name:
        fm_name = gdacs_country_name or iso3
    return {
        "iso3": iso3,
        "admin_level": 0,
        "fm_pcode": iso3,
        "fm_name": fm_name,
        "gmi_admin": gmi_country,
        "gdacs_admin_name": gdacs_country_name or iso3,
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
    """Emit adm1 lookup rows for one country from its humanreview
    crosswalk rows."""
    if policy_action in SUPPRESS_ADM1:
        return []

    rows: list[dict] = []
    # Group by FM polygon — each group decides its own emit behavior
    for fm_pcode, g in country_xw.groupby("fm_pcode"):
        if pd.isna(fm_pcode):
            continue  # gdacs_only orphan rows have no FM
        emit = g[g["status"].isin(EMIT_STATUS)]
        fm_name = g.iloc[0]["fm_name"]

        if len(emit) == 0:
            # No definitive row. If country has fm_adm1_only policy
            # (or humrev policy column says so for this country),
            # emit a single FM-only row with NULL GDACS attach.
            row_policy = (
                g.iloc[0]["policy"] if "policy" in g.columns else None
            )
            if (
                policy_action == "fm_adm1_only"
                or row_policy == "fm_adm1_only"
            ):
                # Prefer any humrev caveat the reviewer set on this
                # FM's rows; else fall back to TOML policy_note.
                humrev_caveat = next(
                    (str(c).strip() for c in g["caveat"]
                     if pd.notna(c) and str(c).strip()),
                    None,
                )
                rows.append(_build_adm1_row(
                    iso3, fm_pcode, fm_name,
                    gmi_admin=None, gdacs_admin_name=None,
                    caveat_kind="fm_adm1_only",
                    caveat_note=humrev_caveat or policy_note,
                ))
            else:
                logger.warning(
                    "FM %s/%s has no definitive row and no fm-only "
                    "policy — skipping (would silently disappear)",
                    iso3, fm_pcode,
                )
            continue

        # One or more definitive rows. Emit one lookup row per
        # definitive row. For status=fm_only, dedupe to a single row.
        statuses = set(emit["status"])
        if "fm_only" in statuses and len(statuses) == 1:
            chosen = emit.iloc[0]
            rows.append(_build_adm1_row(
                iso3, fm_pcode, fm_name,
                gmi_admin=None, gdacs_admin_name=None,
                caveat_kind=STATUS_TO_KIND["fm_only"],
                caveat_note=_caveat(chosen),
            ))
            continue

        # Otherwise emit one lookup row per emit-status row.
        for _, r in emit.iterrows():
            rows.append(_build_adm1_row(
                iso3, fm_pcode, fm_name,
                gmi_admin=r["gmi_admin"],
                gdacs_admin_name=r["gdacs_admin_name"],
                caveat_kind=STATUS_TO_KIND.get(r["status"]),
                caveat_note=_caveat(r),
            ))

    return rows


def _build_adm1_row(
    iso3, fm_pcode, fm_name,
    gmi_admin, gdacs_admin_name,
    caveat_kind, caveat_note,
) -> dict:
    """Construct an adm1 lookup row dict."""
    if gmi_admin is not None and pd.notna(gmi_admin):
        gmi_admin = str(gmi_admin)
    else:
        gmi_admin = None
    return {
        "iso3": iso3,
        "admin_level": 1,
        "fm_pcode": fm_pcode,
        "fm_name": fm_name,
        "gmi_admin": gmi_admin,
        "gdacs_admin_name": (
            gdacs_admin_name if pd.notna(gdacs_admin_name) else None
        ),
        "caveat_kind": caveat_kind,
        "caveat_note": caveat_note,
    }


def _caveat(row: pd.Series) -> str | None:
    """Pick the right caveat text for a per-row emit.

    Only the humanreview `caveat` column is honored (verbatim). We
    deliberately do NOT fall back to the country-level policy_note
    for per-row emits — an empty caveat means "no caveat needed"
    (e.g. a clean match). policy_note is only used by the policy-
    driven FM-only fallback path.
    """
    if (
        "caveat" in row and pd.notna(row["caveat"])
        and str(row["caveat"]).strip()
    ):
        return str(row["caveat"]).strip()
    return None


# ─────────────────────────────────────────────────────────────────────
# Driver
# ─────────────────────────────────────────────────────────────────────

def build_lookup(
    humrev: pd.DataFrame,
    cfg: dict,
    gdacs_admin: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """Produce the full lookup DataFrame in one pass."""
    rows: list[dict] = []
    policies = cfg.get("gdacs_policy", {})

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
        gdacs_country = filter_gdacs_country(gdacs_admin, iso3)
        rows.append(build_adm0_row(
            iso3, fm_adm0, gdacs_country, policy_action, policy_note,
        ))

        # adm1 from humanreview
        country_xw = humrev[humrev["iso3"] == iso3]
        if len(country_xw) == 0:
            continue
        rows.extend(emit_adm1_rows_for_country(
            iso3, country_xw, policy_action, policy_note,
        ))

    df = pd.DataFrame(rows, columns=LOOKUP_COLUMNS)
    df.sort_values(
        ["iso3", "admin_level", "fm_pcode", "gmi_admin"],
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
                    help="write the lookup to storms.gdacs_fm_lookup")
    args = ap.parse_args()

    logger.info("Loading humanreview crosswalk from %s", args.humrev)
    # utf-8-sig: transparently strips BOM if present
    humrev = pd.read_csv(args.humrev, encoding="utf-8-sig")
    logger.info("  %d rows across %d iso3s",
                len(humrev), humrev["iso3"].nunique())

    cfg = load_level_config(args.config)
    n_policies = len(cfg.get("gdacs_policy", {}))
    logger.info("Loaded %d gdacs_policy entries from %s",
                n_policies, args.config)

    gdacs = load_gdacs_admin()
    logger.info("Loaded GDACS admin layer: %d polygons", len(gdacs))

    df = build_lookup(humrev, cfg, gdacs)
    logger.info("Built lookup: %d rows (%d adm0 + %d adm1)",
                len(df), (df["admin_level"] == 0).sum(),
                (df["admin_level"] == 1).sum())

    args.out.parent.mkdir(parents=True, exist_ok=True)
    # utf-8-sig: BOM so Excel-on-Mac doesn't misread the file
    df.to_csv(args.out, index=False, encoding="utf-8-sig")
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
