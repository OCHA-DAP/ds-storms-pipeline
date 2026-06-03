"""LLM pass on data/review/gdacs_fm_crosswalk_humanreview.csv.

First-pass review of the 19 `fragmented` rows that remain after the
migration. Each edit sets `classification_type='llm'` + a note
explaining the reasoning. Idempotent and safe: only edits rows whose
`status` is currently `fragmented` (won't touch human edits made
afterward).

Three categories of fragmented rows:

  A. Below-0.10 IoU boundary spillover next to a clean primary match
     (e.g. GTM Sololá × Suchitepequez 0.054 alongside Sololá × Solola
     0.77 as gdacs_in_fm) → drop the spillover

  B. Pre-split primary match — the migration brought in the old
     build's pre_split_boundary decision but topology calls it
     fragmented because both the namesake pre-split polygon AND the
     adjacent pre-split polygon overlap the modern FM. The namesake
     overlap is the canonical pre-split match → fm_in_gdacs.

  C. Pre-split secondary spillover — the other pre-split polygon
     that also overlaps the FM. Collateral, drop.

After these 19 edits, every FM polygon has a definitive resolution
and no FM silently disappears from the lookup.

Run from repo root::

    uv run python scripts/llm_pass_gdacs_crosswalk.py
"""

import argparse
import logging
import sys
from pathlib import Path

import coloredlogs
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_IN = (
    REPO_ROOT / "data" / "review"
    / "gdacs_fm_crosswalk_humanreview.csv"
)

logger = logging.getLogger(__name__)


# (iso3, fm_pcode, gmi_admin, new_status, note)
EDITS: list[tuple[str, str, str, str, str]] = [
    # ── Category A: boundary spillover drops (9 rows) ──────────────
    (
        "GTM", "GTM-20230404-07", "GTM-SCH", "drop",
        "Boundary spillover at IoU 0.054. FM Sololá's canonical "
        "match is GDACS Solola (IoU 0.77, gdacs_in_fm).",
    ),
    (
        "GTM", "GTM-20230404-09", "GTM-RTL", "drop",
        "Boundary spillover at IoU 0.053. FM Quetzaltenango's "
        "canonical match is GDACS Quezaltenango (IoU 0.76, gdacs_in_fm).",
    ),
    (
        "HTI", "HTI-20230404-01", "HTI-SES", "drop",
        "Boundary spillover at IoU 0.051. FM West's canonical match "
        "is GDACS Ouest (IoU 0.81, gdacs_in_fm).",
    ),
    (
        "JAM", "JAM-20240807-14", "JAM-HNV", "drop",
        "Boundary spillover at IoU 0.071. FM Westmoreland's canonical "
        "match is GDACS Westmoreland (IoU 0.80, gdacs_in_fm).",
    ),
    (
        "MEX", "MEX-20230420-04", "MEX-QRO", "drop",
        "Boundary spillover at IoU 0.054. FM Campeche's canonical "
        "match is GDACS Campeche (IoU 0.87, gdacs_in_fm).",
    ),
    (
        "NIC", "NIC-20231203-03", "NIC-NSE", "drop",
        "Boundary spillover at IoU 0.069. FM Madriz's canonical "
        "match is GDACS Madriz (IoU 0.64, gdacs_in_fm).",
    ),
    (
        "NIC", "NIC-20231203-12", "NIC-RIV", "drop",
        "Boundary spillover at IoU 0.054. FM Granada's canonical "
        "match is GDACS Granada (IoU 0.66, gdacs_in_fm).",
    ),
    (
        "SUR", "SUR-20191024-07", "SUR-WNC", "drop",
        "Boundary spillover at IoU 0.060. FM Paramaribo's canonical "
        "match is GDACS Paramaribo (IoU 0.68, gdacs_in_fm).",
    ),
    (
        "SWE", "SWE-20230119-14", "SWE-VSM", "drop",
        "Boundary spillover at IoU 0.085. FM Uppsala län's canonical "
        "match is GDACS Uppsala (IoU 0.78, gdacs_in_fm).",
    ),

    # ── Category B: pre-split FM-in-coarser-GDACS (5 rows) ─────────
    (
        "CUB", "CUB-20201118-01", "CUB-LHB", "fm_in_gdacs",
        "Pre-split boundary. GDACS uses pre-2011 'La Habana' polygon "
        "which covers both FM Artemisa and FM Mayabeque. Matches the "
        "old build's pre_split_boundary decision (caveat preserved).",
    ),
    (
        "GRL", "GRL-20230119-1", "GRL-VST", "fm_in_gdacs",
        "Pre-2009 reform boundary. GDACS uses 'Vestgrønland' which "
        "covers FM Avannaata + other post-reform municipalities. "
        "Matches the old build's pre_split_boundary decision.",
    ),
    (
        "GRL", "GRL-20230119-3", "GRL-OST", "fm_in_gdacs",
        "Pre-2009 reform boundary. GDACS uses 'Østgrønland' which "
        "covers FM Northeast Greenland National Park + other post-"
        "reform municipalities. Matches the old build's "
        "pre_split_boundary decision.",
    ),
    (
        "GRL", "GRL-20230119-6", "GRL-OST", "fm_in_gdacs",
        "Pre-2009 reform boundary. GDACS uses 'Østgrønland' which "
        "covers FM Sermersooq + other post-reform municipalities. "
        "Matches the old build's pre_split_boundary decision.",
    ),
    (
        "PAN", "PAN-20230404-10", "PAN-BDT", "fm_in_gdacs",
        "Pre-1997 comarca creation. GDACS 'Bocas del Toro' polygon "
        "covers FM Ngöbe Buglé + FM Bocas del Toro. Matches the old "
        "build's pre_split_boundary decision.",
    ),

    # ── Category C: pre-split spillover drops (5 rows) ─────────────
    (
        "CUB", "CUB-20201118-01", "CUB-PDR", "drop",
        "Boundary spillover with GDACS Pinar del Rio (adjacent pre-"
        "split polygon). FM Artemisa's primary match is GDACS La "
        "Habana via the pre-2011 boundary; this row is collateral.",
    ),
    (
        "GRL", "GRL-20230119-1", "GRL-NRD", "drop",
        "Boundary spillover with GDACS Nordgronland (adjacent pre-"
        "2009 polygon). FM Avannaata's primary match is GDACS "
        "Vestgronland.",
    ),
    (
        "GRL", "GRL-20230119-3", "GRL-NRD", "drop",
        "Boundary spillover with GDACS Nordgronland (adjacent pre-"
        "2009 polygon). FM Northeast Greenland National Park's "
        "primary match is GDACS Ostgronland.",
    ),
    (
        "GRL", "GRL-20230119-6", "GRL-VST", "drop",
        "Boundary spillover with GDACS Vestgronland (adjacent pre-"
        "2009 polygon). FM Sermersooq's primary match is GDACS "
        "Ostgronland.",
    ),
    (
        "PAN", "PAN-20230404-10", "PAN-CHR", "drop",
        "Boundary spillover with GDACS Chiriqui (adjacent province). "
        "FM Ngöbe Buglé's primary match is GDACS Bocas del Toro via "
        "the pre-1997 comarca boundary.",
    ),
]


def main() -> int:
    coloredlogs.install(
        level="INFO",
        fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--csv", type=Path, default=DEFAULT_IN)
    args = ap.parse_args()

    df = pd.read_csv(args.csv, encoding="utf-8-sig")
    logger.info("Loaded %d rows from %s", len(df), args.csv)

    # Coerce object dtype so we can write string cells
    for col in ("status", "classification_type", "note"):
        if df[col].dtype != object:
            df[col] = df[col].astype(object)
    df["note"] = df["note"].fillna("")

    applied = 0
    skipped = 0
    not_found = []
    for iso3, fm_pcode, gmi_admin, new_status, note in EDITS:
        mask = (
            (df["iso3"] == iso3)
            & (df["fm_pcode"] == fm_pcode)
            & (df["gmi_admin"] == gmi_admin)
        )
        n = mask.sum()
        if n == 0:
            not_found.append((iso3, fm_pcode, gmi_admin))
            continue
        if n > 1:
            logger.warning(
                "Edit key matched %d rows: %s/%s/%s — skipping",
                n, iso3, fm_pcode, gmi_admin,
            )
            continue
        idx = df.index[mask][0]
        # Only act on rows that are currently fragmented — this keeps
        # the script idempotent and safe to re-run after the user has
        # made other edits.
        if df.at[idx, "status"] != "fragmented":
            skipped += 1
            continue
        df.at[idx, "status"] = new_status
        df.at[idx, "classification_type"] = "llm"
        df.at[idx, "note"] = note
        applied += 1

    # ── Cascade promotions ──────────────────────────────────────────
    # After dropping spillover rows above, several primary rows have
    # stale topology labels (e.g. GTM Sololá's primary was
    # gdacs_in_fm because both Solola and Suchitepequez were above
    # noise; now only Solola is → should be match). Recompute counts
    # honoring drops and promote stale labels.
    NOISE = 0.05
    overlap = df[df["row_kind"] == "overlap"]
    counted = overlap[
        (overlap["iou"] >= NOISE) & (overlap["status"] != "drop")
    ]
    fm_count = counted.groupby(["iso3", "fm_pcode"]).size().to_dict()
    gd_count = counted.groupby(["iso3", "gmi_admin"]).size().to_dict()

    def expected(r) -> str:
        if r["iou"] < NOISE:
            return "noise"
        nf = fm_count.get((r["iso3"], r["fm_pcode"]), 0)
        ng = gd_count.get((r["iso3"], r["gmi_admin"]), 0)
        if nf == 1 and ng == 1:
            return "match"
        if nf > 1 and ng == 1:
            return "gdacs_in_fm"
        if nf == 1 and ng > 1:
            return "fm_in_gdacs"
        return "fragmented"

    cascaded = 0
    for idx in df.index:
        r = df.loc[idx]
        if r["row_kind"] != "overlap":
            continue
        # Only cascade rows we haven't touched (no human/llm override
        # of status in this pass). Skip drops/noise/needs_review.
        if r["status"] in ("drop", "noise", "needs_review"):
            continue
        # Skip rows under a TOML policy override
        if isinstance(r["policy"], str) and r["policy"] in (
            "country_only", "fm_adm1_only", "no_fm_source",
        ):
            continue
        exp = expected(r)
        if r["status"] != exp:
            old_status = r["status"]
            df.at[idx, "status"] = exp
            # Stamp as llm with a cascade note — preserve any existing
            # caveat / note context the migration put there.
            df.at[idx, "classification_type"] = "llm"
            cascade_note = (
                f"Cascade: {old_status} → {exp} after LLM-pass drops "
                f"removed the partner that justified the previous "
                f"topology label. Counts are now "
                f"({fm_count.get((r['iso3'], r['fm_pcode']), 0)} "
                f"above-noise GDACS in this FM, "
                f"{gd_count.get((r['iso3'], r['gmi_admin']), 0)} "
                f"above-noise FMs in this GDACS)."
            )
            existing_note = (
                df.at[idx, "note"]
                if pd.notna(df.at[idx, "note"]) and df.at[idx, "note"]
                else ""
            )
            df.at[idx, "note"] = (
                f"{existing_note}\n{cascade_note}"
                if existing_note else cascade_note
            )
            cascaded += 1

    df.to_csv(args.csv, index=False, encoding="utf-8-sig")
    logger.info(
        "Applied %d primary edits + %d cascade promotions; "
        "skipped %d non-fragmented rows, %d not found",
        applied, cascaded, skipped, len(not_found),
    )
    if not_found:
        logger.warning("Not found: %s", not_found)
    return 0


if __name__ == "__main__":
    sys.exit(main())
