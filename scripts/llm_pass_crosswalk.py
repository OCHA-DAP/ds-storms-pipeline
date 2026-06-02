"""LLM pass on data/adam_fm_crosswalk.csv — Claude's row-level decisions.

This script encodes the LLM (Claude) review of `fragmented` rows and
applies them as overrides with classification_type="llm". It is
idempotent and safe: it only edits rows currently classified as
`spatial` — if you've manually changed a row to `human`, this script
leaves it alone. If you re-run it after rebuilding the CSV, it'll
re-apply the same set of edits.

Decisions are based on:

  * Names — when FM and ADAM disagree spatially but the name match is
    obviously correct (e.g. FM North Andros ↔ ADAM North Andros, even
    though ADAM Central Andros has a higher IoU with the FM polygon).
  * Topology context — when a fragmented label is really an
    adam_in_fm relationship in disguise (e.g. FM Central Abaco
    straddles ADAM N+S Abaco because ADAM has no clean Central
    Abaco polygon).
  * Noise from placeholders — ADAM polygons named "Under National
    Administration" (BHS 901518, USA 902134) or with NaN names
    (NIC 900324, 900325) consistently produce spurious overlaps.
    Drop them.

Run from repo root::

    uv run python scripts/llm_pass_crosswalk.py
"""

import argparse
import logging
import re
import sys
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path

import coloredlogs
import pandas as pd


# Countries where the [adam_policy] = needs_manual_mapping but the row-
# level data is clean enough to resolve algorithmically by name match.
# For each FM polygon in these countries the LLM pass picks the best
# name-similarity ADAM partner above noise (or the sole partner if
# there is only one), promotes to match or fm_in_adam based on shared
# ADAMs across FMs, drops the other above-noise rows as boundary
# spillover, and sets below-noise rows to their natural `noise` label.
LLM_RESOLVE_COUNTRIES = ["BRB", "CUB", "KNA", "PAN", "SLV", "TTO", "VCT"]

NOISE_IOU = 0.05


def normalize_name(s) -> str:
    """Lowercase, strip accents, normalize St./Saint, strip punctuation."""
    if not s or (isinstance(s, float) and pd.isna(s)):
        return ""
    s = str(s).lower().strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"\bst\.?\b", "saint", s)
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def name_match_score(a, b) -> float:
    """Return 0–1 similarity between two place names. Uses
    SequenceMatcher.ratio() on normalized strings, which is robust to
    abbreviations (St. ↔ Saint), accents (Holguín ↔ Holguan), and
    minor typos (Saint Thomas ↔ St. Tomas)."""
    na, nb = normalize_name(a), normalize_name(b)
    if not na or not nb:
        return 0.0
    return SequenceMatcher(None, na, nb).ratio()


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_IN = REPO_ROOT / "data" / "adam_fm_crosswalk.csv"

logger = logging.getLogger(__name__)


# Each edit is (iso3, fm_pcode, adam_admin_id, new_status, note).
# new_status="" means no status change (just adding a note).
EDITS: list[tuple[str, str, float, str, str]] = [
    # ── BHS fragmented (8) ──────────────────────────────────────────
    (
        "BHS", "BHS-20201113-06", 901515.0, "adam_in_fm",
        "FM Central Abaco straddles ADAM's North Abaco + South Abaco "
        "because ADAM has no clean Central Abaco polygon (the one it "
        "carries is tiny — IoU 0.04, below noise). Report alongside "
        "both N + S Abaco ADAMs with a coarseness caveat.",
    ),
    (
        "BHS", "BHS-20201113-06", 901511.0, "adam_in_fm",
        "FM Central Abaco straddles ADAM's North Abaco + South Abaco "
        "because ADAM has no clean Central Abaco polygon. Same caveat "
        "as the South Abaco row.",
    ),
    (
        "BHS", "BHS-20201113-11", 901507.0, "drop",
        "Boundary spillover. FM East Grand Bahama is dominated by "
        "ADAM East Grand Bahama (IoU 0.72, adam_in_fm). The 10% "
        "overlap with ADAM Freeport is digitization fuzz.",
    ),
    (
        "BHS", "BHS-20201113-16", 901518.0, "drop",
        "Spillover from ADAM placeholder polygon 'Under National "
        "Administration'. FM Inagua's canonical match is ADAM Inagua "
        "(IoU 0.92, adam_in_fm).",
    ),
    (
        "BHS", "BHS-20201113-23", 901502.0, "drop",
        "Boundary disagreement between FM and ADAM Andros. ADAM splits "
        "the area into Central + North Andros; FM treats it as one "
        "North Andros polygon. The spatial primary (Central Andros, "
        "IoU 0.47) is collateral — dropped to avoid double-counting. "
        "Canonical match is the name-aligned row below "
        "(FM North Andros ↔ ADAM North Andros).",
    ),
    (
        "BHS", "BHS-20201113-23", 901512.0, "match",
        "Name match wins over spatial. FM and ADAM disagree on the "
        "Central/North Andros boundary — ADAM North Andros polygon "
        "covers a smaller area than FM North Andros (which extends "
        "into what ADAM calls Central Andros). Report this ADAM "
        "number with a caveat that ADAM's polygon is geographically "
        "smaller than the FM polygon it represents.",
    ),
    (
        "BHS", "BHS-20201113-24", 901503.0, "drop",
        "Boundary spillover. FM North Eleuthera is dominated by "
        "ADAM North Eleuthera (IoU 0.60, adam_in_fm). The 22% overlap "
        "with ADAM Central Eleuthera is digitization fuzz.",
    ),
    (
        "BHS", "BHS-20201113-32", 901507.0, "adam_in_fm",
        "FM West Grand Bahama is covered by both ADAM Freeport (the "
        "city, IoU 0.64) and ADAM West Grand Bahama (the rest of the "
        "region, IoU 0.12, already adam_in_fm). Both aggregate into "
        "FM West Grand Bahama. Report with caveat that ADAM splits "
        "this region into city + non-city polygons.",
    ),
    # ── NIC fragmented (6) — all unnamed (N/A) ADAM polygons ────────
    (
        "NIC", "NIC-20231203-06", 900325.0, "drop",
        "Unnamed (N/A) ADAM polygon spilling across FM departments. "
        "Likely one of Nicaragua's autonomous coastal regions (RACCN/"
        "RACCS) but with N/A in upstream adm1_name. FM León's "
        "canonical match is ADAM León (IoU 0.93, adam_in_fm).",
    ),
    (
        "NIC", "NIC-20231203-09", 900325.0, "drop",
        "Unnamed (N/A) ADAM polygon. FM Managua's canonical match is "
        "ADAM Managua (IoU 0.83, adam_in_fm).",
    ),
    (
        "NIC", "NIC-20231203-11", 900324.0, "drop",
        "Unnamed (N/A) ADAM polygon. FM Chontales's canonical match "
        "is ADAM Chontales (IoU 0.80, adam_in_fm).",
    ),
    (
        "NIC", "NIC-20231203-12", 900324.0, "drop",
        "Unnamed (N/A) ADAM polygon. FM Granada's canonical match is "
        "ADAM Granada (IoU 0.52, adam_in_fm).",
    ),
    (
        "NIC", "NIC-20231203-14", 900324.0, "drop",
        "Unnamed (N/A) ADAM polygon overlapping at 30% IoU — the "
        "largest of the N/A overlaps but still without a clean name. "
        "FM Rivas's canonical match is ADAM Rivas (IoU 0.41, "
        "adam_in_fm).",
    ),
    (
        "NIC", "NIC-20231203-15", 900324.0, "drop",
        "Unnamed (N/A) ADAM polygon. FM Río San Juan's canonical "
        "match is ADAM Río San Juan (IoU 0.78, adam_in_fm).",
    ),
    # ── USA fragmented (2) — both 'Under National Administration' ───
    (
        "USA", "USA-20230119-26", 902134.0, "drop",
        "Spillover from ADAM placeholder polygon 'Under National "
        "Administration'. FM Michigan's canonical match is ADAM "
        "Michigan (IoU 0.61, adam_in_fm).",
    ),
    (
        "USA", "USA-20230119-55", 902134.0, "drop",
        "Spillover from ADAM placeholder polygon 'Under National "
        "Administration'. FM Wisconsin's canonical match is ADAM "
        "Wisconsin (IoU 0.85, adam_in_fm).",
    ),
    # ── Size-asymmetric FMs (7) ─────────────────────────────────────
    # FM polygon is small (island, city, reef) but lives inside a
    # larger ADAM polygon. IoU is below noise because of the size
    # asymmetry, not because the relationship is wrong. Without these
    # the FM would silently disappear from the lookup (FM coverage
    # invariant violation).
    (
        "BHS", "BHS-20201113-09", 901507.0, "fm_in_adam",
        "FM 'City of Freeport' is the city-proper subdivision inside "
        "ADAM 'Freeport' (which covers the wider Freeport region). "
        "IoU 0.046 reflects size asymmetry (small city within a large "
        "ADAM polygon), not lack of correspondence. ADAM Freeport also "
        "covers FM 'West Grand Bahama' — share with that FM in the "
        "lookup with a coarseness caveat.",
    ),
    (
        "BHS", "BHS-20201113-13", 901511.0, "fm_in_adam",
        "FM 'Grand Cay' is a tiny island within ADAM 'North Abaco' "
        "coverage area. IoU 0.003 reflects size asymmetry. ADAM North "
        "Abaco also covers FM North Abaco and FM Central Abaco — "
        "lookup reports against ADAM North Abaco with caveat.",
    ),
    (
        "BHS", "BHS-20201113-14", 901513.0, "fm_in_adam",
        "FM 'Harbour Island' is a small island settlement inside "
        "ADAM 'North Eleuthera' coverage area. IoU 0.027 reflects "
        "size asymmetry. ADAM North Eleuthera covers multiple FM "
        "Eleuthera-area polygons.",
    ),
    (
        "BHS", "BHS-20201113-20", 901515.0, "fm_in_adam",
        "FM 'Moore's Island' is a small island within ADAM 'South "
        "Abaco' coverage area. IoU 0.019 reflects size asymmetry.",
    ),
    (
        "BHS", "BHS-20201113-31", 901513.0, "fm_in_adam",
        "FM 'Spanish Wells' is a small settlement on an island inside "
        "ADAM 'North Eleuthera' coverage area. IoU 0.018 reflects size "
        "asymmetry. Sibling FMs sharing ADAM North Eleuthera include "
        "FM North Eleuthera and FM Harbour Island.",
    ),
    (
        "UMI", "UMI_5-20250729", 902605.0, "match",
        "FM 'Kingman Reef' ↔ ADAM 'Kingman Reef' is a clean name match "
        "and sole partner. IoU 0.019 reflects the tiny size of the "
        "reef (geometry-driven, not topology-driven). Treat as 1:1.",
    ),
    (
        "VGB", "VGB-20200401-4", 39506.0, "fm_in_adam",
        "FM 'Great Camanoe' is a small island within ADAM 'Tortola' "
        "coverage area. IoU 0.045 reflects size asymmetry. Multiple "
        "FM polygons share ADAM Tortola.",
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

    xw = pd.read_csv(args.csv)
    logger.info("Loaded %d rows from %s", len(xw), args.csv)
    # `note` is read as float64 when the column is empty. Coerce to
    # object so string assignment works.
    for col in ("status", "classification_type", "note"):
        if xw[col].dtype != object:
            xw[col] = xw[col].astype(object)
    xw["note"] = xw["note"].fillna("")

    applied = 0
    skipped_not_spatial = 0
    not_found = []
    for iso3, fm_pcode, adam_id, new_status, note in EDITS:
        mask = (
            (xw["iso3"] == iso3)
            & (xw["fm_pcode"] == fm_pcode)
            & (xw["adam_admin_id"] == adam_id)
        )
        n = mask.sum()
        if n == 0:
            not_found.append((iso3, fm_pcode, adam_id))
            continue
        if n > 1:
            logger.warning(
                "Edit key matched %d rows: %s/%s/%s — skipping",
                n, iso3, fm_pcode, adam_id,
            )
            continue
        idx = xw.index[mask][0]
        current_cls = xw.at[idx, "classification_type"]
        if current_cls != "spatial":
            skipped_not_spatial += 1
            logger.info(
                "Skipping %s/%s/%s — already classification_type=%s",
                iso3, fm_pcode, adam_id, current_cls,
            )
            continue
        if new_status:
            xw.at[idx, "status"] = new_status
        xw.at[idx, "classification_type"] = "llm"
        xw.at[idx, "note"] = note
        applied += 1

    # ── Resolve needs_manual_mapping countries by name match ────────
    # For each FM polygon, pick best-name-match ADAM partner above
    # noise (or sole partner). Promote to match or fm_in_adam based on
    # whether multiple FMs share the same ADAM as their best partner.
    # Drop other above-noise rows. Set below-noise rows to `noise`.
    resolved = 0
    for iso3 in LLM_RESOLVE_COUNTRIES:
        country = xw[(xw["iso3"] == iso3) & (xw["row_kind"] == "overlap")]
        country_spatial = country[country["classification_type"] == "spatial"]
        if len(country_spatial) == 0:
            continue
        above = country_spatial[country_spatial["iou"] >= NOISE_IOU]

        # For each FM, pick best-name-match ADAM among above-noise.
        # If only one above-noise candidate, it's the primary by default
        # (high IoU + sole candidate = clearly the match, even if name
        # disagrees, e.g. TTO Couva-Tabaquite-Talparo ↔ Couva).
        primary_per_fm: dict[str, tuple[float, float]] = {}  # fm_pcode -> (adam_id, score)
        for fm_pcode, g in above.groupby("fm_pcode"):
            fm_name = g.iloc[0]["fm_name"]
            scored = [
                (r["adam_admin_id"], name_match_score(fm_name, r["adam_admin_name"]))
                for _, r in g.iterrows()
            ]
            scored.sort(key=lambda x: x[1], reverse=True)
            primary_per_fm[fm_pcode] = scored[0]

        # Count how many FMs picked each ADAM as their primary
        # (→ fm_in_adam if shared by multiple FMs).
        fms_per_primary_adam: dict[float, list[str]] = {}
        for fm_pcode, (adam_id, _) in primary_per_fm.items():
            fms_per_primary_adam.setdefault(adam_id, []).append(fm_pcode)

        # Apply edits.
        for idx in country_spatial.index:
            r = xw.loc[idx]
            fm_pcode = r["fm_pcode"]
            adam_id = r["adam_admin_id"]
            iou = r["iou"]
            primary = primary_per_fm.get(fm_pcode)

            if iou < NOISE_IOU:
                # Below-noise rows are already labeled `noise` by the
                # build script (policy override no longer applies below
                # the noise threshold). No LLM decision needed — leave
                # them spatial.
                continue

            if primary is None or adam_id != primary[0]:
                # Above-noise spillover — drop
                primary_name = ""
                if primary is not None:
                    primary_name = above[above["adam_admin_id"] == primary[0]].iloc[0]["adam_admin_name"]
                xw.at[idx, "status"] = "drop"
                xw.at[idx, "classification_type"] = "llm"
                xw.at[idx, "note"] = (
                    f"Above-noise boundary spillover (IoU {iou:.3f}). "
                    f"FM '{r['fm_name']}' canonical match is ADAM "
                    f"'{primary_name}' by name."
                )
                resolved += 1
                continue

            # This is the primary
            score = primary[1]
            shared_fms = fms_per_primary_adam[adam_id]
            if len(shared_fms) == 1:
                xw.at[idx, "status"] = "match"
                xw.at[idx, "classification_type"] = "llm"
                xw.at[idx, "note"] = (
                    f"LLM name-match resolution: FM '{r['fm_name']}' ↔ "
                    f"ADAM '{r['adam_admin_name']}' "
                    f"(name-score {score:.2f}, IoU {iou:.2f}). "
                    f"Country was needs_manual_mapping; this row is a "
                    f"clean 1:1 correspondence."
                )
            else:
                # Multiple FMs share this ADAM as primary → fm_in_adam
                others = [p for p in shared_fms if p != fm_pcode]
                xw.at[idx, "status"] = "fm_in_adam"
                xw.at[idx, "classification_type"] = "llm"
                xw.at[idx, "note"] = (
                    f"LLM name-match resolution: FM '{r['fm_name']}' is "
                    f"one of {len(shared_fms)} FM polygons sharing ADAM "
                    f"'{r['adam_admin_name']}' (others: "
                    f"{', '.join(str(p) for p in others)}). "
                    f"ADAM is coarser than FM for this region."
                )
            resolved += 1

    logger.info("Resolved %d rows across %s",
                resolved, LLM_RESOLVE_COUNTRIES)

    # ── Cascade promotions ──────────────────────────────────────────
    # Dropping a spillover row can collapse the topology count for the
    # ADAM/FM partner that survived. E.g. dropping Inagua×UNA leaves
    # UNA with only 1 above-noise FM partner (New Providence), so
    # New Providence×UNA should now be `match` not `fm_in_adam`.
    # Auto-detect and promote.
    NOISE = 0.05
    overlap = xw[xw["row_kind"] == "overlap"]
    counted = overlap[
        (overlap["iou"] >= NOISE) & (overlap["status"] != "drop")
    ]
    fm_count = counted.groupby(["iso3", "fm_pcode"]).size().to_dict()
    aa_count = counted.groupby(["iso3", "adam_admin_id"]).size().to_dict()

    def expected(r) -> str:
        if r["iou"] < NOISE:
            return "noise"
        nf = fm_count.get((r["iso3"], r["fm_pcode"]), 0)
        na = aa_count.get((r["iso3"], r["adam_admin_id"]), 0)
        if nf == 1 and na == 1:
            return "match"
        if nf > 1 and na == 1:
            return "adam_in_fm"
        if nf == 1 and na > 1:
            return "fm_in_adam"
        return "fragmented"

    cascaded = 0
    for idx in xw.index:
        r = xw.loc[idx]
        # Only cascade spatial overlap rows that aren't already drop/noise
        if r["row_kind"] != "overlap":
            continue
        if r["classification_type"] != "spatial":
            continue
        if r["status"] in ("drop", "noise", "needs_review"):
            continue
        # Skip rows under a policy override (drop / needs_review applied)
        if isinstance(r["policy"], str) and r["policy"]:
            continue
        exp = expected(r)
        if r["status"] != exp:
            old_status = r["status"]
            xw.at[idx, "status"] = exp
            xw.at[idx, "classification_type"] = "llm"
            xw.at[idx, "note"] = (
                f"Cascade: {old_status} → {exp} after LLM-pass drops "
                f"removed the partner that justified the previous "
                f"topology label. Counts are now ({fm_count.get((r['iso3'], r['fm_pcode']), 0)} "
                f"above-noise ADAMs in this FM, "
                f"{aa_count.get((r['iso3'], r['adam_admin_id']), 0)} "
                f"above-noise FMs in this ADAM)."
            )
            cascaded += 1

    xw.to_csv(args.csv, index=False)
    logger.info(
        "Applied %d primary edits, %d cascade promotions, "
        "skipped %d non-spatial rows, %d not found",
        applied, cascaded, skipped_not_spatial, len(not_found),
    )
    if not_found:
        logger.warning("Not found in CSV: %s", not_found)
    return 0


if __name__ == "__main__":
    sys.exit(main())
