"""ONE-SHOT migration: humrev xlsx → clean humanreview csv.

History
-------
The original crosswalk review file was saved as
`data/review/adam_fm_lookup_humrev.xlsx`. When the file was opened in
Excel-on-Mac, Excel imported the UTF-8 bytes as Mac Roman, introducing
mojibake (Estelí → Estel√≠, Bragança → BraganÃ\\x87A, etc.) in the
text columns. The xlsx was then saved with the corrupted strings.

This script produces a clean CSV that becomes the new decision
document going forward. The xlsx is left untouched as a historical
artifact.

What it does
------------
1. Read `data/review/adam_fm_lookup_humrev.xlsx`
   (contains: reviewer decisions in clean form + corrupted text)
2. Read `data/adam_fm_crosswalk.csv`
   (contains: clean text from the build pipeline)
3. Apply ftfy.fix_text to user-typed text columns of humrev
   (caveat, note, internal_note) — ftfy is the mature, deterministic
   Python equivalent of R's stringi::stri_enc_toutf8.
4. Merge: take fm_name and adam_admin_name from the clean source CSV
   (joining on the encoding-immune keys iso3, fm_pcode, adam_admin_id);
   take everything else from humrev.
5. Write to `data/review/adam_fm_crosswalk_humanreview.csv` (gitignored).

After this runs, `build_adam_fm_lookup_v2.py` reads the new
humanreview.csv directly. No rejoin step needed in the regular
pipeline.

DO NOT RE-RUN after the new humanreview.csv is established as the
source of truth — re-running would clobber subsequent reviewer edits
to that CSV.

Run from repo root::

    uv run --with ftfy --with openpyxl python scripts/_oneshot/migrate_humrev_xlsx_to_csv.py
"""

import argparse
import logging
import sys
from pathlib import Path

import coloredlogs
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_XLSX = REPO_ROOT / "data" / "review" / "adam_fm_lookup_humrev.xlsx"
DEFAULT_SOURCE = REPO_ROOT / "data" / "adam_fm_crosswalk.csv"
DEFAULT_OUT = REPO_ROOT / "data" / "review" / "adam_fm_crosswalk_humanreview.csv"

# Decision columns: kept verbatim from the humrev xlsx (with ftfy
# applied to the free-text ones below).
HUMREV_DECISION_COLS = (
    "policy", "status", "classification_type",
    "caveat", "note", "internal_note",
)
# Free-text columns where mojibake from Excel-on-Mac may have crept
# in. ftfy is applied as a best-effort repair.
USER_TEXT_COLS = ("caveat", "note", "internal_note")
# Join key — encoding-immune (ASCII pcodes, integer ids).
JOIN_KEY = ["iso3", "fm_pcode", "adam_admin_id"]

logger = logging.getLogger(__name__)


def main() -> int:
    coloredlogs.install(
        level="INFO",
        fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--xlsx", type=Path, default=DEFAULT_XLSX,
                    help="path to corrupted humrev xlsx")
    ap.add_argument("--source", type=Path, default=DEFAULT_SOURCE,
                    help="path to clean source crosswalk CSV")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT,
                    help="path for the new humanreview csv")
    args = ap.parse_args()

    if args.out.exists():
        logger.error(
            "Output %s already exists. Refusing to overwrite — "
            "this is a one-shot migration. If you really want to "
            "re-run, delete the output file first.", args.out,
        )
        return 1

    import ftfy

    logger.info("Loading xlsx %s", args.xlsx)
    humrev = pd.read_excel(args.xlsx)
    logger.info("  %d rows from xlsx", len(humrev))

    logger.info("Loading source CSV %s", args.source)
    source = pd.read_csv(args.source)
    logger.info("  %d rows from source", len(source))

    # ── Step 1: ftfy on user-typed text columns in humrev ──────────
    n_repaired = 0
    n_residual = 0
    for col in USER_TEXT_COLS:
        if col not in humrev.columns:
            continue
        before = humrev[col].astype(object)
        humrev[col] = humrev[col].apply(
            lambda v: ftfy.fix_text(v) if isinstance(v, str) else v
        )
        after = humrev[col].astype(object)
        changed = (
            (before != after) & before.notna() & after.notna()
        )
        n_repaired += int(changed.sum())
        # Detect any leftover mojibake markers
        residual_mask = humrev[col].apply(
            lambda v: isinstance(v, str)
            and any(m in v for m in ("√", "≠", "Ã", "â€"))
        )
        n_residual += int(residual_mask.sum())
    logger.info(
        "ftfy: repaired %d user-text cells; %d residuals need "
        "manual review",
        n_repaired, n_residual,
    )

    # ── Step 2: merge humrev decisions with source's clean names ───
    # Encode NaN keys to a sentinel so pandas merge can match
    # NaN-to-NaN (which it normally won't).
    SENTINEL = "__NA_KEY__"
    for k in JOIN_KEY:
        humrev[k] = humrev[k].astype(object).where(
            humrev[k].notna(), SENTINEL,
        )
        source[k] = source[k].astype(object).where(
            source[k].notna(), SENTINEL,
        )

    # Drop the corrupted name columns from humrev; we'll replace them
    # with the source's clean versions via the merge.
    humrev_no_names = humrev.drop(
        columns=[c for c in ("fm_name", "adam_admin_name")
                 if c in humrev.columns],
    )
    merged = humrev_no_names.merge(
        source[JOIN_KEY + ["fm_name", "adam_admin_name"]],
        on=JOIN_KEY, how="left",
    )

    if len(merged) != len(humrev):
        logger.warning(
            "Row count changed during merge: %d → %d. Some humrev "
            "rows may not have matched the source crosswalk.",
            len(humrev), len(merged),
        )

    # Restore NaN keys
    for k in JOIN_KEY:
        merged[k] = merged[k].where(merged[k] != SENTINEL, None)

    # Keep humrev's original column order, with the rejoined name
    # columns inserted at their original positions.
    original_cols = list(humrev.columns)
    # All original cols exist in merged now (names came back via merge)
    final_cols = [c for c in original_cols if c in merged.columns]
    merged = merged[final_cols]

    args.out.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(args.out, index=False)
    logger.info("Wrote %d rows to %s", len(merged), args.out)
    logger.info(
        "Going forward, edit %s directly (NEVER open in Excel — use a "
        "text editor or import via Data → Get External Data with "
        "UTF-8). The xlsx at %s is left untouched as a historical "
        "artifact.",
        args.out, args.xlsx,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
