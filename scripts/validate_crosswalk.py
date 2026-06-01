"""Validate `data/adam_fm_crosswalk.csv` after reviewer edits.

Checks:
  * `status` values are in the allowed vocabulary
  * `classification_type` values are in {spatial, llm, human}
  * Rows marked `classification_type=human` actually differ from what
    the spatial heuristic would produce (catches "I forgot to change
    something" mistakes)
  * Topological status agrees with the FM↔ADAM multiplicity counts
    (catches typos like `match` set on a row whose FM has 3 ADAMs)

Outputs:
  * stdout: pass/warn/fail summary, per-issue listings
  * `data/adam_fm_review_queue.csv`: only the rows the reviewer should
    focus on — anything `fragmented`, `needs_review`, anything LLM
    touched, plus any validation warnings

Run from repo root::

    uv run python scripts/validate_crosswalk.py
"""

import argparse
import logging
import sys
from pathlib import Path

import coloredlogs
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_IN = REPO_ROOT / "data" / "adam_fm_crosswalk.csv"
DEFAULT_QUEUE = REPO_ROOT / "data" / "adam_fm_review_queue.csv"

NOISE_IOU = 0.05

VALID_STATUS = {
    "match", "adam_in_fm", "fm_in_adam", "fragmented",
    "noise", "drop", "keep", "needs_review",
    "fm_only", "adam_only",
}
VALID_CLASSIFICATION = {"spatial", "llm", "human"}

logger = logging.getLogger(__name__)


def expected_topological_status(
    iou: float, n_adams_in_fm: int, n_fms_in_adam: int,
) -> str:
    """Return the topological status a row *would* get from the spatial
    heuristic alone (ignoring policy overrides and reviewer edits)."""
    if iou < NOISE_IOU:
        return "noise"
    if n_adams_in_fm == 1 and n_fms_in_adam == 1:
        return "match"
    if n_adams_in_fm > 1 and n_fms_in_adam == 1:
        return "adam_in_fm"
    if n_adams_in_fm == 1 and n_fms_in_adam > 1:
        return "fm_in_adam"
    return "fragmented"


def main() -> int:
    coloredlogs.install(
        level="INFO",
        fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--in", dest="in_path", type=Path, default=DEFAULT_IN)
    ap.add_argument("--queue", type=Path, default=DEFAULT_QUEUE)
    args = ap.parse_args()

    xw = pd.read_csv(args.in_path)
    logger.info("Loaded %d rows from %s", len(xw), args.in_path)

    issues: list[dict] = []
    # idx -> list of reasons that row landed in the queue
    queue_reasons: dict[int, list[str]] = {}

    def flag(idx: int, reason: str) -> None:
        queue_reasons.setdefault(idx, []).append(reason)

    # --- Check 1: valid status values ---
    bad_status = xw[~xw["status"].isin(VALID_STATUS)]
    if len(bad_status):
        logger.error(
            "%d rows have invalid `status` (allowed: %s)",
            len(bad_status), sorted(VALID_STATUS),
        )
        for idx, r in bad_status.iterrows():
            issues.append({
                "iso3": r["iso3"], "fm_pcode": r["fm_pcode"],
                "adam_admin_id": r["adam_admin_id"],
                "issue": f"invalid status '{r['status']}'",
            })
            flag(idx, "invalid_status")

    # --- Check 2: valid classification_type values ---
    bad_class = xw[~xw["classification_type"].isin(VALID_CLASSIFICATION)]
    if len(bad_class):
        logger.error(
            "%d rows have invalid `classification_type` (allowed: %s)",
            len(bad_class), sorted(VALID_CLASSIFICATION),
        )
        for idx, r in bad_class.iterrows():
            issues.append({
                "iso3": r["iso3"], "fm_pcode": r["fm_pcode"],
                "adam_admin_id": r["adam_admin_id"],
                "issue": f"invalid classification_type "
                         f"'{r['classification_type']}'",
            })
            flag(idx, "invalid_classification_type")

    # --- Check 3: topology agrees with counts (overlap rows only) ---
    overlap = xw[xw["row_kind"] == "overlap"].copy()
    # Counts only consider above-noise overlaps, matching the build script
    above_noise = overlap[overlap["iou"] >= NOISE_IOU]
    fm_count = (
        above_noise.groupby(["iso3", "fm_pcode"]).size().to_dict()
    )
    aa_count = (
        above_noise.groupby(["iso3", "adam_admin_id"]).size().to_dict()
    )

    topology_mismatches = 0
    for idx, r in overlap.iterrows():
        # Skip rows that are policy-overridden or reviewer-locked
        if r["classification_type"] in {"human", "llm"}:
            continue
        if r["status"] in {"drop", "keep", "needs_review", "noise"}:
            continue
        nf = fm_count.get((r["iso3"], r["fm_pcode"]), 0)
        na = aa_count.get((r["iso3"], r["adam_admin_id"]), 0)
        expected = expected_topological_status(r["iou"], nf, na)
        if r["status"] != expected:
            topology_mismatches += 1
            issues.append({
                "iso3": r["iso3"], "fm_pcode": r["fm_pcode"],
                "adam_admin_id": r["adam_admin_id"],
                "issue": f"status={r['status']} but counts ({nf} ADAMs, "
                         f"{na} FMs) suggest {expected}",
            })
            flag(idx, "topology_mismatch")
    if topology_mismatches:
        logger.warning(
            "%d spatial rows disagree with topology counts",
            topology_mismatches,
        )

    # --- Check 4: human-classified rows that match the spatial default ---
    suspect_human = 0
    for idx, r in xw[xw["classification_type"] == "human"].iterrows():
        if r["row_kind"] != "overlap":
            continue
        nf = fm_count.get((r["iso3"], r["fm_pcode"]), 0)
        na = aa_count.get((r["iso3"], r["adam_admin_id"]), 0)
        expected = expected_topological_status(r["iou"], nf, na)
        if r["status"] == expected:
            suspect_human += 1
            issues.append({
                "iso3": r["iso3"], "fm_pcode": r["fm_pcode"],
                "adam_admin_id": r["adam_admin_id"],
                "issue": "marked human but status unchanged from spatial "
                         "default — did you forget to change something?",
            })
            flag(idx, "suspect_human_unchanged")
    if suspect_human:
        logger.warning(
            "%d human-classified rows match the spatial default",
            suspect_human,
        )

    # --- Build the review queue ---
    # Reasons (sortable in Excel by queue_reason):
    #   fragmented            — topology says many-to-many, needs judgment
    #   policy_needs_review   — country-level policy = needs_manual_mapping
    #   llm_touched           — Claude reviewed this row
    #   human_touched         — reviewer-locked (for awareness, not action)
    #   suspect_human_*       — validator-flagged suspicious human edits
    #   topology_mismatch     — status disagrees with counts
    #   invalid_status / invalid_classification_type
    for idx, r in xw.iterrows():
        if r["status"] == "fragmented":
            flag(idx, "fragmented")
        if (r["status"] == "needs_review"
                and r["classification_type"] == "spatial"):
            flag(idx, "policy_needs_review")
        if r["classification_type"] == "llm":
            flag(idx, "llm_touched")
        if r["classification_type"] == "human":
            flag(idx, "human_touched")

    queue = xw.loc[sorted(queue_reasons.keys())].copy()
    queue["queue_reason"] = queue.index.map(
        lambda i: "; ".join(queue_reasons.get(i, []))
    )
    # Put queue_reason as the first column for easy sorting
    queue = queue[["queue_reason"] + [c for c in queue.columns
                                       if c != "queue_reason"]]

    args.queue.parent.mkdir(parents=True, exist_ok=True)
    queue.to_csv(args.queue, index=False)

    # --- Summary ---
    status_counts = xw["status"].value_counts().to_dict()
    class_counts = xw["classification_type"].value_counts().to_dict()
    n_reviewed = class_counts.get("human", 0) + class_counts.get("llm", 0)

    print()
    print("=" * 64)
    print(f"Crosswalk:  {len(xw)} rows across {xw['iso3'].nunique()} iso3s")
    print(f"Reviewed:   {n_reviewed} rows "
          f"(human={class_counts.get('human', 0)}, "
          f"llm={class_counts.get('llm', 0)})")
    print(f"Spatial:    {class_counts.get('spatial', 0)} rows")
    print()
    print("Status counts:")
    for k, v in sorted(status_counts.items(), key=lambda x: -x[1]):
        print(f"  {k:<14} {v:>6}")
    print()
    print(f"Issues found: {len(issues)}")
    print(f"Review queue: {len(queue)} rows → {args.queue}")
    # Per-reason counts so the reviewer knows what's in there
    reason_counts: dict[str, int] = {}
    for reasons in queue_reasons.values():
        for r in reasons:
            reason_counts[r] = reason_counts.get(r, 0) + 1
    if reason_counts:
        print("  Queue reasons:")
        for k, v in sorted(reason_counts.items(), key=lambda x: -x[1]):
            print(f"    {k:<28} {v:>6}")
    print("=" * 64)

    return 1 if any("invalid" in i["issue"] for i in issues) else 0


if __name__ == "__main__":
    sys.exit(main())
