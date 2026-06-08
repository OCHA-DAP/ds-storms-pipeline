"""Validate a human-reviewed FM crosswalk before it builds a prod lookup.

Works for both sources (ADAM and GDACS); the join-key column, status
vocabulary and fm-only policy set differ, so they're parameterized per
source below. By default it validates the *humanreview* CSV — the same
input the v2 builders consume — so the FM-uniqueness/coverage checks run
on what actually builds `storms.{adam,gdacs}_fm_lookup`.

Checks:
  * `status` values are in the allowed (per-source) vocabulary
  * `classification_type` values are in {spatial, llm, human}
  * Rows marked `classification_type=human` actually differ from what
    the spatial heuristic would produce (catches "I forgot to change
    something" mistakes)
  * Topological status agrees with the FM↔source multiplicity counts
  * FM uniqueness: each FM polygon resolves to exactly one definitive
    label kind
  * FM coverage: every FM produces a lookup row (definitive label, or a
    country-level policy that waives the source)

Blocking violations (non-zero exit, used to gate the builders'
`--write-db`): invalid status/classification, FM-uniqueness, FM-coverage.
Topology mismatches and suspect human edits are advisory (review queue).

Outputs:
  * stdout: pass/warn/fail summary, per-issue listings
  * `data/{source}_fm_review_queue.csv`: rows the reviewer should focus on

Run from repo root::

    uv run python scripts/validate_crosswalk.py --source adam
    uv run python scripts/validate_crosswalk.py --source gdacs
"""

import argparse
import logging
import sys
from pathlib import Path

import coloredlogs
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
REVIEW_DIR = REPO_ROOT / "data" / "review"

NOISE_IOU = 0.05

VALID_CLASSIFICATION = {"spatial", "llm", "human"}

# Status values valid for any source, regardless of the source-specific
# directional labels below.
BASE_STATUS = {"match", "drop", "fm_only", "noise", "fragmented", "needs_review"}

# Per-source configuration. `in_fm`/`fm_in` are the directional aggregation
# labels; `fm_only_policies` mirrors each builder's SUPPRESS_ADM1 ∪
# {fm_adm1_only} so coverage validation agrees with what the builder emits.
SOURCE_SPEC = {
    "adam": {
        "join_key": "adam_admin_id",
        "in_fm": "adam_in_fm",
        "fm_in": "fm_in_adam",
        "source_only": "adam_only",
        "fm_only_policies": {"country_only", "no_adam_source", "fm_adm1_only"},
        "humrev": REVIEW_DIR / "adam_fm_crosswalk_humanreview.csv",
    },
    "gdacs": {
        "join_key": "gmi_admin",
        "in_fm": "gdacs_in_fm",
        "fm_in": "fm_in_gdacs",
        "source_only": "gdacs_only",
        "fm_only_policies": {"country_only", "no_fm_source", "fm_adm1_only"},
        "humrev": REVIEW_DIR / "gdacs_fm_crosswalk_humanreview.csv",
    },
}

logger = logging.getLogger(__name__)


def expected_topological_status(
    iou: float,
    n_src_in_fm: int,
    n_fm_in_src: int,
    in_fm: str,
    fm_in: str,
) -> str:
    """The topological status a row *would* get from the spatial heuristic
    alone (ignoring policy overrides and reviewer edits)."""
    if iou < NOISE_IOU:
        return "noise"
    if n_src_in_fm == 1 and n_fm_in_src == 1:
        return "match"
    if n_src_in_fm > 1 and n_fm_in_src == 1:
        return in_fm
    if n_src_in_fm == 1 and n_fm_in_src > 1:
        return fm_in
    return "fragmented"


def validate(in_path: Path, source: str, queue_path: Path | None = None) -> int:
    """Validate one crosswalk CSV. Returns the count of *blocking*
    violations (0 == clean). Blocking = invalid status/classification +
    FM-uniqueness + FM-coverage. Importable so the builders can gate on it.
    """
    spec = SOURCE_SPEC[source]
    jk = spec["join_key"]
    in_fm, fm_in = spec["in_fm"], spec["fm_in"]
    valid_status = BASE_STATUS | {in_fm, fm_in, spec["source_only"]}
    fm_only_policies = spec["fm_only_policies"]
    if queue_path is None:
        queue_path = REPO_ROOT / "data" / f"{source}_fm_review_queue.csv"

    # utf-8-sig: the humanreview CSVs carry a BOM.
    xw = pd.read_csv(in_path, encoding="utf-8-sig")
    # Every real crosswalk row is country-scoped, so a row with no iso3 is
    # not an entry — trailing blank lines and stray reviewer notes left in
    # spare cells. Drop them (logged, not silent); rows missing only a
    # status are kept and still flagged below.
    n_no_iso3 = int(xw["iso3"].isna().sum())
    if n_no_iso3:
        logger.warning("Dropping %d row(s) with no iso3 (blank/stray notes)",
                       n_no_iso3)
        xw = xw[xw["iso3"].notna()].reset_index(drop=True)
    logger.info("Loaded %d rows from %s (source=%s)", len(xw), in_path, source)

    issues: list[dict] = []
    queue_reasons: dict[int, list[str]] = {}

    def flag(idx: int, reason: str) -> None:
        queue_reasons.setdefault(idx, []).append(reason)

    def add_issue(iso3, fm_pcode, src_id, text_) -> None:
        issues.append({
            "iso3": iso3, "fm_pcode": fm_pcode,
            "source_admin_id": src_id, "issue": text_,
        })

    invalid_violations = 0

    # --- Check 1: valid status values ---
    bad_status = xw[~xw["status"].isin(valid_status)]
    if len(bad_status):
        invalid_violations += len(bad_status)
        logger.error(
            "%d rows have invalid `status` (allowed: %s)",
            len(bad_status), sorted(valid_status),
        )
        for idx, r in bad_status.iterrows():
            add_issue(r["iso3"], r["fm_pcode"], r[jk],
                      f"invalid status '{r['status']}'")
            flag(idx, "invalid_status")

    # --- Check 2: valid classification_type values ---
    bad_class = xw[~xw["classification_type"].isin(VALID_CLASSIFICATION)]
    if len(bad_class):
        invalid_violations += len(bad_class)
        logger.error(
            "%d rows have invalid `classification_type` (allowed: %s)",
            len(bad_class), sorted(VALID_CLASSIFICATION),
        )
        for idx, r in bad_class.iterrows():
            add_issue(r["iso3"], r["fm_pcode"], r[jk],
                      f"invalid classification_type "
                      f"'{r['classification_type']}'")
            flag(idx, "invalid_classification_type")

    # --- Check 3: topology agrees with counts (overlap rows only) ---
    overlap = xw[xw["row_kind"] == "overlap"].copy()
    above_noise = overlap[overlap["iou"] >= NOISE_IOU]
    fm_count = above_noise.groupby(["iso3", "fm_pcode"]).size().to_dict()
    aa_count = above_noise.groupby(["iso3", jk]).size().to_dict()

    topology_mismatches = 0
    for idx, r in overlap.iterrows():
        if r["classification_type"] in {"human", "llm"}:
            continue
        if r["status"] in {"drop", "keep", "needs_review", "noise"}:
            continue
        nf = fm_count.get((r["iso3"], r["fm_pcode"]), 0)
        na = aa_count.get((r["iso3"], r[jk]), 0)
        expected = expected_topological_status(r["iou"], nf, na, in_fm, fm_in)
        if r["status"] != expected:
            topology_mismatches += 1
            add_issue(r["iso3"], r["fm_pcode"], r[jk],
                      f"status={r['status']} but counts ({nf} src, "
                      f"{na} FMs) suggest {expected}")
            flag(idx, "topology_mismatch")
    if topology_mismatches:
        logger.warning(
            "%d spatial rows disagree with topology counts",
            topology_mismatches,
        )

    # --- Check 3.5: FM uniqueness + coverage invariants ---
    uniqueness_violations = 0
    coverage_violations = 0
    for (iso3, fm_pcode), g in xw[
        xw["row_kind"].isin(["overlap", "fm_only"])
    ].groupby(["iso3", "fm_pcode"]):
        statuses = g["status"].value_counts().to_dict()
        n_match = statuses.get("match", 0)
        n_in_fm = statuses.get(in_fm, 0)
        n_fm_in = statuses.get(fm_in, 0)
        n_fragmented = statuses.get("fragmented", 0)
        n_fm_only = statuses.get("fm_only", 0)
        definitive_types = [
            t for t, n in [("match", n_match), (in_fm, n_in_fm),
                           (fm_in, n_fm_in)] if n > 0
        ]
        has_definitive = n_match + n_in_fm + n_fm_in + n_fm_only > 0
        policy = g.iloc[0]["policy"]
        problem = None
        if n_fragmented > 0:
            problem = f"{n_fragmented} fragmented row(s) unresolved"
        elif len(definitive_types) > 1:
            problem = (f"mixed definitive labels: {n_match}m + "
                       f"{n_in_fm}{in_fm} + {n_fm_in}{fm_in}")
        elif n_match > 1:
            problem = f"{n_match} match rows (expected exactly 1)"
        elif n_fm_in > 1:
            problem = f"{n_fm_in} {fm_in} rows (expected 1)"
        if problem:
            uniqueness_violations += 1
            add_issue(iso3, fm_pcode, None, f"FM uniqueness: {problem}")
            for idx in g.index:
                flag(idx, "fm_uniqueness")
        elif not has_definitive:
            policy_ok = isinstance(policy, str) and policy in fm_only_policies
            if not policy_ok:
                coverage_violations += 1
                add_issue(iso3, fm_pcode, None, (
                    f"FM coverage: no definitive resolution and "
                    f"policy={policy!r} doesn't waive the source. This FM "
                    f"would silently disappear. Statuses present: {statuses}."
                ))
                for idx in g.index:
                    flag(idx, "fm_coverage")
    if uniqueness_violations:
        logger.warning("%d FM polygons violate uniqueness invariant",
                       uniqueness_violations)
    if coverage_violations:
        logger.warning(
            "%d FM polygons would silently disappear from the lookup",
            coverage_violations,
        )

    # --- Check 4: human-classified rows that match the spatial default ---
    suspect_human = 0
    for idx, r in xw[xw["classification_type"] == "human"].iterrows():
        if r["row_kind"] != "overlap":
            continue
        nf = fm_count.get((r["iso3"], r["fm_pcode"]), 0)
        na = aa_count.get((r["iso3"], r[jk]), 0)
        expected = expected_topological_status(r["iou"], nf, na, in_fm, fm_in)
        if r["status"] == expected:
            suspect_human += 1
            add_issue(r["iso3"], r["fm_pcode"], r[jk],
                      "marked human but status unchanged from spatial "
                      "default — did you forget to change something?")
            flag(idx, "suspect_human_unchanged")
    if suspect_human:
        logger.warning("%d human-classified rows match the spatial default",
                       suspect_human)

    # --- Build the review queue ---
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
    queue = queue[["queue_reason"] + [c for c in queue.columns
                                      if c != "queue_reason"]]
    queue_path.parent.mkdir(parents=True, exist_ok=True)
    queue.to_csv(queue_path, index=False)

    # --- Summary ---
    status_counts = xw["status"].value_counts().to_dict()
    class_counts = xw["classification_type"].value_counts().to_dict()
    n_reviewed = class_counts.get("human", 0) + class_counts.get("llm", 0)
    blocking = invalid_violations + uniqueness_violations + coverage_violations

    print()
    print("=" * 64)
    print(f"Source:     {source}")
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
    print(f"Issues found:        {len(issues)}")
    print(f"  invalid:           {invalid_violations}")
    print(f"  fm_uniqueness:     {uniqueness_violations}")
    print(f"  fm_coverage:       {coverage_violations}")
    print(f"  (advisory) topo:   {topology_mismatches}")
    print(f"BLOCKING violations: {blocking}")
    print(f"Review queue: {len(queue)} rows → {queue_path}")
    print("=" * 64)

    return blocking


def main() -> int:
    coloredlogs.install(
        level="INFO",
        fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--source", choices=sorted(SOURCE_SPEC), required=True)
    ap.add_argument("--in", dest="in_path", type=Path, default=None,
                    help="crosswalk CSV (default: the source's humanreview)")
    ap.add_argument("--queue", type=Path, default=None)
    args = ap.parse_args()

    in_path = args.in_path or SOURCE_SPEC[args.source]["humrev"]
    blocking = validate(in_path, args.source, args.queue)
    return 1 if blocking else 0


if __name__ == "__main__":
    sys.exit(main())
