"""DBX dispatcher for the storms pipeline.

Two responsibilities, both DBX-specific glue:

1. Turn the seven positional parameters into one or more
   ``python run_pipeline.py …`` invocations. ``run_pipeline.py`` and the
   pipeline code itself stay pure Python / argparse — they don't know
   about DBX.

2. For the realtime cascade only, hand the ``track_issued_time`` and
   ``wsp_issued_time`` from the ``nhc`` (ETL) stage to downstream tasks
   via ``dbutils.jobs.taskValues``. Each downstream task receives the
   value as positional arg 3 (``issued_time``) from the bundle template
   (``{{tasks.etl.values.track_issued_time}}``), so no DB queries needed.

Positional contract (mirrors the job-level ``parameters:`` in
databricks.yml):
    sys.argv[1] = subcommand     # one of run_pipeline.py's subcommands,
                                 # or a "realtime-…" composite (see below)
    sys.argv[2] = mode           # "dev" | "prod"
    sys.argv[3] = issued_time    # YYYY-MM-DDTHH or ""
    sys.argv[4] = since          # YYYY-MM-DD or "" (inclusive lower)
    sys.argv[5] = until          # YYYY-MM-DD or "" (exclusive upper)
    sys.argv[6] = overwrite      # "true" or ""
    sys.argv[7] = fill_nulls     # "true" or ""
    sys.argv[8] = subcommand_override  # non-empty overrides argv[1]
    sys.argv[9] = sample_json    # URL for test-mode CurrentStorms JSON, or ""

Composite ``realtime-…`` subcommands expand to multiple
run_pipeline.py invocations — used by the DBX task chain so each task
maps to a logical stage instead of a single CLI command. To run the
same work locally, just call ``python run_pipeline.py nhc-realtime``,
which chains every stage in one Python process.
"""

import json
import os
import subprocess
import sys


def _find_script_dir():
    """DBX's spark_python_task exec context doesn't always define
    __file__. Walk through the usual fallbacks."""
    try:
        return os.path.dirname(os.path.abspath(__file__))  # noqa: F821
    except NameError:
        pass
    if sys.argv and sys.argv[0]:
        return os.path.dirname(os.path.abspath(sys.argv[0]))
    return os.getcwd()


SCRIPT_DIR = _find_script_dir()
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))


def _arg(i, default=""):
    return sys.argv[i] if len(sys.argv) > i else default


SUBCOMMAND_DEFAULT = _arg(1, "nhc-realtime")
MODE = _arg(2, "prod")
ISSUED_TIME = _arg(3)
SINCE = _arg(4)
UNTIL = _arg(5)
OVERWRITE = _arg(6)
FILL_NULLS = _arg(7)
SUBCOMMAND_OVERRIDE = _arg(8)
SAMPLE_JSON = _arg(9)

# A non-empty override (from job.parameters.subcommand) trumps the task's
# hardcoded default. Lets you pick a specific CLI subcommand at run-time
# without writing a separate task — e.g. right-click wsp_exposure → "Run
# task" → set subcommand=nhc-wsp-exp to backfill just the full-WSP
# exposure without also running the fcastonly variant.
SUBCOMMAND = SUBCOMMAND_OVERRIDE if SUBCOMMAND_OVERRIDE else SUBCOMMAND_DEFAULT


def _fallback_issued_time_from_etl() -> str:
    """If the caller didn't supply an issued_time and we're running
    as part of a job DAG, try to read the one the etl task emitted.

    Returns "" if dbutils isn't available, the etl task didn't run
    yet, or the key doesn't exist.

    Picks the right key based on the subcommand: WSP-related
    subcommands use ``wsp_issued_time`` (the WSP issuance, ~0–3h
    earlier than tracks), everything else uses ``track_issued_time``.
    """
    sub_l = SUBCOMMAND.lower()
    if "wsp" in sub_l:
        key = "wsp_issued_time"
    else:
        key = "track_issued_time"
    try:
        from databricks.sdk.runtime import dbutils
    except ImportError:
        return ""
    try:
        v = dbutils.jobs.taskValues.get(
            taskKey="etl", key=key, default="", debugValue=""
        )
        return v or ""
    except Exception as e:
        print(f"(task-value fallback failed: {e})")
        return ""


if not ISSUED_TIME:
    fallback = _fallback_issued_time_from_etl()
    if fallback:
        ISSUED_TIME = fallback
        print(f"(picked up issued_time={ISSUED_TIME!r} from etl task value)")

# Composite subcommands. Each key is a sentinel the bundle uses; the
# value is a list of [subcommand, *extra_args] entries the dispatcher
# expands and runs sequentially against the same shared (mode,
# issued_time, …) context. Used by the realtime task chain so one DBX
# task = one logical stage.
COMPOSITES: dict[str, list[list[str]]] = {
    "realtime-tracks-processing": [
        ["nhc-tracks-fcast-buffers"],
        ["nhc-tracks-obsv-buffers"],
        ["nhc-tracks-fcastonly-buffers"],
    ],
    "realtime-tracks-exposure": [
        ["nhc-realtime-tracks-exp"],
    ],
    "realtime-wsp-processing": [
        ["nhc-wsp-polygon-matched", "--overwrite"],
        ["nhc-wsp-fcastonly-polygons", "--overwrite"],
    ],
    "realtime-wsp-exposure": [
        ["nhc-realtime-wsp-exp"],
    ],
}

ISSUED_TIMES_JSON = "/tmp/nhc_issued_times.json"


def build_cmd(sub: str, extra: list[str] | tuple[str, ...] = ()) -> list[str]:
    """Build the ``python run_pipeline.py …`` invocation for one
    subcommand. Empty positional parameters are skipped (rather than
    passed as ``""``), which is what argparse expects."""
    cmd = [
        sys.executable,
        os.path.join(REPO_ROOT, "run_pipeline.py"),
        sub,
        "--mode",
        MODE,
    ]
    cmd.extend(extra)
    # The etl subcommand has no --issued-time flag (it always fetches whatever
    # NHC currently has); skip the append so an explicit job-level issued_time
    # doesn't crash etl with `unrecognized arguments`. Downstream tasks still
    # receive --issued-time as usual.
    if ISSUED_TIME and sub != "nhc":
        cmd += ["--issued-time", ISSUED_TIME]
    # --since and --until are accepted by every backfillable subcommand
    # (buffers, WSP processing, all exposure variants). The etl subcommand
    # ignores them; argparse rejects unknown flags, so gate to the etl
    # case. --year was retired — use --since/--until on year boundaries
    # instead.
    if SINCE and sub != "nhc":
        cmd += ["--since", SINCE]
    if UNTIL and sub != "nhc":
        cmd += ["--until", UNTIL]
    # Same story for --overwrite: the etl subcommand doesn't expose it
    # (always upserts). Skip the append to avoid "unrecognized arguments".
    if OVERWRITE.lower() == "true" and sub != "nhc":
        cmd += ["--overwrite"]
    if FILL_NULLS.lower() == "true":
        cmd += ["--fill-nulls"]
    # Test mode: only meaningful for the nhc ETL subcommand. Downstream
    # tasks ignore SAMPLE_JSON — they read the resulting issued_time from
    # the etl task value, no different from a realtime run.
    if SAMPLE_JSON and sub == "nhc":
        cmd += ["--sample-json", SAMPLE_JSON]
    # Cleanup: from DBX, the only realistic scrub is the sample one.
    # For ad-hoc atcf_id scrubs, invoke run_pipeline.py locally.
    if sub == "nhc-scrub":
        cmd += ["--sample"]
    # After the ETL stage runs, run_pipeline.py writes the issued_times
    # to this file; we then emit them as task values below.
    if sub == "nhc":
        cmd += ["--out-issued-times-json", ISSUED_TIMES_JSON]
    return cmd


def emit_etl_task_values():
    """Read /tmp/nhc_issued_times.json (written by ``run_pipeline.py
    nhc --out-issued-times-json``) and surface its keys as DBX task
    values so downstream realtime tasks can reference them via
    ``{{tasks.etl.values.track_issued_time}}`` etc.

    Always emits the keys (as empty strings when the value is None),
    so downstream reference resolution never fails. Downstream tasks
    then short-circuit on the empty string.
    """
    if not os.path.exists(ISSUED_TIMES_JSON):
        print(f"(no {ISSUED_TIMES_JSON} — skipping task value emission)")
        return
    with open(ISSUED_TIMES_JSON) as f:
        payload = json.load(f)
    try:
        from databricks.sdk.runtime import dbutils
    except ImportError:
        print("(databricks.sdk.runtime missing — skipping task value emission)")
        return
    for k in ("track_issued_time", "wsp_issued_time"):
        v = payload.get(k)
        s = v if v else ""
        dbutils.jobs.taskValues.set(k, s)
        print(f"task value: {k}={s!r}")


def run_one(sub: str, extra: list[str] | tuple[str, ...] = ()) -> None:
    cmd = build_cmd(sub, extra)
    print("Running:", " ".join(cmd), flush=True)
    rc = subprocess.run(cmd, check=False, cwd=REPO_ROOT).returncode
    # DBX's IPython exec context treats sys.exit() as a task failure
    # (even SystemExit(0)). Propagate non-zero via a regular exception
    # and let success return naturally.
    if rc != 0:
        raise RuntimeError(f"{sub} exited with code {rc}")


if __name__ == "__main__":
    print(
        f"DBX dispatcher: subcommand={SUBCOMMAND} mode={MODE} "
        f"issued_time={ISSUED_TIME!r} cwd={REPO_ROOT}",
        flush=True,
    )
    # Realtime composites typically need an issued_time from the ETL stage.
    # No-op only when ALL time filters are empty (the realtime-cron / no-
    # active-storms case). If the user passed a range (since/until) or an
    # explicit issued_time, run the composite normally — its inner subcommand
    # now accepts --since/--until and treats this as a backfill.
    no_time_filter = not (ISSUED_TIME or SINCE or UNTIL)
    if SUBCOMMAND in COMPOSITES and no_time_filter:
        print(
            f"(no issued_time/since/until supplied for {SUBCOMMAND} — nothing to do)"
        )
    elif SUBCOMMAND in COMPOSITES:
        for parts in COMPOSITES[SUBCOMMAND]:
            run_one(parts[0], parts[1:])
    else:
        run_one(SUBCOMMAND)
        if SUBCOMMAND == "nhc":
            emit_etl_task_values()
    print("OK")
