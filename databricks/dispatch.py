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
    sys.argv[4] = since          # YYYY-MM-DD or ""
    sys.argv[5] = year           # YYYY or ""
    sys.argv[6] = overwrite      # "true" or ""
    sys.argv[7] = fill_nulls     # "true" or ""

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


SUBCOMMAND = _arg(1, "nhc-realtime")
MODE = _arg(2, "prod")
ISSUED_TIME = _arg(3)
SINCE = _arg(4)
YEAR = _arg(5)
OVERWRITE = _arg(6)
FILL_NULLS = _arg(7)

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
        ["nhc-track-exp"],
        ["nhc-obsv-exp"],
        ["nhc-fcastonly-exp"],
    ],
    "realtime-wsp-processing": [
        ["nhc-wsp-polygon-matched", "--overwrite"],
        ["nhc-wsp-fcastonly-polygons", "--overwrite"],
    ],
    "realtime-wsp-exposure": [
        ["nhc-wsp-exp"],
        ["nhc-wsp-fcastonly-exp"],
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
    if ISSUED_TIME:
        cmd += ["--issued-time", ISSUED_TIME]
    if SINCE:
        cmd += ["--since", SINCE]
    if YEAR:
        cmd += ["--year", YEAR]
    if OVERWRITE.lower() == "true":
        cmd += ["--overwrite"]
    if FILL_NULLS.lower() == "true":
        cmd += ["--fill-nulls"]
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
    # Realtime composites need an issued_time from the ETL stage. If the
    # ETL emitted "" (no active storms), there's nothing for downstream
    # to do — return successfully so the DAG continues / completes
    # without firing a backfill.
    if SUBCOMMAND in COMPOSITES and not ISSUED_TIME:
        print(f"(no issued_time supplied for {SUBCOMMAND} — nothing to do)")
    elif SUBCOMMAND in COMPOSITES:
        for parts in COMPOSITES[SUBCOMMAND]:
            run_one(parts[0], parts[1:])
    else:
        run_one(SUBCOMMAND)
        if SUBCOMMAND == "nhc":
            emit_etl_task_values()
    print("OK")
