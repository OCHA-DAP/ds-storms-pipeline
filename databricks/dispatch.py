"""DBX dispatcher for the storms pipeline.

This is the entry point for the Databricks job. It exists because
``spark_python_task`` has a fixed-length positional parameter list,
while ``run_pipeline.py`` accepts a variable-shape CLI. This script
reads the seven job parameters (subcommand + 6 optional flags) and
turns them into a normal ``python run_pipeline.py …`` call.

DBX-specific. The actual pipeline work lives in ``run_pipeline.py``
and ``src/pipelines/nhc.py`` — both runnable locally exactly the same
way. To verify equivalence: the command this script prints (``Running:
…``) is the command you'd run in a shell.

Positional contract (mirrors the job's job-level ``parameters:``):
    sys.argv[1] = subcommand          e.g. "nhc-realtime", "nhc-wsp-fcastonly-exp"
    sys.argv[2] = mode                "dev" | "prod"
    sys.argv[3] = issued_time         YYYY-MM-DDTHH or ""
    sys.argv[4] = since               YYYY-MM-DD or ""
    sys.argv[5] = year                YYYY or ""
    sys.argv[6] = overwrite           "true" | ""
    sys.argv[7] = fill_nulls          "true" | ""

Empty values are skipped.
"""

import os
import subprocess
import sys

# DBX's spark_python_task exec context doesn't define __file__. Try the
# usual ways to find the dispatcher's directory, then walk one level up
# to the repo root.
def _find_script_dir():
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


def build_cmd():
    cmd = [
        sys.executable,
        os.path.join(REPO_ROOT, "run_pipeline.py"),
        SUBCOMMAND,
        "--mode",
        MODE,
    ]
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
    return cmd


if __name__ == "__main__":
    cmd = build_cmd()
    print("Running:", " ".join(cmd), flush=True)
    print("cwd:", REPO_ROOT, flush=True)
    sys.exit(subprocess.run(cmd, check=False, cwd=REPO_ROOT).returncode)
