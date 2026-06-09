"""DBX dispatcher for the GDACS/ADAM/match pipeline.

DBX-specific glue, and the *only* thing in this job that knows about
Databricks. It turns the seven positional parameters from
``databricks.yml`` into a single ``python run_pipeline.py <sub> …``
invocation. ``run_pipeline.py`` and ``src/`` stay pure argparse Python —
they run identically locally and in DBX.

This is intentionally simpler than the NHC dispatcher (on the
``wsp-adm0-exp`` branch): the gdacs -> adam -> match cascade has no
composite subcommands and needs no cross-task value passing (``match``
just reads whatever the DB currently holds), so there is no
``dbutils.jobs.taskValues`` plumbing here.

Positional contract (mirrors the per-task ``parameters:`` in
databricks.yml):
    sys.argv[1] = subcommand     # "gdacs" | "adam" | "match"
    sys.argv[2] = mode           # "dev" | "prod"
    sys.argv[3] = from_date      # YYYY-MM-DD or "" (archive lower bound)
    sys.argv[4] = to_date        # YYYY-MM-DD or "" (archive upper bound)
    sys.argv[5] = days_back      # int as str or "" (current-mode window)
    sys.argv[6] = source         # "NOAA" | "JTWC" or ""
    sys.argv[7] = all_episodes   # "true" or ""

Mode selection follows run_pipeline.py: if ``from_date`` is set, gdacs
and adam run in archive mode (``--from-date``/``--to-date``); otherwise
they run current/rolling mode (``--days-back`` if provided, else the
pipeline default). ``match`` only accepts ``--mode`` — the date/source
params are ignored for it.
"""

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


SUBCOMMAND = _arg(1)
MODE = _arg(2, "dev")
FROM_DATE = _arg(3)
TO_DATE = _arg(4)
DAYS_BACK = _arg(5)
SOURCE = _arg(6)
ALL_EPISODES = _arg(7)

# Date/source/episode filters apply only to the data-pull subcommands.
# `match` reads the DB and takes nothing but --mode.
DATE_AWARE = {"gdacs", "adam"}


def build_cmd(sub: str) -> list[str]:
    """Build the ``python run_pipeline.py …`` invocation. Empty
    parameters are skipped (rather than passed as ``""``) — argparse
    would otherwise treat ``--from-date ""`` as a real value and wrongly
    flip the pipeline into archive mode."""
    cmd = [
        sys.executable,
        os.path.join(REPO_ROOT, "run_pipeline.py"),
        sub,
        "--mode",
        MODE,
    ]
    if sub in DATE_AWARE:
        if FROM_DATE:
            # Archive mode: from_date (and optional to_date) drive the run.
            cmd += ["--from-date", FROM_DATE]
            if TO_DATE:
                cmd += ["--to-date", TO_DATE]
        elif DAYS_BACK:
            # Current mode with an explicit rolling window. Omitting this
            # lets run_pipeline.py fall back to its per-pipeline default
            # (gdacs 7d, adam 14d).
            cmd += ["--days-back", DAYS_BACK]
        if SOURCE:
            cmd += ["--source", SOURCE]
        if ALL_EPISODES.lower() == "true":
            cmd += ["--all-episodes"]
    return cmd


def run_one(sub: str) -> None:
    cmd = build_cmd(sub)
    print("Running:", " ".join(cmd), flush=True)
    rc = subprocess.run(cmd, check=False, cwd=REPO_ROOT).returncode
    # DBX's IPython exec context treats sys.exit() as a task failure
    # (even SystemExit(0)). Propagate non-zero via a regular exception
    # and let success return naturally.
    if rc != 0:
        raise RuntimeError(f"{sub} exited with code {rc}")


if __name__ == "__main__":
    if SUBCOMMAND not in {"gdacs", "adam", "match"}:
        raise SystemExit(
            f"dispatch.py: unexpected subcommand {SUBCOMMAND!r} "
            "(expected gdacs | adam | match)"
        )
    print(
        f"DBX dispatcher: subcommand={SUBCOMMAND} mode={MODE} "
        f"from_date={FROM_DATE!r} to_date={TO_DATE!r} "
        f"days_back={DAYS_BACK!r} source={SOURCE!r} "
        f"all_episodes={ALL_EPISODES!r} cwd={REPO_ROOT}",
        flush=True,
    )
    run_one(SUBCOMMAND)
    print("OK")
