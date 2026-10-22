"""DBX entry point for ``src/pipelines/fms.py`` (one-off ingests/backfills).

A spark_python_task can't run ``python -m``, so this runs the module as a
subprocess from the repo root, the way dispatch.py runs run_pipeline.py.
Arguments pass straight through, e.g.::

    databricks/run_fms.py backfill --blob-prefix <prefix> --mode dev

Never ``sys.exit`` here: the DBX exec context treats even ``SystemExit(0)``
as a failed task. A non-zero exit from the module raises instead.

The live ingest does not use this file: the pa-aa-fji-storms job clones
this repo and runs ``python -m src.pipelines.fms ingest ...`` itself.
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


REPO_ROOT = os.path.abspath(os.path.join(_find_script_dir(), ".."))

cmd = [sys.executable, "-m", "src.pipelines.fms", *sys.argv[1:]]
print(f"run_fms.py: {' '.join(cmd)} (cwd={REPO_ROOT})", flush=True)
rc = subprocess.run(cmd, cwd=REPO_ROOT, check=False).returncode
if rc != 0:
    raise RuntimeError(f"src.pipelines.fms exited with code {rc}")
