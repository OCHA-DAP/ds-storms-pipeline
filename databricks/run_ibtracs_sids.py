"""One-off: recompute IBTrACS wind buffers and adm0 exposure for given SIDs.

Step 5 of the PR #64 prod run order. After the dedupe migration and the
IBTrACS refresh, two storms whose duplicate track copies differed in
position need their buffers and exposure rebuilt from the refreshed tracks:
2025274N16087 and 2026254N11252 (Nolo). The CLI (run_pipeline.py) does not
expose ``sids=`` yet, so this calls the pipeline functions directly.

Usage (DBX spark_python_task, source GIT)::

    databricks/run_ibtracs_sids.py --mode prod --sids 2025274N16087 2026254N11252

Both steps run with overwrite: buffers upsert on (sid, wind_speed_kt), and
exposure rewrites every country's row for these SIDs (zeros included), so no
stale row survives.

Never ``sys.exit`` here: the DBX exec context treats even ``SystemExit(0)``
as a failed task.
"""

import argparse
import logging
import os
import sys


def _find_script_dir():
    try:
        return os.path.dirname(os.path.abspath(__file__))  # noqa: F821
    except NameError:
        pass
    if sys.argv and sys.argv[0]:
        return os.path.dirname(os.path.abspath(sys.argv[0]))
    return os.getcwd()


REPO_ROOT = os.path.abspath(os.path.join(_find_script_dir(), ".."))
sys.path.insert(0, REPO_ROOT)
os.chdir(REPO_ROOT)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s"
)

parser = argparse.ArgumentParser()
parser.add_argument("--mode", choices=["dev", "prod"], required=True)
parser.add_argument("--sids", nargs="+", required=True)
args = parser.parse_args()

from src.pipelines.ibtracs import run_ibtracs_exp, run_wind_buffers  # noqa: E402

print(f"run_ibtracs_sids.py: mode={args.mode} sids={args.sids}", flush=True)
run_wind_buffers(write_mode=args.mode, overwrite=True, sids=args.sids)
run_ibtracs_exp(mode=args.mode, overwrite=True, sids=args.sids)
print("run_ibtracs_sids.py: done", flush=True)
