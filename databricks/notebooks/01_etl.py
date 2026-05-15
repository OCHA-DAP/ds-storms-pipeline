# Databricks notebook source
# MAGIC %md
# MAGIC # Task A — NHC ETL
# MAGIC Scrapes `CurrentStorms.json` + WSP feed, writes raw tracks and raw WSP.
# MAGIC Emits two task values for downstream tasks:
# MAGIC - `track_issued_time` — max issued_time across the scraped tracks
# MAGIC - `wsp_issued_time`   — issued_time of the scraped WSP shapefile
# MAGIC
# MAGIC Both are pulled **from the scraped JSON / shapefile directly**, not via
# MAGIC `MAX(issued_time)` on the DB — so they're safe against concurrent writers.

# COMMAND ----------
dbutils.widgets.text("mode", "dev")
mode = dbutils.widgets.get("mode")

# COMMAND ----------
import os, sys
# Resolve the bundle's files/ root from the notebook's own path so this
# works regardless of who deployed or which target.
_nb_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().notebookPath().get()
)
_files_root = _nb_path.rsplit("/databricks/notebooks/", 1)[0]
sys.path.insert(0, _files_root)
from src.pipelines.nhc import run_nhc_current

result = run_nhc_current(mode=mode, save_to_blob=False, save_dir="/tmp")

# COMMAND ----------
# Emit task values for B and D to consume. Cast to ISO string so they
# survive the JSON serialization the DBX task-values store does.
track_it = result.get("track_issued_time")
wsp_it = result.get("wsp_issued_time")

if track_it is not None:
    dbutils.jobs.taskValues.set("track_issued_time", str(track_it))
if wsp_it is not None:
    dbutils.jobs.taskValues.set("wsp_issued_time", str(wsp_it))

print(f"track_issued_time={track_it}  wsp_issued_time={wsp_it}")

# If nothing came back (no active storms), exit cleanly so dependents
# can short-circuit on the empty task value.
if track_it is None and wsp_it is None:
    dbutils.notebook.exit("no-active-storms")
