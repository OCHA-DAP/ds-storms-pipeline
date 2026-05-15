# Databricks notebook source
# MAGIC %md
# MAGIC # Task D — WSP processing
# MAGIC Build `nhc_wsp_polygon_matched` (with strict line-intersection +
# MAGIC containment fallback), then `nhc_wsp_fcastonly_polygon` (subtracts
# MAGIC obsv buffer from B).
# MAGIC
# MAGIC Uses `wsp_issued_time` from Task A — typically 0 or 3h earlier than
# MAGIC the track issued_time.

# COMMAND ----------
dbutils.widgets.text("mode", "dev")
dbutils.widgets.text("wsp_issued_time", "")
mode = dbutils.widgets.get("mode")
wsp_it_str = dbutils.widgets.get("wsp_issued_time") or None

if wsp_it_str is None:
    dbutils.notebook.exit("no-wsp-issued-time")

# COMMAND ----------
import os, sys
import pandas as pd
_nb_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().notebookPath().get()
)
sys.path.insert(0, _nb_path.rsplit("/databricks/notebooks/", 1)[0])
from src.pipelines.nhc import (
    run_nhc_wsp_polygon_matched,
    run_nhc_wsp_fcastonly_polygons,
)

wsp_it = pd.to_datetime(wsp_it_str).to_pydatetime()
run_nhc_wsp_polygon_matched(mode=mode, issued_time=wsp_it, overwrite=True)
run_nhc_wsp_fcastonly_polygons(mode=mode, issued_time=wsp_it, overwrite=True)
