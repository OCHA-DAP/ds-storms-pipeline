# Databricks notebook source
# MAGIC %md
# MAGIC # Task B — Tracks processing
# MAGIC fcast / obsv / fcastonly buffer pipelines, all scoped to `issued_time`.

# COMMAND ----------
dbutils.widgets.text("mode", "dev")
dbutils.widgets.text("issued_time", "")
mode = dbutils.widgets.get("mode")
issued_time = dbutils.widgets.get("issued_time") or None

if issued_time is None:
    dbutils.notebook.exit("no-issued-time")

# COMMAND ----------
import pandas as pd, sys
sys.path.insert(0, "/Workspace/Repos/ds-storms-pipeline")
from src.pipelines.nhc import (
    run_nhc_tracks_fcast_buffers,
    run_nhc_tracks_obsv_buffers,
    run_nhc_tracks_fcastonly_buffers,
)

it = pd.to_datetime(issued_time).to_pydatetime()
run_nhc_tracks_fcast_buffers(write_mode=mode, issued_time=it)
run_nhc_tracks_obsv_buffers(write_mode=mode, issued_time=it)
run_nhc_tracks_fcastonly_buffers(write_mode=mode, issued_time=it)
