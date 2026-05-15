# Databricks notebook source
# MAGIC %md
# MAGIC # Task C — Tracks exposure
# MAGIC Population exposure from the three buffer tables for this issued_time.

# COMMAND ----------
dbutils.widgets.text("mode", "dev")
dbutils.widgets.text("issued_time", "")
mode = dbutils.widgets.get("mode")
issued_time = dbutils.widgets.get("issued_time") or None

if issued_time is None:
    dbutils.notebook.exit("no-issued-time")

# COMMAND ----------
import os, sys
import pandas as pd
_nb_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().notebookPath().get()
)
sys.path.insert(0, _nb_path.rsplit("/databricks/notebooks/", 1)[0])
from src.pipelines.nhc import (
    run_nhc_tracks_fcast_exp,
    run_nhc_tracks_obsv_exp,
    run_nhc_tracks_fcastonly_exp,
)

it = pd.to_datetime(issued_time).to_pydatetime()
run_nhc_tracks_fcast_exp(mode=mode, issued_time=it)
run_nhc_tracks_obsv_exp(mode=mode, valid_time=it)  # obsv keyed by valid_time
run_nhc_tracks_fcastonly_exp(mode=mode, issued_time=it)
