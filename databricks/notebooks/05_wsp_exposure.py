# Databricks notebook source
# MAGIC %md
# MAGIC # Task E — WSP exposure
# MAGIC Population exposure from the full WSP and the fcastonly WSP.

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
from src.pipelines.nhc import run_nhc_wsp_exp, run_nhc_wsp_fcastonly_exp

wsp_it = pd.to_datetime(wsp_it_str).to_pydatetime()
run_nhc_wsp_exp(mode=mode, issued_time=wsp_it)
run_nhc_wsp_fcastonly_exp(mode=mode, issued_time=wsp_it)
