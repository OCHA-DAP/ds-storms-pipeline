# Databricks notebook source
# MAGIC %md
# MAGIC # NHC Backfill (manual)
# MAGIC
# MAGIC Re-run any one pipeline step over historical data. Click
# MAGIC **Run now with different parameters** in the DBX UI to set inputs.
# MAGIC
# MAGIC **Pick a `step`**, then narrow the work with one of:
# MAGIC - `since` (YYYY-MM-DD): process everything from this date forward.
# MAGIC - `issued_time` (YYYY-MM-DDTHH): process exactly one issuance.
# MAGIC - `year` (YYYY): process one calendar year (only for the exposure steps).
# MAGIC
# MAGIC Flags:
# MAGIC - `overwrite=true` recomputes rows that already exist.
# MAGIC - `fill_nulls=true` (only for `step=wsp-matched`) re-runs the
# MAGIC   containment-fallback pass against existing NULL atcf_id rows.

# COMMAND ----------
dbutils.widgets.dropdown(
    "step",
    "wsp-fcastonly-polygons",
    [
        "wsp-matched",
        "wsp-fcastonly-polygons",
        "wsp-exp",
        "wsp-fcastonly-exp",
        "tracks-fcast-buffers",
        "tracks-obsv-buffers",
        "tracks-fcastonly-buffers",
        "tracks-fcast-exp",
        "tracks-obsv-exp",
        "tracks-fcastonly-exp",
    ],
)
dbutils.widgets.text("since", "")
dbutils.widgets.text("issued_time", "")
dbutils.widgets.text("year", "")
dbutils.widgets.dropdown("overwrite", "true", ["true", "false"])
dbutils.widgets.dropdown("fill_nulls", "false", ["true", "false"])
dbutils.widgets.dropdown("mode", "dev", ["dev", "prod"])

step = dbutils.widgets.get("step")
since = dbutils.widgets.get("since") or None
issued_time_str = dbutils.widgets.get("issued_time") or None
year_str = dbutils.widgets.get("year") or None
overwrite = dbutils.widgets.get("overwrite").lower() == "true"
fill_nulls = dbutils.widgets.get("fill_nulls").lower() == "true"
mode = dbutils.widgets.get("mode")

# COMMAND ----------
import os
import sys

import pandas as pd

_nb_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)
sys.path.insert(0, _nb_path.rsplit("/databricks/notebooks/", 1)[0])

issued_time = (
    pd.to_datetime(issued_time_str).to_pydatetime() if issued_time_str else None
)
year = int(year_str) if year_str else None

from src.pipelines.nhc import (
    run_fill_null_wsp_polygon_matched,
    run_nhc_tracks_fcast_buffers,
    run_nhc_tracks_fcast_exp,
    run_nhc_tracks_fcastonly_buffers,
    run_nhc_tracks_fcastonly_exp,
    run_nhc_tracks_obsv_buffers,
    run_nhc_tracks_obsv_exp,
    run_nhc_wsp_exp,
    run_nhc_wsp_fcastonly_exp,
    run_nhc_wsp_fcastonly_polygons,
    run_nhc_wsp_polygon_matched,
)

DISPATCH = {
    "wsp-matched": lambda: (
        run_fill_null_wsp_polygon_matched(
            mode=mode, since=since, issued_time=issued_time
        )
        if fill_nulls
        else run_nhc_wsp_polygon_matched(
            mode=mode, since=since, issued_time=issued_time, overwrite=overwrite
        )
    ),
    "wsp-fcastonly-polygons": lambda: run_nhc_wsp_fcastonly_polygons(
        mode=mode, since=since, issued_time=issued_time, overwrite=overwrite
    ),
    "wsp-exp": lambda: run_nhc_wsp_exp(
        mode=mode,
        since=since,
        issued_time=issued_time,
        overwrite=overwrite,
    ),
    "wsp-fcastonly-exp": lambda: run_nhc_wsp_fcastonly_exp(
        mode=mode,
        since=since,
        issued_time=issued_time,
        year=year,
        overwrite=overwrite,
    ),
    "tracks-fcast-buffers": lambda: run_nhc_tracks_fcast_buffers(
        write_mode=mode, issued_time=issued_time, overwrite=overwrite
    ),
    "tracks-obsv-buffers": lambda: run_nhc_tracks_obsv_buffers(
        write_mode=mode, issued_time=issued_time, overwrite=overwrite
    ),
    "tracks-fcastonly-buffers": lambda: run_nhc_tracks_fcastonly_buffers(
        write_mode=mode, issued_time=issued_time, overwrite=overwrite
    ),
    "tracks-fcast-exp": lambda: run_nhc_tracks_fcast_exp(
        mode=mode, since=since, issued_time=issued_time, overwrite=overwrite
    ),
    "tracks-obsv-exp": lambda: run_nhc_tracks_obsv_exp(
        mode=mode, since=since, valid_time=issued_time, overwrite=overwrite
    ),
    "tracks-fcastonly-exp": lambda: run_nhc_tracks_fcastonly_exp(
        mode=mode, since=since, issued_time=issued_time, overwrite=overwrite
    ),
}

if step not in DISPATCH:
    raise ValueError(f"Unknown step '{step}'. Choices: {sorted(DISPATCH.keys())}")

print(
    f"Running step={step} mode={mode} since={since} "
    f"issued_time={issued_time} year={year} "
    f"overwrite={overwrite} fill_nulls={fill_nulls}"
)
DISPATCH[step]()
print("Done.")
