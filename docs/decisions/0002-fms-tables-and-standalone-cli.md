---
status: "accepted"
date: 2026-10-22
decision-makers: Tristan Downing
consulted: Zack Arno (review of #61), Claude (pairing session)
informed: ds-storms-pipeline contributors
---

# FMS tracks: exact values without range CHECKs, and a standalone CLI

## Context and Problem Statement

RSMC Nadi (FMS) emails each forecast track as a CSV. `src/pipelines/fms.py`
writes them to `storms.fms_storms`, `fms_forecasts` and `fms_tracks_geo`, and
the Fiji AA monitor (OCHA-DAP/pa-aa-fji-storms) reads the forecast back from
those tables a few minutes after it arrives, on its own Databricks job.

Two choices differ from the NHC tables and pipelines:

1. How strict `fms_tracks_geo` is about its physical values (`nhc_tracks_geo`
   uses `REAL` and range CHECKs).
2. Where the code runs from (every other pipeline is a `run_pipeline.py`
   subcommand).

## Decision Drivers

* The tables feed a live alert path. When ingest fails, the monitor falls
  back to the emailed CSV (`run_if: ALL_DONE`), so a failure costs little.
  A write that succeeds with wrong values goes straight into the trigger.
* The DB path must give the monitor exactly what it gets from the CSV.
* The Fiji job runs on a Python 3.11 cluster pinned for the monitor's own
  dependencies (numpy 1.25.1, shapely 2.0.1), without this repo's other
  dependencies.

## Considered Options

* **Tables:** NHC-style `REAL` columns with range CHECKs, or `DOUBLE PRECISION`
  without range CHECKs.
* **Entry point:** a `run_pipeline.py fms` subcommand, or a standalone
  `python -m src.pipelines.fms` CLI.

## Decision Outcome

* **`DOUBLE PRECISION`, no range CHECKs.** `REAL` rounded one archived
  pressure value (967.8333129882812 → 967.8333), which broke the
  CSV-to-DB equivalence. Range CHECKs would refuse a whole forecast over one
  odd value in a column the trigger doesn't use.
* **A standalone CLI** that imports only pandas, sqlalchemy, python-dotenv,
  ocha-stratus and the standard library. `run_pipeline.py` imports every
  pipeline and so needs this repo's full dependency set.

### Consequences

* The parser checks what the database doesn't. A value that isn't a finite
  number (`inf` included) fails the file (`FmsParseError`) if the Fiji
  trigger reads that column (`TRIGGER_COLS`: position, wind, category,
  uncertainty, 34/50/64 kt radii), so the monitor falls back to the CSV. In
  any other column it is logged and stored as NULL. Blank cells and pandas'
  NA markers (`NaN`, `NA`, `N/A`, `null`, ...) are missing values. A file
  without one of the trigger's columns (the symmetric radii are optional),
  with no points, or with a point without a readable time or position also
  fails.
* Good: the alert path never runs on a silently degraded forecast, and an
  odd value elsewhere never stops a forecast landing.
* Bad: the FMS tables don't follow the NHC tables' conventions. Code that
  reads both must not assume the CHECKs.
* Bad: `fms.py` can't use this repo's helpers beyond its four packages.
* The Fiji job clones this repo at a tag (`fms-v*`), not `main`, so a
  change here reaches the live alert path only when that job's
  `storms_pipeline_ref` is bumped.

### Confirmation

* `tests/test_fms.py` covers the parse rules above and
  `test_imports_stay_light` fails if `fms.py` imports anything else.
* pa-aa-fji-storms `scripts/check_fms_db_equivalence.py` compares the DB
  path with the CSV path for every archived Official forecast.

## More Information

* Table definitions and the same reasoning: `src/schemas/sql/fms_tables.sql`.
* PR #61 (review by Zack Arno, 2026-10-21).
