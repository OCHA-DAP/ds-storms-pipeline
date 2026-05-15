# `databricks/` — DBX-specific glue

This directory contains the **only** code in the repo that's specific to
Databricks. Everything else (`src/`, `run_pipeline.py`, `scripts/`) is plain
Python with a CLI interface and runs the same way regardless of where it's
invoked.

## Architecture

```
                      ┌──────────────────────────────────────────┐
                      │   src/pipelines/nhc.py — run_* functions │
                      │   (the actual pipeline work — DB I/O,    │
                      │   raster ops, exposure calcs, etc.)      │
                      └──────────────┬───────────────────────────┘
                                     │  imported and called identically
              ┌──────────────────────┼──────────────────────┐
              │                                             │
              ▼                                             ▼
   ┌─────────────────────┐                       ┌─────────────────────┐
   │   run_pipeline.py   │                       │  databricks/        │
   │   (argparse CLI)    │                       │  notebooks/*.py     │
   │                     │                       │  (dbutils widgets)  │
   └─────────────────────┘                       └─────────────────────┘
              │                                             │
              ▼                                             ▼
       LOCAL / SCRIPT                                 DBX JOB / UI
```

The DBX notebooks are **thin shims**: read a parameter from
`dbutils.widgets`, parse it, call the corresponding `run_*` function from
`src.pipelines.nhc` with identical kwargs. No business logic lives in the
notebooks. **Anything you can run as a `databricks bundle run`, you can
also run as `uv run python run_pipeline.py …` locally** (modulo task-value
chaining, see below).

## File map

| File | Purpose | Local equivalent |
|---|---|---|
| `databricks.yml` | Asset Bundle: jobs, schedule, cluster, task DAG, parameters | — |
| `notebooks/01_etl.py` | Task A: scrape JSON + WSP, write raw tables, emit issued_times as task values | `python run_pipeline.py nhc --mode <mode> --out-issued-times-json /tmp/it.json` |
| `notebooks/02_tracks_processing.py` | Task B: fcast / obsv / fcastonly buffers for one issued_time | `python run_pipeline.py nhc-tracks-{fcast,obsv,fcastonly}-buffers --issued-time T --mode <mode>` |
| `notebooks/03_tracks_exposure.py` | Task C: 3 track exposure tables for one issued_time | `python run_pipeline.py nhc-{track,obsv,fcastonly}-exp --issued-time T --mode <mode>` |
| `notebooks/04_wsp_processing.py` | Task D: matched table + fcastonly polygons for one wsp_issued_time | `python run_pipeline.py nhc-wsp-polygon-matched --issued-time T --mode <mode> --overwrite` + `nhc-wsp-fcastonly-polygons --issued-time T --mode <mode> --overwrite` |
| `notebooks/05_wsp_exposure.py` | Task E: full WSP + fcastonly WSP exposure for one wsp_issued_time | `python run_pipeline.py nhc-wsp-exp --issued-time T --mode <mode>` + `nhc-wsp-fcastonly-exp --issued-time T --mode <mode>` |
| `notebooks/backfill.py` | Manual: dispatch to one pipeline step over historical data | `python run_pipeline.py <step> [--since YYYY-MM-DD] [--issued-time T] [--year YYYY] [--overwrite] [--mode <mode>]` |

## DBX-only patterns the notebooks use

| Pattern | Why | Local fallback |
|---|---|---|
| `dbutils.widgets.text / dropdown` | Read parameters from the job/task | `argparse` flags |
| `dbutils.jobs.taskValues.set / get` | Pass `track_issued_time` / `wsp_issued_time` from Task A to B–E without a DB round-trip | `--out-issued-times-json` writes them to a file; downstream subcommand calls take them as `--issued-time` |
| `dbutils.notebook.entry_point.…notebookPath()` | Compute the bundle's `files/` root to import `src/` | Just `cd` into the repo |

## Running locally — the equivalent of a full DBX realtime run

```bash
# 1) Task A: ETL + emit issued_times
uv run python run_pipeline.py nhc --mode dev --out-issued-times-json /tmp/nhc_it.json

# Read them back into shell vars
TRACK_IT=$(jq -r .track_issued_time /tmp/nhc_it.json)
WSP_IT=$(jq -r .wsp_issued_time   /tmp/nhc_it.json)

# 2) Task B: tracks processing
uv run python run_pipeline.py nhc-tracks-fcast-buffers     --mode dev --issued-time "$TRACK_IT"
uv run python run_pipeline.py nhc-tracks-obsv-buffers      --mode dev --issued-time "$TRACK_IT"
uv run python run_pipeline.py nhc-tracks-fcastonly-buffers --mode dev --issued-time "$TRACK_IT"

# 3) Task C: tracks exposure
uv run python run_pipeline.py nhc-track-exp      --mode dev --issued-time "$TRACK_IT"
uv run python run_pipeline.py nhc-obsv-exp       --mode dev --issued-time "$TRACK_IT"
uv run python run_pipeline.py nhc-fcastonly-exp  --mode dev --issued-time "$TRACK_IT"

# 4) Task D: WSP processing
uv run python run_pipeline.py nhc-wsp-polygon-matched     --mode dev --issued-time "$WSP_IT" --overwrite
uv run python run_pipeline.py nhc-wsp-fcastonly-polygons  --mode dev --issued-time "$WSP_IT" --overwrite

# 5) Task E: WSP exposure
uv run python run_pipeline.py nhc-wsp-exp           --mode dev --issued-time "$WSP_IT"
uv run python run_pipeline.py nhc-wsp-fcastonly-exp --mode dev --issued-time "$WSP_IT"
```

Or, for the ETL-only "fetch + write raw" without the cascade,
`python run_pipeline.py nhc-realtime` does the whole 5-task chain
in-process — that's what to call from cron or a one-off shell.

## Running a one-off backfill — local vs DBX

| | DBX UI | Locally |
|---|---|---|
| Re-run fcastonly exposure for 2024 | Open `nhc_backfill` job → **Run now with different parameters** → `step=wsp-fcastonly-exp`, `year=2024`, `overwrite=true` | `uv run python run_pipeline.py nhc-wsp-fcastonly-exp --year 2024 --mode dev --overwrite` |
| Fill NULL atcf_ids in matched | `step=wsp-matched`, `fill_nulls=true` | `uv run python run_pipeline.py nhc-wsp-polygon-matched --fill-nulls --mode dev` |
| Rebuild matched for a date range | `step=wsp-matched`, `since=2024-10-01`, `overwrite=true` | `uv run python run_pipeline.py nhc-wsp-polygon-matched --since 2024-10-01 --mode dev --overwrite` |

The `nhc_backfill` notebook's dispatch table (in `backfill.py`) is just a
big dict mapping `step` strings to lambdas that call the same `run_*`
functions. Reading that dict is the single source of truth for what each
`step` actually does — start there if you're ever unsure.

## Verifying DBX and local runs are doing the same thing

The cleanest verification is **inputs identical → DB rows identical**.
For any given `issued_time`:

```sql
-- After a DBX run:
SELECT COUNT(*), SUM(pop_exposed)
FROM storms.nhc_wsp_fcastonly_exposure
WHERE issued_time = '2024-10-10 00:00';

-- After a local run with the same --issued-time, should produce
-- the same row count and sum (modulo rasterio numerical noise of
-- ≤0.1% at the country level).
```

Because both paths converge on the same `run_nhc_wsp_fcastonly_exp` call
with the same kwargs, any divergence indicates a difference in
**environment** (different Python version, different `ocha-lens` git ref,
different DB/blob credentials), not in **code path**.

If you want to spot-check the call chain, search for the function name
the dispatch table uses (e.g. `run_nhc_wsp_fcastonly_exp`) — there's
exactly one definition, in `src/pipelines/nhc.py`, called by both the
local CLI and the DBX notebook.

## When you do need DBX-specific behavior

The only "DBX-only" capability with no local equivalent is **inter-task
task values** (used by `01_etl.py` to hand `track_issued_time` and
`wsp_issued_time` to subsequent tasks). Locally, the equivalent is the
`--out-issued-times-json` flag on the `nhc` subcommand: it writes the
same payload to a file you read in the next shell step.

```bash
# DBX:
#   01_etl.py:  dbutils.jobs.taskValues.set("track_issued_time", "2024-10-10T00")
#   02_*.py:    dbutils.jobs.taskValues.get(taskKey="etl", key="track_issued_time")
#
# Local:
uv run python run_pipeline.py nhc --mode dev --out-issued-times-json /tmp/it.json
T=$(jq -r .track_issued_time /tmp/it.json)
uv run python run_pipeline.py nhc-tracks-fcast-buffers --mode dev --issued-time "$T"
```

Everything else — argparse, dbutils.widgets, even the `step` dropdown in
`backfill.py` — is just two surfaces (CLI vs UI form) over the same
underlying Python functions.
