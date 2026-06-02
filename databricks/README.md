# `databricks/` — DBX-specific glue

This directory contains the **only** code in the repo that's specific to
Databricks. Everything else (`src/`, `run_pipeline.py`, `scripts/`) is plain
Python with an argparse CLI and runs identically regardless of where it's
invoked.

## Architecture

The bundle is a single job (`nhc_pipeline`) with **five chained tasks**, each
of which delegates to `databricks/dispatch.py`. The dispatcher turns the
job-level parameters into one or more `python run_pipeline.py …` invocations.

```
                    Job parameters (10):
                    mode, issued_time, since, until, overwrite,
                    fill_nulls, subcommand, sample_json, admin_level
                                 │
                                 ▼
                       ┌─────────────────────┐
                       │       etl           │   nhc  (ETL: storms, tracks, WSP raw)
                       └─────────┬───────────┘   emits track_issued_time + wsp_issued_time
                                 │                as DBX task values
                                 ▼
                       ┌─────────────────────┐
                       │ tracks_processing   │   realtime-tracks-processing
                       └─────────┬───────────┘   (fcast + obsv + fcastonly buffers)
                            │         │
                            ▼         ▼
                ┌─────────────────┐   ┌─────────────────────┐
                │ tracks_exposure │   │   wsp_processing    │
                └─────────────────┘   └──────────┬──────────┘
                                                 ▼
                                      ┌─────────────────────┐
                                      │   wsp_exposure      │
                                      └─────────────────────┘
```

`run_pipeline.py` is the single entry point. **DBX runs exactly the same
command a local user would** — the dispatcher just turns job parameters into
argv and shells out.

## Files in this directory

| File | Purpose |
|---|---|
| `dispatch.py` | DBX entry point. Reads 10 positional args from `spark_python_task`, expands realtime "composite" subcommands, builds the `python run_pipeline.py …` argv, execs. For the ETL task, also emits `track_issued_time` / `wsp_issued_time` as DBX task values so downstream tasks can pick them up. |
| `README.md` | This file. |

The bundle YAML lives one level up at [`../databricks.yml`](../databricks.yml).

## Job parameters

| Parameter | Default | Notes |
|---|---|---|
| `mode` | `dev` / `prod` (per target) | Forwarded as `--mode`. |
| `issued_time` | `""` | `YYYY-MM-DDTHH`. Overrides the ETL-emitted task value when set. |
| `since` | `""` | `YYYY-MM-DD`. Inclusive lower bound for backfills. |
| `until` | `""` | `YYYY-MM-DD`. Exclusive upper bound for backfills. |
| `overwrite` | `""` | `"true"` to force upsert of existing rows. |
| `fill_nulls` | `""` | `"true"` for `nhc-wsp-polygon-matched` rematch-NULLs mode. |
| `subcommand` | `""` | Non-empty overrides the task's hardcoded composite — lets you reuse a task slot for any single CLI subcommand (e.g. `subcommand=nhc-wsp-exp` on the `wsp_exposure` task). |
| `sample_json` | `""` | URL of a frozen CurrentStorms JSON. ETL fetches this instead of the live NHC endpoint. End-to-end smoke test only — embedded URLs in the sample rot. |
| `admin_level` | `""` | `"0"`, `"1"`, or `"0,1"` (default both). Exposure subcommands only. |

## How a run flows

1. **Schedule or "Run now"**: with default parameters and the realtime cron
   (every 3h), the full 5-task cascade runs.
2. **`etl` task** runs `python run_pipeline.py nhc --out-issued-times-json /tmp/nhc_issued_times.json`,
   scrapes the live NHC JSON, writes raw rows to `nhc_storms`, `nhc_tracks_geo`,
   `nhc_wsp_polygon_raw`. Then `dispatch.py` reads the JSON and emits
   `track_issued_time` and `wsp_issued_time` as DBX task values.
3. **Downstream tasks** receive the relevant task value via
   `{{tasks.etl.values.track_issued_time}}` / `{{tasks.etl.values.wsp_issued_time}}`
   as positional arg 3 (the `issued_time` parameter), unless the user passed
   an explicit `issued_time` at job launch (which takes precedence).
4. Each downstream task expands its realtime composite into 1–3
   `run_pipeline.py` invocations:
   - `tracks_processing` → `nhc-tracks-{fcast,obsv,fcastonly}-buffers`
   - `tracks_exposure` → `nhc-realtime-tracks-exp` (fcast + obsv + fcastonly in one process)
   - `wsp_processing` → `nhc-wsp-polygon-matched`, `nhc-wsp-fcastonly-polygons`
   - `wsp_exposure` → `nhc-realtime-wsp-exp` (wsp + wsp-fcastonly in one process)

## Running locally vs in DBX

### Locally

```bash
# Full realtime cascade in one process
uv run python run_pipeline.py nhc-realtime --mode dev

# A single advisory, end-to-end (e.g. replay 2024-10-09T18)
uv run python run_pipeline.py nhc-tracks-fcast-buffers       --issued-time 2024-10-09T18 --overwrite
uv run python run_pipeline.py nhc-tracks-obsv-buffers        --issued-time 2024-10-09T18 --overwrite
uv run python run_pipeline.py nhc-tracks-fcastonly-buffers   --issued-time 2024-10-09T18 --overwrite
uv run python run_pipeline.py nhc-wsp-polygon-matched        --issued-time 2024-10-09T18 --overwrite
uv run python run_pipeline.py nhc-wsp-fcastonly-polygons     --issued-time 2024-10-09T18 --overwrite
uv run python run_pipeline.py nhc-realtime-tracks-exp        --issued-time 2024-10-09T18 --overwrite
uv run python run_pipeline.py nhc-realtime-wsp-exp           --issued-time 2024-10-09T18 --overwrite

# Backfill a specific stage
uv run python run_pipeline.py nhc-wsp-fcastonly-exp --since 2024-01-01 --until 2025-01-01 --mode dev --overwrite

# Surgically fill rows with NULL atcf_id
uv run python run_pipeline.py nhc-wsp-polygon-matched --fill-nulls --mode dev
```

### In DBX

| Goal | "Run now with different parameters" |
|---|---|
| Realtime run (live JSON) | All defaults — click **Run** |
| Replay one advisory through the whole cascade | `issued_time=2024-10-09T18`, `overwrite=true` |
| Replay skipping ETL (no live JSON fetch) | CLI: `databricks jobs run-now --json '{"job_id":…, "only":["tracks_processing","tracks_exposure","wsp_processing","wsp_exposure"], "job_parameters":{"issued_time":"2024-10-09T18","overwrite":"true"}}'` |
| Backfill WSP fcastonly exposure for a date range | Right-click `wsp_exposure` → Run task → `subcommand=nhc-wsp-fcastonly-exp`, `since=2024-01-01`, `until=2025-01-01`, `overwrite=true` |
| Fill NULL atcf_ids in matched WSP | Right-click `wsp_processing` → Run task → `subcommand=nhc-wsp-polygon-matched`, `fill_nulls=true` |
| End-to-end smoke test on fixture | `sample_json=https://www.nhc.noaa.gov/productexamples/NHC_JSON_Sample.json`, then `subcommand=nhc-scrub` to clean up |
| Resume one half of an exposure backfill (adm1 only) | `admin_level=1` |

Cron uses the defaults. Backfill = manual run with parameter overrides.
Right-click a task → "Run task" only runs that task; "Run with different
parameters" on the job runs the whole DAG with overrides.

## Composite subcommands

The `dispatch.py` `COMPOSITES` table maps realtime-stage sentinels to the
`run_pipeline.py` subcommand sequence they expand into:

| Composite | Expands to |
|---|---|
| `realtime-tracks-processing` | `nhc-tracks-fcast-buffers` → `nhc-tracks-obsv-buffers` → `nhc-tracks-fcastonly-buffers` |
| `realtime-tracks-exposure` | `nhc-realtime-tracks-exp` |
| `realtime-wsp-processing` | `nhc-wsp-polygon-matched --overwrite` → `nhc-wsp-fcastonly-polygons --overwrite` |
| `realtime-wsp-exposure` | `nhc-realtime-wsp-exp` |

Composites are inert when no time filter is set (the
"no-active-storms cron tick" case — the ETL task ran but found no storms).

## What `subcommand` can be

Anything `run_pipeline.py` accepts. Common picks for one-off DBX runs:

- `nhc` — ETL only
- `nhc-tracks-fcast-buffers`, `nhc-tracks-obsv-buffers`, `nhc-tracks-fcastonly-buffers`
- `nhc-track-exp`, `nhc-obsv-exp`, `nhc-fcastonly-exp`
- `nhc-wsp-polygon-matched` (with optional `fill_nulls=true`)
- `nhc-wsp-fcastonly-polygons`
- `nhc-wsp-exp`, `nhc-wsp-fcastonly-exp`
- `nhc-realtime`, `nhc-realtime-tracks-exp`, `nhc-realtime-wsp-exp`
- `nhc-scrub` (always passed `--sample` from DBX; ad-hoc atcf_id scrubs run locally)

## Boundary between DBX and pure Python

The boundary is `databricks/dispatch.py`. Above it (the bundle YAML,
secrets, scheduling) is DBX. Below it (`run_pipeline.py`, `src/`) is plain
Python. To swap DBX for another orchestrator (Airflow, GHA, k8s, cron): write
a different wrapper that calls `python run_pipeline.py …`; the pipeline work
doesn't change.

## Verifying DBX and local runs agree

The cleanest test: **inputs identical → DB row counts identical**.

```sql
-- After a DBX run with issued_time=2024-10-10T00:
SELECT COUNT(*), SUM(pop_exposed)
FROM storms.nhc_wsp_fcastonly_exposure
WHERE issued_time = '2024-10-10 00:00';

-- After the equivalent local run, should match (modulo <0.1% rasterio
-- numerical noise at the country level):
--   uv run python run_pipeline.py nhc-wsp-fcastonly-exp \
--       --issued-time 2024-10-10T00 --mode dev --overwrite
```

Both paths converge on the same `run_*` function with the same kwargs;
divergence indicates an **environment** difference (different Python version,
different `ocha-lens` pin, different DB/blob creds), not a code difference.
