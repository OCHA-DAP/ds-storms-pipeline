# `databricks/` — DBX-specific glue

This directory contains the **only** code in the repo that's specific to
Databricks. Everything else (`src/`, `run_pipeline.py`, `scripts/`) is
plain Python with a CLI interface and runs the same way regardless of
where it's invoked.

## Architecture

```
                       ┌───────────────────────────────────────────┐
                       │   src/pipelines/nhc.py — run_* functions  │
                       │   (the actual pipeline work — DB I/O,     │
                       │   raster ops, exposure calcs, etc.)       │
                       └─────────────────┬─────────────────────────┘
                                         │
                                         ▼
                       ┌───────────────────────────────────────────┐
                       │   run_pipeline.py — argparse CLI          │
                       │   `python run_pipeline.py <subcmd> [...]` │
                       └─────────────────┬─────────────────────────┘
                            │            │
                            ▼            ▼
                  LOCAL SHELL    databricks/dispatch.py
                  (run directly) (turns DBX job params into the
                                  same CLI invocation, then execs)
                                         │
                                         ▼
                                   DBX JOB / UI
```

`run_pipeline.py` is the single entry point. **DBX runs exactly the
same command a local user would**. The dispatcher just turns the
seven job parameters into argv.

## Files in this directory

| File | Purpose |
|---|---|
| `dispatch.py` | DBX entry point. Reads 7 positional args from `spark_python_task`, builds the `python run_pipeline.py …` argv, execs. |
| `README.md` | This file. |

The bundle YAML lives one level up at `databricks.yml`. It defines a
single job (`nhc_pipeline`) with a git_source pointing at this repo,
job-level parameters mirroring the CLI shape, and one task that runs
`databricks/dispatch.py`.

## How a run flows

1. **DBX schedules or you click "Run now"**: with defaults, that's
   `subcommand=nhc-realtime`, `mode=prod`, all other params empty.
2. **Task spawns** on the configured cluster. The cluster clones the
   repo at the configured `git_branch` to `/databricks/driver/<repo>/`.
3. **`dispatch.py` runs** with the 7 parameters as `sys.argv[1:8]`.
4. Dispatcher builds `python run_pipeline.py nhc-realtime --mode prod`
   (skipping empty optional flags) and prints it to stdout.
5. `run_pipeline.py` dispatches to the appropriate `run_*` function
   in `src/pipelines/nhc.py`.

## Running locally vs in DBX

### Locally
```bash
# Realtime (full 5-stage cascade in one process)
uv run python run_pipeline.py nhc-realtime --mode dev

# Specific backfill steps
uv run python run_pipeline.py nhc-wsp-fcastonly-exp --year 2024 --mode dev --overwrite
uv run python run_pipeline.py nhc-wsp-polygon-matched --fill-nulls --mode dev
```

### In DBX (one job, parameter-driven)

| Goal | DBX UI: click "Run now with different parameters" |
|---|---|
| Realtime run | All defaults — just click **Run** |
| Fcastonly exposure for 2024 | `subcommand=nhc-wsp-fcastonly-exp`, `year=2024`, `overwrite=true` |
| Fill NULL atcf_ids | `subcommand=nhc-wsp-polygon-matched`, `fill_nulls=true` |
| Rebuild matched since a date | `subcommand=nhc-wsp-polygon-matched`, `since=2024-10-01`, `overwrite=true` |
| Re-run track exposure for one advisory | `subcommand=nhc-track-exp`, `issued_time=2024-10-10T00`, `overwrite=true` |

Cron uses the defaults. Backfill = manual run with parameter overrides.

## What `subcommand` can be

All argparse subcommands of `run_pipeline.py`, e.g.:
- `nhc-realtime` — full cascade (default)
- `nhc` — ETL only (scrape + write raw)
- `nhc-tracks-fcast-buffers`, `nhc-tracks-obsv-buffers`, `nhc-tracks-fcastonly-buffers`
- `nhc-track-exp`, `nhc-obsv-exp`, `nhc-fcastonly-exp`
- `nhc-wsp-polygon-matched` (with optional `fill_nulls=true`)
- `nhc-wsp-fcastonly-polygons`
- `nhc-wsp-exp`, `nhc-wsp-fcastonly-exp`

`run_pipeline.py --help` for the full list. The dispatcher passes the
parameter through untouched, so anything `run_pipeline.py` accepts is
fair game.

## Boundary between DBX and pure Python

The boundary is `databricks/dispatch.py`. Above it (the bundle YAML,
secrets references, scheduling) is DBX. Below it (`run_pipeline.py`,
`src/`) is plain Python. To swap DBX for another orchestrator
(Airflow, GHA, k8s, cron): write a different wrapper that calls
`python run_pipeline.py …`; the pipeline work doesn't change.

## Verifying DBX and local runs are doing the same thing

The cleanest test: **inputs identical → DB row counts identical**.

```sql
-- After a DBX run with issued_time=2024-10-10T00:
SELECT COUNT(*), SUM(pop_exposed)
FROM storms.nhc_wsp_fcastonly_exposure
WHERE issued_time = '2024-10-10 00:00';

-- After the equivalent local run, should match (modulo ≤0.1%
-- rasterio numerical noise at the country level):
--   uv run python run_pipeline.py nhc-wsp-fcastonly-exp \
--       --issued-time 2024-10-10T00 --mode dev --overwrite
```

Both paths converge on the same `run_*` function with the same kwargs;
divergence indicates a **environment** difference (different Python
version, different `ocha-lens` git ref, different DB/blob creds), not
a code difference.
