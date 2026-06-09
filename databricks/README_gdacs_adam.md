# `databricks/` — GDACS/ADAM DBX glue

This documents the **GDACS/ADAM** bundle (`../databricks.yml`, bundle name
`ds-storms-gdacs-adam`). It is separate from the NHC bundle (bundle name
`ds-storms-pipeline`, defined on the `wsp-adm0-exp` branch). Both share this
`databricks/` directory but currently target different jobs.

> **Consolidation note.** Two top-level `databricks.yml` files cannot coexist
> at one repo root. While the GDACS/ADAM and NHC work live on separate
> branches this is fine, but when they merge, fold both jobs into one
> `databricks.yml` (or `include: resources/*.yml`) and reconcile the two
> dispatchers. `dispatch.py` (NHC, composites + task values) and this
> `dispatch.py` (GDACS/ADAM, linear) will conflict on merge — that's expected.

## Architecture

One job (`gdacs_adam_pipeline`) with **three chained tasks**, each delegating
to `dispatch.py`, which shells out to `run_pipeline.py`:

```
   ┌─────────┐     ┌─────────┐     ┌─────────┐
   │  gdacs  │ ──▶ │  adam   │ ──▶ │  match  │
   └─────────┘     └─────────┘     └─────────┘
   GDACS exposure   ADAM exposure   retry ATCF linkage for any
   + inline ATCF    + adam_eventid  GDACS event still missing
   match            linkage         an atcf_id
```

This mirrors `run_pipeline.py`'s `all` cascade and the `all` path in
`.github/workflows/run-storm-pipelines.yml`.

`run_pipeline.py` is the single entry point — **DBX runs exactly the command a
local user would**; `dispatch.py` just turns job parameters into argv.

## Job parameters

| Parameter | Default | Notes |
|---|---|---|
| `mode` | `dev` / `prod` (per target) | Forwarded as `--mode`. |
| `from_date` | `""` | `YYYY-MM-DD`. Set => gdacs + adam run **archive** mode. |
| `to_date` | `""` | `YYYY-MM-DD`. Archive upper bound (defaults to today). |
| `days_back` | `""` | Current-mode rolling window. Empty => pipeline defaults (gdacs 7d, adam 14d). Ignored when `from_date` is set. |
| `source` | `NOAA` | `NOAA` or `JTWC`. |
| `all_episodes` | `""` | `"true"` => fetch every episode per event, not just the latest. |

`match` only takes `--mode`; the other parameters are ignored for it.

## How a run flows

1. **Run now / schedule** with default params: `gdacs` and `adam` run in
   current mode (rolling windows), then `match` retries ATCF linkage against
   whatever the DB now holds.
2. **Backfill a date range**: "Run with different parameters" →
   `from_date=2024-01-01`, `to_date=2025-01-01`. gdacs + adam switch to
   archive mode; match runs as usual.
3. **Single stage**: right-click a task → "Run task" to run just `gdacs`,
   `adam`, or `match`.

## Compute

| Target | Cluster |
|---|---|
| `dev` (default) | An existing interactive cluster (`var.dev_cluster_id`) for fast iteration. |
| `prod` | Ephemeral single-node job cluster (`Standard_DS3_v2`), spun up per run. |

The pipeline is plain Python (HTTP + pandas + Postgres writes) — no Spark
distribution — so a single node is sufficient.

> **Library isolation.** Each task declares its libraries (`ocha-lens` pinned
> to a specific commit, `ocha-stratus`, etc.) which DBX installs onto the
> cluster. The NHC job pins a *different* `ocha-lens` commit. Two jobs that
> install conflicting pins onto the **same** interactive cluster will clobber
> each other — so dev should run on a cluster dedicated to this job, not a
> shared one. Ephemeral job clusters (prod) sidestep this entirely.

## Bundle commands

```bash
databricks bundle validate -t dev  -p DEFAULT
databricks bundle deploy   -t dev  -p DEFAULT
databricks bundle run gdacs_adam_pipeline -t dev -p DEFAULT

# prod
databricks bundle deploy -t prod -p DEFAULT
```

Because the bundle uses `git_source` (`source: GIT`), the cluster pulls
`dispatch.py` / `run_pipeline.py` **from GitHub at `var.git_branch`**. A run
will only see code that has been **pushed** to that branch — deploy creates
the job, but push before you run.

## Running locally

```bash
uv run python run_pipeline.py gdacs --mode dev          # current, 7d window
uv run python run_pipeline.py adam  --mode dev          # current, 14d window
uv run python run_pipeline.py match --mode dev

# Archive backfill
uv run python run_pipeline.py gdacs --from-date 2024-01-01 --to-date 2025-01-01 --mode dev
uv run python run_pipeline.py adam  --from-date 2024-01-01 --to-date 2025-01-01 --mode dev
```
