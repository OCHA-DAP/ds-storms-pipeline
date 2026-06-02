# Storms Pipeline

Download, process, and compute population exposure for tropical-cyclone data
from three sources:

| Source | Subcommand prefix | Coverage |
|---|---|---|
| **IBTrACS** | `ibtracs*`, `wind-buffers`, `ibtracs-track-exp` | Historical best-track, all basins |
| **NHC** (National Hurricane Center) | `nhc*` | Realtime forecasts + archive, NA + EP basins |
| **ECMWF** | `ecmwf` | Ensemble cyclone tracks |

All tables live in the Postgres `storms` schema. Pipelines read from PROD and
write to the target environment (default `dev`); the DBX bundle drives the
NHC realtime cascade on a 3-hourly cron — see [`databricks/README.md`](databricks/README.md).

## Running pipelines

`run_pipeline.py` is a single argparse CLI. Every subcommand accepts:

| Flag | Notes |
|---|---|
| `--mode {dev,prod}` | Target DB. Default `dev`. |
| `--chunksize N` | Rows per SQL insert batch. Default `10000`. |

Pipelines that work on rows keyed by `issued_time` / `valid_time` (every NHC
buffer / WSP / exposure subcommand) share these time-filter flags:

| Flag | Notes |
|---|---|
| `--issued-time YYYY-MM-DDTHH` | Process exactly this single advisory. |
| `--since YYYY-MM-DD` | Inclusive lower bound. |
| `--until YYYY-MM-DD` | Exclusive upper bound. |
| `--overwrite` | Recompute & upsert even if rows already exist. Without it, runs are resumable — already-written keys are skipped. |
| `--basin NA\|EP` | Limit to one NHC basin (`genesis_basin` column). |

Exposure subcommands additionally accept:

| Flag | Notes |
|---|---|
| `--countries ISO3 [ISO3…]` | Limit to specific countries. |
| `--admin-level {0,1}` | Repeatable. Default: both. |

Use `uv run python run_pipeline.py <subcmd> --help` for the full per-subcommand list.

### IBTrACS

```bash
# ETL: historical track data
uv run python run_pipeline.py ibtracs --dataset-type {last3years|ACTIVE|ALL}

# Wind buffers from IBTrACS tracks (one polygon per (sid, wind_speed_kt))
uv run python run_pipeline.py wind-buffers --basin NA --start-year 2020

# Population exposure from IBTrACS wind buffers
uv run python run_pipeline.py ibtracs-track-exp --since 2020 --countries HTI JAM

# Realtime cascade: ETL + buffers + exposure for active storms only
uv run python run_pipeline.py ibtracs-realtime
```

### ECMWF

```bash
# Defaults to yesterday's data
uv run python run_pipeline.py ecmwf --start-date 2024-10-01 --end-date 2024-10-07
```

### NHC — ETL

```bash
# Current active storms (no args)
uv run python run_pipeline.py nhc

# Archive backfill for a year range
uv run python run_pipeline.py nhc --start-year 2020 --end-year 2024

# Test mode: fetch a frozen sample CurrentStorms JSON instead of the live endpoint
uv run python run_pipeline.py nhc --sample-json https://www.nhc.noaa.gov/productexamples/NHC_JSON_Sample.json
```

The sample-JSON path is for end-to-end smoke tests. The URLs embedded in the
sample JSON (forecast advisories, WSP zip) are live NHC paths that get
rotated, so this path is only reliable while the sample is fresh.

### NHC — track wind buffers

Three pipelines, each one buffer polygon per
`(atcf_id, issued_time | valid_time, wind_speed_kt)`. Reads from
`storms.nhc_tracks_geo`.

```bash
uv run python run_pipeline.py nhc-tracks-fcast-buffers       # forecast cone
uv run python run_pipeline.py nhc-tracks-obsv-buffers        # cumulative observed swath
uv run python run_pipeline.py nhc-tracks-fcastonly-buffers   # forecast minus observed
```

`fcastonly-buffers` depends on the two above being populated for the same
issued_time.

### NHC — WSP (wind speed probability) polygons

```bash
# Match raw basin-wide WSP polygons to individual storms (run after nhc ETL)
uv run python run_pipeline.py nhc-wsp-polygon-matched

# Fill rows with NULL atcf_id using containment-fallback against existing matches
uv run python run_pipeline.py nhc-wsp-polygon-matched --fill-nulls

# WSP minus cumulative observed swath (analogous to fcastonly-buffers)
uv run python run_pipeline.py nhc-wsp-fcastonly-polygons
```

### NHC — exposure

Five exposure pipelines, all keyed by `(admin_level, iso3, pcode)` plus the
relevant polygon's identity columns:

```bash
uv run python run_pipeline.py nhc-track-exp         # fcast buffers → fcast exposure
uv run python run_pipeline.py nhc-obsv-exp          # obsv buffers → obsv exposure
uv run python run_pipeline.py nhc-fcastonly-exp     # fcastonly buffers → fcastonly exposure
uv run python run_pipeline.py nhc-wsp-exp           # matched WSP → WSP exposure
uv run python run_pipeline.py nhc-wsp-fcastonly-exp # WSP fcastonly → WSP fcastonly exposure
```

`nhc-obsv-exp` additionally accepts `--final-only` to keep just the last
cumulative buffer per `(atcf_id, wind_speed_kt)` — useful for historical
backfills where intermediate advisories aren't needed.

### NHC — realtime composites

One process per logical stage, sharing the WorldPop COG, FieldMaps boundaries,
and DB engine across the inner pipelines. Used by the DBX bundle.

```bash
# Full local cascade: ETL → buffers → tracks_exposure → wsp_processing → wsp_exposure
uv run python run_pipeline.py nhc-realtime

# Stage-level composites (accept the same time filters as inner pipelines)
uv run python run_pipeline.py nhc-realtime-tracks-exp --issued-time 2024-10-09T18
uv run python run_pipeline.py nhc-realtime-wsp-exp    --issued-time 2024-10-09T18
```

### NHC — scrub

Cleanup utility for removing rows from every NHC table (storms, tracks_geo,
buffers, WSP, exposure) for specific atcf_ids / issued_times:

```bash
# Auto-resolve from the sample JSON (use after a sample-JSON test run)
uv run python run_pipeline.py nhc-scrub --sample

# Manual targeting
uv run python run_pipeline.py nhc-scrub --atcf-id AL142024 --issued-time 2024-10-09T18

# Dry-run shows counts without deleting
uv run python run_pipeline.py nhc-scrub --sample --dry-run
```

## Database schema

All tables live in the `storms` schema. Geometries are EPSG:4326.

### IBTrACS

| Table | Key | Notes |
|---|---|---|
| `ibtracs_storms` | `sid` | One row per storm. `genesis_basin` ∈ `{NA, WP, EP, SI, SP, NI, SA}`. |
| `ibtracs_tracks_geo` | `(sid, valid_time)` | Track points. Includes WMO + USA agency quadrant wind radii. Point geometry. |
| `ibtracs_wind_buffers` | `(sid, wind_speed_kt)` | Union of wind discs along the storm track. Polygon / MultiPolygon. |
| `ibtracs_wind_exposure` | `(sid, wind_speed_kt, admin_level, pcode)` | Population exposed per admin unit. |

### NHC — core

| Table | Key | Notes |
|---|---|---|
| `nhc_storms` | `atcf_id` (e.g. `AL142024`) | `genesis_basin` ∈ `{NA, EP}` (CP storms file under EP). |
| `nhc_tracks_geo` | `(atcf_id, issued_time, leadtime)` | Forecast track points. `leadtime=0` is the observed position. |

### NHC — track buffers

All three keyed by storm × wind threshold × time, geometry is the wind-buffer
union.

| Table | Time key | Notes |
|---|---|---|
| `nhc_tracks_fcast_buffers` | `issued_time` | Forecast cone buffer. |
| `nhc_tracks_obsv_buffers` | `valid_time` | Cumulative observed swath up to that advisory. |
| `nhc_tracks_fcastonly_buffers` | `issued_time` | Forecast minus observed. NULL geometry when fully covered. |

### NHC — track exposure

Same key tuple in all three: `(atcf_id, issued_time | valid_time, wind_speed_kt, admin_level, iso3, pcode)`, plus `pop_exposed INTEGER`.

| Table | Time key | Source buffers |
|---|---|---|
| `nhc_tracks_fcast_exposure` | `issued_time` | `nhc_tracks_fcast_buffers` |
| `nhc_tracks_obsv_exposure` | `valid_time` | `nhc_tracks_obsv_buffers` |
| `nhc_tracks_fcastonly_exposure` | `issued_time` | `nhc_tracks_fcastonly_buffers` |

### NHC — WSP polygons

| Table | Key | Notes |
|---|---|---|
| `nhc_wsp_polygon_raw` | `(issued_time, wind_threshold_kt, percentage)` | Raw basin-wide NHC 5km shapefile output. Multi-storm issuances are a single MultiPolygon — no atcf_id. |
| `nhc_wsp_polygon_matched` | `(issued_time, wind_threshold_kt, percentage, atcf_id)` | Raw polygons split per storm via `ocha_lens.match_wsp_to_tracks`. `atcf_id` NULL when no track matched (treated as distinct via `NULLS NOT DISTINCT`). |
| `nhc_wsp_fcastonly_polygon` | `(issued_time, wind_threshold_kt, percentage, atcf_id)` | Matched WSP minus cumulative observed swath. `obsv_valid_time` records which obsv buffer was actually used (issued_time + 3h normally, or NULL if no obsv buffer found). |

### NHC — WSP exposure

Same key tuple in both: `(issued_time, wind_threshold_kt, percentage, atcf_id, admin_level, iso3, pcode)`, plus `pop_exposed INTEGER`.

| Table | Source polygons |
|---|---|
| `nhc_wsp_exposure` | `nhc_wsp_polygon_matched` |
| `nhc_wsp_fcastonly_exposure` | `nhc_wsp_fcastonly_polygon` |

### Shared

| Table | Key | Notes |
|---|---|---|
| `admin_population` | `(admin_level, iso3, pcode)` | Total WorldPop population per FieldMaps admin unit. Static — recompute via `scripts/compute_admin_population.py` when WorldPop is bumped. Use as the denominator for any exposure table (`pop_exposed / total_pop`). |

## Development setup

This project uses [`uv`](https://docs.astral.sh/uv/) for environment
management. `requirements.txt` is kept for legacy tooling but `uv` is the
canonical workflow.

```bash
# Install (creates .venv automatically)
uv sync

# Run any subcommand
uv run python run_pipeline.py <subcmd> [...]
```

Create a local `.env` with the DB credentials (provided on request):

```
DSCI_AZ_DB_DEV_PW_WRITE=<...>
DSCI_AZ_DB_DEV_UID_WRITE=<...>
DSCI_AZ_DB_DEV_HOST=<...>
```

PostgreSQL requires SSL — usually `export PGSSLMODE=require` is needed.

### Code quality

Ruff + pre-commit:

```bash
pre-commit install
```

## Helper scripts

In `scripts/`:

| Script | Purpose |
|---|---|
| `compute_admin_population.py` | Build / refresh `storms.admin_population`. |
| `mirror_fieldmaps_to_blob.py` | Mirror FieldMaps per-country adm0/adm1 parquets to the `global` blob container (one-time, re-run when FieldMaps refreshes). |
| `cleanup_stale_exposure_rows.py` | Anti-join scrub: delete exposure rows whose key tuple the current code wouldn't produce. |
| `backfill_tracks_prod_to_dev.py` / `backfill_wsp_polygons.py` | One-off DEV bootstrap from PROD. |
| `init_db_ecmwf.py` | Create ECMWF tables. |

## Known limitations

- **WSP polygons truncated at -180**: upstream NHC 5km shapefiles extend
  slightly past the dateline into the western Pacific; our raw download
  step lops the wraparound off. Only matters for storms whose footprint
  crosses ±180°, which is beyond current NA + EP coverage.
- **Pre-2002 wind radii gap**: NHC didn't archive quadrant wind radii (the
  R34/R50/R64 values) consistently before ~2002, so track buffers are
  thin/missing for older storms. IBTrACS buffers (USA agency radii) have
  similar coverage gaps before ~2004.
- **KIR dateline coverage**: Kiribati's three island groups straddle the
  dateline without any polygon vertex at ±180°, so its country bbox
  spans ~351° "the long way around" and rio.clip's pixel window misses
  the actual islands. Affects only KIR exposure.
