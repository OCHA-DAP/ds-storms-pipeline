# Storms Pipeline

This repository contains code to download and process storm forecasts and observations from various sources. 

## Running pipelines

The `run_pipeline.py` script provides a command-line interface for running data pipelines. Each pipeline has its own subcommand with relevant options.

### IBTrACS pipeline

Downloads and processes historical storm track data from the IBTrACS dataset.
```
python run_pipeline.py ibtracs [OPTIONS]
```

Options:
- `--mode {dev,prod}`: Database environment (default: `dev`)
- `--dataset-type {last3years,ACTIVE,ALL}`: Which dataset to download (default: `last3years`)
- `--save-to-blob`: Upload the downloaded netcdf file to Azure blob storage
- `--save-dir PATH`: Directory for downloaded files (default: `/tmp`)
- `--chunksize N`: Number of records per SQL insert batch (default: `10000`)

For example: 

Run the IBTrACS pipeline with default settings (last 3 years of data, dev mode):
```
python run_pipeline.py ibtracs
```

Run the IBTrACS pipeline for all historical data in production mode:
```
python run_pipeline.py ibtracs --mode prod --dataset-type ALL
```


### ECMWF pipeline

Downloads and processes ECMWF storm forecast data for a specified date range.
```
python run_pipeline.py ecmwf [OPTIONS]
```

Options:
- `--mode {dev,prod}`: Database environment (default: `dev`)
- `--start-date YYYY-MM-DD`: Start of date range (default: yesterday)
- `--end-date YYYY-MM-DD`: End of date range (default: yesterday)
- `--chunksize N`: Number of records per SQL insert batch (default: `10000`)

For example:

Run the ECMWF pipeline for yesterday's data (default):
```
python run_pipeline.py ecmwf
```

Run the ECMWF pipeline for a specific date range:
```
python run_pipeline.py ecmwf --start-date 2024-01-01 --end-date 2024-01-07
```

### Wind buffers pipelines

Two pipelines compute wind buffer polygons from quadrant wind radii (34/50/64 kt) and store them in the database. Both read from PROD and write to the target environment (default: `dev`), and both require `PGSSLMODE=require`:

```
export PGSSLMODE=require
```

Both require the `usa-wind-buffers` branch of `ocha-lens` (provides `calculate_wind_buffers_gdf` and `expand_quad_col`). Buffers are written to the DB in batches of 50 as they are calculated, so a crash mid-run is recoverable — just rerun and it will skip already-computed entries.

#### IBTrACS wind buffers (`wind-buffers`)

One buffer polygon per (storm, wind speed threshold) across the full historical track. Uses USA agency quadrant radii from `storms.ibtracs_tracks_geo`. Run the IBTrACS pipeline first.

```
python run_pipeline.py wind-buffers [OPTIONS]
```

Options:
- `--mode {dev,prod}`: Target database environment (default: `dev`). Always reads from PROD.
- `--basin CODE`: Filter to a single basin (e.g. `NA`, `WP`, `EP`, `SI`, `SP`, `NI`, `SA`)
- `--start-year YYYY`: Only process storms with track points from this year onwards
- `--overwrite`: Recalculate even for storms already in the database
- `--chunksize N`: Rows per SQL insert batch (default: `1000`)

```bash
# Full historical backfill
python run_pipeline.py wind-buffers --mode dev

# Subset for testing
python run_pipeline.py wind-buffers --mode dev --basin NA --start-year 2020
```

#### NHC forecast wind buffers (`nhc-tracks-fcast-buffers`)

One buffer polygon per (storm, forecast issuance, wind speed threshold) from NHC forecast tracks. Uses `storms.nhc_tracks_geo`. Run the NHC pipeline first.

```
python run_pipeline.py nhc-tracks-fcast-buffers [OPTIONS]
```

Options:
- `--mode {dev,prod}`: Target database environment (default: `dev`). Always reads from PROD.
- `--basin {NA,EP}`: Filter to a single basin
- `--start-year YYYY`: Only process issuances from this year onwards
- `--overwrite`: Recalculate even for issuances already in the database
- `--chunksize N`: Rows per SQL insert batch (default: `1000`)

```bash
# Full historical backfill
python run_pipeline.py nhc-tracks-fcast-buffers --mode dev

# Subset for testing
python run_pipeline.py nhc-tracks-fcast-buffers --mode dev --basin NA --start-year 2023
```

#### NHC observational track buffers (`nhc-tracks-obsv-buffers`)

Cumulative wind buffer swaths built from observed (leadtime=0) NHC positions. For each storm and each advisory, the buffer is the union of all observed positions up to that advisory — a growing footprint over the storm's lifetime. Uses `storms.nhc_tracks_geo`. Run the NHC pipeline first.

```
python run_pipeline.py nhc-tracks-obsv-buffers [OPTIONS]
```

Options:
- `--mode {dev,prod}`: Target database environment (default: `dev`). Always reads from PROD.
- `--basin {NA,EP}`: Filter to a single basin
- `--start-year YYYY`: Only process storms from this year onwards
- `--overwrite`: Recalculate even for advisories already in the database
- `--chunksize N`: Rows per SQL insert batch (default: `1000`)

```bash
# Full historical backfill
python run_pipeline.py nhc-tracks-obsv-buffers --mode dev

# Subset for testing
python run_pipeline.py nhc-tracks-obsv-buffers --mode dev --basin NA --start-year 2023
```

#### NHC forecast-only track buffers (`nhc-tracks-fcastonly-buffers`)

For each forecast advisory, the forecast wind buffer minus the cumulative observed track swath at that issued_time — i.e. only the area the storm is predicted to reach that it hasn't already passed through. Reads from `storms.nhc_tracks_fcast_buffers` and `storms.nhc_tracks_obsv_buffers` (both must be populated first). If no obsv buffer exists for an advisory (possible at storm start before leadtime=0 wind radii are available), the full forecast geometry is stored and a warning is logged.

```
python run_pipeline.py nhc-tracks-fcastonly-buffers [OPTIONS]
```

Options:
- `--mode {dev,prod}`: Target database environment (default: `dev`). Reads and writes to the same environment.
- `--basin {NA,EP}`: Filter to a single basin
- `--start-year YYYY`: Only process storms from this year onwards
- `--overwrite`: Recalculate even for advisories already in the database
- `--chunksize N`: Rows per SQL insert batch (default: `1000`)

```bash
# Full historical backfill
python run_pipeline.py nhc-tracks-fcastonly-buffers --mode dev

# Subset for testing
python run_pipeline.py nhc-tracks-fcastonly-buffers --mode dev --basin NA --start-year 2023
```

### Note on backfilling

These pipelines do not support automated backfilling. Unlike datasets with regular update schedules, cyclone data is published based on storm activity rather than a fixed cadence. When historical data needs to be reprocessed, it should be done manually using the appropriate date ranges or dataset types.

## Development setup

1. Create a virtual environment

```
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install Python dependencies

```
pip install -r requirements.txt
```

3. Create a local `.env` file with the following (to write to the `dev` database):

```
DSCI_AZ_DB_DEV_PW_WRITE=<provided-on-request>
DSCI_AZ_DB_DEV_UID_WRITE=<provided-on-request>
DSCI_AZ_DB_DEV_HOST=<provided-on-request>
```

### Code quality 

This project uses:
- Ruff for linting and formatting
- pre-commit hooks for code quality checks

Set up pre-commit with:

```
pre-commit install
```



### NHC pipeline

Downloads and processes National Hurricane Center (NHC) storm forecast data. Supports two modes: current active storms or historical archive data.
```
python run_pipeline.py nhc [OPTIONS]
```

Options:
- `--mode {dev,prod}`: Database environment (default: `dev`)
- `--save-to-blob`: Upload downloaded files to Azure blob storage
- `--save-dir PATH`: Directory for downloaded files (default: `/tmp`)
- `--start-year YYYY`: Start year for archive processing (e.g., 2020). If not provided, processes current active storms.
- `--end-year YYYY`: End year for archive processing (e.g., 2024). If not provided, only processes start-year.
- `--chunksize N`: Number of records per SQL insert batch (default: `10000`)

Examples:
```
# Process current active storms
python run_pipeline.py nhc

# Process archive data for a single year
python run_pipeline.py nhc --start-year 2023

# Process archive data for a range of years
python run_pipeline.py nhc --start-year 2020 --end-year 2024
```

## Database schema

All tables live in the `storms` schema. The pipeline reads from PROD and writes to DEV by default.

### `storms.ibtracs_storms`
One row per storm. Primary key: `sid` (IBTrACS serial ID).

| Column | Type | Description |
|---|---|---|
| `sid` | VARCHAR | IBTrACS serial identifier (e.g. `2023249N12323`) |
| `atcf_id` | VARCHAR | ATCF identifier (e.g. `AL092023`) |
| `name` | VARCHAR | Storm name (uppercase) |
| `season` | BIGINT | Season year |
| `genesis_basin` | VARCHAR | Basin where storm originated (NA, WP, EP, SI, SP, NI, SA) |
| `provisional` | BOOLEAN | Whether data is provisional (not yet finalized in IBTrACS) |
| `storm_id` | VARCHAR | Standardized ID: `{name}_{basin}_{season}` (lowercase) |

### `storms.ibtracs_tracks_geo`
One row per observation point. Populated by the IBTrACS pipeline.

| Column | Type | Description |
|---|---|---|
| `sid` | VARCHAR | FK → ibtracs_storms |
| `valid_time` | TIMESTAMP | Observation time (UTC) |
| `basin` | VARCHAR | Current basin at this point |
| `wind_speed` | INTEGER | Max sustained wind speed (knots) |
| `quadrant_radius_34/50/64` | TEXT | JSON array `[NE, SE, SW, NW]` — WMO best-track wind radii (nm) |
| `usa_quadrant_radius_34/50/64` | TEXT | JSON array `[NE, SE, SW, NW]` — USA agency wind radii (nm), available ~2004+ |
| `geometry` | geometry(Point, 4326) | Track point location |

### `storms.ibtracs_wind_buffers`
One row per (storm, wind speed threshold). Populated by the wind-buffers pipeline. Depends on `ibtracs_tracks_geo` being populated first.

| Column | Type | Description |
|---|---|---|
| `sid` | VARCHAR | IBTrACS storm ID |
| `wind_speed_kt` | SMALLINT | Wind speed threshold: 34, 50, or 64 knots |
| `geometry` | geometry(Geometry, 4326) | Union of all wind buffer discs along the storm track. Polygon or MultiPolygon (antimeridian-crossing storms are split at ±180°) |

The buffers use USA agency quadrant radii and basin-appropriate map projections to correctly handle storms near the antimeridian (WP, SP, EP basins).

### `storms.nhc_storms`
One row per NHC storm. Primary key: `atcf_id` (e.g. `AL092023`). Only covers NA and EP basins.

### `storms.nhc_tracks_geo`
One row per NHC forecast point. Populated by the NHC pipeline.

| Column | Type | Description |
|---|---|---|
| `atcf_id` | VARCHAR | FK → nhc_storms |
| `issued_time` | TIMESTAMP | When the forecast was issued (UTC) |
| `valid_time` | TIMESTAMP | Forecast valid time (UTC) |
| `leadtime` | INTEGER | Hours ahead of issuance (0 = observation) |
| `basin` | VARCHAR | NA or EP |
| `wind_speed` | REAL | Max sustained wind speed (knots) |
| `quadrant_radius_34/50/64` | TEXT | JSON array `[NE, SE, SW, NW]` — wind radii (nm) |
| `geometry` | geometry(Point, 4326) | Forecast track point location |

### `storms.nhc_tracks_fcast_buffers`
One row per (storm, forecast issuance, wind speed threshold). Populated by the `nhc-tracks-fcast-buffers` pipeline. Depends on `nhc_tracks_geo` being populated first.

| Column | Type | Description |
|---|---|---|
| `atcf_id` | VARCHAR | ATCF storm identifier |
| `issued_time` | TIMESTAMP | Forecast issuance time (UTC) |
| `wind_speed_kt` | SMALLINT | Wind speed threshold: 34, 50, or 64 knots |
| `geometry` | geometry(Geometry, 4326) | Union of wind buffer discs across the forecast track. Polygon or MultiPolygon. |

### `storms.nhc_tracks_obsv_buffers`
One row per (storm, advisory, wind speed threshold). Each row is the cumulative union of all observed (leadtime=0) wind buffer discs up to that advisory. Populated by the `nhc-tracks-obsv-buffers` pipeline. Depends on `nhc_tracks_geo` being populated first.

| Column | Type | Description |
|---|---|---|
| `atcf_id` | VARCHAR | ATCF storm identifier |
| `valid_time` | TIMESTAMP | Most recent advisory included in this cumulative buffer (UTC) |
| `wind_speed_kt` | SMALLINT | Wind speed threshold: 34, 50, or 64 knots |
| `geometry` | geometry(Geometry, 4326) | Cumulative union of observed wind buffer discs up to valid_time. Polygon or MultiPolygon. |

### `storms.nhc_tracks_fcastonly_buffers`
One row per (storm, forecast issuance, wind speed threshold). The forecast wind buffer minus the cumulative observed track swath at that issued_time — the area the storm is predicted to reach that it hasn't already passed through. Populated by the `nhc-tracks-fcastonly-buffers` pipeline. NULL geometry when the forecast is fully covered by the observed track.

| Column | Type | Description |
|---|---|---|
| `atcf_id` | VARCHAR | ATCF storm identifier |
| `issued_time` | TIMESTAMP | Forecast issuance time (UTC) |
| `wind_speed_kt` | SMALLINT | Wind speed threshold: 34, 50, or 64 knots |
| `geometry` | geometry(Geometry, 4326) | Forecast buffer minus observed swath. NULL if forecast fully covered. |

### `storms.nhc_wsp_polygon` *(pending — `add-wsp-data` PR)*
Basin-wide NHC wind speed probability polygons. One row per (issued_time, wind_threshold_kt, percentage band).

**TODO:** the ingested WSP polygons are currently truncated at longitude -180. The upstream NHC shapefiles (e.g. `https://www.nhc.noaa.gov/gis/forecast/archive/{YYYYMMDDhh}_wsp_120hr5km.zip`) actually extend past -180 into the western Pacific; our raw-download step lops that wraparound off. Not blocking today because it only matters for storms whose probability footprint crosses the dateline, which is beyond the CP basin coverage we care about right now. Revisit if/when we extend to WP or otherwise need full dateline-aware polygons.
