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



