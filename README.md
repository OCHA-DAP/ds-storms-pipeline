# Storms Pipeline

This repository contains code to download and process storm forecasts and observations from various sources. 


## Getting started

### Development setup

1. Create a virtual environment

```
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install Python dependencies

```
pip install -r requirements.txt
```

3. Create a local `.env` file with the following:

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

