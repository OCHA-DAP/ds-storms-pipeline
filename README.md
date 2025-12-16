# Storms Pipeline

This repository contains code to download and process storm forecasts and observations from various sources. 

Sample data can be explored in the following apps:
- [Explore ECMWF](https://chd-ds-storms-explore-ecmwf-c8ctc8fzesceaqgb.eastus2-01.azurewebsites.net/)
- [Explore IBTrACS](https://chd-ds-storms-explore-ibtracs-hvf2cycjgmfrfuca.eastus2-01.azurewebsites.net/)

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