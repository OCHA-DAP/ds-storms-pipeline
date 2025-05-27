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
AZURE_DB_UID=<provided-on-request>
AZURE_DB_PWD_DEV=<provided-on-request>
AZURE_DB_PWD_PROD=<provided-on-request>
```

### Code quality 

This project uses:
- Ruff for linting and formatting
- pre-commit hooks for code quality checks

Set up pre-commit with:

```
pre-commit install
```