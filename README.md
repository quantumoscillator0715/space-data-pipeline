# Space Data Pipeline

## Overview

This project demonstrates the construction of a simple end-to-end data pipeline using exoplanet data.  
Raw CSV data is extracted, cleaned, and transformed into curated datasets suitable for downstream analysis.  
The pipeline is designed to be reproducible and idempotent, following common data engineering practices.  
Intermediate datasets are stored as CSV files, and final outputs are written to both CSV and a SQLite database.

## Project Structure

```text
space-data-pipeline/
├── data/
│   ├── raw/        # Original source CSV files
│   ├── staging/    # Schema-aligned intermediate data
│   ├── curated/    # Cleaned and validated datasets
│   └── db/         # SQLite database output
├── etl/
│   ├── extract/    # Data extraction logic
│   ├── transform/  # Schema and semantic transformations
│   ├── load/       # Database loading logic
│   └── pipeline.py # ETL orchestration entry point
├── notebooks/
│   └── exploration.ipynb
├── .gitignore
├── README.md
└── requirements.txt
'''

## Data Flow (ETL)

The pipeline follows a layered ETL design:

1. Extract
2. Staging
3. Curated
4. Load

### 1. Extract
Raw exoplanet CSV files are discovered in the `data/raw/` directory.  
No modifications are applied at this stage.

### 2. Staging
Raw data is standardized into a consistent schema.
Column names are normalized, data types are enforced, and structurally invalid records are filtered.
The result is written to `data/staging/` as a schema-aligned CSV file.

### 3. Curated
Staged data undergoes semantic cleaning and feature engineering.
Invalid physical values are handled, categorical values are normalized, and derived features
(e.g. density and size classification) are created.
The curated dataset is written to `data/curated/`.

### 4. Load
The curated dataset is loaded into a SQLite database.
The load step is idempotent and replaces existing table data on each run.

## How to Run

From the project root:

python -m etl.pipeline

Or from a Jupyter notebook:

from etl.pipeline import run_pipeline
run_pipeline()

## Outputs

After a successful pipeline run, the following artifacts are produced:

- `data/staging/exoplanets_staging.csv`  
  Schema-aligned intermediate dataset.

- `data/curated/exoplanets_curated.csv`  
  Cleaned and validated dataset with derived features.

- `data/db/exoplanets.sqlite`  
  SQLite database containing the `exoplanets` table.

## Design Decisions

- The pipeline is split into raw, staging, and curated layers to clearly separate
  schema enforcement from semantic data cleaning.

- Pandas is used for transformations to allow rapid development and explicit
  data validation logic.

- Intermediate datasets are persisted as CSV files to make each pipeline stage
  observable and debuggable.

- SQLite is used as the final storage backend to keep the project lightweight
  while still demonstrating relational loading.

- The pipeline is designed to be idempotent so that it can be safely re-run
  without manual cleanup.

## Tech Stack

- Python
- Pandas / NumPy
- SQLite
- pathlib
- Jupyter (development only)
