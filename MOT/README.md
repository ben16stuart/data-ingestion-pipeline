# MOT Ingestion Pipeline

Enterprise-grade Python 3.11+ ETL pipeline for ingesting XLSX files into Google BigQuery.

## Overview

Idempotent pipeline that:
- Discovers XLSX files in a local directory
- Detects changes via content checksums
- Normalizes schema across files
- Converts data to Parquet/CSV
- Bulk loads into BigQuery via GCS
- Tracks processing state in BigQuery registry

## Usage

### Basic Execution

```bash
python pyxlsxscanner.py --config config/config.yaml
```

### Dry Run

```bash
python pyxlsxscanner.py --config config/config.yaml --dry-run
```

### Environment Variables

```bash
export MOT_INPUT_DIR=./input
export MOT_BQ_PROJECT=my-project
export MOT_BQ_DATASET=my_dataset
export MOT_BQ_TABLE=my_table
export MOT_GCS_BUCKET=my-bucket

python pyxlsxscanner.py
```

### Programmatic Usage

```python
from pathlib import Path
from mot_ingestion.config import IngestionConfig
from mot_ingestion.pipeline import IngestionPipeline

config = IngestionConfig.from_yaml("config/config.yaml")
pipeline = IngestionPipeline(config)
exit_code = pipeline.run()
```

## Configuration

Edit `config/config.yaml`:

```yaml
input_directory: ./input
output_format: parquet

bigquery:
  project_id: your-project
  dataset: your_dataset
  table: your_table

gcs:
  bucket: your-bucket
  prefix: mot-ingestion

schema:
  - name: id
    type: INTEGER
    mode: REQUIRED
  - name: name
    type: STRING
  - name: email
    type: STRING
```

## BigQuery Setup

```bash
bq query --use_legacy_sql=false < sql/create_file_registry.sql
bq query --use_legacy_sql=false < sql/create_target_table.sql
```

## Exit Codes

- `0`: Success
- `1`: Partial failure (some files failed)
- `2`: Pipeline initialization failure
- `3`: Fatal error

## GCP Authentication

```bash
gcloud auth application-default login
# OR
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

## Architecture

```
XLSX Files → Discovery → Checksum → State Check → Parse →
Normalize → Serialize → GCS Upload → BigQuery Load → Update Registry
```

## Features

- **Idempotent**: Safe to run repeatedly
- **Change Detection**: SHA256 checksums with BigQuery registry
- **Schema Enforcement**: Drop unknown columns, add missing as NULL, type casting
- **Audit Trail**: source_file, checksum, ingest_ts on every row
- **Bulk Loading**: Efficient BigQuery LOAD jobs from GCS
- **Structured Logging**: JSON logs for monitoring (stdlib only, no external deps)
- **Dry Run Mode**: Test without loading data
- **Partial Failure Handling**: Individual file failures don't stop pipeline

## Dependencies

- xlsx2csv>=0.8.0
- pyarrow>=15.0.0
- google-cloud-storage>=2.14.0
- google-cloud-bigquery>=3.17.0
- pyyaml>=6.0.1
