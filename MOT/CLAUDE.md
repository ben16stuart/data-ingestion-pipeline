# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

MOT Ingestion is an enterprise-grade Python 3.11+ ETL pipeline that ingests XLSX files into Google BigQuery. The pipeline is idempotent, change-aware via checksums, and designed for production environments.

## Running the Pipeline

### Basic Execution
```bash
# Using config file
python pyxlsxscanner.py --config config/config.yaml

# Using environment variables
export MOT_INPUT_DIR=./input
export MOT_BQ_PROJECT=my-project
export MOT_BQ_DATASET=my_dataset
export MOT_BQ_TABLE=my_table
export MOT_GCS_BUCKET=my-bucket
python pyxlsxscanner.py

# Dry run (no data loading)
python pyxlsxscanner.py --config config/config.yaml --dry-run

# Debug logging
python pyxlsxscanner.py --config config/config.yaml --log-level DEBUG
```

### Programmatic Usage
```python
from mot_ingestion.config import IngestionConfig
from mot_ingestion.pipeline import IngestionPipeline

config = IngestionConfig.from_yaml("config/config.yaml")
pipeline = IngestionPipeline(config)
exit_code = pipeline.run()
```

## Architecture

### Pipeline Flow

1. **Discovery** (`discovery.py`): Recursively scans input directory for XLSX files, filters temporary files
2. **Checksum** (`checksum.py`): Computes SHA256/MD5 hash for change detection
3. **State Check** (`state.py`): Queries BigQuery registry to determine if file needs processing
4. **Parse** (`parser.py`): Reads XLSX using xlsx2csv (converts to CSV in memory, then reads with csv.DictReader), forcing all values to strings initially
5. **Normalize** (`schema.py`): Enforces canonical schema (drop unknown columns, add missing as NULL, cast types, append audit fields)
6. **Serialize** (`serializer.py`): Outputs Parquet (preferred) or CSV with date-based partitioning
7. **Upload** (`storage.py`): Pushes serialized files to GCS with resumable uploads
8. **Load** (`bigquery.py`): Triggers BigQuery LOAD jobs from GCS URIs with explicit schema
9. **Update State** (`state.py`): Records processing status (SUCCESS/FAILED) with metadata

### Module Responsibilities

- `config.py`: Configuration dataclasses, YAML/env loading, schema definitions
- `pipeline.py`: End-to-end orchestration, error handling, metrics
- `__main__.py`: CLI argument parsing, logging setup, entrypoint
- `discovery.py`: File system traversal with pattern matching and ignore rules
- `checksum.py`: File hashing (MD5/SHA256) with chunked reading
- `parser.py`: XLSX parsing with xlsx2csv (converts to CSV in memory, reads with csv.DictReader), returns list of dicts
- `schema.py`: Schema normalization (drop/add/reorder/cast columns, append audit fields), manual type casting
- `serializer.py`: List of dicts to Parquet (pyarrow) / CSV (csv module) with date partitioning
- `storage.py`: GCS uploads with resumable protocol
- `bigquery.py`: BigQuery LOAD jobs from GCS, explicit schema enforcement
- `state.py`: File registry queries and status recording

### Configuration System

Configuration supports YAML files and environment variables:

**YAML Configuration:**
```yaml
input_directory: ./input
file_pattern: "**/*.xlsx"
sheet_name: 0  # or "Sheet1"
output_format: parquet  # or csv
checksum_algorithm: sha256  # or md5

bigquery:
  project_id: my-project
  dataset: my_dataset
  table: my_table
  registry_table: file_registry
  location: US

gcs:
  bucket: my-bucket
  prefix: mot-ingestion

schema:
  - name: id
    type: INTEGER
    mode: REQUIRED
  - name: name
    type: STRING
    mode: NULLABLE
```

**Environment Variables:**
- `MOT_INPUT_DIR`: Input directory path
- `MOT_BQ_PROJECT`: BigQuery project ID
- `MOT_BQ_DATASET`: BigQuery dataset name
- `MOT_BQ_TABLE`: BigQuery table name
- `MOT_GCS_BUCKET`: GCS bucket name
- `MOT_DRY_RUN`: Set to "true" for dry run
- `MOT_LOG_LEVEL`: Logging level (INFO, DEBUG, etc.)
- `GOOGLE_APPLICATION_CREDENTIALS`: Path to service account key

### State Management

The `file_registry` table in BigQuery maintains processing history:
- `file_name`: Source filename
- `checksum`: File content hash
- `processed_at`: Processing timestamp
- `status`: SUCCESS or FAILED
- `error_message`: Error details if failed
- `row_count`: Number of rows processed

Files are reprocessed only if:
1. Not present in registry
2. Checksum differs from last successful run

### Audit Trail

Every row in the target table includes:
- `source_file`: Original XLSX file path
- `checksum`: File checksum at ingestion time
- `ingest_ts`: UTC timestamp of ingestion

## Schema Customization

To modify the target schema, update `config/config.yaml`:

```yaml
schema:
  - name: column_name
    type: STRING|INTEGER|FLOAT|BOOLEAN|DATE|TIMESTAMP
    mode: NULLABLE|REQUIRED|REPEATED
```

The pipeline will:
- Cast columns to target types with error handling (coerce to NULL on failure)
- Add missing columns from schema as NULL
- Drop columns present in XLSX but not in schema

## BigQuery Setup

Initialize tables before first run:

```bash
# Update project/dataset in SQL files first
bq query --use_legacy_sql=false < sql/create_file_registry.sql
bq query --use_legacy_sql=false < sql/create_target_table.sql
```

The pipeline will auto-create tables if they don't exist (requires `bigquery.tables.create` permission).

## GCP Authentication

Pipeline uses Application Default Credentials:

```bash
# Local development
gcloud auth application-default login

# Production (service account key)
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

Required GCP IAM roles:
- `roles/bigquery.dataEditor` on dataset
- `roles/bigquery.jobUser` on project
- `roles/storage.objectAdmin` on GCS bucket

## Exit Codes

- `0`: Success (all files processed or skipped)
- `1`: Partial failure (one or more files failed)
- `2`: Pipeline initialization failure
- `3`: Fatal error (configuration, permissions, etc.)

## Observability

Structured JSON logging is enabled by default (using stdlib json module):
```bash
python pyxlsxscanner.py --config config/config.yaml --json-logs
```

Key log fields:
- `timestamp`: ISO 8601 timestamp (UTC)
- `name`: Logger name (module)
- `levelname`: DEBUG|INFO|WARNING|ERROR
- `message`: Log message
- `exc_info`: Exception traceback (if present)

The JSON formatter is implemented using Python's built-in `json` and `logging` modules, requiring no external logging dependencies.

## Production Considerations

- **Idempotency**: Run pipeline on schedule; unchanged files are skipped via checksum comparison
- **Partial Failures**: Individual file failures are isolated; pipeline continues processing remaining files
- **Resumable Uploads**: GCS uploads support resumption on network interruptions
- **BigQuery Quotas**: Bulk LOAD jobs consume quota; monitor `bigquery.googleapis.com/quota/load_bytes` metric
- **File Locking**: Pipeline does not implement file locking; ensure upstream processes write atomically
- **Large Files**: Pandas reads entire XLSX into memory; for multi-GB files, consider chunking or Dask
- **Schema Evolution**: Adding columns is safe; removing columns requires backfill

## Troubleshooting

**BigQuery permission denied:**
```bash
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:SA@PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"
```

**GCS upload timeout:**
Increase timeout in `storage.py:upload()` or check network connectivity.

**xlsx2csv parsing errors:**
Ensure XLSX files are valid and not password-protected. xlsx2csv converts XLSX to CSV in memory before loading into pandas.

**Checksum mismatches on unchanged files:**
Verify file system does not modify timestamps/metadata. Use `sha256` (default) for better collision resistance.

**Files not being processed:**
Check file registry:
```sql
SELECT file_name, checksum, status, processed_at
FROM `project.dataset.file_registry`
WHERE file_name = 'your_file.xlsx'
ORDER BY processed_at DESC;
```

Delete entry to force reprocessing:
```sql
DELETE FROM `project.dataset.file_registry`
WHERE file_name = 'your_file.xlsx';
```
