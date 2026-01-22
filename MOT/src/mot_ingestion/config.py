"""Configuration management for MOT ingestion pipeline."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import yaml


@dataclass
class BigQueryConfig:
    """BigQuery configuration."""

    project_id: str
    dataset: str
    table: str
    registry_table: str = "file_registry"
    location: str = "US"


@dataclass
class GCSConfig:
    """Google Cloud Storage configuration."""

    bucket: str
    prefix: str = "mot-ingestion"
    project_id: str | None = None


@dataclass
class SchemaField:
    """Schema field definition."""

    name: str
    type: Literal["STRING", "INTEGER", "FLOAT", "BOOLEAN", "DATE", "TIMESTAMP"]
    mode: Literal["NULLABLE", "REQUIRED", "REPEATED"] = "NULLABLE"


@dataclass
class IngestionConfig:
    """Main ingestion pipeline configuration."""

    input_directory: Path
    bigquery: BigQueryConfig
    gcs: GCSConfig
    schema: list[SchemaField]
    file_pattern: str = "**/*.xlsx"
    sheet_name: str | int = 0
    output_format: Literal["parquet", "csv"] = "parquet"
    checksum_algorithm: Literal["md5", "sha256"] = "sha256"
    dry_run: bool = False
    temp_directory: Path = field(default_factory=lambda: Path("/tmp/mot-ingestion"))
    log_level: str = "INFO"
    ignore_patterns: list[str] = field(
        default_factory=lambda: ["~$*", ".~*", "*.tmp", "*.temp"]
    )

    @classmethod
    def from_yaml(cls, path: str | Path) -> "IngestionConfig":
        """Load configuration from YAML file."""
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "IngestionConfig":
        """Construct configuration from dictionary."""
        data = data.copy()

        data["input_directory"] = Path(data["input_directory"])
        data["temp_directory"] = Path(data.get("temp_directory", "/tmp/mot-ingestion"))

        data["bigquery"] = BigQueryConfig(**data["bigquery"])
        data["gcs"] = GCSConfig(**data["gcs"])

        schema_data = data.get("schema", [])
        data["schema"] = [SchemaField(**field) for field in schema_data]

        return cls(**data)

    @classmethod
    def from_env(cls) -> "IngestionConfig":
        """Load configuration from environment variables."""
        return cls.from_dict(
            {
                "input_directory": os.getenv("MOT_INPUT_DIR", "./input"),
                "bigquery": {
                    "project_id": os.getenv("MOT_BQ_PROJECT"),
                    "dataset": os.getenv("MOT_BQ_DATASET"),
                    "table": os.getenv("MOT_BQ_TABLE"),
                    "registry_table": os.getenv("MOT_BQ_REGISTRY_TABLE", "file_registry"),
                    "location": os.getenv("MOT_BQ_LOCATION", "US"),
                },
                "gcs": {
                    "bucket": os.getenv("MOT_GCS_BUCKET"),
                    "prefix": os.getenv("MOT_GCS_PREFIX", "mot-ingestion"),
                    "project_id": os.getenv("MOT_GCS_PROJECT"),
                },
                "schema": [],
                "dry_run": os.getenv("MOT_DRY_RUN", "false").lower() == "true",
                "log_level": os.getenv("MOT_LOG_LEVEL", "INFO"),
            }
        )
