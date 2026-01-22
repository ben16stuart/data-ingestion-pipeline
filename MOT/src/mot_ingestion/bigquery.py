"""BigQuery operations."""

import logging
from typing import Literal

from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, SourceFormat, WriteDisposition

from mot_ingestion.config import SchemaField

logger = logging.getLogger(__name__)


class BigQueryLoader:
    """Loads data into BigQuery from GCS."""

    def __init__(
        self,
        project_id: str,
        dataset: str,
        table: str,
        location: str = "US",
    ):
        """Initialize BigQuery loader.

        Args:
            project_id: GCP project ID
            dataset: BigQuery dataset name
            table: BigQuery table name
            location: BigQuery location
        """
        self.project_id = project_id
        self.dataset = dataset
        self.table = table
        self.location = location
        self.client = bigquery.Client(project=project_id, location=location)
        self.table_ref = f"{project_id}.{dataset}.{table}"

    def load_from_gcs(
        self,
        gcs_uri: str,
        schema: list[SchemaField],
        source_format: Literal["parquet", "csv"] = "parquet",
    ) -> str:
        """Load data from GCS into BigQuery.

        Args:
            gcs_uri: GCS URI (gs://bucket/path)
            schema: Table schema
            source_format: Source file format

        Returns:
            Job ID

        Raises:
            RuntimeError: If load job fails
        """
        logger.info(f"Loading {gcs_uri} into {self.table_ref}")

        bq_schema = self._convert_schema(schema)

        job_config = LoadJobConfig(
            schema=bq_schema,
            source_format=SourceFormat.PARQUET
            if source_format == "parquet"
            else SourceFormat.CSV,
            write_disposition=WriteDisposition.WRITE_APPEND,
            autodetect=False,
        )

        if source_format == "csv":
            job_config.skip_leading_rows = 1

        try:
            load_job = self.client.load_table_from_uri(
                gcs_uri, self.table_ref, job_config=job_config
            )

            load_job.result()

            logger.info(
                f"Loaded {load_job.output_rows} rows into {self.table_ref} (job: {load_job.job_id})"
            )
            return load_job.job_id

        except Exception as e:
            logger.error(f"BigQuery load job failed: {e}")
            raise RuntimeError(f"BigQuery load job failed: {e}") from e

    def _convert_schema(self, schema: list[SchemaField]) -> list[bigquery.SchemaField]:
        """Convert internal schema to BigQuery schema.

        Args:
            schema: Internal schema definition

        Returns:
            BigQuery schema fields
        """
        bq_schema = []
        for field in schema:
            bq_schema.append(
                bigquery.SchemaField(
                    name=field.name,
                    field_type=field.type,
                    mode=field.mode,
                )
            )

        bq_schema.extend(
            [
                bigquery.SchemaField("source_file", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("checksum", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("ingest_ts", "TIMESTAMP", mode="NULLABLE"),
            ]
        )

        return bq_schema

    def table_exists(self) -> bool:
        """Check if target table exists.

        Returns:
            True if table exists
        """
        try:
            self.client.get_table(self.table_ref)
            return True
        except Exception:
            return False

    def create_table(self, schema: list[SchemaField]) -> None:
        """Create target table if it doesn't exist.

        Args:
            schema: Table schema
        """
        if self.table_exists():
            logger.info(f"Table {self.table_ref} already exists")
            return

        bq_schema = self._convert_schema(schema)
        table = bigquery.Table(self.table_ref, schema=bq_schema)

        try:
            self.client.create_table(table)
            logger.info(f"Created table {self.table_ref}")
        except Exception as e:
            logger.error(f"Failed to create table {self.table_ref}: {e}")
            raise
