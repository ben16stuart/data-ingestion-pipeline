"""File processing state management."""

import logging
from datetime import datetime
from pathlib import Path
from typing import Literal

from google.cloud import bigquery

logger = logging.getLogger(__name__)


class FileRegistry:
    """Manages file processing state in BigQuery."""

    def __init__(self, project_id: str, dataset: str, table: str = "file_registry"):
        """Initialize file registry.

        Args:
            project_id: GCP project ID
            dataset: BigQuery dataset
            table: Registry table name
        """
        self.project_id = project_id
        self.dataset = dataset
        self.table = table
        self.client = bigquery.Client(project=project_id)
        self.table_ref = f"{project_id}.{dataset}.{table}"

    def ensure_registry_exists(self) -> None:
        """Ensure registry table exists, create if missing."""
        schema = [
            bigquery.SchemaField("file_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("checksum", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("processed_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("row_count", "INTEGER", mode="NULLABLE"),
        ]

        try:
            self.client.get_table(self.table_ref)
            logger.debug(f"Registry table {self.table_ref} exists")
        except Exception:
            logger.info(f"Creating registry table {self.table_ref}")
            table = bigquery.Table(self.table_ref, schema=schema)
            self.client.create_table(table)

    def get_checksum(self, file_name: str) -> str | None:
        """Get stored checksum for a file.

        Args:
            file_name: File name to look up

        Returns:
            Checksum if found, None otherwise
        """
        query = f"""
            SELECT checksum
            FROM `{self.table_ref}`
            WHERE file_name = @file_name
              AND status = 'SUCCESS'
            ORDER BY processed_at DESC
            LIMIT 1
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("file_name", "STRING", file_name),
            ]
        )

        try:
            result = self.client.query(query, job_config=job_config).result()
            row = next(iter(result), None)
            return row.checksum if row else None
        except Exception as e:
            logger.warning(f"Failed to query checksum for {file_name}: {e}")
            return None

    def record_processing(
        self,
        file_name: str,
        checksum: str,
        status: Literal["SUCCESS", "FAILED"],
        error_message: str | None = None,
        row_count: int | None = None,
    ) -> None:
        """Record file processing status.

        Args:
            file_name: File name
            checksum: File checksum
            status: Processing status
            error_message: Error message if failed
            row_count: Number of rows processed
        """
        row = {
            "file_name": file_name,
            "checksum": checksum,
            "processed_at": datetime.utcnow().isoformat(),
            "status": status,
            "error_message": error_message,
            "row_count": row_count,
        }

        try:
            errors = self.client.insert_rows_json(self.table_ref, [row])
            if errors:
                logger.error(f"Failed to record processing state: {errors}")
            else:
                logger.debug(f"Recorded {status} for {file_name}")
        except Exception as e:
            logger.error(f"Failed to insert registry row: {e}")

    def should_process(self, file_path: Path, checksum: str) -> bool:
        """Determine if file should be processed based on checksum.

        Args:
            file_path: File path
            checksum: Current file checksum

        Returns:
            True if file should be processed
        """
        stored_checksum = self.get_checksum(file_path.name)

        if stored_checksum is None:
            logger.info(f"File {file_path.name} not in registry, will process")
            return True

        if stored_checksum != checksum:
            logger.info(f"File {file_path.name} checksum changed, will process")
            return True

        logger.info(f"File {file_path.name} unchanged, skipping")
        return False
