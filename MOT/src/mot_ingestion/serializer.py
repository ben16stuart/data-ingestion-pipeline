"""Data serialization to Parquet and CSV."""

import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import Literal

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


class DataSerializer:
    """Serializes data rows to Parquet or CSV format."""

    def __init__(
        self,
        output_dir: Path,
        format: Literal["parquet", "csv"] = "parquet",
        partition_by_date: bool = True,
    ):
        """Initialize data serializer.

        Args:
            output_dir: Output directory for serialized files
            format: Output format (parquet or csv)
            partition_by_date: Whether to partition by ingest date
        """
        self.output_dir = output_dir
        self.format = format
        self.partition_by_date = partition_by_date
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def serialize(self, data: list[dict[str, any]], file_stem: str) -> Path:
        """Serialize data to file.

        Args:
            data: List of dictionaries to serialize
            file_stem: Base filename (without extension)

        Returns:
            Path to serialized file
        """
        if self.partition_by_date:
            date_str = datetime.utcnow().strftime("%Y%m%d")
            output_subdir = self.output_dir / f"ingest_date={date_str}"
            output_subdir.mkdir(parents=True, exist_ok=True)
        else:
            output_subdir = self.output_dir

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"{file_stem}_{timestamp}.{self.format}"
        output_path = output_subdir / filename

        try:
            if self.format == "parquet":
                self._write_parquet(data, output_path)
            elif self.format == "csv":
                self._write_csv(data, output_path)
            else:
                raise ValueError(f"Unsupported format: {self.format}")

            logger.info(f"Serialized {len(data)} rows to {output_path}")
            return output_path

        except Exception as e:
            logger.error(f"Failed to serialize to {output_path}: {e}")
            raise

    def _write_parquet(self, data: list[dict[str, any]], output_path: Path) -> None:
        """Write data to Parquet file using pyarrow.

        Args:
            data: List of dictionaries
            output_path: Output file path
        """
        if not data:
            # Create empty table with no schema
            table = pa.table({})
        else:
            # Convert list of dicts to PyArrow table
            table = pa.Table.from_pylist(data)

        # Write with snappy compression
        pq.write_table(table, output_path, compression="snappy")

    def _write_csv(self, data: list[dict[str, any]], output_path: Path) -> None:
        """Write data to CSV file using csv module.

        Args:
            data: List of dictionaries
            output_path: Output file path
        """
        if not data:
            # Create empty file
            output_path.touch()
            return

        # Get field names from first row
        fieldnames = list(data[0].keys())

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
