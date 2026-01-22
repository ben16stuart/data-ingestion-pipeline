"""Schema normalization and validation."""

import logging
from datetime import datetime
from pathlib import Path

from mot_ingestion.config import SchemaField

logger = logging.getLogger(__name__)


class SchemaNormalizer:
    """Normalizes data rows to canonical schema."""

    def __init__(self, schema: list[SchemaField]):
        """Initialize schema normalizer.

        Args:
            schema: Target schema definition
        """
        self.schema = schema
        self.field_map = {field.name: field for field in schema}

    def normalize(
        self, data: list[dict[str, str]], source_file: Path, checksum: str
    ) -> list[dict[str, any]]:
        """Normalize data rows to target schema.

        Steps:
        1. Drop unknown columns
        2. Add missing columns as NULL
        3. Reorder columns
        4. Cast to target types
        5. Append audit fields

        Args:
            data: Input list of dictionaries
            source_file: Source file path
            checksum: File checksum

        Returns:
            Normalized list of dictionaries
        """
        if not data:
            return []

        target_columns = [field.name for field in self.schema]

        # Process each row
        normalized_data = []
        for row in data:
            # Drop unknown columns
            unknown_columns = set(row.keys()) - set(target_columns)
            if unknown_columns:
                logger.debug(f"Dropping unknown columns from row: {unknown_columns}")

            # Create new row with only target columns
            normalized_row = {}
            for field in self.schema:
                col_name = field.name
                raw_value = row.get(col_name, "")

                # Cast to target type
                normalized_row[col_name] = self._cast_value(raw_value, field.type, col_name)

            # Add audit fields
            normalized_row["source_file"] = str(source_file)
            normalized_row["checksum"] = checksum
            normalized_row["ingest_ts"] = datetime.utcnow().isoformat()

            normalized_data.append(normalized_row)

        column_count = len(target_columns) + 3  # schema columns + 3 audit fields
        logger.info(f"Normalized to {len(normalized_data)} rows, {column_count} columns")
        return normalized_data

    def _cast_value(self, value: str, target_type: str, column_name: str) -> any:
        """Cast a single value to target type.

        Args:
            value: Input string value
            target_type: Target type (STRING, INTEGER, FLOAT, BOOLEAN, DATE, TIMESTAMP)
            column_name: Column name for logging

        Returns:
            Casted value or None on error
        """
        if value == "" or value is None:
            return None

        try:
            if target_type == "STRING":
                return str(value)
            elif target_type == "INTEGER":
                return int(float(value)) if value else None
            elif target_type == "FLOAT":
                return float(value) if value else None
            elif target_type == "BOOLEAN":
                return value.lower() in ("true", "1", "yes", "y") if value else None
            elif target_type == "DATE":
                # Keep as string in ISO format for now
                return value if value else None
            elif target_type == "TIMESTAMP":
                # Keep as string in ISO format for now
                return value if value else None
            else:
                logger.warning(f"Unknown type {target_type} for column {column_name}")
                return value
        except (ValueError, TypeError) as e:
            logger.debug(f"Failed to cast '{value}' to {target_type} for column {column_name}: {e}")
            return None
