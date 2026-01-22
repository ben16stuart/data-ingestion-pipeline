"""XLSX file parsing."""

import csv
import io
import logging
from pathlib import Path

from xlsx2csv import Xlsx2csv

logger = logging.getLogger(__name__)


class XLSXParser:
    """Parses XLSX files using xlsx2csv."""

    def __init__(self, sheet_name: str | int = 0):
        """Initialize XLSX parser.

        Args:
            sheet_name: Sheet name or index to parse (0-indexed for numeric)
        """
        self.sheet_name = sheet_name

    def parse(self, file_path: Path) -> list[dict[str, str]]:
        """Parse XLSX file into list of dictionaries.

        All values are read as strings for consistent processing.

        Args:
            file_path: Path to XLSX file

        Returns:
            List of dictionaries where keys are column headers

        Raises:
            ValueError: If file cannot be parsed
        """
        try:
            logger.info(f"Parsing {file_path}, sheet={self.sheet_name}")

            # Convert XLSX to CSV in memory
            csv_buffer = io.StringIO()
            converter = Xlsx2csv(str(file_path), outputencoding="utf-8")

            # Convert sheet name to sheet index if string
            if isinstance(self.sheet_name, str):
                # Get list of sheet names
                sheet_names = converter.workbook.sheets
                try:
                    sheet_id = sheet_names.index(self.sheet_name)
                except ValueError:
                    raise ValueError(f"Sheet '{self.sheet_name}' not found in {file_path}")
            else:
                sheet_id = self.sheet_name

            # Convert to CSV
            converter.convert(csv_buffer, sheetid=sheet_id + 1)  # xlsx2csv uses 1-indexed sheets
            csv_buffer.seek(0)

            # Read CSV using built-in csv module
            reader = csv.DictReader(csv_buffer)
            data = list(reader)

            if not data:
                logger.warning(f"No data rows found in {file_path}")
                return []

            columns = list(data[0].keys()) if data else []
            logger.info(f"Parsed {len(data)} rows, {len(columns)} columns from {file_path}")
            return data

        except Exception as e:
            logger.error(f"Failed to parse {file_path}: {e}")
            raise ValueError(f"Failed to parse {file_path}: {e}") from e
