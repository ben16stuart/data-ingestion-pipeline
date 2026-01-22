"""File checksum computation."""

import hashlib
import logging
from pathlib import Path
from typing import Literal

logger = logging.getLogger(__name__)


class ChecksumCalculator:
    """Calculates file checksums for change detection."""

    CHUNK_SIZE = 65536  # 64KB

    def __init__(self, algorithm: Literal["md5", "sha256"] = "sha256"):
        """Initialize checksum calculator.

        Args:
            algorithm: Hash algorithm to use
        """
        self.algorithm = algorithm

    def calculate(self, file_path: Path) -> str:
        """Calculate file checksum.

        Args:
            file_path: Path to file

        Returns:
            Hexadecimal checksum string
        """
        if self.algorithm == "md5":
            hasher = hashlib.md5()
        elif self.algorithm == "sha256":
            hasher = hashlib.sha256()
        else:
            raise ValueError(f"Unsupported algorithm: {self.algorithm}")

        try:
            with open(file_path, "rb") as f:
                while chunk := f.read(self.CHUNK_SIZE):
                    hasher.update(chunk)

            checksum = hasher.hexdigest()
            logger.debug(f"Calculated {self.algorithm} checksum for {file_path}: {checksum}")
            return checksum

        except Exception as e:
            logger.error(f"Failed to calculate checksum for {file_path}: {e}")
            raise
