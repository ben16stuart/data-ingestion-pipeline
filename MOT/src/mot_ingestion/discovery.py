"""File discovery utilities."""

import fnmatch
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class FileDiscoverer:
    """Discovers XLSX files in a directory tree."""

    def __init__(self, root_dir: Path, pattern: str = "**/*.xlsx", ignore_patterns: list[str] | None = None):
        """Initialize file discoverer.

        Args:
            root_dir: Root directory to search
            pattern: Glob pattern for file matching
            ignore_patterns: List of patterns to ignore (e.g., temp files)
        """
        self.root_dir = root_dir
        self.pattern = pattern
        self.ignore_patterns = ignore_patterns or ["~$*", ".~*", "*.tmp", "*.temp"]

    def discover(self) -> list[Path]:
        """Discover all matching files.

        Returns:
            List of discovered file paths
        """
        if not self.root_dir.exists():
            logger.error(f"Root directory does not exist: {self.root_dir}")
            return []

        discovered = []
        for file_path in self.root_dir.glob(self.pattern):
            if not file_path.is_file():
                continue

            if self._should_ignore(file_path):
                logger.debug(f"Ignoring file: {file_path}")
                continue

            discovered.append(file_path)

        logger.info(f"Discovered {len(discovered)} files in {self.root_dir}")
        return sorted(discovered)

    def _should_ignore(self, file_path: Path) -> bool:
        """Check if file should be ignored based on ignore patterns.

        Args:
            file_path: File path to check

        Returns:
            True if file should be ignored
        """
        file_name = file_path.name
        return any(fnmatch.fnmatch(file_name, pattern) for pattern in self.ignore_patterns)
