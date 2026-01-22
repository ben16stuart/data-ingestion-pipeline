"""CLI entrypoint for MOT ingestion pipeline."""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

from mot_ingestion.config import IngestionConfig
from mot_ingestion.pipeline import IngestionPipeline


class JSONFormatter(logging.Formatter):
    """Custom JSON formatter using stdlib only."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON.

        Args:
            record: Log record

        Returns:
            JSON formatted string
        """
        log_data = {
            "timestamp": datetime.fromtimestamp(record.created, tz=None).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "name": record.name,
            "levelname": record.levelname,
            "message": record.getMessage(),
        }

        if record.exc_info:
            log_data["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(log_data)


def setup_logging(level: str = "INFO", json_format: bool = True) -> None:
    """Configure structured logging.

    Args:
        level: Log level
        json_format: Use JSON formatter
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    handler = logging.StreamHandler(sys.stdout)

    if json_format:
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

    handler.setFormatter(formatter)
    root_logger.addHandler(handler)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="MOT Ingestion Pipeline - XLSX to BigQuery ETL",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--config",
        type=Path,
        help="Path to YAML configuration file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform a dry run without loading data",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level",
    )
    parser.add_argument(
        "--json-logs",
        action="store_true",
        default=True,
        help="Output logs in JSON format",
    )

    return parser.parse_args()


def main() -> int:
    """Main entrypoint.

    Returns:
        Exit code
    """
    args = parse_args()

    setup_logging(level=args.log_level, json_format=args.json_logs)
    logger = logging.getLogger(__name__)

    try:
        if args.config:
            logger.info(f"Loading configuration from {args.config}")
            config = IngestionConfig.from_yaml(args.config)
        else:
            logger.info("Loading configuration from environment variables")
            config = IngestionConfig.from_env()

        if args.dry_run:
            config.dry_run = True

        if config.log_level:
            logging.getLogger().setLevel(config.log_level)

        pipeline = IngestionPipeline(config)
        exit_code = pipeline.run()

        return exit_code

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 3


if __name__ == "__main__":
    sys.exit(main())
