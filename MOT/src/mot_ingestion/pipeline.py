"""Main ingestion pipeline orchestration."""

import logging
from pathlib import Path

from mot_ingestion.bigquery import BigQueryLoader
from mot_ingestion.checksum import ChecksumCalculator
from mot_ingestion.config import IngestionConfig
from mot_ingestion.discovery import FileDiscoverer
from mot_ingestion.parser import XLSXParser
from mot_ingestion.schema import SchemaNormalizer
from mot_ingestion.serializer import DataSerializer
from mot_ingestion.state import FileRegistry
from mot_ingestion.storage import GCSUploader

logger = logging.getLogger(__name__)


class IngestionPipeline:
    """Orchestrates the end-to-end ingestion pipeline."""

    def __init__(self, config: IngestionConfig):
        """Initialize pipeline with configuration.

        Args:
            config: Pipeline configuration
        """
        self.config = config

        self.discoverer = FileDiscoverer(
            root_dir=config.input_directory,
            pattern=config.file_pattern,
            ignore_patterns=config.ignore_patterns,
        )
        self.checksum_calc = ChecksumCalculator(algorithm=config.checksum_algorithm)
        self.parser = XLSXParser(sheet_name=config.sheet_name)
        self.normalizer = SchemaNormalizer(schema=config.schema)
        self.serializer = DataSerializer(
            output_dir=config.temp_directory,
            format=config.output_format,
        )
        self.uploader = GCSUploader(
            bucket_name=config.gcs.bucket,
            prefix=config.gcs.prefix,
            project_id=config.gcs.project_id,
        )
        self.bq_loader = BigQueryLoader(
            project_id=config.bigquery.project_id,
            dataset=config.bigquery.dataset,
            table=config.bigquery.table,
            location=config.bigquery.location,
        )
        self.registry = FileRegistry(
            project_id=config.bigquery.project_id,
            dataset=config.bigquery.dataset,
            table=config.bigquery.registry_table,
        )

    def run(self) -> int:
        """Execute the ingestion pipeline.

        Returns:
            Exit code (0 for success, non-zero for failure)
        """
        try:
            logger.info("Starting MOT ingestion pipeline")

            self.registry.ensure_registry_exists()

            if not self.bq_loader.table_exists():
                logger.info("Creating target table")
                self.bq_loader.create_table(self.config.schema)

            files = self.discoverer.discover()
            if not files:
                logger.warning("No files discovered")
                return 0

            processed_count = 0
            skipped_count = 0
            failed_count = 0

            for file_path in files:
                try:
                    if self._process_file(file_path):
                        processed_count += 1
                    else:
                        skipped_count += 1
                except Exception as e:
                    logger.error(f"Failed to process {file_path}: {e}", exc_info=True)
                    failed_count += 1

            logger.info(
                f"Pipeline complete: {processed_count} processed, "
                f"{skipped_count} skipped, {failed_count} failed"
            )

            return 1 if failed_count > 0 else 0

        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            return 2

    def _process_file(self, file_path: Path) -> bool:
        """Process a single file.

        Args:
            file_path: Path to file

        Returns:
            True if processed, False if skipped
        """
        logger.info(f"Processing {file_path}")

        checksum = self.checksum_calc.calculate(file_path)

        if not self.registry.should_process(file_path, checksum):
            return False

        if self.config.dry_run:
            logger.info(f"[DRY RUN] Would process {file_path}")
            return True

        try:
            data = self.parser.parse(file_path)

            data_normalized = self.normalizer.normalize(
                data=data,
                source_file=file_path,
                checksum=checksum,
            )

            serialized_path = self.serializer.serialize(
                data=data_normalized,
                file_stem=file_path.stem,
            )

            gcs_uri = self.uploader.upload(serialized_path)

            job_id = self.bq_loader.load_from_gcs(
                gcs_uri=gcs_uri,
                schema=self.config.schema,
                source_format=self.config.output_format,
            )

            self.registry.record_processing(
                file_name=file_path.name,
                checksum=checksum,
                status="SUCCESS",
                row_count=len(data_normalized),
            )

            logger.info(f"Successfully processed {file_path} (job: {job_id})")
            return True

        except Exception as e:
            self.registry.record_processing(
                file_name=file_path.name,
                checksum=checksum,
                status="FAILED",
                error_message=str(e),
            )
            raise
