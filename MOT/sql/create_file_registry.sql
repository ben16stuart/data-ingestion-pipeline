CREATE TABLE IF NOT EXISTS `your-gcp-project-id.your_dataset.file_registry` (
  file_name STRING NOT NULL,
  checksum STRING NOT NULL,
  processed_at TIMESTAMP NOT NULL,
  status STRING NOT NULL,
  error_message STRING,
  row_count INT64
)
CLUSTER BY file_name, processed_at
OPTIONS(
  description="File processing registry for MOT ingestion pipeline"
);
