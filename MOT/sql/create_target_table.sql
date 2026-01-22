CREATE TABLE IF NOT EXISTS `your-gcp-project-id.your_dataset.your_table` (
  id INT64 NOT NULL,
  name STRING,
  email STRING,
  created_date DATE,
  amount FLOAT64,
  active BOOL,
  source_file STRING,
  checksum STRING,
  ingest_ts TIMESTAMP
)
PARTITION BY DATE(ingest_ts)
CLUSTER BY source_file
OPTIONS(
  description="Target table for MOT ingestion pipeline"
);
