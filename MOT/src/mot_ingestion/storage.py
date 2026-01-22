"""Google Cloud Storage operations."""

import logging
from pathlib import Path

from google.cloud import storage
from google.cloud.storage import Blob

logger = logging.getLogger(__name__)


class GCSUploader:
    """Uploads files to Google Cloud Storage."""

    def __init__(self, bucket_name: str, prefix: str = "", project_id: str | None = None):
        """Initialize GCS uploader.

        Args:
            bucket_name: GCS bucket name
            prefix: Object key prefix
            project_id: GCP project ID (optional)
        """
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.project_id = project_id
        self.client = storage.Client(project=project_id)
        self.bucket = self.client.bucket(bucket_name)

    def upload(self, local_path: Path, blob_name: str | None = None) -> str:
        """Upload file to GCS with resumable upload.

        Args:
            local_path: Local file path
            blob_name: Destination blob name (defaults to local filename)

        Returns:
            GCS URI (gs://bucket/path)
        """
        if blob_name is None:
            blob_name = local_path.name

        if self.prefix:
            blob_name = f"{self.prefix}/{blob_name}"

        blob: Blob = self.bucket.blob(blob_name)

        try:
            logger.info(f"Uploading {local_path} to gs://{self.bucket_name}/{blob_name}")

            blob.upload_from_filename(
                str(local_path),
                timeout=300,
            )

            gcs_uri = f"gs://{self.bucket_name}/{blob_name}"
            logger.info(f"Upload complete: {gcs_uri}")
            return gcs_uri

        except Exception as e:
            logger.error(f"Failed to upload {local_path} to GCS: {e}")
            raise

    def list_blobs(self, prefix: str | None = None) -> list[str]:
        """List blobs in bucket with optional prefix.

        Args:
            prefix: Blob name prefix filter

        Returns:
            List of blob names
        """
        search_prefix = f"{self.prefix}/{prefix}" if prefix else self.prefix
        blobs = self.client.list_blobs(self.bucket_name, prefix=search_prefix)
        return [blob.name for blob in blobs]
