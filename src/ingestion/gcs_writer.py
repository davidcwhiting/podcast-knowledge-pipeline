"""Write raw ingestion data to Google Cloud Storage."""

import json
import os

from google.cloud import storage


def get_gcs_client():
    """Get an authenticated GCS client."""
    return storage.Client(project=os.environ["GCP_PROJECT_ID"])


def write_raw_video(channel_name: str, video_id: str, data: dict) -> str:
    """Write raw video data (metadata + transcript) to GCS as JSON.

    Returns the GCS URI of the written object.
    """
    client = get_gcs_client()
    bucket_name = os.environ["GCS_BUCKET_NAME"]
    bucket = client.bucket(bucket_name)

    blob_path = f"raw/{channel_name}/{video_id}.json"
    blob = bucket.blob(blob_path)
    blob.upload_from_string(
        json.dumps(data, ensure_ascii=False, indent=2),
        content_type="application/json",
    )

    return f"gs://{bucket_name}/{blob_path}"
