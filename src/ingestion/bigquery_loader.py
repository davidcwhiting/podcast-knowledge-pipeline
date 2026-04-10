"""Load raw ingestion data into BigQuery staging tables."""

import os
from datetime import datetime

from google.cloud import bigquery


def get_bq_client():
    """Get an authenticated BigQuery client."""
    return bigquery.Client(project=os.environ["GCP_PROJECT_ID"])


def get_dataset_ref() -> str:
    """Return the fully qualified dataset reference."""
    project = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ["BQ_DATASET"]
    return f"{project}.{dataset}"


def ensure_staging_tables():
    """Create staging tables if they don't exist."""
    client = get_bq_client()
    dataset_ref = get_dataset_ref()

    stg_videos_schema = [
        bigquery.SchemaField("video_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("published_at", "TIMESTAMP"),
        bigquery.SchemaField("channel_id", "STRING"),
        bigquery.SchemaField("channel_title", "STRING"),
        bigquery.SchemaField("view_count", "INTEGER"),
        bigquery.SchemaField("duration", "STRING"),
        bigquery.SchemaField("gcs_uri", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]

    stg_transcripts_schema = [
        bigquery.SchemaField("video_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("transcript_text", "STRING"),
        bigquery.SchemaField("segment_count", "INTEGER"),
        bigquery.SchemaField("has_transcript", "BOOLEAN"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]

    for table_name, schema in [
        ("stg_videos", stg_videos_schema),
        ("stg_transcripts", stg_transcripts_schema),
    ]:
        table_ref = f"{dataset_ref}.{table_name}"
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table, exists_ok=True)


def get_high_watermark(channel_id: str) -> datetime | None:
    """Get the most recent published_at timestamp for a channel.

    Returns None if no videos have been ingested for this channel.
    """
    client = get_bq_client()
    dataset_ref = get_dataset_ref()

    query = f"""
        SELECT MAX(published_at) as max_published
        FROM `{dataset_ref}.stg_videos`
        WHERE channel_id = @channel_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("channel_id", "STRING", channel_id),
        ]
    )

    result = client.query(query, job_config=job_config).result()
    row = next(iter(result), None)
    if row and row.max_published:
        return row.max_published
    return None


def load_video_metadata(video: dict, gcs_uri: str):
    """Insert or update a video record in stg_videos (upsert on video_id)."""
    client = get_bq_client()
    dataset_ref = get_dataset_ref()

    # Use MERGE for idempotent upserts
    query = f"""
        MERGE `{dataset_ref}.stg_videos` AS target
        USING (SELECT @video_id AS video_id) AS source
        ON target.video_id = source.video_id
        WHEN MATCHED THEN UPDATE SET
            title = @title,
            view_count = @view_count,
            gcs_uri = @gcs_uri,
            ingested_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            video_id, title, description, published_at, channel_id,
            channel_title, view_count, duration, gcs_uri, ingested_at
        ) VALUES (
            @video_id, @title, @description, @published_at, @channel_id,
            @channel_title, @view_count, @duration, @gcs_uri, CURRENT_TIMESTAMP()
        )
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("video_id", "STRING", video["video_id"]),
            bigquery.ScalarQueryParameter("title", "STRING", video["title"]),
            bigquery.ScalarQueryParameter("description", "STRING", video.get("description", "")),
            bigquery.ScalarQueryParameter("published_at", "TIMESTAMP", video["published_at"]),
            bigquery.ScalarQueryParameter("channel_id", "STRING", video["channel_id"]),
            bigquery.ScalarQueryParameter("channel_title", "STRING", video["channel_title"]),
            bigquery.ScalarQueryParameter("view_count", "INT64", video.get("view_count", 0)),
            bigquery.ScalarQueryParameter("duration", "STRING", video.get("duration", "")),
            bigquery.ScalarQueryParameter("gcs_uri", "STRING", gcs_uri),
        ]
    )

    client.query(query, job_config=job_config).result()


def load_transcript(video_id: str, transcript_text: str | None, segment_count: int):
    """Insert or update a transcript record in stg_transcripts."""
    client = get_bq_client()
    dataset_ref = get_dataset_ref()

    query = f"""
        MERGE `{dataset_ref}.stg_transcripts` AS target
        USING (SELECT @video_id AS video_id) AS source
        ON target.video_id = source.video_id
        WHEN MATCHED THEN UPDATE SET
            transcript_text = @transcript_text,
            segment_count = @segment_count,
            has_transcript = @has_transcript,
            ingested_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            video_id, transcript_text, segment_count, has_transcript, ingested_at
        ) VALUES (
            @video_id, @transcript_text, @segment_count, @has_transcript, CURRENT_TIMESTAMP()
        )
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("video_id", "STRING", video_id),
            bigquery.ScalarQueryParameter("transcript_text", "STRING", transcript_text or ""),
            bigquery.ScalarQueryParameter("segment_count", "INT64", segment_count),
            bigquery.ScalarQueryParameter("has_transcript", "BOOL", transcript_text is not None),
        ]
    )

    client.query(query, job_config=job_config).result()
