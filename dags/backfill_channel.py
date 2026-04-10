"""Backfill DAG: process entire history of a single channel.

Triggered manually with a channel_name parameter. Used when adding a new channel
to the system or re-processing an existing channel's full history.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.ingestion.youtube_client import (
    CHANNELS,
    get_video_transcript,
    get_channel_videos,
    transcript_to_text,
)
from src.ingestion.gcs_writer import write_raw_video
from src.ingestion.bigquery_loader import (
    ensure_staging_tables,
    load_video_metadata,
    load_transcript,
)

logger = logging.getLogger(__name__)

default_args = {
    "owner": "david",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def backfill(channel_name: str, max_videos: int = 200, **kwargs):
    """Backfill all videos for a channel (no watermark — fetches from the beginning)."""
    max_videos = int(max_videos)  # Jinja renders params as strings
    if channel_name not in CHANNELS:
        raise ValueError(
            f"Unknown channel: {channel_name}. "
            f"Available: {list(CHANNELS.keys())}"
        )

    channel_id = CHANNELS[channel_name]
    ensure_staging_tables()

    logger.info("Backfilling channel %s (max %d videos)", channel_name, max_videos)

    videos = get_channel_videos(
        channel_id, published_after=None, max_results=max_videos
    )
    logger.info("Channel %s: found %d videos", channel_name, len(videos))

    ingested_count = 0
    skipped_count = 0

    for video in videos:
        video_id = video["video_id"]

        segments = get_video_transcript(video_id)
        if segments is None:
            logger.warning("Video %s: no transcript available", video_id)
            skipped_count += 1
            load_transcript(video_id, None, 0)
            load_video_metadata(video, "")
            continue

        transcript_text = transcript_to_text(segments)

        raw_data = {
            "metadata": video,
            "transcript_segments": segments,
            "transcript_text": transcript_text,
        }
        gcs_uri = write_raw_video(channel_name, video_id, raw_data)

        load_video_metadata(video, gcs_uri)
        load_transcript(video_id, transcript_text, len(segments))

        ingested_count += 1
        if ingested_count % 10 == 0:
            logger.info("Progress: %d/%d videos ingested", ingested_count, len(videos))

    logger.info(
        "Backfill complete for %s: ingested %d, skipped %d",
        channel_name,
        ingested_count,
        skipped_count,
    )


with DAG(
    dag_id="backfill_channel",
    default_args=default_args,
    description="Backfill entire history for a single podcast channel",
    schedule=None,  # Manual trigger only
    start_date=datetime(2026, 4, 10),
    catchup=False,
    tags=["podcast", "backfill"],
    params={"channel_name": "lex_fridman", "max_videos": 200},
) as dag:

    backfill_task = PythonOperator(
        task_id="backfill_channel",
        python_callable=backfill,
        op_kwargs={
            "channel_name": "{{ params.channel_name }}",
            "max_videos": "{{ params.max_videos }}",
        },
    )
