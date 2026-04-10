"""Main pipeline DAG: ingest new YouTube videos, extract knowledge, model, embed.

Runs daily. For each configured channel, fetches videos published since the last
successful run, downloads transcripts, stages to BigQuery, and (in later phases)
extracts entities, runs DBT models, and generates embeddings.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
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
    get_high_watermark,
    load_video_metadata,
    load_transcript,
)
from src.extraction.chunker import chunk_transcript
from src.extraction.extractor import extract_episode
from src.extraction.merger import merge_extractions
from src.extraction.bigquery_loader import (
    ensure_extraction_tables,
    load_extraction,
)

logger = logging.getLogger(__name__)

default_args = {
    "owner": "david",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def ingest_channel(channel_name: str, channel_id: str, **kwargs):
    """Fetch new videos for a single channel and stage to BigQuery."""
    ensure_staging_tables()

    # Get high watermark for incremental loading
    watermark = get_high_watermark(channel_id)
    logger.info(
        "Channel %s: fetching videos after %s",
        channel_name,
        watermark or "beginning of time",
    )

    videos = get_channel_videos(channel_id, published_after=watermark)
    logger.info("Channel %s: found %d new videos", channel_name, len(videos))

    ingested_count = 0
    skipped_count = 0

    for video in videos:
        video_id = video["video_id"]

        # Download transcript
        segments = get_video_transcript(video_id)
        if segments is None:
            logger.warning("Video %s: no transcript available, skipping", video_id)
            skipped_count += 1
            # Still record that we tried (so we don't retry endlessly)
            load_transcript(video_id, None, 0)
            load_video_metadata(video, "")
            continue

        transcript_text = transcript_to_text(segments)

        # Write raw data to GCS
        raw_data = {
            "metadata": video,
            "transcript_segments": segments,
            "transcript_text": transcript_text,
        }
        gcs_uri = write_raw_video(channel_name, video_id, raw_data)

        # Stage to BigQuery
        load_video_metadata(video, gcs_uri)
        load_transcript(video_id, transcript_text, len(segments))

        ingested_count += 1
        logger.info("Video %s (%s): ingested", video_id, video["title"][:60])

    logger.info(
        "Channel %s: ingested %d videos, skipped %d (no transcript)",
        channel_name,
        ingested_count,
        skipped_count,
    )


with DAG(
    dag_id="ingest_and_extract",
    default_args=default_args,
    description="Ingest new YouTube podcast videos, extract knowledge, model, embed",
    schedule="@daily",
    start_date=datetime(2026, 4, 10),
    catchup=False,
    tags=["podcast", "ingestion", "extraction"],
) as dag:

    # Create one ingestion task per channel (they run in parallel)
    ingest_tasks = []
    for channel_name, channel_id in CHANNELS.items():
        task = PythonOperator(
            task_id=f"ingest_{channel_name}",
            python_callable=ingest_channel,
            op_kwargs={"channel_name": channel_name, "channel_id": channel_id},
        )
        ingest_tasks.append(task)

    def extract_new_episodes(**kwargs):
        """Extract structured entities from any unprocessed transcripts."""
        from google.cloud import bigquery
        import os

        ensure_extraction_tables()
        client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
        proj = os.environ["GCP_PROJECT_ID"]
        ds = os.environ["BQ_DATASET"]

        # Find videos with transcripts that haven't been extracted yet
        query = f"""
            SELECT v.video_id, t.transcript_text, v.title, v.channel_title
            FROM `{proj}.{ds}.stg_transcripts` t
            JOIN `{proj}.{ds}.stg_videos` v ON t.video_id = v.video_id
            LEFT JOIN `{proj}.{ds}.raw_claims` c ON t.video_id = c.video_id
            WHERE t.has_transcript = true AND c.video_id IS NULL
        """
        rows = list(client.query(query).result())
        logger.info("Found %d episodes to extract", len(rows))

        for row in rows:
            logger.info("Extracting: %s", row.title[:60])
            chunks = chunk_transcript(row.transcript_text)

            chunk_results = extract_episode(
                transcript_text=row.transcript_text,
                chunks=chunks,
                episode_title=row.title,
                channel_title=row.channel_title,
            )

            merged = merge_extractions(chunk_results, row.video_id)
            load_extraction(merged)
            logger.info(
                "Extracted %d guests, %d topics, %d claims",
                len(merged["guests"]),
                len(merged["topics"]),
                len(merged["claims"]),
            )

    extract_task = PythonOperator(
        task_id="extract_entities",
        python_callable=extract_new_episodes,
    )

    # Ingestion tasks run in parallel, then extraction runs after all complete
    ingest_tasks >> extract_task

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/models && "
            "/home/airflow/dbt-venv/bin/python -c \""
            "from dbt.cli.main import dbtRunner; "
            "res = dbtRunner().invoke(['run', '--profiles-dir', '.']); "
            "assert res.success, 'dbt run failed'\""
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/models && "
            "/home/airflow/dbt-venv/bin/python -c \""
            "from dbt.cli.main import dbtRunner; "
            "res = dbtRunner().invoke(['test', '--profiles-dir', '.']); "
            "assert res.success, 'dbt test failed'\""
        ),
    )

    def generate_embeddings(**kwargs):
        """Generate embeddings for any episodes not yet embedded."""
        from src.embedding.embedder import chunk_for_embedding, embed_chunks
        from src.embedding.pgvector_loader import (
            ensure_embeddings_table,
            load_embeddings,
            get_connection,
        )
        from google.cloud import bigquery
        import os

        ensure_embeddings_table()
        client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
        proj = os.environ["GCP_PROJECT_ID"]
        ds = os.environ["BQ_DATASET"]

        # Get already-embedded video IDs
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT video_id FROM transcript_embeddings")
        embedded_ids = {row[0] for row in cur.fetchall()}
        cur.close()
        conn.close()

        # Find videos with transcripts not yet embedded
        query = f"""
            SELECT v.video_id, t.transcript_text, v.title, v.channel_title
            FROM `{proj}.{ds}.stg_transcripts` t
            JOIN `{proj}.{ds}.stg_videos` v ON t.video_id = v.video_id
            WHERE t.has_transcript = true
        """
        rows = list(client.query(query).result())
        to_embed = [r for r in rows if r.video_id not in embedded_ids]
        logger.info("Found %d episodes to embed", len(to_embed))

        for row in to_embed:
            logger.info("Embedding: %s", row.title[:60])
            chunks = chunk_for_embedding(row.transcript_text)
            embedded = embed_chunks(chunks)
            load_embeddings(row.video_id, row.title, row.channel_title, embedded)

    embed_task = PythonOperator(
        task_id="generate_embeddings",
        python_callable=generate_embeddings,
    )

    # Ingest (parallel) >> Extract >> DBT run >> DBT test >> Embed
    ingest_tasks >> extract_task >> dbt_run >> dbt_test >> embed_task
