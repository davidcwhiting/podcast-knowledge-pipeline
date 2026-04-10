"""Load extraction results into BigQuery."""

import json
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


def ensure_extraction_tables():
    """Create extraction tables if they don't exist."""
    client = get_bq_client()
    dataset_ref = get_dataset_ref()

    tables = {
        "raw_guests": [
            bigquery.SchemaField("video_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("organization", "STRING"),
            bigquery.SchemaField("expertise_areas", "STRING"),  # JSON array
            bigquery.SchemaField("extracted_at", "TIMESTAMP"),
        ],
        "raw_topics": [
            bigquery.SchemaField("video_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("category", "STRING"),
            bigquery.SchemaField("extracted_at", "TIMESTAMP"),
        ],
        "raw_claims": [
            bigquery.SchemaField("video_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("text", "STRING"),
            bigquery.SchemaField("speaker", "STRING"),
            bigquery.SchemaField("topic", "STRING"),
            bigquery.SchemaField("claim_type", "STRING"),
            bigquery.SchemaField("extracted_at", "TIMESTAMP"),
        ],
        "raw_tools_products": [
            bigquery.SchemaField("video_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("category", "STRING"),
            bigquery.SchemaField("context", "STRING"),
            bigquery.SchemaField("extracted_at", "TIMESTAMP"),
        ],
        "raw_recommendations": [
            bigquery.SchemaField("video_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("item", "STRING"),
            bigquery.SchemaField("type", "STRING"),
            bigquery.SchemaField("recommended_by", "STRING"),
            bigquery.SchemaField("context", "STRING"),
            bigquery.SchemaField("extracted_at", "TIMESTAMP"),
        ],
    }

    for table_name, schema in tables.items():
        table_ref = f"{dataset_ref}.{table_name}"
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table, exists_ok=True)


def load_extraction(merged: dict):
    """Load merged extraction results for a single episode into BigQuery.

    Deletes any existing extraction rows for this video_id first (idempotent).
    """
    client = get_bq_client()
    dataset_ref = get_dataset_ref()
    video_id = merged["video_id"]
    now = datetime.utcnow().isoformat()

    # Delete existing extraction data for this video (idempotent reload)
    for table_name in ["raw_guests", "raw_topics", "raw_claims", "raw_tools_products", "raw_recommendations"]:
        delete_query = f"DELETE FROM `{dataset_ref}.{table_name}` WHERE video_id = @video_id"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("video_id", "STRING", video_id),
            ]
        )
        client.query(delete_query, job_config=job_config).result()

    # Insert new extraction data
    _insert_rows(client, f"{dataset_ref}.raw_guests", [
        {
            "video_id": video_id,
            "name": g["name"],
            "title": g.get("title", ""),
            "organization": g.get("organization", ""),
            "expertise_areas": json.dumps(g.get("expertise_areas", [])),
            "extracted_at": now,
        }
        for g in merged["guests"]
    ])

    _insert_rows(client, f"{dataset_ref}.raw_topics", [
        {
            "video_id": video_id,
            "name": t["name"],
            "category": t["category"],
            "extracted_at": now,
        }
        for t in merged["topics"]
    ])

    _insert_rows(client, f"{dataset_ref}.raw_claims", [
        {
            "video_id": video_id,
            "text": c["text"],
            "speaker": c.get("speaker", ""),
            "topic": c.get("topic", ""),
            "claim_type": c["claim_type"],
            "extracted_at": now,
        }
        for c in merged["claims"]
    ])

    _insert_rows(client, f"{dataset_ref}.raw_tools_products", [
        {
            "video_id": video_id,
            "name": t["name"],
            "category": t["category"],
            "context": t["context"],
            "extracted_at": now,
        }
        for t in merged["tools_products"]
    ])

    _insert_rows(client, f"{dataset_ref}.raw_recommendations", [
        {
            "video_id": video_id,
            "item": r["item"],
            "type": r["type"],
            "recommended_by": r["recommended_by"],
            "context": r.get("context", ""),
            "extracted_at": now,
        }
        for r in merged["recommendations"]
    ])


def _insert_rows(client, table_ref: str, rows: list[dict]):
    """Insert rows into a BigQuery table. Skip if empty."""
    if not rows:
        return
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors for {table_ref}: {errors}")
