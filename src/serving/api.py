"""FastAPI application serving podcast knowledge via REST endpoints and RAG chat."""

import os
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from google.cloud import bigquery
from pydantic import BaseModel

from src.serving.rag import rag_query

app = FastAPI(
    title="Podcast Knowledge Pipeline",
    description="REST API for querying structured knowledge extracted from podcast transcripts",
    version="0.1.0",
)


def get_bq_client():
    return bigquery.Client(project=os.environ["GCP_PROJECT_ID"])


def get_dataset():
    return f"{os.environ['GCP_PROJECT_ID']}.{os.environ['BQ_DATASET']}"


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.get("/episodes")
def list_episodes(
    channel: Optional[str] = None,
    limit: int = Query(default=20, le=100),
):
    """List episodes, optionally filtered by channel."""
    client = get_bq_client()
    ds = get_dataset()

    where = ""
    params = []
    if channel:
        where = "WHERE channel_title LIKE @channel"
        params.append(bigquery.ScalarQueryParameter("channel", "STRING", f"%{channel}%"))

    query = f"""
        SELECT video_id, episode_title, channel_title, published_at,
               view_count, claim_count, guest_count, topic_count
        FROM `{ds}.dim_episodes`
        {where}
        ORDER BY published_at DESC
        LIMIT @limit
    """
    params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    rows = list(client.query(query, job_config=job_config).result())

    return [dict(row) for row in rows]


@app.get("/episodes/{video_id}")
def get_episode(video_id: str):
    """Get full episode detail with extracted entities."""
    client = get_bq_client()
    ds = get_dataset()

    # Episode info
    q = f"SELECT * FROM `{ds}.dim_episodes` WHERE video_id = @vid"
    cfg = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("vid", "STRING", video_id)]
    )
    episode = next(iter(client.query(q, job_config=cfg).result()), None)
    if not episode:
        raise HTTPException(status_code=404, detail="Episode not found")

    # Claims
    q = f"SELECT claim_text, speaker, topic, claim_type FROM `{ds}.fact_claims` WHERE video_id = @vid"
    claims = [dict(r) for r in client.query(q, job_config=cfg).result()]

    # Guests
    q = f"SELECT name, title, organization FROM `{ds}.raw_guests` WHERE video_id = @vid"
    guests = [dict(r) for r in client.query(q, job_config=cfg).result()]

    # Topics
    q = f"SELECT name, category FROM `{ds}.raw_topics` WHERE video_id = @vid"
    topics = [dict(r) for r in client.query(q, job_config=cfg).result()]

    # Recommendations
    q = f"SELECT item, type, recommended_by, context FROM `{ds}.raw_recommendations` WHERE video_id = @vid"
    recs = [dict(r) for r in client.query(q, job_config=cfg).result()]

    return {
        "episode": dict(episode),
        "guests": guests,
        "topics": topics,
        "claims": claims,
        "recommendations": recs,
    }


@app.get("/guests")
def list_guests(limit: int = Query(default=20, le=100)):
    """List guests ranked by appearance count."""
    client = get_bq_client()
    ds = get_dataset()

    query = f"""
        SELECT guest_name, guest_title, guest_organization,
               appearance_count, channel_count, channels_appeared_on
        FROM `{ds}.dim_guests`
        ORDER BY appearance_count DESC
        LIMIT @limit
    """
    cfg = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("limit", "INT64", limit)]
    )
    rows = list(client.query(query, job_config=cfg).result())
    return [dict(row) for row in rows]


@app.get("/topics/trending")
def trending_topics(limit: int = Query(default=20, le=100)):
    """List topics ranked by how many episodes discuss them."""
    client = get_bq_client()
    ds = get_dataset()

    query = f"""
        SELECT topic_name, category, episode_count, channel_count
        FROM `{ds}.dim_topics`
        ORDER BY episode_count DESC, channel_count DESC
        LIMIT @limit
    """
    cfg = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("limit", "INT64", limit)]
    )
    rows = list(client.query(query, job_config=cfg).result())
    return [dict(row) for row in rows]


@app.get("/claims/search")
def search_claims(
    q: str = Query(..., min_length=2),
    limit: int = Query(default=20, le=100),
):
    """Full-text search across extracted claims."""
    client = get_bq_client()
    ds = get_dataset()

    query = f"""
        SELECT claim_text, speaker, topic, claim_type,
               episode_title, channel_title, published_at
        FROM `{ds}.fact_claims`
        WHERE LOWER(claim_text) LIKE LOWER(@search)
           OR LOWER(speaker) LIKE LOWER(@search)
           OR LOWER(topic) LIKE LOWER(@search)
        ORDER BY published_at DESC
        LIMIT @limit
    """
    cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("search", "STRING", f"%{q}%"),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ]
    )
    rows = list(client.query(query, job_config=cfg).result())
    return [dict(row) for row in rows]


@app.get("/recommendations")
def list_recommendations(limit: int = Query(default=20, le=100)):
    """List recommendations ranked by mention count."""
    client = get_bq_client()
    ds = get_dataset()

    query = f"""
        SELECT item, recommendation_type, mention_count,
               channel_count, recommended_by, channels_mentioned_on
        FROM `{ds}.mart_recommendations`
        ORDER BY mention_count DESC
        LIMIT @limit
    """
    cfg = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("limit", "INT64", limit)]
    )
    rows = list(client.query(query, job_config=cfg).result())
    return [dict(row) for row in rows]


class ChatRequest(BaseModel):
    question: str


@app.post("/chat")
def chat(request: ChatRequest):
    """RAG-powered chat endpoint. Ask questions about podcast content."""
    return rag_query(request.question)
