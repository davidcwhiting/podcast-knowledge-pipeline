"""Store and retrieve embeddings using pgvector in PostgreSQL."""

import logging
import os

import psycopg2
from pgvector.psycopg2 import register_vector

from src.embedding.embedder import EMBEDDING_DIM

logger = logging.getLogger(__name__)


def get_connection():
    """Get a PostgreSQL connection with pgvector support."""
    dsn = os.environ["PGVECTOR_CONN"]
    # Strip SQLAlchemy dialect prefix if present
    dsn = dsn.replace("postgresql+psycopg2://", "postgresql://")
    conn = psycopg2.connect(dsn)
    register_vector(conn)
    return conn


def ensure_embeddings_table():
    """Create the embeddings table if it doesn't exist."""
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS transcript_embeddings (
            id SERIAL PRIMARY KEY,
            video_id TEXT NOT NULL,
            chunk_index INTEGER NOT NULL,
            chunk_text TEXT NOT NULL,
            embedding vector({EMBEDDING_DIM}) NOT NULL,
            channel_name TEXT,
            episode_title TEXT,
            UNIQUE(video_id, chunk_index)
        )
    """)

    # Create HNSW index for fast similarity search
    cur.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_embeddings_hnsw
        ON transcript_embeddings
        USING hnsw (embedding vector_cosine_ops)
    """)

    conn.commit()
    cur.close()
    conn.close()


def load_embeddings(
    video_id: str,
    episode_title: str,
    channel_name: str,
    chunks: list[dict],
):
    """Load embedded chunks into pgvector. Replaces existing embeddings for the video."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM transcript_embeddings WHERE video_id = %s", (video_id,))
            for chunk in chunks:
                cur.execute(
                    """INSERT INTO transcript_embeddings
                    (video_id, chunk_index, chunk_text, embedding, channel_name, episode_title)
                    VALUES (%s, %s, %s, %s, %s, %s)""",
                    (
                        video_id,
                        chunk["chunk_index"],
                        chunk["text"],
                        chunk["embedding"],
                        channel_name,
                        episode_title,
                    ),
                )
        conn.commit()
        logger.info("Loaded %d embeddings for video %s", len(chunks), video_id)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def search_similar(query_embedding: list[float], top_k: int = 5) -> list[dict]:
    """Search for the most similar transcript chunks to a query embedding."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT video_id, chunk_index, chunk_text, channel_name, episode_title,
                          1 - (embedding <=> %s::vector) as similarity
                   FROM transcript_embeddings
                   ORDER BY embedding <=> %s::vector
                   LIMIT %s""",
                (query_embedding, query_embedding, top_k),
            )

            results = []
            for row in cur.fetchall():
                results.append({
                    "video_id": row[0],
                    "chunk_index": row[1],
                    "chunk_text": row[2],
                    "channel_name": row[3],
                    "episode_title": row[4],
                    "similarity": float(row[5]),
                })
        return results
    finally:
        conn.close()
