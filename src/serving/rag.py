"""RAG (Retrieval-Augmented Generation) logic for the chat interface.

Uses a hybrid approach: aggregate/analytical questions are answered from
structured BigQuery marts. Content-specific questions use vector search
over raw transcripts.
"""

import os

import anthropic
from google.cloud import bigquery
from sentence_transformers import SentenceTransformer

from src.embedding.pgvector_loader import search_similar

_model = None


def get_embedding_model():
    """Get the sentence-transformers model (singleton)."""
    global _model
    if _model is None:
        _model = SentenceTransformer("all-MiniLM-L6-v2")
    return _model


def _get_structured_context(question: str) -> str | None:
    """Check if the question is better answered by structured data.

    Returns formatted context from BigQuery marts, or None if this
    should fall through to vector search.
    """
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    # Ask Claude to classify the question and generate a query plan
    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=256,
        system="""Classify this question about podcasts. Reply with ONLY one of:
- STRUCTURED: if it asks about rankings, counts, trends, "most", "top", "how many", comparisons across episodes/channels/guests
- CONTENT: if it asks about what someone said, opinions, specific claims, or details from a conversation

Reply with just the word STRUCTURED or CONTENT, nothing else.""",
        messages=[{"role": "user", "content": question}],
    )

    classification = response.content[0].text.strip().upper()

    if "CONTENT" in classification:
        return None

    # Fetch structured data from BigQuery marts
    bq = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
    proj = os.environ["GCP_PROJECT_ID"]
    ds = os.environ["BQ_DATASET"]

    context_parts = []

    # Top topics
    q = f"""SELECT topic_name, category, episode_count
    FROM `{proj}.{ds}.dim_topics` ORDER BY episode_count DESC LIMIT 15"""
    rows = list(bq.query(q).result())
    if rows:
        context_parts.append("TOP TOPICS (by episode count):\n" + "\n".join(
            f"- {r.topic_name} [{r.category}]: {r.episode_count} episodes" for r in rows
        ))

    # Top guests
    q = f"""SELECT guest_name, guest_title, guest_organization, appearance_count, channels_appeared_on
    FROM `{proj}.{ds}.dim_guests` ORDER BY appearance_count DESC LIMIT 10"""
    rows = list(bq.query(q).result())
    if rows:
        context_parts.append("TOP GUESTS (by appearances):\n" + "\n".join(
            f"- {r.guest_name} ({r.guest_title or ''} @ {r.guest_organization or ''}): {r.appearance_count} appearances on {r.channels_appeared_on}" for r in rows
        ))

    # Top recommendations
    q = f"""SELECT item, recommendation_type, mention_count, recommended_by
    FROM `{proj}.{ds}.mart_recommendations` ORDER BY mention_count DESC LIMIT 10"""
    rows = list(bq.query(q).result())
    if rows:
        context_parts.append("TOP RECOMMENDATIONS:\n" + "\n".join(
            f"- {r.item} ({r.recommendation_type}): {r.mention_count}x, by {r.recommended_by}" for r in rows
        ))

    # Claim type breakdown
    q = f"""SELECT claim_type, count(*) as cnt
    FROM `{proj}.{ds}.fact_claims` GROUP BY claim_type ORDER BY cnt DESC"""
    rows = list(bq.query(q).result())
    if rows:
        context_parts.append("CLAIM TYPES:\n" + "\n".join(
            f"- {r.claim_type}: {r.cnt}" for r in rows
        ))

    # Channel stats
    q = f"""SELECT channel_title, episode_count, total_views
    FROM `{proj}.{ds}.dim_channels` ORDER BY episode_count DESC"""
    rows = list(bq.query(q).result())
    if rows:
        context_parts.append("CHANNELS:\n" + "\n".join(
            f"- {r.channel_title}: {r.episode_count} episodes, {r.total_views:,} views" for r in rows
        ))

    return "\n\n".join(context_parts)


def rag_query(question: str, top_k: int = 5) -> dict:
    """Answer a question using hybrid RAG. Handles errors gracefully.

    1. Classify question as structured vs content
    2. For structured: query BigQuery marts
    3. For content: vector search over transcripts
    4. Send context + question to Claude
    5. Return answer with source info
    """
    try:
        return _rag_query_inner(question, top_k)
    except anthropic.RateLimitError:
        return {"answer": "The service is temporarily rate-limited. Please try again in a few seconds.", "sources": []}
    except anthropic.AuthenticationError:
        return {"answer": "Service configuration error. Please check API credentials.", "sources": []}
    except Exception as e:
        logging.getLogger(__name__).error("RAG query failed: %s", e, exc_info=True)
        return {"answer": "An error occurred while processing your question. Please try again.", "sources": []}


def _rag_query_inner(question: str, top_k: int = 5) -> dict:
    """Internal RAG logic — called by rag_query with error handling."""
    # Try structured approach first
    structured_context = _get_structured_context(question)

    if structured_context:
        client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            system="You answer questions about podcasts using structured data from a knowledge base. Be concise, use specific numbers, and present information clearly. The data comes from LLM-extracted entities across multiple podcast episodes.",
            messages=[{
                "role": "user",
                "content": f"Structured data from podcast knowledge base:\n\n{structured_context}\n\nQuestion: {question}",
            }],
        )

        return {
            "answer": response.content[0].text,
            "sources": [{"type": "structured_data", "note": "Answered from BigQuery dimensional model"}],
        }

    # Fall through to vector search for content questions
    model = get_embedding_model()
    query_embedding = model.encode(question).tolist()

    results = search_similar(query_embedding, top_k=top_k)

    if not results:
        return {
            "answer": "I don't have enough information to answer that question. Try ingesting more episodes first.",
            "sources": [],
        }

    context_parts = []
    for i, r in enumerate(results):
        context_parts.append(
            f"[Source {i+1}: {r['episode_title']} ({r['channel_name']})]\n{r['chunk_text']}"
        )
    context = "\n\n".join(context_parts)

    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1024,
        system="You answer questions about podcasts using provided transcript excerpts. Cite sources by number [Source N]. Be concise and accurate. If the excerpts don't contain the answer, say so.",
        messages=[{
            "role": "user",
            "content": f"Context from podcast transcripts:\n\n{context}\n\nQuestion: {question}",
        }],
    )

    sources = [
        {
            "episode": r["episode_title"],
            "channel": r["channel_name"],
            "similarity": round(r["similarity"], 3),
            "excerpt": r["chunk_text"][:200] + "...",
        }
        for r in results
    ]

    return {"answer": response.content[0].text, "sources": sources}
