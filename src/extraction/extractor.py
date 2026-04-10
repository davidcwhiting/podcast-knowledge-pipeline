"""Call Claude Haiku to extract structured entities from transcript chunks."""

import json
import logging
import os
import time

import anthropic

from src.extraction.prompts import (
    EXTRACTION_SYSTEM_PROMPT,
    EXTRACTION_USER_PROMPT,
)

logger = logging.getLogger(__name__)

MODEL = "claude-haiku-4-5-20251001"
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5


def get_anthropic_client():
    """Get an authenticated Anthropic client."""
    return anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])


def extract_from_chunk(
    chunk_text: str,
    chunk_index: int,
    total_chunks: int,
    episode_title: str,
    channel_title: str,
) -> dict:
    """Extract structured entities from a single transcript chunk.

    Returns a dict with keys: guests, topics, claims, tools_products, recommendations.
    """
    client = get_anthropic_client()

    user_prompt = EXTRACTION_USER_PROMPT.format(
        channel_title=channel_title,
        episode_title=episode_title,
        chunk_index=chunk_index + 1,
        total_chunks=total_chunks,
        chunk_text=chunk_text,
    )

    for attempt in range(MAX_RETRIES):
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=2048,
                system=EXTRACTION_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_prompt}],
            )

            response_text = response.content[0].text

            # Parse the JSON from the response
            # Handle cases where the model wraps JSON in markdown code blocks
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0]
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0]

            return json.loads(response_text.strip())

        except anthropic.RateLimitError:
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_DELAY_SECONDS * (attempt + 1)
                logger.warning("Rate limited, waiting %ds (attempt %d/%d)", wait, attempt + 1, MAX_RETRIES)
                time.sleep(wait)
            else:
                raise
        except (json.JSONDecodeError, IndexError, KeyError) as e:
            if attempt < MAX_RETRIES - 1:
                logger.warning("Parse error on attempt %d/%d: %s", attempt + 1, MAX_RETRIES, e)
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                logger.error(
                    "Failed to parse extraction after %d attempts. Last response: %s",
                    MAX_RETRIES,
                    response_text[:500] if "response_text" in dir() else "<no response>",
                )
                raise RuntimeError(f"Extraction parse failed after {MAX_RETRIES} attempts")

    return _empty_extraction()


def extract_episode(
    transcript_text: str,
    chunks: list[dict],
    episode_title: str,
    channel_title: str,
) -> list[dict]:
    """Extract entities from all chunks of an episode.

    Returns a list of extraction results, one per chunk.
    """
    total_chunks = len(chunks)
    results = []

    for chunk in chunks:
        logger.info(
            "Extracting chunk %d/%d for: %s",
            chunk["chunk_index"] + 1,
            total_chunks,
            episode_title[:50],
        )

        result = extract_from_chunk(
            chunk_text=chunk["text"],
            chunk_index=chunk["chunk_index"],
            total_chunks=total_chunks,
            episode_title=episode_title,
            channel_title=channel_title,
        )
        results.append(result)

    return results


def _empty_extraction() -> dict:
    """Return an empty extraction result."""
    return {
        "guests": [],
        "topics": [],
        "claims": [],
        "tools_products": [],
        "recommendations": [],
    }
