"""Split transcripts into overlapping chunks for LLM extraction."""

import tiktoken


def count_tokens(text: str, model: str = "cl100k_base") -> int:
    """Count tokens in text using tiktoken."""
    enc = tiktoken.get_encoding(model)
    return len(enc.encode(text))


def chunk_transcript(
    text: str,
    max_tokens: int = 6000,
    overlap_tokens: int = 200,
) -> list[dict]:
    """Split transcript text into overlapping chunks.

    Each chunk is roughly max_tokens tokens, with overlap_tokens of overlap
    between consecutive chunks for context continuity.

    Returns a list of dicts with 'chunk_index', 'text', and 'token_count'.
    """
    enc = tiktoken.get_encoding("cl100k_base")
    tokens = enc.encode(text)
    total_tokens = len(tokens)

    if total_tokens <= max_tokens:
        return [{"chunk_index": 0, "text": text, "token_count": total_tokens}]

    chunks = []
    start = 0
    chunk_index = 0
    step = max_tokens - overlap_tokens

    while start < total_tokens:
        end = min(start + max_tokens, total_tokens)
        chunk_tokens = tokens[start:end]
        chunk_text = enc.decode(chunk_tokens)

        chunks.append({
            "chunk_index": chunk_index,
            "text": chunk_text,
            "token_count": len(chunk_tokens),
        })

        if end >= total_tokens:
            break

        start += step
        chunk_index += 1

    return chunks
