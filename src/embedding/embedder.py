"""Generate embeddings for transcript chunks using sentence-transformers."""

import logging

from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)

MODEL_NAME = "all-MiniLM-L6-v2"
EMBEDDING_DIM = 384
CHUNK_SIZE = 500  # characters per embedding chunk (smaller than extraction chunks)


def get_model():
    """Load the sentence-transformers model (cached after first call)."""
    return SentenceTransformer(MODEL_NAME)


def chunk_for_embedding(transcript_text: str, chunk_size: int = CHUNK_SIZE) -> list[dict]:
    """Split transcript into smaller chunks sized for embedding.

    These are smaller than extraction chunks — ~500 chars each for
    more granular retrieval in RAG.
    """
    words = transcript_text.split()
    chunks = []
    current_chunk = []
    current_len = 0
    chunk_index = 0

    for word in words:
        word_len = len(word) + 1  # +1 for space
        if current_len + word_len > chunk_size and current_chunk:
            chunks.append({
                "chunk_index": chunk_index,
                "text": " ".join(current_chunk),
            })
            chunk_index += 1
            current_chunk = []
            current_len = 0

        current_chunk.append(word)
        current_len += word_len

    if current_chunk:
        chunks.append({
            "chunk_index": chunk_index,
            "text": " ".join(current_chunk),
        })

    return chunks


def embed_chunks(chunks: list[dict]) -> list[dict]:
    """Generate embeddings for a list of text chunks.

    Returns the same list with an 'embedding' key added to each chunk.
    """
    model = get_model()
    texts = [c["text"] for c in chunks]

    # Batch encode for efficiency
    embeddings = model.encode(texts, show_progress_bar=False, batch_size=64)

    for chunk, embedding in zip(chunks, embeddings):
        chunk["embedding"] = embedding.tolist()

    return chunks
