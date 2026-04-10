"""Structured extraction prompts for Claude Haiku."""

EXTRACTION_SYSTEM_PROMPT = """Extract structured entities from podcast transcripts as JSON. Only extract what is explicitly stated."""

EXTRACTION_USER_PROMPT = """Extract from this podcast transcript chunk. Return JSON only, no other text.

Podcast: {channel_title} | Episode: {episode_title} | Chunk {chunk_index}/{total_chunks}

<transcript>
{chunk_text}
</transcript>

Return this JSON structure (empty lists if nothing found):
{{"guests":[{{"name":"str","title":"str","organization":"str"}}],"topics":[{{"name":"str","category":"AI|Technology|Science|Health|Military|Politics|Business|Philosophy|History|Culture|Other"}}],"claims":[{{"text":"str","speaker":"str","topic":"str","claim_type":"factual|opinion|prediction|anecdote"}}],"tools_products":[{{"name":"str","category":"Software|Hardware|Book|Service|Company|Other","context":"endorsement|criticism|neutral"}}],"recommendations":[{{"item":"str","type":"book|paper|person|product|practice|other","recommended_by":"str"}}]}}

Rules:
- Max 5 claims per chunk (most significant only)
- Only actual guests/speakers, not passing mentions
- Broad topic categories, not one per sentence
- Be concise in claim text"""

# JSON schema for structured output validation
EXTRACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "guests": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "title": {"type": "string"},
                    "organization": {"type": "string"},
                },
                "required": ["name"],
            },
        },
        "topics": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "category": {"type": "string"},
                },
                "required": ["name", "category"],
            },
        },
        "claims": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "text": {"type": "string"},
                    "speaker": {"type": "string"},
                    "topic": {"type": "string"},
                    "claim_type": {"type": "string"},
                },
                "required": ["text", "speaker", "claim_type"],
            },
        },
        "tools_products": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "category": {"type": "string"},
                    "context": {"type": "string"},
                },
                "required": ["name", "category", "context"],
            },
        },
        "recommendations": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "item": {"type": "string"},
                    "type": {"type": "string"},
                    "recommended_by": {"type": "string"},
                },
                "required": ["item", "type", "recommended_by"],
            },
        },
    },
    "required": ["guests", "topics", "claims", "tools_products", "recommendations"],
}
