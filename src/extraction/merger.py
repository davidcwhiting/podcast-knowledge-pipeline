"""Merge and deduplicate extraction results across chunks for a single episode."""


def merge_extractions(chunk_results: list[dict], video_id: str) -> dict:
    """Merge extraction results from multiple chunks into a single episode result.

    Deduplicates guests by name, topics by name, and recommendations by item.
    Claims are kept as-is (each is unique).
    """
    merged = {
        "video_id": video_id,
        "guests": [],
        "topics": [],
        "claims": [],
        "tools_products": [],
        "recommendations": [],
    }

    seen_guests = {}
    seen_topics = {}
    seen_tools = {}
    seen_recommendations = {}

    for result in chunk_results:
        # Merge guests — dedupe by normalized name
        for guest in result.get("guests", []):
            key = guest["name"].strip().lower()
            if key not in seen_guests:
                seen_guests[key] = guest
            else:
                # Merge expertise areas
                existing = seen_guests[key]
                new_areas = guest.get("expertise_areas", [])
                existing_areas = existing.get("expertise_areas", [])
                combined = list(set(existing_areas + new_areas))
                existing["expertise_areas"] = combined
                # Fill in missing fields
                if not existing.get("title") and guest.get("title"):
                    existing["title"] = guest["title"]
                if not existing.get("organization") and guest.get("organization"):
                    existing["organization"] = guest["organization"]

        # Merge topics — dedupe by normalized name
        for topic in result.get("topics", []):
            key = topic["name"].strip().lower()
            if key not in seen_topics:
                seen_topics[key] = topic

        # Claims — keep all (each is unique per chunk)
        for claim in result.get("claims", []):
            merged["claims"].append(claim)

        # Merge tools/products — dedupe by normalized name
        for tool in result.get("tools_products", []):
            key = tool["name"].strip().lower()
            if key not in seen_tools:
                seen_tools[key] = tool

        # Merge recommendations — dedupe by normalized item name
        for rec in result.get("recommendations", []):
            key = rec["item"].strip().lower()
            if key not in seen_recommendations:
                seen_recommendations[key] = rec

    merged["guests"] = list(seen_guests.values())
    merged["topics"] = list(seen_topics.values())
    merged["tools_products"] = list(seen_tools.values())
    merged["recommendations"] = list(seen_recommendations.values())

    return merged
