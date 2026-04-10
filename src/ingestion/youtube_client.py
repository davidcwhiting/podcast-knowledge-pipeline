"""YouTube API client for fetching video metadata and transcripts."""

import os
from datetime import datetime

from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi


# Channel IDs for seed podcasts
CHANNELS = {
    "joe_rogan": "UCzQUP1qoWDoEbmsQxvdjxgQ",
    "huberman_lab": "UC2D2CMWXMOVWx7giW1n3LIg",
    "shawn_ryan": "UCkoujZQZatbqy4KGcgjpVxQ",
    "lex_fridman": "UCSHZKyawb77ixDdsGog4iWA",
}


def get_youtube_service():
    """Build an authenticated YouTube Data API v3 service."""
    api_key = os.environ["YOUTUBE_API_KEY"]
    return build("youtube", "v3", developerKey=api_key)


def get_channel_videos(
    channel_id: str,
    published_after: datetime | None = None,
    max_results: int = 50,
) -> list[dict]:
    """Fetch video metadata from a channel, optionally filtered by publish date.

    Returns a list of dicts with video metadata (id, title, description,
    published_at, duration, view_count, channel_title).
    """
    youtube = get_youtube_service()

    # First, get the uploads playlist ID for this channel
    channel_resp = youtube.channels().list(
        part="contentDetails", id=channel_id
    ).execute()

    if not channel_resp.get("items"):
        return []

    uploads_playlist_id = channel_resp["items"][0]["contentDetails"][
        "relatedPlaylists"
    ]["uploads"]

    # Fetch videos from the uploads playlist
    videos = []
    next_page_token = None

    while True:
        playlist_resp = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=uploads_playlist_id,
            maxResults=min(max_results - len(videos), 50),
            pageToken=next_page_token,
        ).execute()

        for item in playlist_resp.get("items", []):
            published_at = datetime.fromisoformat(
                item["snippet"]["publishedAt"].replace("Z", "+00:00")
            )

            # Skip videos published before our cutoff
            if published_after and published_at <= published_after:
                continue

            videos.append({
                "video_id": item["contentDetails"]["videoId"],
                "title": item["snippet"]["title"],
                "description": item["snippet"]["description"],
                "published_at": item["snippet"]["publishedAt"],
                "channel_id": channel_id,
                "channel_title": item["snippet"]["channelTitle"],
            })

        next_page_token = playlist_resp.get("nextPageToken")
        if not next_page_token or len(videos) >= max_results:
            break

    # Enrich with video statistics (view count, duration)
    if videos:
        video_ids = [v["video_id"] for v in videos]
        for i in range(0, len(video_ids), 50):
            batch = video_ids[i : i + 50]
            stats_resp = youtube.videos().list(
                part="statistics,contentDetails",
                id=",".join(batch),
            ).execute()

            stats_map = {
                item["id"]: item for item in stats_resp.get("items", [])
            }

            for video in videos:
                if video["video_id"] in stats_map:
                    stats = stats_map[video["video_id"]]
                    video["view_count"] = int(
                        stats.get("statistics", {}).get("viewCount", 0)
                    )
                    video["duration"] = stats.get("contentDetails", {}).get(
                        "duration", ""
                    )

    return videos


def get_video_transcript(video_id: str) -> list[dict] | None:
    """Fetch transcript for a video using youtube-transcript-api v1.x.

    Returns a list of dicts with 'text', 'start', 'duration' keys,
    or None if no transcript is available.
    """
    try:
        ytt = YouTubeTranscriptApi()
        transcript = ytt.fetch(video_id)
        return [
            {"text": s.text, "start": s.start, "duration": s.duration}
            for s in transcript.snippets
        ]
    except Exception:
        return None


def transcript_to_text(segments: list[dict]) -> str:
    """Convert transcript segments to a single text string."""
    return " ".join(segment["text"] for segment in segments)
