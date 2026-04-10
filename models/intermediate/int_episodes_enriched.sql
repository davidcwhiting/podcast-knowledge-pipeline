with videos as (
    select * from {{ source('podcast_pipeline', 'stg_videos') }}
),

transcripts as (
    select * from {{ source('podcast_pipeline', 'stg_transcripts') }}
)

select
    v.video_id,
    v.title as episode_title,
    v.description,
    v.published_at,
    v.channel_id,
    v.channel_title,
    v.view_count,
    v.duration,
    v.gcs_uri,
    t.has_transcript,
    t.segment_count,
    length(t.transcript_text) as transcript_char_count,
    v.ingested_at
from videos v
left join transcripts t on v.video_id = t.video_id
