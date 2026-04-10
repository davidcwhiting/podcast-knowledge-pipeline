with episodes as (
    select * from {{ ref('int_episodes_enriched') }}
),

claim_counts as (
    select video_id, count(*) as claim_count
    from {{ ref('stg_claims') }}
    group by video_id
),

guest_counts as (
    select video_id, count(*) as guest_count
    from {{ ref('stg_guests') }}
    group by video_id
),

topic_counts as (
    select video_id, count(*) as topic_count
    from {{ ref('stg_topics') }}
    group by video_id
)

select
    e.video_id,
    e.episode_title,
    e.description,
    e.published_at,
    e.channel_id,
    e.channel_title,
    e.view_count,
    e.duration,
    e.has_transcript,
    e.transcript_char_count,
    coalesce(cc.claim_count, 0) as claim_count,
    coalesce(gc.guest_count, 0) as guest_count,
    coalesce(tc.topic_count, 0) as topic_count
from episodes e
left join claim_counts cc on e.video_id = cc.video_id
left join guest_counts gc on e.video_id = gc.video_id
left join topic_counts tc on e.video_id = tc.video_id
