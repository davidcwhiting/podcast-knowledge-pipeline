with claims as (
    select * from {{ ref('stg_claims') }}
),

episodes as (
    select * from {{ ref('int_episodes_enriched') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['c.video_id', 'c.claim_text', 'c.speaker']) }} as claim_id,
    c.video_id,
    e.episode_title,
    e.channel_id,
    e.channel_title,
    e.published_at,
    c.claim_text,
    c.speaker,
    c.topic,
    c.claim_type,
    e.view_count as episode_view_count
from claims c
join episodes e on c.video_id = e.video_id
