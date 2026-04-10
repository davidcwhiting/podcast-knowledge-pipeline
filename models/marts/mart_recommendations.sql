with recs as (
    select * from {{ ref('stg_recommendations') }}
),

episodes as (
    select * from {{ ref('int_episodes_enriched') }}
)

select
    lower(trim(r.item)) as recommendation_id,
    max(r.item) as item,
    max(r.recommendation_type) as recommendation_type,
    count(distinct r.video_id) as mention_count,
    count(distinct e.channel_id) as channel_count,
    array_agg(distinct r.recommended_by ignore nulls) as recommended_by,
    array_agg(distinct e.channel_title ignore nulls) as channels_mentioned_on,
    min(e.published_at) as first_mentioned,
    max(e.published_at) as latest_mentioned
from recs r
join episodes e on r.video_id = e.video_id
group by lower(trim(r.item))
