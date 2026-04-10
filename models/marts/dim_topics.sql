with topics as (
    select * from {{ ref('stg_topics') }}
),

episodes as (
    select * from {{ ref('int_episodes_enriched') }}
)

select
    lower(trim(t.topic_name)) as topic_id,
    max(t.topic_name) as topic_name,
    max(t.category) as category,
    count(distinct t.video_id) as episode_count,
    count(distinct e.channel_id) as channel_count,
    array_agg(distinct e.channel_title ignore nulls) as channels_discussed_on
from topics t
join episodes e on t.video_id = e.video_id
group by lower(trim(t.topic_name))
