with episodes as (
    select * from {{ ref('int_episodes_enriched') }}
)

select
    channel_id,
    channel_title,
    count(distinct video_id) as episode_count,
    sum(view_count) as total_views,
    min(published_at) as earliest_episode,
    max(published_at) as latest_episode
from episodes
group by channel_id, channel_title
