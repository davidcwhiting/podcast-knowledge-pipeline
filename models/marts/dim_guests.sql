with guests as (
    select * from {{ ref('stg_guests') }}
),

episodes as (
    select * from {{ ref('int_episodes_enriched') }}
)

select
    lower(trim(g.name)) as guest_id,
    max(g.name) as guest_name,
    max(g.title) as guest_title,
    max(g.organization) as guest_organization,
    count(distinct g.video_id) as appearance_count,
    count(distinct e.channel_id) as channel_count,
    array_agg(distinct e.channel_title ignore nulls) as channels_appeared_on,
    min(e.published_at) as first_appearance,
    max(e.published_at) as latest_appearance
from guests g
join episodes e on g.video_id = e.video_id
group by lower(trim(g.name))
