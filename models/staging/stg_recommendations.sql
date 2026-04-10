with source as (
    select * from {{ source('podcast_pipeline', 'raw_recommendations') }}
)

select
    video_id,
    item,
    type as recommendation_type,
    recommended_by,
    context,
    extracted_at
from source
