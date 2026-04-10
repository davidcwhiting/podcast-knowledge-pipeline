with source as (
    select * from {{ source('podcast_pipeline', 'raw_topics') }}
)

select
    video_id,
    name as topic_name,
    category,
    extracted_at
from source
