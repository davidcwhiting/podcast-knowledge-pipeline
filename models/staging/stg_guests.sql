with source as (
    select * from {{ source('podcast_pipeline', 'raw_guests') }}
)

select
    video_id,
    name,
    title,
    organization,
    JSON_EXTRACT_STRING_ARRAY(expertise_areas) as expertise_areas,
    extracted_at
from source
