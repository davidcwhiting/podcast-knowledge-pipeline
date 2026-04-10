with source as (
    select * from {{ source('podcast_pipeline', 'raw_tools_products') }}
)

select
    video_id,
    name as tool_name,
    category,
    context as mention_context,
    extracted_at
from source
