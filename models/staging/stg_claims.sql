with source as (
    select * from {{ source('podcast_pipeline', 'raw_claims') }}
)

select
    video_id,
    text as claim_text,
    speaker,
    topic,
    claim_type,
    extracted_at
from source
