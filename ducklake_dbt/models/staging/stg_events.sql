select
    id as event_id,
    user_id,
    event_name,
    event_timestamp,
    properties
from {{ source('raw', 'events') }}
