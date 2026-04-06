with events as (
    select * from {{ ref('stg_events') }}
),

lagged as (
    select
        *,
        lag(event_timestamp) over (partition by user_id order by event_timestamp) as prev_event_at
    from events
),

sessioned as (
    select
        *,
        case
            when prev_event_at is null
                or event_timestamp - prev_event_at > interval '30 minutes'
                then 1
            else 0
        end as is_new_session
    from lagged
)

select
    event_id,
    user_id,
    event_name,
    event_timestamp,
    properties,
    sum(is_new_session) over (
        partition by user_id order by event_timestamp
        rows unbounded preceding
    ) as session_number
from sessioned
