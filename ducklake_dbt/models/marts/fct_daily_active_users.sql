{{ config(materialized='table') }}

with events as (
    select * from {{ ref('stg_events') }}
)

select
    cast(event_timestamp as date) as activity_date,
    count(distinct user_id) as active_users,
    count(*) as total_events,
    count(*) filter (where event_name = 'page_view') as page_views,
    count(*) filter (where event_name = 'signup') as signups,
    count(*) filter (where event_name = 'upgrade') as upgrades,
    count(*) filter (where event_name = 'downgrade') as downgrades
from events
group by 1
