{{ config(materialized='table') }}

with users as (
    select * from {{ ref('stg_users') }}
),

revenue as (
    select * from {{ ref('int_revenue_by_user') }}
),

tickets as (
    select
        user_id,
        count(*) as total_tickets,
        count(*) filter (where status in ('resolved', 'closed')) as resolved_tickets
    from {{ ref('stg_tickets') }}
    group by user_id
)

select
    u.user_id,
    u.email,
    u.full_name,
    u.plan,
    u.created_at,
    u.country_code,
    coalesce(r.total_revenue, 0) as lifetime_revenue,
    coalesce(r.total_invoices, 0) as total_invoices,
    r.first_invoice_at,
    r.last_invoice_at,
    coalesce(t.total_tickets, 0) as total_tickets,
    coalesce(t.resolved_tickets, 0) as resolved_tickets
from users u
left join revenue r using (user_id)
left join tickets t using (user_id)
