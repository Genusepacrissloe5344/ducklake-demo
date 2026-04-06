{{ config(materialized='table') }}

with invoices as (
    select * from {{ ref('stg_invoices') }}
),

users as (
    select * from {{ ref('stg_users') }}
)

select
    i.issued_at as revenue_date,
    u.plan,
    u.country_code,
    i.status as invoice_status,
    count(*) as invoice_count,
    sum(i.amount) as gross_amount,
    sum(i.amount) filter (where i.status = 'paid') as net_revenue,
    sum(i.amount) filter (where i.status = 'refunded') as refunded_amount
from invoices i
join users u on i.user_id = u.user_id
group by 1, 2, 3, 4
