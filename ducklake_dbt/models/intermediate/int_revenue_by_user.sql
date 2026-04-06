with invoices as (
    select * from {{ ref('stg_invoices') }}
)

select
    user_id,
    count(*) as total_invoices,
    count(*) filter (where status = 'paid') as paid_invoices,
    coalesce(sum(amount) filter (where status = 'paid'), 0) as total_revenue,
    coalesce(sum(amount) filter (where status = 'refunded'), 0) as total_refunded,
    min(issued_at) as first_invoice_at,
    max(issued_at) as last_invoice_at
from invoices
group by user_id
