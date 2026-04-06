select
    id as invoice_id,
    user_id,
    amount,
    currency,
    status,
    issued_at,
    paid_at
from {{ source('raw', 'invoices') }}
