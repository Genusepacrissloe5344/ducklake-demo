select
    id as ticket_id,
    user_id,
    subject,
    priority,
    status,
    created_at,
    resolved_at
from {{ source('raw', 'support_tickets') }}
