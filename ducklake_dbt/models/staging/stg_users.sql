select
    id as user_id,
    email,
    full_name,
    plan,
    created_at,
    country_code
from {{ source('raw', 'users') }}
