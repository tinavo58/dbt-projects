select *
from {{ source('public', 'leave_requests') }}
