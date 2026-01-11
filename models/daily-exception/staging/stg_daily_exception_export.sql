select *
from {{ source('public', 'daily_exception_export') }}