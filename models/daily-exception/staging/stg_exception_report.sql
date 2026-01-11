select *
from {{ source('public', 'exception_report') }}