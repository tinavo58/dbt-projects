select *
from {{ source('public', 'deputy_staff') }}
where access_role not in ('Location Manager', 'System Administrator', 'eStore Casual', 'Supervisor', 'eStore FullTime')