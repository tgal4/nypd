select
    complaint_number
from
    {{ ref('dim_complaint') }}
where 
    complaint_from_dt is null