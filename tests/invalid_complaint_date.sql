select
    complaint_number
from
    {{ ref('dim_complaint') }}
where 
    extract('year' FROM complaint_from_dt::date) < 2020 or extract('year' FROM complaint_from_dt::date) > extract('year' from current_date )