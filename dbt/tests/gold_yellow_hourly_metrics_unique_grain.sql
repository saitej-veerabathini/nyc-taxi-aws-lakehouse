select
    pickup_date,
    pickup_hour,
    count(*) as row_count
from {{ ref('gold_yellow_hourly_demand') }}
group by
    pickup_date,
    pickup_hour
having count(*) > 1