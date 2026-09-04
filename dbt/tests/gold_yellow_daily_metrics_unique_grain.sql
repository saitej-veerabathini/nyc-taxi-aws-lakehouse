select
    pickup_date,
    vendor_name,
    count(*) as row_count
from {{ ref('gold_yellow_daily_metrics') }}
group by
    pickup_date,
    vendor_name
having count(*) > 1