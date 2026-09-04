with silver_totals as (
    select
        count(*) as trip_count,
        coalesce(sum(total_amount), 0) as total_amount
    from {{ ref('stg_yellow_trips') }}
    where trip_pickup_datetime is not null
),

gold_totals as (
    select
        coalesce(sum(trip_count), 0) as trip_count,
        coalesce(sum(total_amount), 0) as total_amount
    from {{ ref('gold_yellow_daily_metrics') }}
)

select
    s.trip_count as silver_trip_count,
    g.trip_count as gold_trip_count,
    s.total_amount as silver_total_amount,
    g.total_amount as gold_total_amount
from silver_totals as s
cross join gold_totals as g
where
    s.trip_count <> g.trip_count
    or s.total_amount <> g.total_amount