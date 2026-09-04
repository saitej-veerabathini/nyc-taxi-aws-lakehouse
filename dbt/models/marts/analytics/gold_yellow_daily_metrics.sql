{{
  config(
    materialized='table',
    format='parquet',
    write_compression='snappy'
  )
}}

select
    date(trip_pickup_datetime) as pickup_date,
    vendor_name,

    count(*) as trip_count,

    sum(
        case
            when trip_dropoff_datetime >= trip_pickup_datetime
            then 1
            else 0
        end
    ) as valid_duration_trip_count,

    sum(
        case
            when trip_distance = 0
            then 1
            else 0
        end
    ) as zero_distance_trip_count,

    sum(
        case
            when dq_quality_warnings_count > 0
            then 1
            else 0
        end
    ) as warning_trip_count,

    sum(passenger_count) as total_passengers,
    sum(trip_distance) as total_trip_distance,
    sum(fare_amount) as total_fare_amount,
    sum(tip_amount) as total_tip_amount,
    sum(total_amount) as total_amount,

    avg(trip_distance) as average_trip_distance,
    avg(total_amount) as average_total_amount

from {{ ref('stg_yellow_trips') }}

where trip_pickup_datetime is not null

group by
    date(trip_pickup_datetime),
    vendor_name