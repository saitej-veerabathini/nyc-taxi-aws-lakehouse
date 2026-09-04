{{
  config(
    materialized='table',
    format='parquet',
    write_compression='snappy'
  )
}}

SELECT 
        date(s.trip_pickup_datetime) as pickup_date,
        d.day_name as pickup_day,
        hour(s.trip_pickup_datetime) as pickup_hour,
        count(*) as trip_count,
        sum(s.trip_distance) as total_trip_distance,
        sum(s.passenger_count) as total_passenger_count,
        sum(s.total_amount) as sum_of_total_amount,
        avg(s.trip_distance) as average_trip_distance,
        avg(s.total_amount) as average_total_amount

FROM {{ ref('stg_yellow_trips') }} AS s
INNER JOIN {{ ref('dim_date') }} AS d
    ON date(s.trip_pickup_datetime) = d.calendar_date

GROUP BY 
    date(s.trip_pickup_datetime),
    d.day_name,
    hour(s.trip_pickup_datetime)

