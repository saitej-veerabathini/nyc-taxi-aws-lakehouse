{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partitioned_by=['pickup_year', 'pickup_month'],
        on_schema_change='sync_all_columns',
        format='parquet',
        write_compression='snappy'
    )
}}

with trips as (

    select
        date(trip_pickup_datetime) as pickup_date,
        hour(trip_pickup_datetime) as pickup_hour,

        trip_distance,
        passenger_count,
        total_amount,

        year(trip_pickup_datetime) as pickup_year,
        month(trip_pickup_datetime) as pickup_month

    from {{ ref('stg_yellow_trips') }}

    where trip_pickup_datetime is not null

    {% if is_incremental() %}

        and year(trip_pickup_datetime) = {{ var('load_year') }}
        and month(trip_pickup_datetime) = {{ var('load_month') }}

    {% endif %}

)

select
    t.pickup_date,
    d.day_name as pickup_day,
    t.pickup_hour,

    count(*) as trip_count,
    sum(t.trip_distance) as total_trip_distance,
    sum(t.passenger_count) as total_passenger_count,
    sum(t.total_amount) as sum_of_total_amount,
    avg(t.trip_distance) as average_trip_distance,
    avg(t.total_amount) as average_total_amount,

    -- Athena partition columns must be last
    t.pickup_year,
    t.pickup_month

from trips as t

inner join {{ ref('dim_date') }} as d
    on t.pickup_date = d.calendar_date

group by
    t.pickup_date,
    d.day_name,
    t.pickup_hour,
    t.pickup_year,
    t.pickup_month