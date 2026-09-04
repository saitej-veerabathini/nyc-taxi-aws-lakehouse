{{ config(
    materialized='table',
    format='parquet',
    write_compression='snappy'
) }}

with date_boundaries as (

    select
        min(date(trip_pickup_datetime)) as start_date,
        max(date(trip_pickup_datetime)) as end_date
    from {{ ref('stg_yellow_trips') }}
    where trip_pickup_datetime is not null

),

date_spine as (

    select cast(generated_calendar_date as date) as calendar_date
    from date_boundaries
    cross join unnest(
        sequence(
            start_date,
            end_date,
            interval '1' day
        )
    ) as generated_dates(generated_calendar_date)

)

select
    cast(date_format(calendar_date, '%Y%m%d') as integer) as date_key,
    calendar_date,

    year(calendar_date) as year_number,
    quarter(calendar_date) as quarter_number,
    month(calendar_date) as month_number,
    date_format(calendar_date, '%M') as month_name,
    week(calendar_date) as week_number,

    day(calendar_date) as day_of_month,
    day_of_week(calendar_date) as day_of_week_number,
    date_format(calendar_date, '%W') as day_name,

    case
        when day_of_week(calendar_date) in (6, 7) then true
        else false
    end as is_weekend

from date_spine
