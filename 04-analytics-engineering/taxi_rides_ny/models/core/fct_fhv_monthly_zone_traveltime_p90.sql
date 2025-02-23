{{ config(materialized='table') }}

with fhv_trips_data as (
    select *,
        timestamp_diff(dropoff_datetime, pickup_datetime, second) as trip_duration
    from {{ ref('dim_fhv_trips') }}
)

select *,
    percentile_cont(trip_duration, 0.90) over (partition by year, month, pickup_locationid, dropoff_locationid) as p90
from fhv_trips_data
where month = 11 and year = 2019
    and pickup_zone in ('Newark Airport', 'SoHo', 'Yorkville East')

order by pickup_zone asc, p90 desc