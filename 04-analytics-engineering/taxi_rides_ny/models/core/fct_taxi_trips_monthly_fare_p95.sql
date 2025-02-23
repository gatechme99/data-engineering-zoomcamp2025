{{ config(materialized='table') }}

with monthly_fare as (

    select

        service_type,
        fare_amount,
        trip_distance,
        payment_type_description,
        extract(year from pickup_datetime) as year,
        extract(month FROM pickup_datetime) as month

    from {{ ref('fact_trips') }}
    where extract(year from pickup_datetime) in (2019, 2020)
        and lower(payment_type_description) in ('cash', 'credit card')
        and fare_amount > 0
        and trip_distance > 0

)

select
    service_type,
    fare_amount,
    year,
    month,
    percentile_cont(fare_amount, 0.97) over (partition by service_type, year, month) as p97,
    percentile_cont(fare_amount, 0.95) over (partition by service_type, year, month) as p95,
    percentile_cont(fare_amount, 0.90) over (partition by service_type, year, month) as p90
from monthly_fare
where month = 4 and year = 2020
order by fare_amount