{{ config(materialized='table') }}


with quarterly_revenue as (
    select
        service_type,
        extract(year from pickup_datetime) as year,
        extract(quarter FROM pickup_datetime) as quarter,
        sum(total_amount) as revenue

    from {{ ref('fact_trips') }}
    where extract(year from pickup_datetime) in (2019, 2020)
    group by 1, 2, 3
),

quarterly_growth as (
    select 
        year,
        quarter,
        service_type,
        revenue,
        lag(revenue) over (partition by service_type, quarter order by year) as prev_year_revenue,
        (revenue - lag(revenue) over (partition by service_type, quarter order by year)) / 
        nullif((lag(revenue) over (partition by service_type, quarter order by year)),0) as yoy_growth
    from quarterly_revenue
)

select * from quarterly_growth
order by 1, 3, 2