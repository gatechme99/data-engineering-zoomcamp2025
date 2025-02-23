{{ 
    config(materialized='view',
    location='us-east1')
}}

with tripdata as 
(
  select *
  from {{ source('staging','external_fhv_tripdata') }}
  where dispatching_base_num is not null 
)

select

    dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid
    
from tripdata