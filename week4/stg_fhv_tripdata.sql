with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select
        -- identifiers
        cast(dispatching_base_num as integer) as dispatching_base_num,
        cast(Affiliated_base_number as integer) as Affiliated_base_number,
        cast(PUlocationID as integer) as pickup_location_id,
        cast(DOlocationID as integer) as dropoff_location_id,

        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,  -- lpep = Licensed Passenger Enhancement Program (green taxis)
        cast(dropOff_datetime as timestamp) as dropOff_datetime,

        -- trip info
        cast(SR_Flag as integer) as SR_Flag,
        
    from source
    -- Filter out records with null vendor_id (data quality requirement)
    where dispatching_base_num is not null
)

select * from renamed