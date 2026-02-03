{{
    config(
        alias='print_deliveries'
    )
}}

with source as (
    select * from {{ source('raw', 'print_deliveries') }}
),

cleaned as (
    select
        -- Primary key
        {{ dbt_utils.generate_surrogate_key(['delivery_id']) }} as print_delivery_pk,

        -- Natural key
        cast(delivery_id as varchar) as delivery_id,

        -- Foreign keys
        cast(subscription_id as varchar) as subscription_id,
        cast(reader_id as varchar) as reader_id,
        cast(edition_id as varchar) as edition_id,
        cast(route_id as varchar) as route_id,
        cast(carrier_id as varchar) as carrier_id,

        -- Delivery details
        cast(scheduled_date as date) as scheduled_date,
        lower(trim(delivery_status)) as delivery_status,
        cast(delivery_timestamp as timestamp_ntz) as delivery_timestamp,
        lower(trim(delivery_location)) as delivery_location,

        -- Exceptions
        trim(exception_code) as exception_code,
        trim(exception_notes) as exception_notes,

        -- Derived flags
        case
            when lower(trim(delivery_status)) = 'delivered' then true
            else false
        end as is_delivered,
        case
            when exception_code is not null then true
            else false
        end as has_exception,

        -- Metadata
        cast(created_at as timestamp_ntz) as created_at,
        cast(updated_at as timestamp_ntz) as updated_at

    from source
    where delivery_id is not null
)

select * from cleaned
