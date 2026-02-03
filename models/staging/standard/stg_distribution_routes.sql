{{
    config(
        alias='distribution_routes'
    )
}}

with source as (
    select * from {{ source('raw', 'distribution_routes') }}
),

cleaned as (
    select
        -- Primary key
        {{ dbt_utils.generate_surrogate_key(['route_id']) }} as distribution_route_pk,

        -- Natural key
        cast(route_id as varchar) as route_id,

        -- Foreign keys
        cast(distribution_center_id as varchar) as distribution_center_id,
        cast(carrier_id as varchar) as carrier_id,

        -- Route details
        trim(route_name) as route_name,
        upper(trim(zone_code)) as zone_code,
        cast(total_stops as integer) as total_stops,
        lower(trim(route_type)) as route_type,
        coalesce(cast(is_active as boolean), true) as is_active,

        -- Metadata
        cast(created_at as timestamp_ntz) as created_at,
        cast(updated_at as timestamp_ntz) as updated_at

    from source
    where route_id is not null
)

select * from cleaned
