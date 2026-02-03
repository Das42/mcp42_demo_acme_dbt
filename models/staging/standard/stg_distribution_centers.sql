{{
    config(
        alias='distribution_centers'
    )
}}

with source as (
    select * from {{ source('raw', 'distribution_centers') }}
),

cleaned as (
    select
        -- Primary key
        {{ dbt_utils.generate_surrogate_key(['center_id']) }} as distribution_center_pk,

        -- Natural key
        cast(center_id as varchar) as center_id,

        -- Center details
        trim(center_name) as center_name,
        trim(address) as address,
        trim(city) as city,
        upper(trim(state)) as state,
        trim(postal_code) as postal_code,
        lower(trim(region)) as region,
        cast(capacity as integer) as capacity,
        coalesce(cast(is_active as boolean), true) as is_active,

        -- Metadata
        cast(created_at as timestamp_ntz) as created_at,
        cast(updated_at as timestamp_ntz) as updated_at

    from source
    where center_id is not null
)

select * from cleaned
