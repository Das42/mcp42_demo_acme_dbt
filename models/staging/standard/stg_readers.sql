{{
    config(
        alias='readers'
    )
}}

with source as (
    select * from {{ source('raw', 'readers') }}
),

cleaned as (
    select
        -- Primary key
        {{ dbt_utils.generate_surrogate_key(['reader_id']) }} as reader_pk,

        -- Natural key
        cast(reader_id as varchar) as reader_id,

        -- Contact info
        lower(trim(email)) as email,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        trim(first_name) || ' ' || trim(last_name) as full_name,
        regexp_replace(phone_number, '[^0-9]', '') as phone_number,

        -- Address
        trim(address_line_1) as address_line_1,
        trim(address_line_2) as address_line_2,
        trim(city) as city,
        upper(trim(state)) as state,
        trim(postal_code) as postal_code,
        upper(trim(country)) as country,

        -- Demographics
        cast(birth_date as date) as birth_date,
        lower(trim(gender)) as gender,

        -- Acquisition
        lower(trim(acquisition_source)) as acquisition_source,
        cast(acquisition_campaign_id as varchar) as acquisition_campaign_id,

        -- Metadata
        cast(created_at as timestamp_ntz) as created_at,
        cast(updated_at as timestamp_ntz) as updated_at,
        coalesce(cast(is_deleted as boolean), false) as is_deleted

    from source
    where reader_id is not null
)

select * from cleaned
