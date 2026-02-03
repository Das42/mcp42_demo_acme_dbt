{{
    config(
        alias='publications'
    )
}}

with source as (
    select * from {{ source('raw', 'publications') }}
),

cleaned as (
    select
        -- Primary key
        {{ dbt_utils.generate_surrogate_key(['publication_id']) }} as publication_pk,

        -- Natural key
        cast(publication_id as varchar) as publication_id,

        -- Attributes
        trim(publication_name) as publication_name,
        lower(trim(publication_type)) as publication_type,
        lower(trim(frequency)) as frequency,
        cast(launch_date as date) as launch_date,
        coalesce(cast(is_active as boolean), true) as is_active,

        -- Metadata
        cast(created_at as timestamp_ntz) as created_at,
        cast(updated_at as timestamp_ntz) as updated_at

    from source
    where publication_id is not null
)

select * from cleaned
