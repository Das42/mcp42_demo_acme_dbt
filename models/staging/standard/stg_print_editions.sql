{{
    config(
        alias='print_editions'
    )
}}

with source as (
    select * from {{ source('raw', 'print_editions') }}
),

cleaned as (
    select
        -- Primary key
        {{ dbt_utils.generate_surrogate_key(['edition_id']) }} as print_edition_pk,

        -- Natural key
        cast(edition_id as varchar) as edition_id,

        -- Foreign key
        cast(publication_id as varchar) as publication_id,

        -- Edition details
        cast(edition_date as date) as edition_date,
        lower(trim(edition_type)) as edition_type,
        cast(page_count as integer) as page_count,
        cast(print_run_quantity as integer) as print_run_quantity,

        -- Metadata
        cast(created_at as timestamp_ntz) as created_at

    from source
    where edition_id is not null
)

select * from cleaned
