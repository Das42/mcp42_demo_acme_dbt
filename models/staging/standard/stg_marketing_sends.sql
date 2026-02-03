{{
    config(
        alias='marketing_sends'
    )
}}

with source as (
    select * from {{ source('raw', 'marketing_sends') }}
),

cleaned as (
    select
        -- Primary key
        {{ dbt_utils.generate_surrogate_key(['send_id']) }} as marketing_send_pk,

        -- Natural key
        cast(send_id as varchar) as send_id,

        -- Foreign keys
        cast(campaign_id as varchar) as campaign_id,
        cast(reader_id as varchar) as reader_id,
        cast(template_id as varchar) as template_id,

        -- Send details
        lower(trim(channel)) as channel,
        cast(send_timestamp as timestamp_ntz) as send_timestamp,
        lower(trim(status)) as status,

        -- Metadata
        cast(created_at as timestamp_ntz) as created_at

    from source
    where send_id is not null
)

select * from cleaned
