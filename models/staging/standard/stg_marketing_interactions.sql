{{
    config(
        alias='marketing_interactions'
    )
}}

with source as (
    select * from {{ source('raw', 'marketing_interactions') }}
),

cleaned as (
    select
        -- Primary key
        {{ dbt_utils.generate_surrogate_key(['interaction_id']) }} as marketing_interaction_pk,

        -- Natural key
        cast(interaction_id as varchar) as interaction_id,

        -- Foreign keys
        cast(send_id as varchar) as send_id,
        cast(campaign_id as varchar) as campaign_id,
        cast(reader_id as varchar) as reader_id,

        -- Interaction details
        lower(trim(interaction_type)) as interaction_type,
        cast(interaction_timestamp as timestamp_ntz) as interaction_timestamp,
        trim(link_url) as link_url,
        lower(trim(device_type)) as device_type,

        -- Metadata
        cast(created_at as timestamp_ntz) as created_at

    from source
    where interaction_id is not null
)

select * from cleaned
