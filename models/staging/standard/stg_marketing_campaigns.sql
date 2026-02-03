{{
    config(
        alias='marketing_campaigns'
    )
}}

with source as (
    select * from {{ source('raw', 'marketing_campaigns') }}
),

cleaned as (
    select
        -- Primary key
        {{ dbt_utils.generate_surrogate_key(['campaign_id']) }} as marketing_campaign_pk,

        -- Natural key
        cast(campaign_id as varchar) as campaign_id,

        -- Campaign details
        trim(campaign_name) as campaign_name,
        lower(trim(campaign_type)) as campaign_type,
        lower(trim(channel)) as channel,

        -- Dates
        cast(start_date as date) as start_date,
        cast(end_date as date) as end_date,

        -- Budget and targeting
        cast(budget as decimal(12,2)) as budget,
        lower(trim(target_audience)) as target_audience,
        lower(trim(status)) as status,

        -- Ownership
        trim(created_by) as created_by,

        -- Metadata
        cast(created_at as timestamp_ntz) as created_at,
        cast(updated_at as timestamp_ntz) as updated_at

    from source
    where campaign_id is not null
)

select * from cleaned
