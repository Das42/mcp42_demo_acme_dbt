{{
    config(
        alias='subscriptions'
    )
}}

with source as (
    select * from {{ source('raw', 'subscriptions') }}
),

cleaned as (
    select
        -- Primary key
        {{ dbt_utils.generate_surrogate_key(['subscription_id']) }} as subscription_pk,

        -- Natural key
        cast(subscription_id as varchar) as subscription_id,

        -- Foreign keys
        cast(reader_id as varchar) as reader_id,
        cast(publication_id as varchar) as publication_id,

        -- Plan details
        lower(trim(plan_type)) as plan_type,
        lower(trim(plan_tier)) as plan_tier,
        lower(trim(channel)) as channel,

        -- Dates
        cast(start_date as date) as start_date,
        cast(end_date as date) as end_date,

        -- Status and billing
        lower(trim(status)) as status,
        cast(monthly_rate as decimal(10,2)) as monthly_rate,
        lower(trim(billing_frequency)) as billing_frequency,
        coalesce(cast(auto_renew as boolean), false) as auto_renew,

        -- Metadata
        cast(created_at as timestamp_ntz) as created_at,
        cast(updated_at as timestamp_ntz) as updated_at

    from source
    where subscription_id is not null
)

select * from cleaned
