{{
    config(
        alias='dim_subscription'
    )
}}

with subscriptions as (
    select * from {{ ref('stg_subscriptions') }}
),

publications as (
    select * from {{ ref('stg_publications') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['s.subscription_id']) }} as subscription_sk,

        -- Natural key
        s.subscription_id,

        -- Foreign keys
        s.reader_id,
        s.publication_id,

        -- Plan details
        s.plan_type,
        s.plan_tier,
        s.channel as acquisition_channel,

        -- Dates
        s.start_date,
        s.end_date,

        -- Status and billing
        s.status,
        s.monthly_rate,
        s.billing_frequency,
        s.auto_renew,

        -- Publication details
        p.publication_name,
        p.publication_type,
        p.frequency as publication_frequency,

        -- Metadata
        s.created_at,
        s.updated_at

    from subscriptions s
    left join publications p
        on s.publication_id = p.publication_id
)

select * from final
