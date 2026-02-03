{{
    config(
        alias='subscriber_status'
    )
}}

/*
    Current subscriber status combining subscription details with reader info.
    Provides a point-in-time view of each subscriber's current state.
*/

with subscriptions as (
    select * from {{ ref('stg_subscriptions') }}
),

readers as (
    select * from {{ ref('stg_readers') }}
),

latest_events as (
    select
        subscription_id,
        max(event_timestamp) as last_event_at,
        max(case when event_type = 'renewal' then event_timestamp end) as last_renewal_at,
        count(case when event_type = 'renewal' then 1 end) as total_renewals,
        count(case when event_type in ('upgrade', 'downgrade') then 1 end) as total_plan_changes
    from {{ ref('stg_subscription_events') }}
    group by 1
),

subscription_enriched as (
    select
        s.subscription_pk,
        s.subscription_id,
        s.reader_id,
        s.publication_id,
        s.plan_type,
        s.plan_tier,
        s.channel as acquisition_channel,
        s.status,
        s.start_date,
        s.end_date,
        s.monthly_rate,
        s.billing_frequency,
        s.auto_renew,

        -- Tenure calculations
        datediff('day', s.start_date, current_date()) as tenure_days,
        datediff('month', s.start_date, current_date()) as tenure_months,

        -- Event history
        le.last_event_at,
        le.last_renewal_at,
        coalesce(le.total_renewals, 0) as total_renewals,
        coalesce(le.total_plan_changes, 0) as total_plan_changes,

        -- Status flags
        case
            when s.status = 'active' then true
            else false
        end as is_active,
        case
            when s.status = 'active' and s.end_date <= current_date() + 30 then true
            else false
        end as is_at_risk_expiring,
        case
            when s.status = 'cancelled' then true
            else false
        end as is_churned,
        case
            when s.plan_tier = 'premium' then true
            else false
        end as is_premium,

        s.created_at,
        s.updated_at

    from subscriptions s
    left join latest_events le
        on s.subscription_id = le.subscription_id
)

select
    se.subscription_pk,
    se.subscription_id,
    se.reader_id,
    r.reader_pk,
    r.email,
    r.full_name,
    r.city,
    r.state,
    r.country,
    se.publication_id,
    se.plan_type,
    se.plan_tier,
    se.acquisition_channel,
    se.status,
    se.start_date,
    se.end_date,
    se.monthly_rate,
    se.billing_frequency,
    se.auto_renew,
    se.tenure_days,
    se.tenure_months,
    se.last_event_at,
    se.last_renewal_at,
    se.total_renewals,
    se.total_plan_changes,
    se.is_active,
    se.is_at_risk_expiring,
    se.is_churned,
    se.is_premium,
    se.created_at,
    se.updated_at
from subscription_enriched se
left join readers r
    on se.reader_id = r.reader_id
