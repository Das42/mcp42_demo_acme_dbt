{{
    config(
        alias='fct_subscription_events'
    )
}}

/*
    Subscription events fact table - HIGHEST PRIORITY.
    Captures all subscriber lifecycle events including signups, renewals,
    upgrades, downgrades, and cancellations.
*/

with subscription_events as (
    select * from {{ ref('stg_subscription_events') }}
),

subscriptions as (
    select * from {{ ref('stg_subscriptions') }}
),

readers as (
    select * from {{ ref('stg_readers') }}
),

dates as (
    select * from {{ ref('dim_date') }}
)

select
    se.subscription_event_pk as subscription_event_pk,
    se.event_id,

    -- dimension keys
    r.reader_pk,
    s.subscription_pk,
    d.date_pk as event_date_pk,

    -- natural keys for filtering
    se.subscription_id,
    se.reader_id,

    -- event details
    se.event_type,
    se.event_timestamp,
    cast(se.event_timestamp as date) as event_date,

    -- state changes
    se.previous_status,
    se.new_status,
    se.previous_plan,
    se.new_plan,

    -- reason
    se.reason_code,
    se.reason_text,
    se.processed_by,

    -- subscription context at time of event
    s.plan_type,
    s.plan_tier,
    s.channel as acquisition_channel,
    s.monthly_rate,
    s.billing_frequency,

    -- event type flags
    case when se.event_type = 'signup' then true else false end as is_signup,
    case when se.event_type = 'renewal' then true else false end as is_renewal,
    case when se.event_type = 'cancellation' then true else false end as is_cancellation,
    case when se.event_type = 'upgrade' then true else false end as is_upgrade,
    case when se.event_type = 'downgrade' then true else false end as is_downgrade,
    case when se.event_type = 'pause' then true else false end as is_pause,
    case when se.event_type = 'reactivation' then true else false end as is_reactivation,

    -- measures
    case
        when se.event_type = 'signup' then s.monthly_rate
        else 0
    end as new_mrr,
    case
        when se.event_type = 'cancellation' then s.monthly_rate
        else 0
    end as churned_mrr,
    case
        when se.event_type = 'upgrade' then s.monthly_rate
        when se.event_type = 'downgrade' then -s.monthly_rate
        else 0
    end as expansion_mrr,

    -- count measures
    1 as event_count,

    -- metadata
    se.created_at

from subscription_events se
left join subscriptions s
    on se.subscription_id = s.subscription_id
left join readers r
    on se.reader_id = r.reader_id
left join dates d
    on cast(se.event_timestamp as date) = d.date_day;
