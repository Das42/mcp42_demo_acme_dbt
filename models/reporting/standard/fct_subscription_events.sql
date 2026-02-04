{{
    config(
        alias='fct_subscription_events'
    )
}}

with subscription_events as (
    select *
    from
        {{ ref('stg_subscription_events') }}
),

dim_reader as (
    select
        reader_sk,
        reader_id
    from
        {{ ref('dim_reader') }}
),

dim_subscription as (
    select
        subscription_sk,
        subscription_id
    from
        {{ ref('dim_subscription') }}
),

dim_date as (
    select
        date_sk,
        date_actual
    from
        {{ ref('dim_date') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['se.event_id']) }} as subscription_event_sk,

        -- Natural key
        se.event_id,

        -- Dimension foreign keys
        dr.reader_sk,
        ds.subscription_sk,
        dd.date_sk as event_date_sk,

        -- Degenerate dimensions
        se.reason_code,
        se.reason_text,
        se.processed_by,

        -- Event details
        se.event_type,
        se.event_timestamp,
        cast(se.event_timestamp as date) as event_date,

        -- State changes
        se.previous_status,
        se.new_status,
        se.previous_plan,
        se.new_plan,

        -- Flags
        iff(se.event_type = 'signup', true, false) as is_signup,
        iff(se.event_type = 'renewal', true, false) as is_renewal,
        iff(se.event_type = 'upgrade', true, false) as is_upgrade,
        iff(se.event_type = 'downgrade', true, false) as is_downgrade,
        iff(se.event_type = 'cancellation', true, false) as is_cancellation,
        iff(se.event_type in ('upgrade', 'downgrade'), true, false) as is_plan_change,

        -- Metadata
        se.created_at

    from
        subscription_events as se
        left join dim_reader as dr
            on se.reader_id = dr.reader_id
        left join dim_subscription as ds
            on se.subscription_id = ds.subscription_id
        left join dim_date as dd
            on cast(se.event_timestamp as date) = dd.date_actual
)

select * from final
