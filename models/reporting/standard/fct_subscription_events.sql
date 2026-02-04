{{
    config(
        alias='fct_subscription_events'
    )
}}

with subscription_events as (
    select * from {{ ref('stg_subscription_events') }}
),

dim_reader as (
    select reader_sk, reader_id from {{ ref('dim_reader') }}
),

dim_subscription as (
    select subscription_sk, subscription_id from {{ ref('dim_subscription') }}
),

dim_date as (
    select date_sk, date_actual from {{ ref('dim_date') }}
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
        case when se.event_type = 'signup' then true else false end as is_signup,
        case when se.event_type = 'renewal' then true else false end as is_renewal,
        case when se.event_type = 'upgrade' then true else false end as is_upgrade,
        case when se.event_type = 'downgrade' then true else false end as is_downgrade,
        case when se.event_type = 'cancellation' then true else false end as is_cancellation,
        case when se.event_type in ('upgrade', 'downgrade') then true else false end as is_plan_change,

        -- Metadata
        se.created_at

    from subscription_events se
    left join dim_reader dr
        on se.reader_id = dr.reader_id
    left join dim_subscription ds
        on se.subscription_id = ds.subscription_id
    left join dim_date dd
        on cast(se.event_timestamp as date) = dd.date_actual
)

select * from final
