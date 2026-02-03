{{
    config(
        alias='subscription_events'
    )
}}

with source as (
    select * from {{ source('raw', 'subscription_events') }}
),

cleaned as (
    select
        -- Primary key
        {{ dbt_utils.generate_surrogate_key(['event_id']) }} as subscription_event_pk,

        -- Natural key
        cast(event_id as varchar) as event_id,

        -- Foreign keys
        cast(subscription_id as varchar) as subscription_id,
        cast(reader_id as varchar) as reader_id,

        -- Event details
        lower(trim(event_type)) as event_type,
        cast(event_timestamp as timestamp_ntz) as event_timestamp,

        -- State changes
        lower(trim(previous_status)) as previous_status,
        lower(trim(new_status)) as new_status,
        lower(trim(previous_plan)) as previous_plan,
        lower(trim(new_plan)) as new_plan,

        -- Reason
        trim(reason_code) as reason_code,
        trim(reason_text) as reason_text,
        trim(processed_by) as processed_by,

        -- Metadata
        cast(created_at as timestamp_ntz) as created_at

    from source
    where event_id is not null
)

select * from cleaned
