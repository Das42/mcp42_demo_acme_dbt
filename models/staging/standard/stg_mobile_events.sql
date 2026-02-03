{{
    config(
        alias='mobile_events'
    )
}}

with source as (
    select * from {{ source('raw', 'mobile_events') }}
),

cleaned as (
    select
        -- Primary key
        {{ dbt_utils.generate_surrogate_key(['event_id']) }} as mobile_event_pk,

        -- Natural key
        cast(event_id as varchar) as event_id,

        -- Session and identity
        cast(session_id as varchar) as session_id,
        cast(reader_id as varchar) as reader_id,
        cast(device_id as varchar) as device_id,

        -- Event details
        lower(trim(event_type)) as event_type,
        cast(event_timestamp as timestamp_ntz) as event_timestamp,

        -- Screen and content
        trim(screen_name) as screen_name,
        cast(article_id as varchar) as article_id,

        -- App and device info
        trim(app_version) as app_version,
        trim(device_model) as device_model,
        lower(trim(os_name)) as os_name,
        trim(os_version) as os_version,

        -- Location
        lower(trim(geo_country)) as geo_country,
        lower(trim(geo_region)) as geo_region,
        lower(trim(geo_city)) as geo_city,

        -- Settings
        coalesce(cast(push_enabled as boolean), false) as push_enabled,

        -- Engagement metrics
        cast(time_in_view_seconds as integer) as time_in_view_seconds,
        cast(scroll_depth_percent as decimal(5,2)) as scroll_depth_percent,

        -- Metadata
        cast(created_at as timestamp_ntz) as created_at

    from source
    where event_id is not null
)

select * from cleaned
