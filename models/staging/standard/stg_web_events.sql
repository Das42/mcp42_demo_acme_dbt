{{
    config(
        alias='web_events'
    )
}}

with source as (
    select * from {{ source('raw', 'web_events') }}
),

cleaned as (
    select
        -- Primary key
        {{ dbt_utils.generate_surrogate_key(['event_id']) }} as web_event_pk,

        -- Natural key
        cast(event_id as varchar) as event_id,

        -- Session and identity
        cast(session_id as varchar) as session_id,
        cast(reader_id as varchar) as reader_id,
        cast(anonymous_id as varchar) as anonymous_id,

        -- Event details
        lower(trim(event_type)) as event_type,
        cast(event_timestamp as timestamp_ntz) as event_timestamp,

        -- Page info
        trim(page_url) as page_url,
        trim(page_title) as page_title,
        trim(referrer_url) as referrer_url,
        cast(article_id as varchar) as article_id,

        -- Device and browser
        lower(trim(device_type)) as device_type,
        lower(trim(browser)) as browser,
        lower(trim(os)) as os,

        -- Location (excluding raw IP for privacy)
        lower(trim(geo_country)) as geo_country,
        lower(trim(geo_region)) as geo_region,
        lower(trim(geo_city)) as geo_city,

        -- UTM parameters
        lower(trim(utm_source)) as utm_source,
        lower(trim(utm_medium)) as utm_medium,
        lower(trim(utm_campaign)) as utm_campaign,
        lower(trim(utm_content)) as utm_content,
        lower(trim(utm_term)) as utm_term,

        -- Engagement metrics
        cast(time_on_page_seconds as integer) as time_on_page_seconds,
        cast(scroll_depth_percent as decimal(5,2)) as scroll_depth_percent,

        -- Metadata
        cast(created_at as timestamp_ntz) as created_at

    from source
    where event_id is not null
)

select * from cleaned
