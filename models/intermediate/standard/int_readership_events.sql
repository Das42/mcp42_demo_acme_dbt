{{
    config(
        alias='readership_events'
    )
}}

/*
    Unified readership events combining:
    - Web clickstream events (article views, page views)
    - Mobile app events (screen views, article reads)
    - Print delivery events (newspaper delivered/read)

    This model normalizes all reader engagement into a single event stream
    for consistent analysis across channels.
*/

with web_events as (
    select
        web_event_pk as readership_event_pk,
        event_id as source_event_id,
        'web' as channel,
        session_id,
        reader_id,
        anonymous_id as device_identifier,
        event_type,
        event_timestamp,
        article_id,
        page_url as content_location,
        page_title as content_title,
        device_type,
        os as device_os,
        geo_country,
        geo_region,
        geo_city,
        time_on_page_seconds as engagement_duration_seconds,
        scroll_depth_percent,
        utm_source,
        utm_medium,
        utm_campaign,
        created_at
    from {{ ref('stg_web_events') }}
    where event_type in ('page_view', 'article_view', 'article_read', 'scroll', 'video_play')
),

mobile_events as (
    select
        mobile_event_pk as readership_event_pk,
        event_id as source_event_id,
        'mobile' as channel,
        session_id,
        reader_id,
        device_id as device_identifier,
        event_type,
        event_timestamp,
        article_id,
        screen_name as content_location,
        null as content_title,
        'mobile' as device_type,
        os_name as device_os,
        geo_country,
        geo_region,
        geo_city,
        time_in_view_seconds as engagement_duration_seconds,
        scroll_depth_percent,
        null as utm_source,
        null as utm_medium,
        null as utm_campaign,
        created_at
    from {{ ref('stg_mobile_events') }}
    where event_type in ('screen_view', 'article_view', 'article_read', 'scroll', 'video_play')
),

print_deliveries as (
    select
        pd.print_delivery_pk as readership_event_pk,
        pd.delivery_id as source_event_id,
        'print' as channel,
        null as session_id,
        pd.reader_id,
        null as device_identifier,
        case
            when pd.is_delivered then 'print_delivered'
            else 'print_attempted'
        end as event_type,
        coalesce(pd.delivery_timestamp, pd.scheduled_date::timestamp_ntz) as event_timestamp,
        null as article_id,
        pd.delivery_location as content_location,
        pe.edition_type as content_title,
        'print' as device_type,
        null as device_os,
        null as geo_country,
        null as geo_region,
        null as geo_city,
        null as engagement_duration_seconds,
        null as scroll_depth_percent,
        null as utm_source,
        null as utm_medium,
        null as utm_campaign,
        pd.created_at
    from {{ ref('stg_print_deliveries') }} pd
    left join {{ ref('stg_print_editions') }} pe
        on pd.edition_id = pe.edition_id
),

unioned as (
    select * from web_events
    union all
    select * from mobile_events
    union all
    select * from print_deliveries
)

select
    readership_event_pk,
    source_event_id,
    channel,
    session_id,
    reader_id,
    device_identifier,
    event_type,
    event_timestamp,
    cast(event_timestamp as date) as event_date,
    article_id,
    content_location,
    content_title,
    device_type,
    device_os,
    geo_country,
    geo_region,
    geo_city,
    engagement_duration_seconds,
    scroll_depth_percent,
    utm_source,
    utm_medium,
    utm_campaign,
    case
        when reader_id is not null then true
        else false
    end as is_identified,
    created_at
from unioned
