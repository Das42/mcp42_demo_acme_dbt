{{
    config(
        alias='fct_readership_events'
    )
}}

/*
    Readership events fact table capturing all engagement activity.
    Unified events from web, mobile, and print channels.
*/

with readership_events as (
    select * from {{ ref('int_readership_events') }}
),

readers as (
    select * from {{ ref('stg_readers') }}
),

articles as (
    select * from {{ ref('stg_content_articles') }}
),

dates as (
    select * from {{ ref('dim_date') }}
)

select
    re.readership_event_pk,
    re.source_event_id,

    -- dimension keys
    r.reader_pk,
    a.article_pk,
    d.date_pk as event_date_pk,

    -- natural keys for filtering
    re.reader_id,
    re.article_id,

    -- event details
    re.channel,
    re.session_id,
    re.device_identifier,
    re.event_type,
    re.event_timestamp,
    re.event_date,

    -- content context
    re.content_location,
    re.content_title,

    -- device context
    re.device_type,
    re.device_os,

    -- geographic context
    re.geo_country,
    re.geo_region,
    re.geo_city,

    -- marketing attribution
    re.utm_source,
    re.utm_medium,
    re.utm_campaign,

    -- engagement measures
    coalesce(re.engagement_duration_seconds, 0) as engagement_duration_seconds,
    re.scroll_depth_percent,

    -- flags
    re.is_identified,
    case when re.channel = 'web' then true else false end as is_web_event,
    case when re.channel = 'mobile' then true else false end as is_mobile_event,
    case when re.channel = 'print' then true else false end as is_print_event,
    case
        when re.event_type in ('article_read', 'article_view') then true
        else false
    end as is_article_engagement,

    -- count measures
    1 as event_count,

    -- metadata
    re.created_at

from readership_events re
left join readers r
    on re.reader_id = r.reader_id
left join articles a
    on re.article_id = a.article_id
left join dates d
    on re.event_date = d.date_day;
