{{
    config(
        alias='content_engagement'
    )
}}

/*
    Aggregated content engagement metrics by article.
    Combines web and mobile engagement for unified content performance analysis.
*/

with readership_events as (
    select * from {{ ref('int_readership_events') }}
    where article_id is not null
),

articles as (
    select * from {{ ref('stg_content_articles') }}
),

engagement_metrics as (
    select
        article_id,

        -- Total engagement
        count(*) as total_events,
        count(distinct reader_id) as unique_readers,
        count(distinct session_id) as unique_sessions,

        -- Channel breakdown
        count(case when channel = 'web' then 1 end) as web_events,
        count(case when channel = 'mobile' then 1 end) as mobile_events,

        -- Engagement quality
        avg(engagement_duration_seconds) as avg_engagement_seconds,
        avg(scroll_depth_percent) as avg_scroll_depth,
        sum(engagement_duration_seconds) as total_engagement_seconds,

        -- Time range
        min(event_timestamp) as first_engagement_at,
        max(event_timestamp) as last_engagement_at,

        -- Device breakdown
        count(distinct device_type) as device_types_count,
        count(case when device_type = 'mobile' then 1 end) as mobile_device_events,
        count(case when device_type = 'desktop' then 1 end) as desktop_device_events,
        count(case when device_type = 'tablet' then 1 end) as tablet_device_events

    from readership_events
    group by 1
)

select
    {{ dbt_utils.generate_surrogate_key(['a.article_id']) }} as content_engagement_pk,
    a.article_id,
    a.article_pk,
    a.publication_id,
    a.title,
    a.category,
    a.subcategory,
    a.author_id,
    a.is_premium,
    a.publish_date,
    a.word_count,

    -- Engagement metrics
    coalesce(em.total_events, 0) as total_events,
    coalesce(em.unique_readers, 0) as unique_readers,
    coalesce(em.unique_sessions, 0) as unique_sessions,
    coalesce(em.web_events, 0) as web_events,
    coalesce(em.mobile_events, 0) as mobile_events,
    em.avg_engagement_seconds,
    em.avg_scroll_depth,
    coalesce(em.total_engagement_seconds, 0) as total_engagement_seconds,
    em.first_engagement_at,
    em.last_engagement_at,
    coalesce(em.device_types_count, 0) as device_types_count,
    coalesce(em.mobile_device_events, 0) as mobile_device_events,
    coalesce(em.desktop_device_events, 0) as desktop_device_events,
    coalesce(em.tablet_device_events, 0) as tablet_device_events,

    -- Calculated metrics
    case
        when em.unique_readers > 0 then em.total_events::float / em.unique_readers
        else 0
    end as events_per_reader,
    case
        when a.word_count > 0 and em.avg_engagement_seconds is not null
        then em.avg_engagement_seconds / (a.word_count / 200.0)  -- assuming 200 wpm reading speed
        else null
    end as read_completion_ratio,
    datediff('day', a.publish_date, current_timestamp()) as days_since_publish,

    a.created_at,
    a.updated_at

from articles a
left join engagement_metrics em
    on a.article_id = em.article_id
