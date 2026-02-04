{{
    config(
        alias='fct_content_daily'
    )
}}

/*
    Daily content performance fact table.
    Aggregated article engagement metrics by date.
*/

with readership_events as (
    select * from {{ ref('int_readership_events') }}
    where article_id is not null
),

articles as (
    select * from {{ ref('stg_content_articles') }}
),

dates as (
    select * from {{ ref('dim_date') }}
),

daily_metrics as (
    select
        article_id,
        event_date,

        -- engagement counts
        count(*) as total_events,
        count(distinct reader_id) as unique_readers,
        count(distinct session_id) as unique_sessions,

        -- channel breakdown
        count(case when channel = 'web' then 1 end) as web_events,
        count(case when channel = 'mobile' then 1 end) as mobile_events,

        -- engagement quality
        avg(engagement_duration_seconds) as avg_engagement_seconds,
        sum(engagement_duration_seconds) as total_engagement_seconds,
        avg(scroll_depth_percent) as avg_scroll_depth,

        -- device breakdown
        count(case when device_type = 'desktop' then 1 end) as desktop_events,
        count(case when device_type = 'mobile' then 1 end) as mobile_device_events,
        count(case when device_type = 'tablet' then 1 end) as tablet_events

    from readership_events
    group by 1, 2
)

select
    {{ dbt_utils.generate_surrogate_key(['dm.article_id', 'dm.event_date']) }} as content_daily_pk,

    -- dimension keys
    a.article_pk,
    d.date_pk as event_date_pk,

    -- natural keys
    dm.article_id,
    dm.event_date,

    -- article context
    a.publication_id,
    a.category,
    a.subcategory,
    a.is_premium,

    -- engagement measures
    dm.total_events,
    dm.unique_readers,
    dm.unique_sessions,
    dm.web_events,
    dm.mobile_events,
    dm.avg_engagement_seconds,
    dm.total_engagement_seconds,
    dm.avg_scroll_depth,
    dm.desktop_events,
    dm.mobile_device_events,
    dm.tablet_events,

    -- calculated metrics
    case
        when dm.unique_readers > 0 then dm.total_events::float / dm.unique_readers
        else 0
    end as events_per_reader,

    -- days since publish
    datediff('day', a.publish_date, dm.event_date) as days_since_publish

from daily_metrics dm
left join articles a
    on dm.article_id = a.article_id
left join dates d
    on dm.event_date = d.date_day;
