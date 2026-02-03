{{
    config(
        alias='marketing_touchpoints'
    )
}}

/*
    Normalized marketing touchpoints combining sends and interactions.
    Provides a unified view of all marketing contacts with readers.
*/

with sends as (
    select * from {{ ref('stg_marketing_sends') }}
),

interactions as (
    select * from {{ ref('stg_marketing_interactions') }}
),

campaigns as (
    select * from {{ ref('stg_marketing_campaigns') }}
),

readers as (
    select * from {{ ref('stg_readers') }}
),

send_interactions as (
    select
        s.send_id,
        count(case when i.interaction_type = 'open' then 1 end) as opens,
        count(case when i.interaction_type = 'click' then 1 end) as clicks,
        count(case when i.interaction_type = 'conversion' then 1 end) as conversions,
        count(case when i.interaction_type = 'unsubscribe' then 1 end) as unsubscribes,
        count(case when i.interaction_type = 'bounce' then 1 end) as bounces,
        min(case when i.interaction_type = 'open' then i.interaction_timestamp end) as first_open_at,
        min(case when i.interaction_type = 'click' then i.interaction_timestamp end) as first_click_at
    from sends s
    left join interactions i
        on s.send_id = i.send_id
    group by 1
)

select
    s.marketing_send_pk as marketing_touchpoint_pk,
    s.send_id,
    s.campaign_id,
    s.reader_id,
    s.template_id,
    s.channel,
    s.send_timestamp,
    cast(s.send_timestamp as date) as send_date,
    s.status as send_status,

    -- Campaign details
    c.campaign_name,
    c.campaign_type,
    c.target_audience,
    c.start_date as campaign_start_date,
    c.end_date as campaign_end_date,

    -- Reader details
    r.email,
    r.full_name,
    r.acquisition_source as reader_acquisition_source,

    -- Interaction metrics
    coalesce(si.opens, 0) as opens,
    coalesce(si.clicks, 0) as clicks,
    coalesce(si.conversions, 0) as conversions,
    coalesce(si.unsubscribes, 0) as unsubscribes,
    coalesce(si.bounces, 0) as bounces,
    si.first_open_at,
    si.first_click_at,

    -- Engagement flags
    case when coalesce(si.opens, 0) > 0 then true else false end as was_opened,
    case when coalesce(si.clicks, 0) > 0 then true else false end as was_clicked,
    case when coalesce(si.conversions, 0) > 0 then true else false end as had_conversion,
    case when coalesce(si.unsubscribes, 0) > 0 then true else false end as had_unsubscribe,
    case when coalesce(si.bounces, 0) > 0 then true else false end as had_bounce,

    -- Time to engagement
    case
        when si.first_open_at is not null
        then datediff('minute', s.send_timestamp, si.first_open_at)
    end as minutes_to_open,
    case
        when si.first_click_at is not null
        then datediff('minute', s.send_timestamp, si.first_click_at)
    end as minutes_to_click,

    s.created_at

from sends s
left join send_interactions si
    on s.send_id = si.send_id
left join campaigns c
    on s.campaign_id = c.campaign_id
left join readers r
    on s.reader_id = r.reader_id
