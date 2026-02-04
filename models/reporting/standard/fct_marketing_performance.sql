{{
    config(
        alias='fct_marketing_performance'
    )
}}

/*
    Marketing performance fact table.
    Tracks marketing send effectiveness with engagement metrics.
*/

with marketing_touchpoints as (
    select * from {{ ref('int_marketing_touchpoints') }}
),

readers as (
    select * from {{ ref('stg_readers') }}
),

campaigns as (
    select * from {{ ref('stg_marketing_campaigns') }}
),

dates as (
    select * from {{ ref('dim_date') }}
)

select
    mt.marketing_touchpoint_pk,
    mt.send_id,

    -- dimension keys
    r.reader_pk,
    c.marketing_campaign_pk as campaign_pk,
    d.date_pk as send_date_pk,

    -- natural keys
    mt.reader_id,
    mt.campaign_id,
    mt.template_id,

    -- send details
    mt.channel,
    mt.send_timestamp,
    mt.send_date,
    mt.send_status,

    -- campaign context
    mt.campaign_name,
    mt.campaign_type,
    mt.target_audience,
    mt.campaign_start_date,
    mt.campaign_end_date,

    -- reader context
    mt.email,
    mt.reader_acquisition_source,

    -- interaction measures
    mt.opens,
    mt.clicks,
    mt.conversions,
    mt.unsubscribes,
    mt.bounces,
    mt.first_open_at,
    mt.first_click_at,

    -- engagement flags
    mt.was_opened,
    mt.was_clicked,
    mt.had_conversion,
    mt.had_unsubscribe,
    mt.had_bounce,

    -- time to engagement
    mt.minutes_to_open,
    mt.minutes_to_click,

    -- count measures
    1 as send_count,
    case when mt.was_opened then 1 else 0 end as open_count,
    case when mt.was_clicked then 1 else 0 end as click_count,
    case when mt.had_conversion then 1 else 0 end as conversion_count,

    -- metadata
    mt.created_at

from marketing_touchpoints mt
left join readers r
    on mt.reader_id = r.reader_id
left join campaigns c
    on mt.campaign_id = c.campaign_id
left join dates d
    on mt.send_date = d.date_day;
