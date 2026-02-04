{{
    config(
        alias='dim_campaign'
    )
}}

/*
    Marketing campaign dimension containing campaign attributes.
    Includes campaign type, channel, targeting, and budget information.
*/

with campaigns as (
    select * from {{ ref('stg_marketing_campaigns') }}
)

select
    marketing_campaign_pk as campaign_pk,
    campaign_id,

    -- campaign details
    campaign_name,
    campaign_type,
    channel,

    -- dates
    start_date,
    end_date,
    datediff('day', start_date, coalesce(end_date, current_date())) as campaign_duration_days,

    -- budget and targeting
    budget,
    target_audience,
    status,

    -- derived attributes
    case
        when status = 'active' then true
        else false
    end as is_active,
    case
        when end_date < current_date() then true
        else false
    end as is_completed,

    -- ownership
    created_by,

    -- metadata
    created_at,
    updated_at

from campaigns;
