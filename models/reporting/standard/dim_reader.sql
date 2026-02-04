{{
    config(
        alias='dim_reader'
    )
}}

/*
    Reader/subscriber dimension containing customer attributes.
    Includes demographic, geographic, and acquisition information.
*/

with readers as (
    select * from {{ ref('stg_readers') }}
    where is_deleted = false
),

subscriber_status as (
    select * from {{ ref('int_subscriber_status') }}
),

reader_metrics as (
    select
        reader_id,
        count(distinct subscription_pk) as count_subscriptions,
        count(distinct case when is_active then subscription_pk end) as count_active_subscriptions,
        min(start_date) as first_subscription_date,
        max(start_date) as latest_subscription_date,
        sum(case when is_active then monthly_rate else 0 end) as current_mrr,
        max(case when is_active then is_premium else false end) as has_premium_subscription,
        max(is_churned) as has_churned_subscription
    from subscriber_status
    group by 1
)

select
    r.reader_pk,
    r.reader_id,

    -- contact info
    r.email,
    r.first_name,
    r.last_name,
    r.full_name,
    r.phone_number,

    -- address
    r.address_line_1,
    r.address_line_2,
    r.city,
    r.state,
    r.postal_code,
    r.country,

    -- demographics
    r.birth_date,
    r.gender,
    case
        when r.birth_date is null then null
        else floor(datediff('year', r.birth_date, current_date()))
    end as age,
    case
        when r.birth_date is null then 'unknown'
        when floor(datediff('year', r.birth_date, current_date())) < 25 then '18-24'
        when floor(datediff('year', r.birth_date, current_date())) < 35 then '25-34'
        when floor(datediff('year', r.birth_date, current_date())) < 45 then '35-44'
        when floor(datediff('year', r.birth_date, current_date())) < 55 then '45-54'
        when floor(datediff('year', r.birth_date, current_date())) < 65 then '55-64'
        else '65+'
    end as age_group,

    -- acquisition
    r.acquisition_source,
    r.acquisition_campaign_id,

    -- subscription metrics
    coalesce(rm.count_subscriptions, 0) as count_subscriptions,
    coalesce(rm.count_active_subscriptions, 0) as count_active_subscriptions,
    rm.first_subscription_date,
    rm.latest_subscription_date,
    coalesce(rm.current_mrr, 0) as current_mrr,
    coalesce(rm.has_premium_subscription, false) as has_premium_subscription,
    coalesce(rm.has_churned_subscription, false) as has_churned_subscription,

    -- subscriber flags
    case
        when rm.count_active_subscriptions > 0 then true
        else false
    end as is_active_subscriber,
    case
        when rm.count_subscriptions > 0 then true
        else false
    end as is_subscriber,
    case
        when rm.first_subscription_date is not null
            and datediff('day', rm.first_subscription_date, current_date()) <= 30
        then true
        else false
    end as is_new_subscriber,

    -- metadata
    r.created_at,
    r.updated_at

from readers r
left join reader_metrics rm
    on r.reader_id = rm.reader_id;
