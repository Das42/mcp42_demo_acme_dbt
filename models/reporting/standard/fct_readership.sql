{{
    config(
        alias='fct_readership'
    )
}}

with readership_events as (
    select * from {{ ref('int_readership_events') }}
),

articles as (
    select
        article_id,
        publication_id
    from {{ ref('stg_content_articles') }}
),

subscriber_ranked as (
    select
        reader_id,
        publication_id as subscription_publication_id,
        plan_type,
        plan_tier,
        is_active,
        is_premium,
        tenure_months,
        row_number() over (
            partition by reader_id
            order by is_active desc, start_date desc
        ) as rn
    from {{ ref('int_subscriber_status') }}
),

current_subscriber as (
    select *
    from subscriber_ranked
    where rn = 1
),

final as (
    select
        -- Primary key
        re.readership_event_pk,

        -- Dimension foreign keys
        {{ dbt_utils.generate_surrogate_key(['re.reader_id']) }} as reader_key,
        {{ dbt_utils.generate_surrogate_key(['re.article_id']) }} as piece_key,
        {{ dbt_utils.generate_surrogate_key(['coalesce(a.publication_id, cs.subscription_publication_id)']) }} as publication_key,
        {{ dbt_utils.generate_surrogate_key(['re.event_date']) }} as date_key,

        -- Degenerate dimensions
        re.source_event_id,
        re.channel,
        re.session_id,
        re.event_type,
        re.event_timestamp,
        re.event_date,
        re.device_type,
        re.device_os,

        -- Natural keys for traceability
        re.reader_id,
        re.article_id,
        coalesce(a.publication_id, cs.subscription_publication_id) as publication_id,

        -- Measures
        re.engagement_duration_seconds,
        re.scroll_depth_percent,
        re.is_identified,

        -- Subscription context (denormalized)
        cs.plan_type,
        cs.plan_tier,
        cs.is_active as subscription_is_active,
        cs.is_premium as subscription_is_premium,
        cs.tenure_months as subscription_tenure_months

    from readership_events re
    left join articles a
        on re.article_id = a.article_id
    left join current_subscriber cs
        on re.reader_id = cs.reader_id
)

select * from final
