{{
    config(
        alias='dim_article'
    )
}}

/*
    Article/content dimension containing article metadata.
    Includes category, author, and content attributes.
*/

with articles as (
    select * from {{ ref('stg_content_articles') }}
),

content_engagement as (
    select * from {{ ref('int_content_engagement') }}
)

select
    a.article_pk,
    a.article_id,
    a.publication_id,

    -- content attributes
    a.title,
    a.category,
    a.subcategory,
    a.author_id,
    a.is_premium,
    a.publish_date,
    a.word_count,

    -- derived attributes
    case
        when a.word_count < 500 then 'short'
        when a.word_count < 1500 then 'medium'
        else 'long'
    end as content_length_category,
    datediff('day', a.publish_date, current_date()) as days_since_publish,

    -- engagement summary (denormalized for convenience)
    coalesce(ce.total_events, 0) as lifetime_total_events,
    coalesce(ce.unique_readers, 0) as lifetime_unique_readers,
    ce.avg_engagement_seconds as lifetime_avg_engagement_seconds,
    ce.avg_scroll_depth as lifetime_avg_scroll_depth,

    -- metadata
    a.created_at,
    a.updated_at

from articles a
left join content_engagement ce
    on a.article_id = ce.article_id;
