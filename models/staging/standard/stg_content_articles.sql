{{
    config(
        alias='content_articles'
    )
}}

with source as (
    select * from {{ source('raw', 'content_articles') }}
),

cleaned as (
    select
        -- Primary key
        {{ dbt_utils.generate_surrogate_key(['article_id']) }} as article_pk,

        -- Natural key
        cast(article_id as varchar) as article_id,

        -- Foreign keys
        cast(publication_id as varchar) as publication_id,
        cast(author_id as varchar) as author_id,

        -- Content attributes
        trim(title) as title,
        lower(trim(slug)) as slug,
        lower(trim(category)) as category,
        lower(trim(subcategory)) as subcategory,
        trim(tags) as tags,
        cast(word_count as integer) as word_count,
        cast(publish_date as timestamp_ntz) as publish_date,
        coalesce(cast(is_premium as boolean), false) as is_premium,

        -- Metadata
        cast(created_at as timestamp_ntz) as created_at,
        cast(updated_at as timestamp_ntz) as updated_at

    from source
    where article_id is not null
)

select * from cleaned
