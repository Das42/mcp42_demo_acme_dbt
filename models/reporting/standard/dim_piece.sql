{{
    config(
        alias='dim_piece'
    )
}}

with source as (
    select * from {{ ref('stg_content_articles') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['article_id']) }} as piece_key,

        -- Natural key
        article_id,

        -- Foreign keys
        publication_id,
        author_id,

        -- Content attributes
        title,
        slug,
        category,
        subcategory,
        tags,
        word_count,
        publish_date,
        is_premium,

        -- Metadata
        created_at,
        updated_at

    from source
)

select * from final
