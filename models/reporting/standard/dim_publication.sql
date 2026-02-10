{{
    config(
        alias='dim_publication'
    )
}}

with source as (
    select * from {{ ref('stg_publications') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['publication_id']) }} as publication_key,

        -- Natural key
        publication_id,

        -- Attributes
        publication_name,
        publication_type,
        frequency,
        launch_date,
        is_active,

        -- Metadata
        created_at,
        updated_at

    from source
)

select * from final
