{{
    config(
        alias='dim_reader'
    )
}}

with source as (
    select * from {{ ref('stg_readers') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['reader_id']) }} as reader_key,

        -- Natural key
        reader_id,

        -- Demographics
        first_name,
        last_name,
        full_name,
        email,
        phone_number,
        gender,
        birth_date,

        -- Geography
        city,
        state,
        postal_code,
        country,

        -- Acquisition
        acquisition_source,
        acquisition_campaign_id,

        -- Metadata
        created_at,
        updated_at,
        is_deleted

    from source
)

select * from final
