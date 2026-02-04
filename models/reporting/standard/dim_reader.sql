{{
    config(
        alias='dim_reader'
    )
}}

with readers as (
    select * from {{ ref('stg_readers') }}
    where is_deleted = false
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['reader_id']) }} as reader_sk,

        -- Natural key
        reader_id,

        -- Contact info
        email,
        first_name,
        last_name,
        full_name,
        phone_number,

        -- Address
        address_line_1,
        address_line_2,
        city,
        state,
        postal_code,
        country,

        -- Demographics
        birth_date,
        gender,

        -- Acquisition
        acquisition_source,
        acquisition_campaign_id,

        -- Metadata
        created_at,
        updated_at

    from readers
)

select * from final
