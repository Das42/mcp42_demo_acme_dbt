{{
    config(
        alias='dim_publication'
    )
}}

/*
    Publication dimension containing newspaper and magazine attributes.
    Includes publication type, frequency, and active status.
*/

with publications as (
    select * from {{ ref('stg_publications') }}
)

select
    publication_pk,
    publication_id,

    -- attributes
    publication_name,
    publication_type,
    frequency,
    launch_date,
    is_active,

    -- derived attributes
    case
        when frequency in ('daily', 'weekly') then 'high'
        when frequency in ('biweekly', 'monthly') then 'medium'
        else 'low'
    end as publication_frequency_group,
    datediff('year', launch_date, current_date()) as years_since_launch,

    -- metadata
    created_at,
    updated_at

from publications;
