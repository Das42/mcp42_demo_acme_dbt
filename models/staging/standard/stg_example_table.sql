{{
    config(
        alias='example_table'
    )
}}

-- Staging layer: Cleaned and standardized data
-- This model demonstrates a standard staging view (transient)
-- Apply data type casting, renaming, and basic cleaning

select
    id,
    trim(upper(name)) as name,
    _loaded_at

from {{ ref('lnd_example_table') }}
