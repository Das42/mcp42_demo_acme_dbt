{{
    config(
        alias='example_table'
    )
}}

-- Landing layer: Raw data ingestion
-- This model demonstrates a standard (full refresh) landing table
-- Replace with actual source data

select
    1 as id,
    'example' as name,
    current_timestamp() as _loaded_at
