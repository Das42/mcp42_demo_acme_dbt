{{
    config(
        alias='example_events',
        unique_key='event_id'
    )
}}

-- Landing layer: Incremental event ingestion
-- This model demonstrates an incremental landing table
-- Replace with actual source data

select
    1 as event_id,
    'click' as event_type,
    current_timestamp() as event_timestamp,
    current_timestamp() as _loaded_at

{% if is_incremental() %}
where event_timestamp > (select max(event_timestamp) from {{ this }})
{% endif %}
