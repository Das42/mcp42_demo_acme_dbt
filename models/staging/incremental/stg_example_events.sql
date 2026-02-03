{{
    config(
        alias='example_events',
        unique_key='event_id'
    )
}}

-- Staging layer: Incremental cleaned events
-- This model demonstrates an incremental staging table (transient)
-- Apply standardization to event data

select
    event_id,
    lower(event_type) as event_type,
    event_timestamp,
    _loaded_at

from {{ ref('lnd_example_events') }}

{% if is_incremental() %}
where _loaded_at > (select max(_loaded_at) from {{ this }})
{% endif %}
