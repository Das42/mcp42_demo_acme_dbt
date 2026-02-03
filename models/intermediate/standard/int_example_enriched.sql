{{
    config(
        alias='example_enriched'
    )
}}

-- Intermediate layer: Business logic transformations
-- This model demonstrates joining and enriching staged data
-- Apply business rules and calculations

select
    t.id,
    t.name,
    count(e.event_id) as total_events,
    max(e.event_timestamp) as last_event_at,
    current_timestamp() as _transformed_at

from {{ ref('stg_example_table') }} t
left join {{ ref('stg_example_events') }} e
    on t.id = e.event_id

group by t.id, t.name
