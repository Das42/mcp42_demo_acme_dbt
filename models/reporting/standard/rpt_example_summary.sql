{{
    config(
        alias='example_summary'
    )
}}

-- Reporting layer: Business-ready summary
-- This model demonstrates a final reporting table
-- Optimized for BI tools and analyst consumption

select
    id,
    name,
    total_events,
    last_event_at,
    case
        when total_events > 100 then 'high'
        when total_events > 10 then 'medium'
        else 'low'
    end as engagement_tier,
    _transformed_at as last_updated_at

from {{ ref('int_example_enriched') }}
