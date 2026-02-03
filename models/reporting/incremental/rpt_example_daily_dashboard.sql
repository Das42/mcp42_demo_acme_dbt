{{
    config(
        alias='example_daily_dashboard',
        unique_key='metric_date'
    )
}}

-- Reporting layer: Incremental dashboard metrics
-- This model demonstrates an incremental reporting table
-- Use for large aggregated datasets consumed by dashboards

select
    metric_date,
    sum(event_count) as total_events,
    count(distinct event_type) as unique_event_types,
    current_timestamp() as last_refreshed_at

from {{ ref('int_example_daily_metrics') }}

{% if is_incremental() %}
where metric_date >= (select max(metric_date) from {{ this }})
{% endif %}

group by 1
