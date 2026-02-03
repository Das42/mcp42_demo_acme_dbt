{{
    config(
        alias='example_daily_metrics',
        unique_key='metric_date'
    )
}}

-- Intermediate layer: Incremental aggregated metrics
-- This model demonstrates daily metric aggregation
-- Use for time-series transformations

select
    date_trunc('day', event_timestamp)::date as metric_date,
    event_type,
    count(*) as event_count,
    current_timestamp() as _transformed_at

from {{ ref('stg_example_events') }}

{% if is_incremental() %}
where date_trunc('day', event_timestamp)::date >= (
    select max(metric_date) from {{ this }}
)
{% endif %}

group by 1, 2
