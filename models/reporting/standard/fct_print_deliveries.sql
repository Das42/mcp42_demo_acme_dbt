{{
    config(
        alias='fct_print_deliveries'
    )
}}

/*
    Print delivery fact table.
    Tracks print distribution operations and delivery performance.
*/

with print_distribution as (
    select * from {{ ref('int_print_distribution') }}
),

readers as (
    select * from {{ ref('stg_readers') }}
),

dates as (
    select * from {{ ref('dim_date') }}
)

select
    pd.print_delivery_pk,
    pd.delivery_id,

    -- dimension keys
    r.reader_pk,
    d.date_pk as scheduled_date_pk,

    -- natural keys
    pd.subscription_id,
    pd.reader_id,
    pd.edition_id,
    pd.route_id,
    pd.carrier_id,
    pd.distribution_center_id,

    -- delivery details
    pd.scheduled_date,
    pd.delivery_status,
    pd.delivery_timestamp,
    pd.delivery_location,
    pd.is_delivered,
    pd.has_exception,
    pd.exception_code,
    pd.exception_notes,

    -- route context
    pd.route_name,
    pd.zone_code,
    pd.route_type,
    pd.route_total_stops,

    -- distribution center context
    pd.center_name,
    pd.center_city,
    pd.center_state,
    pd.center_region,

    -- edition context
    pd.publication_id,
    pd.edition_date,
    pd.edition_type,
    pd.page_count,

    -- performance metrics
    pd.delivery_variance_hours,
    pd.delivery_hour,
    pd.is_on_time,

    -- count measures
    1 as delivery_count,
    case when pd.is_delivered then 1 else 0 end as successful_delivery_count,
    case when pd.has_exception then 1 else 0 end as exception_count,
    case when pd.is_on_time then 1 else 0 end as on_time_delivery_count,

    -- metadata
    pd.created_at,
    pd.updated_at

from print_distribution pd
left join readers r
    on pd.reader_id = r.reader_id
left join dates d
    on pd.scheduled_date = d.date_day;
