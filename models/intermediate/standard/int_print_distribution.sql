{{
    config(
        alias='print_distribution'
    )
}}

/*
    Print distribution performance combining deliveries, routes, and centers.
    Provides visibility into print delivery operations and performance.
*/

with deliveries as (
    select * from {{ ref('stg_print_deliveries') }}
),

routes as (
    select * from {{ ref('stg_distribution_routes') }}
),

centers as (
    select * from {{ ref('stg_distribution_centers') }}
),

editions as (
    select * from {{ ref('stg_print_editions') }}
)

select
    d.print_delivery_pk,
    d.delivery_id,
    d.subscription_id,
    d.reader_id,
    d.edition_id,
    d.route_id,
    d.carrier_id,

    -- Delivery details
    d.scheduled_date,
    d.delivery_status,
    d.delivery_timestamp,
    d.delivery_location,
    d.is_delivered,
    d.has_exception,
    d.exception_code,
    d.exception_notes,

    -- Route details
    r.route_name,
    r.zone_code,
    r.route_type,
    r.total_stops as route_total_stops,

    -- Distribution center
    r.distribution_center_id,
    c.center_name,
    c.city as center_city,
    c.state as center_state,
    c.region as center_region,

    -- Edition details
    e.publication_id,
    e.edition_date,
    e.edition_type,
    e.page_count,

    -- Derived metrics
    case
        when d.delivery_timestamp is not null and d.scheduled_date is not null
        then datediff('hour', d.scheduled_date::timestamp_ntz, d.delivery_timestamp)
    end as delivery_variance_hours,
    case
        when d.delivery_timestamp is not null
        then extract(hour from d.delivery_timestamp)
    end as delivery_hour,

    -- Performance flags
    case
        when d.is_delivered and d.delivery_timestamp <= d.scheduled_date::timestamp_ntz + interval '6 hours'
        then true
        else false
    end as is_on_time,

    d.created_at,
    d.updated_at

from deliveries d
left join routes r
    on d.route_id = r.route_id
left join centers c
    on r.distribution_center_id = c.center_id
left join editions e
    on d.edition_id = e.edition_id
