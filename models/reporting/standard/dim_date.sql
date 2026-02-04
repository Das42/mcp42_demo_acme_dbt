{{
    config(
        alias='dim_date'
    )
}}

/*
    Date dimension with standard calendar attributes.
    Covers a 10-year range centered on the current date.
*/

with date_spine as (
    {{
        dbt_utils.date_spine(
            datepart='day',
            start_date="dateadd('year', -5, current_date())",
            end_date="dateadd('year', 5, current_date())"
        )
    }}
),

date_attributes as (
    select
        {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_pk,
        cast(date_day as date) as date_day,

        -- date parts
        extract(year from date_day) as year_number,
        extract(quarter from date_day) as quarter_number,
        extract(month from date_day) as month_number,
        extract(week from date_day) as week_number,
        extract(dayofweek from date_day) as day_of_week_number,
        extract(dayofyear from date_day) as day_of_year_number,

        -- formatted values
        to_char(date_day, 'YYYY-MM') as year_month,
        to_char(date_day, 'YYYY-Q') || extract(quarter from date_day) as year_quarter,
        to_char(date_day, 'MMMM') as month_name,
        left(to_char(date_day, 'MMMM'), 3) as month_name_short,
        to_char(date_day, 'DY') as day_name_short,
        to_char(date_day, 'Day') as day_name,

        -- period start/end dates
        date_trunc('week', date_day) as week_start_date,
        dateadd('day', 6, date_trunc('week', date_day)) as week_end_date,
        date_trunc('month', date_day) as month_start_date,
        last_day(date_day, 'month') as month_end_date,
        date_trunc('quarter', date_day) as quarter_start_date,
        last_day(date_day, 'quarter') as quarter_end_date,
        date_trunc('year', date_day) as year_start_date,
        last_day(date_day, 'year') as year_end_date,

        -- flags
        case
            when extract(dayofweek from date_day) in (0, 6) then true
            else false
        end as is_weekend,
        case
            when date_day = current_date() then true
            else false
        end as is_today,
        case
            when date_day < current_date() then true
            else false
        end as is_past,
        case
            when date_day > current_date() then true
            else false
        end as is_future

    from date_spine
)

select * from date_attributes;
