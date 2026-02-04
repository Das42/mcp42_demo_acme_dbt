{{
    config(
        alias='dim_date'
    )
}}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2023-01-01' as date)",
        end_date="cast('2026-12-31' as date)"
    ) }}
),

final as (
    select
        -- Surrogate key (YYYYMMDD format)
        cast(to_char(date_day, 'YYYYMMDD') as integer) as date_sk,

        -- Natural key
        date_day as date_actual,

        -- Year attributes
        extract(year from date_day) as year_actual,
        extract(quarter from date_day) as quarter_actual,
        extract(month from date_day) as month_actual,
        extract(week from date_day) as week_of_year,
        extract(dayofweek from date_day) as day_of_week,
        extract(dayofyear from date_day) as day_of_year,

        -- Date names
        to_char(date_day, 'MMMM') as month_name,
        to_char(date_day, 'Mon') as month_name_short,
        to_char(date_day, 'Day') as day_name,
        to_char(date_day, 'Dy') as day_name_short,

        -- Fiscal calendar (assuming calendar year = fiscal year)
        extract(year from date_day) as fiscal_year,
        extract(quarter from date_day) as fiscal_quarter,
        extract(month from date_day) as fiscal_month,

        -- Period keys
        cast(to_char(date_day, 'YYYYMM') as integer) as year_month_key,
        cast(to_char(date_day, 'YYYYQ') || extract(quarter from date_day) as varchar) as year_quarter_key,

        -- Flags
        case
            when extract(dayofweek from date_day) in (0, 6) then true
            else false
        end as is_weekend,
        case
            when extract(dayofweek from date_day) between 1 and 5 then true
            else false
        end as is_weekday,
        case
            when date_day = last_day(date_day) then true
            else false
        end as is_last_day_of_month

    from date_spine
)

select * from final
