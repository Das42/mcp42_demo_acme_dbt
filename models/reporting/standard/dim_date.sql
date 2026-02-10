{{
    config(
        alias='dim_date'
    )
}}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="dateadd(day, 1, current_date())"
    ) }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_key,

        -- Date value
        cast(date_day as date) as date_day,

        -- Calendar attributes
        year(date_day) as calendar_year,
        quarter(date_day) as calendar_quarter,
        month(date_day) as calendar_month,
        weekofyear(date_day) as calendar_week,
        day(date_day) as calendar_day,
        dayofweek(date_day) as day_of_week,
        dayname(date_day) as day_name,
        monthname(date_day) as month_name,

        -- Flags
        case
            when dayofweek(date_day) in (0, 6) then true
            else false
        end as is_weekend

    from date_spine
)

select * from final
