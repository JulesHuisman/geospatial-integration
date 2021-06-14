with date_dimension as (
    {{ dbt_date.get_date_dimension('1800-01-01', '2022-12-31') }}
),

date_dimension_renamed as (
    select
        cast(FORMAT_DATETIME("%Y%m%d", date_day) as int64) as date_key,
        date_day as calendar_date,
        cast(FORMAT_DATETIME("%Y%m", date_day) as int64) as calendar_year_month,
        month_start_date as calendar_year_month_start,
        year_number as calendar_year,
    from
        date_dimension
)

select *
from date_dimension_renamed