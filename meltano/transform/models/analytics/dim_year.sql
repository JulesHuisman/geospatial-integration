with year_dimension as (

    select 
        dd.calendar_year as year_key,
        dd.calendar_year
    from 
        {{ ref('dim_date') }} dd
    group by 
        dd.calendar_year

)

select yd.*
from year_dimension yd
order by year_key