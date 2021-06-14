with neighbourhood_demographics as (

    select 
        *
    from 
        {{ ref('stg_neighbourhood_demographics') }}

)

select 
    *
from 
    neighbourhood_demographics