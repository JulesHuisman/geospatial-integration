with pc6_dimension as (

    select
        *
    from 
        {{ ref('stg_pc6') }}

)

select *
from pc6_dimension