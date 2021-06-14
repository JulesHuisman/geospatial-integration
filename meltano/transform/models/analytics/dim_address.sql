with address_dimension as (

    select 
        *
    from
        {{ ref('stg_address') }}

)

select *
from address_dimension