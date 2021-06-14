with energy_labels as (

    select 
        *
    from 
        {{ ref('stg_energy_label') }}
    where
        (housing_id is not null)
        or 
        (pc6_key is not null)

),

housing_address as (

    select
        a.address_key,
        a.neighbourhood_key,
        b.housing_id
    from 
        {{ ref('fct_housing') }} a
    inner join
        {{ ref('dim_address') }} b
    on 
        a.address_key = b.address_key

),

add_keys as (

    select
        a.*,
        b.address_key,
        b.neighbourhood_key
    from 
        energy_labels a
    left join
        housing_address as b
    on 
        a.housing_id = b.housing_id

)

select 
    measurement_date_key,
    address_key,
    pc6_key,
    neighbourhood_key,
    energy_label_numeric
from 
    add_keys







