with addresses_in_use as (

    select 
        address_key,
        address_geometry,
        address_surface_area,
        address_function_types,
        pc6_name
    from
        {{ ref('stg_address') }}
    where
        address_is_latest = true 
        and 
        address_currently_in_use = true

),

neighbourhood_join as (

    select 
        a.*,
        b.neighbourhood_key
    from 
        addresses_in_use a,
        {{ ref('dim_neighbourhood') }} b
    where
        ST_DWithin(
            b.neighbourhood_geometry,
            a.address_geometry,
            0
        )

)

-- pc6_join as (

--     select 
--         a.*,
--         b.pc6_key
--     from 
--         neighbourhood_join a
--     join
--         {{ ref('dim_pc6') }} b
--     on
--         ST_Within(
--             b.pc6_geometry,
--             a.address_geometry
--         )

-- )

select
    address_key,
    pc6_name as pc6_key,
    neighbourhood_key,
    address_surface_area as housing_surface_area,
    address_function_types as housing_function_types,
    address_geometry as housing_geometry,
from 
    neighbourhood_join






