with points_of_interest as (

    select
        osm_id as poi_id,
        name as poi_name, 
        {{ parse_osm_tag('amenity') }},
        {{ parse_osm_tag('shop') }},
        highway,
        {{ parse_osm_tag('addr:postcode', 'pc6_name') }},
        lower({{ parse_osm_tag('addr:housenumber', 'address_housenumber_letter') }}),
        geometry as poi_geometry
    from
        {{ source('ingestion', 'points') }}
    where 
        (highway is null)
        or 
        (highway = 'bus_stop')
        
),

points_of_interest_type as (

    select
        * except(amenity, shop, highway),
        coalesce(amenity, shop, highway) as poi_type
    from 
        points_of_interest

)

select 
    *
from 
    points_of_interest_type
where 
    poi_type is not null