with dim_address as (

    select 
        address_key,
        pc6_name,
        COALESCE(CONCAT(address_housenumber, LOWER(address_houseletter)), CAST(address_housenumber AS String)) AS address_housenumber_letter,
    from 
        {{ ref('dim_address') }}
    where 
        (address_is_latest = true)
        and
        (address_currently_in_use = true)
    
),

point_of_interest_address as (

    select 
        stg_poi.*,
        dim_address.address_key,

        -- stg_poi.*,
        -- dim_address.address_id,
        -- dim_address.pc6_name,
        -- dim_address.housenumber_letter,
    from
        {{ ref('stg_point_of_interest') }} as stg_poi
    left join 
        dim_address
    on 
        (dim_address.address_housenumber_letter = lower(stg_poi.address_housenumber_letter))
        and
        (dim_address.pc6_name = stg_poi.pc6_name)

),

neighbourhood_join as (

    select 
        a.*,
        b.neighbourhood_key
    from 
        point_of_interest_address a,
        {{ ref('dim_neighbourhood') }} b
    where
        ST_DWithin(
            b.neighbourhood_geometry,
            a.poi_geometry,
            0
        )

)

select 
    address_key,
    pc6_name as pc6_key,
    neighbourhood_key,
    poi_name,
    poi_type,
    poi_geometry
from 
    neighbourhood_join









-- WITH dim_address AS (

--     SELECT 
--         address_id,
--         pc6_name,
--         COALESCE(CONCAT(housenumber, LOWER(houseletter)), CAST(housenumber AS String)) AS housenumber_letter,
--         geometry
--     FROM 
--         {{ ref('dim_address') }}
--     WHERE 
--         end_usage_date IS NULL
    
-- ),

-- -- First we try to join the osm data with the address dimension based on the
-- -- provided address in the osm dataset

-- point_of_interest_address AS (

--     SELECT 
--         stg_point_of_interest.id,
--         dim_address.address_id AS address_candidate,

--         -- stg_point_of_interest.*,
--         -- dim_address.address_id,
--         -- dim_address.pc6_name,
--         -- dim_address.housenumber_letter,
--     FROM
--         {{ ref('stg_point_of_interest') }} AS stg_point_of_interest
--     LEFT JOIN 
--         dim_address
--     ON 
--         dim_address.housenumber_letter = LOWER(stg_point_of_interest.housenumber)
--         AND
--         (dim_address.pc6_name = stg_point_of_interest.postcode)

-- ),

-- -- Next, we try to find the closest addresses that does not have a living function

-- neighbours AS (

--     SELECT 
--         stg_point_of_interest.id,
--         -- stg_point_of_interest.name,
--         -- stg_point_of_interest.type,
--         -- stg_point_of_interest.city,
--         -- stg_point_of_interest.street,
--         -- stg_point_of_interest.postcode,
--         -- stg_point_of_interest.housenumber,
--         dim_address.address_id,
--         -- dim_address.pc6_name,
--         -- dim_address.housenumber,
--         -- dim_address.houseletter,
--         -- dim_address.last_function_types,
--         ST_DISTANCE(stg_point_of_interest.geometry, dim_address.geometry) as distance
--     FROM
--         {{ ref('stg_point_of_interest') }} AS stg_point_of_interest,
--         {{ ref('dim_address') }} AS dim_address
--     WHERE 
--         ST_DISTANCE(stg_point_of_interest.geometry, dim_address.geometry) < 30
--         AND
--         dim_address.last_function_types != 'woonfunctie'

-- ),

-- distance_rank AS (

--     -- SELECT id, MIN(distance) AS min_distance
--     SELECT 
--         id,
--         address_id,
--         ROW_NUMBER() OVER (PARTITION BY id ORDER BY distance ASC) AS rank
--     FROM 
--         neighbours
--     -- GROUP BY id

-- ),

-- -- WITH ranked_livability AS (
-- --     SELECT livability.*, ROW_NUMBER() OVER (PARTITION BY id) AS rank
-- --     FROM {{ source('ingestion', 'leefbaarometer_buurten_2018') }} as livability
-- -- ),

-- nearest_neighbour AS (

--     -- SELECT 
--     --     min_distance.id, 
--     --     address_id as closest_candidate,
--     --     neighbours.distance,
--     --     min_distance.min_distance
--     -- FROM 
--     --     neighbours 
--     -- INNER JOIN
--     --     min_distance
--     -- ON 
--     --     neighbours.id = min_distance.id 
--     --     AND 
--     --     neighbours.distance = min_distance.min_distance

--     SELECT 
--         id, 
--         address_id as closest_candidate
--     FROM 
--         distance_rank
--     WHERE
--         rank = 1

-- )

-- -- SELECT *
-- -- FROM nearest_neighbour
-- -- ORDER BY id

-- -- SELECT id, count(id) as cnt
-- -- FROM nearest_neighbour
-- -- GROUP BY id
-- -- ORDER BY cnt DESC

-- SELECT 
--     stg_point_of_interest.id,
--     name, 
--     type,
--     -- address_candidate,
--     -- closest_candidate,
--     -- Use the address if we have it, otherwise use the closest candidate
--     COALESCE(address_candidate, closest_candidate) AS address_id,
--     geometry
-- FROM 
--     {{ ref('stg_point_of_interest') }} AS stg_point_of_interest
-- LEFT JOIN 
--     point_of_interest_address
-- ON 
--     stg_point_of_interest.id = point_of_interest_address.id
-- LEFT JOIN 
--     nearest_neighbour
-- ON 
--     stg_point_of_interest.id = nearest_neighbour.id



