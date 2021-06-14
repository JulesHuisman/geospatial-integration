with livability as (

    select
        * except(geometry, neighbourhood_name),
        id as neighbourhood_id
    from
        {{ ref('stg_livability') }}

)

select 
    neighbourhood_key,
    year as year_key,
    livability_score,
    housing_score,
    inhabitant_score,
    utility_score,
    safety_score,
    environment_score
from 
    livability
inner join 
    {{ ref('dim_neighbourhood') }} dim_neighbourhood
on 
    livability.neighbourhood_id = dim_neighbourhood.neighbourhood_id