with renamed as (

    select 
        {{ dbt_utils.surrogate_key(['buurtcode']) }} as neighbourhood_key,
        buurtcode as neighbourhood_id,
        buurtnaam as neighbourhood_name,
        geometry as neighbourhood_geometry,
        wijkcode as district_id,
        wijknaam as district_name,
        gemeentecode as municipality_id,
        gemeentenaam as municipality_name,
        water as neighbourhood_is_water,
    from
        {{ source('ingestion', 'wijkenbuurten2020_cbs_buurten_2020') }}

),

no_water as (

    select 
        * except(neighbourhood_is_water)
    from
        renamed
    where
        neighbourhood_is_water = 'NEE'

)

select *
from no_water