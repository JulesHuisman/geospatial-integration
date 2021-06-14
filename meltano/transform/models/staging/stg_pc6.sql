with renamed as (

    select 
        pc6.PC6 as pc6_name,
        geometry as pc6_geometry
    from 
        {{ source('ingestion', 'pc6') }} pc6

),

lower_granularities as (

    select 
        substring(pc6_name, 1, 1) as pc1_name,
        substring(pc6_name, 1, 2) as pc2_name,
        substring(pc6_name, 1, 3) as pc3_name,
        substring(pc6_name, 1, 4) as pc4_name,
        substring(pc6_name, 1, 5) as pc5_name,
        pc6_name,
        pc6_geometry
    from 
        renamed
)

select
    pc6_name as pc6_key,
    pc6_name,
    pc5_name,
    pc4_name,
    pc3_name,
    pc2_name,
    pc1_name,
    pc6_geometry
from
    lower_granularities