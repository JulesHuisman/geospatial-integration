with energy_labels as (

    SELECT 
        cast(format_datetime("%Y%m%d", PARSE_DATE('%Y%m%d', CAST(pand_opnamedatum as STRING))) as int64) as measurement_date_key,
        pand_postcode as pc6_key,
        pand_energieklasse as energy_label,
        pand_gebouwtype as building_type,
        pand_gebouwsubtype as building_sub_type,
        pand_bagverblijfsobjectid as housing_id
    from 
        {{ source('ingestion', 'energy_labels') }}

),

numeric_label as (

    select 
        *,
        case energy_label 
            when 'A+++++' then 1
            when 'A++++'  then 2
            when 'A+++'   then 3
            when 'A++'    then 4
            when 'A+'     then 5
            when 'A'      then 6
            when 'B'      then 7
            when 'C'      then 8
            when 'D'      then 9
            when 'E'      then 10
            when 'F'      then 11
            when 'G'      then 12
            end
        as energy_label_numeric
    from 
        energy_labels
        
)

select *
from numeric_label