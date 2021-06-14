with remove_duplicates as (

    select
        identificatie as housing_id,
        hoofdadresNummeraanduidingRef as address_id,
        beginGeldigheid as address_start_date,
        eindGeldigheid as address_end_date,
        gebruiksdoel as address_function_types,
        oppervlakte as address_surface_area,
        status as address_state,
        voorkomenIdentificatie as address_state_order,
        geometry as address_geometry

    from 
        {{ source('ingestion', 'verblijfsobject') }}
    where 
        ((beginGeldigheid != eindGeldigheid) OR (eindGeldigheid IS NULL))
        -- and
        -- ST_WITHIN(geometry, ST_GEOGFROMTEXT('POLYGON((5.34847954386648 51.51098066562578,5.593611990155543 51.51098066562578,5.593611990155543 51.37359667543468,5.34847954386648 51.37359667543468,5.34847954386648 51.51098066562578))'))
    -- limit 50000
),

flatten_function_types as (

    select
        * except(address_function_types),
        ARRAY_TO_STRING(address_function_types, ' ') as address_function_types
    from 
        remove_duplicates
        
),

extract_ids as (

    select 
        * except(housing_id, address_id),
        {{ dbt_utils.surrogate_key([
                'address_id', 
                'address_surface_area',
                'address_function_types',
                'address_state',
                'address_start_date', 
                'address_end_date'
            ]) 
        }} as address_key,
        CAST(REGEXP_EXTRACT(housing_id, r'Verblijfsobject.(.*)') as INT64) as housing_id,
        CAST(REGEXP_EXTRACT(address_id, r'Nummeraanduiding.(.*)') as INT64) as address_id
    from 
        flatten_function_types

),

parsed_dates as (

    select
        * except(address_start_date, address_end_date),
        cast(format_datetime("%Y%m%d", parse_date('%Y/%m/%d',  address_start_date)) as int64) as address_start_date,
        cast(format_datetime("%Y%m%d", parse_date('%Y/%m/%d',  address_end_date)) as int64) as address_end_date
    from
        extract_ids

),

clean_dates as (

    select
        * except(address_start_date),
        case
            when address_start_date < 18000101 then 0
            else address_start_date
            end
        as address_start_date
    from
        parsed_dates

),

rename_states as (

    select
        * except(address_state),
        case address_state 
            when 'Verblijfsobject gevormd'                     then 'housing_formed'
            when 'Niet gerealiseerd verblijfsobject'           then 'housing_not_realized'
            when 'Verblijfsobject in gebruik (niet ingemeten)' then 'housing_in_use_not_measured'
            when 'Verblijfsobject in gebruik'                  then 'housing_in_use'
            when 'Verblijfsobject ingetrokken'                 then 'housing_destroyed'
            when 'Verblijfsobject buiten gebruik'              then 'housing_disabled'
            when 'Verbouwing verblijfsobject'                  then 'housing_rebuilding'
            when 'Verblijfsobject ten onrechte opgevoerd'      then 'housing_invalid_creation'
            end
        as address_state
    from
        clean_dates

),

latest_address as (

    select 
        a.*,
        case 
            when a.address_state_order = b.max_address_state_order then true else false
            end
        as address_is_latest
    from
        rename_states a
    left join
        (
            select
                address_id,
                max(address_state_order) as max_address_state_order
            from 
                rename_states
            group by 
                address_id
        ) as b
    on 
        a.address_id = b.address_id

),

currently_in_use as (

    select
        *, 
        case 
            when 
                address_is_latest = true
                and address_state like 'housing_in_use%'
                and address_end_date is null
            then 
                true
            else
                false
            end
        as address_currently_in_use
    from 
        latest_address

),

house_numbers as (

    select 
        a.*,
        b.* except(address_id)
    from
        currently_in_use as a
    left join 
        {{ ref('stg_house_numbers') }} as b
    on 
        a.address_id = b.address_id

),

building_type as (

    select
        a.*,
        b.building_type as address_building_type,
        b.building_sub_type as address_building_sub_type,
        b.energy_label as address_energy_label
    from 
        house_numbers a
    left join
        {{ ref('stg_energy_label') }} b
    on
        a.housing_id = b.housing_id

)

select 
    address_key,
    housing_id,
    address_id,
    pc6_name,
    address_housenumber,
    address_houseletter,
    address_housenumber_suffix,
    address_function_types,
    address_state,
    address_energy_label,
    address_building_type,
    address_building_sub_type,
    address_surface_area,
    address_start_date,
    address_end_date,
    address_is_latest,
    address_currently_in_use,
    address_geometry
from 
    building_type
order by
    address_id, address_start_date











