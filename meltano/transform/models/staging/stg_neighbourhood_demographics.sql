{% 

    set measurement_columns = [
        "bevolkingsdichtheid_inwoners_per_km2",
        "aantal_inwoners",
        "percentage_personen_0_tot_15_jaar",	
        "percentage_personen_15_tot_25_jaar",	
        "percentage_personen_25_tot_45_jaar",	
        "percentage_personen_45_tot_65_jaar",	
        "percentage_personen_65_jaar_en_ouder",	
        "percentage_ongehuwd",	
        "percentage_gehuwd",	
        "percentage_gescheid",	
        "percentage_verweduwd",
        "percentage_eenpersoonshuishoudens",	
        "percentage_huishoudens_zonder_kinderen",	
        "percentage_huishoudens_met_kinderen",	
        "gemiddelde_huishoudsgrootte",	
        "percentage_westerse_migratieachtergrond",	
        "percentage_niet_westerse_migratieachtergrond",	
        "percentage_uit_marokko",	
        "percentage_uit_nederlandse_antillen_en_aruba",	
        "percentage_uit_suriname",	
        "percentage_uit_turkije",	
        "percentage_overige_nietwestersemigratieachtergrond",
        "percentage_bewoond",
        "percentage_koopwoningen",
        "percentage_huurwoningen",
    ] 

%}

with demographics_union as ( 

    {{ dbt_utils.union_relations( 
        relations=[ 
            source('ingestion', 'wijkenbuurten2019_cbs_buurten_2019'), 
            source('ingestion', 'wijkenbuurten2020_cbs_buurten_2020')
        ], 
        include=[ 
            "buurtcode",  
            "water",
            "bevolkingsdichtheid_inwoners_per_km2",
            "aantal_inwoners",
            "percentage_personen_0_tot_15_jaar",	
            "percentage_personen_15_tot_25_jaar",	
            "percentage_personen_25_tot_45_jaar",	
            "percentage_personen_45_tot_65_jaar",	
            "percentage_personen_65_jaar_en_ouder",	
            "percentage_ongehuwd",	
            "percentage_gehuwd",	
            "percentage_gescheid",	
            "percentage_verweduwd",
            "percentage_eenpersoonshuishoudens",	
            "percentage_huishoudens_zonder_kinderen",	
            "percentage_huishoudens_met_kinderen",	
            "gemiddelde_huishoudsgrootte",	
            "percentage_westerse_migratieachtergrond",	
            "percentage_niet_westerse_migratieachtergrond",	
            "percentage_uit_marokko",	
            "percentage_uit_nederlandse_antillen_en_aruba",	
            "percentage_uit_suriname",	
            "percentage_uit_turkije",	
            "percentage_overige_nietwestersemigratieachtergrond",
            "percentage_bewoond",
            "percentage_koopwoningen",
            "percentage_huurwoningen",
        ]
    ) }} 

),

no_water as (

    select 
        * except(water)
    from 
        demographics_union
    where 
        water = 'NEE'

),

parse_year as (

    select
        cast(substring(_dbt_source_relation, -5, 4) as int64) as year_key,
        * except(_dbt_source_relation)
    from  
        demographics_union

),

nullify_invalid as (

    select
        year_key,
        buurtcode,
        {% for measurement_column in measurement_columns %}
            case when {{ measurement_column }} < -1 then null else {{ measurement_column }} end as {{ measurement_column }},
        {% endfor %}
    from
        parse_year

),

renamed as (

    select
        year_key,
        buurtcode as neighbourhood_id,
        bevolkingsdichtheid_inwoners_per_km2 as inhabitant_density,
        aantal_inwoners as n_inhabitants,
        percentage_personen_0_tot_15_jaar as percentage_0_15_years,	
        percentage_personen_15_tot_25_jaar as percentage_15_25_years,
        percentage_personen_25_tot_45_jaar as percentage_25_45_years,	
        percentage_personen_45_tot_65_jaar as percentage_45_65_years,	
        percentage_personen_65_jaar_en_ouder as percentage_65_plus_years,	
        percentage_ongehuwd as percentage_unmarried,
        percentage_gehuwd as percentage_married,	
        percentage_gescheid as percentage_divorced,	
        percentage_verweduwd as percentage_widowed,
        percentage_eenpersoonshuishoudens as percentage_single_household,	
        percentage_huishoudens_zonder_kinderen as percentage_no_kids_household,	
        percentage_huishoudens_met_kinderen as percentage_kids_household,	
        gemiddelde_huishoudsgrootte as average_household_size,	
        percentage_westerse_migratieachtergrond as percentage_western_migration_background,	
        percentage_niet_westerse_migratieachtergrond as percentage_non_western_migration_background,	
        percentage_uit_marokko as percentage_from_morocco,	
        percentage_uit_nederlandse_antillen_en_aruba as percentage_dutch_antilles,	
        percentage_uit_suriname as percentage_suriname,	
        percentage_uit_turkije as percentage_turkey,	
        percentage_overige_nietwestersemigratieachtergrond as percentage_other_non_western,
        percentage_bewoond as percentage_inhabited,
        percentage_koopwoningen as percentage_owner_occupied,
        percentage_huurwoningen as percentage_rental,
    from
        nullify_invalid

),

join_neighbourhoods as (

    select
        b.neighbourhood_key,
        a.* except(neighbourhood_id),
    from 
        renamed a
    inner join
        {{ ref('dim_neighbourhood') }} b
    on 
        a.neighbourhood_id = b.neighbourhood_id

)

select *
from join_neighbourhoods







