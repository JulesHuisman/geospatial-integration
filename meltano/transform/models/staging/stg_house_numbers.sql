WITH house_numbers AS (

    SELECT
        CAST(REGEXP_EXTRACT(identificatie, r'Nummeraanduiding.(.*)') AS INT64) AS address_id,
        huisnummer AS address_housenumber,
        huisletter AS address_houseletter,
        huisnummerToevoeging AS address_housenumber_suffix,
        postcode as pc6_name,
        voorkomenIdentificatie as address_order
    FROM
        {{ source('ingestion', 'nummeraanduiding') }}

),

ranked_house_numbers AS (
  SELECT 
    * except(address_order), 
    row_number() over (partition by address_id order by address_order desc) as rank
  FROM 
    house_numbers
)

SELECT 
    * except(rank)
FROM 
    ranked_house_numbers 
WHERE 
    rank = 1