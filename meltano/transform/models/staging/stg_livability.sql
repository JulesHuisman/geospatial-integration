WITH ranked_livability AS (
    SELECT livability.*, ROW_NUMBER() OVER (PARTITION BY id) AS rank
    FROM {{ source('ingestion', 'leefbaarometer_buurten_2018') }} as livability
),

livability AS (

    SELECT
        id,
        name as neighbourhood_name,
        rlbwon AS housing_score,
        rlbbev AS inhabitant_score,
        rlbvrz AS utility_score,
        rlbvei AS safety_score,
        rlbfys AS environment_score,
        score AS livability_score,
        year,
        geometry
    FROM
        ranked_livability
    WHERE 
        rank = 1

)

SELECT *
FROM livability