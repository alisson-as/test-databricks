-- Gold Table: First Astronaut by Country
-- This query uses window functions to identify the first mission for an astronaut from each country.
-- It's a 'fact' table capturing a specific historical milestone.
-- Granularity: One row per country.

WITH ranked_missions AS (
    SELECT
        nationality_cleaned,
        astronaut_name,
        mission_year,
        mission_title,
        -- Assign a rank to each mission within a country, ordered by year
        ROW_NUMBER() OVER(PARTITION BY nationality_cleaned ORDER BY mission_year ASC, astronaut_id ASC) as mission_rank
    FROM
        silver.astronauts.missions
    WHERE
        nationality_cleaned IS NOT NULL
)

SELECT
    nationality_cleaned AS country,
    astronaut_name AS first_astronaut,
    mission_year AS first_mission_year,
    mission_title AS first_mission_title,
    
    -- ETL Metadata
    current_timestamp() AS gold_load_timestamp

FROM ranked_missions
WHERE 1=1
  AND mission_rank = 1 -- Select only the first mission for each country
  AND mission_year > 0
ORDER BY
    first_mission_year ASC;