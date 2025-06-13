-- Gold Table: Statistics by Astronaut Occupation
-- This query provides insights into the roles and responsibilities during missions.
-- It cleans the occupation field for better grouping.
-- Granularity: One row per occupation.

SELECT
    LOWER(TRIM(occupation)) as occupation,
    
    -- Counts
    COUNT(mission_id) AS number_of_assignments,
    COUNT(DISTINCT astronaut_id) AS unique_astronauts_in_role,
    
    -- Averages
    ROUND(AVG(mission_duration_hours), 2) AS avg_mission_hours_for_role,
    
    -- ETL Metadata
    current_timestamp() AS gold_load_timestamp

FROM silver.astronauts.missions

WHERE
    occupation IS NOT NULL

GROUP BY
    LOWER(TRIM(occupation))

-- Optional: Filter out very rare occupations to focus on common roles
HAVING
    COUNT(mission_id) > 2

ORDER BY
    number_of_assignments DESC;