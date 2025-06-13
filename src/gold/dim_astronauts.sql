-- Gold Table: Astronaut Dimension
-- This query creates a master table for astronauts, aggregating all their career metrics.
-- Granularity: One row per astronaut.

SELECT
    s.astronaut_id,
    s.astronaut_name,
    s.sex,
    s.status,
    MIN(s.birth_year) AS birth_year,
    MAX(s.nationality_cleaned) AS current_nationality,
    
    -- Career Metrics
    COUNT(s.mission_id) AS total_missions,
    MIN(s.mission_year) AS first_mission_year,
    MAX(s.mission_year) AS last_mission_year,
    (MAX(s.mission_year) - MIN(s.mission_year)) AS career_duration_years,
    
    -- Aggregated Flight and EVA Statistics
    ROUND(SUM(s.mission_duration_hours), 2) AS total_flight_hours,
    ROUND(SUM(s.mission_eva_hours), 2) AS total_eva_hours,
    
    -- ETL Metadata
    current_timestamp() AS gold_load_timestamp

FROM silver.astronauts.missions s

WHERE 1=1
AND s.astronaut_id IS NOT NULL

GROUP BY
    s.astronaut_id,
    s.astronaut_name,
    s.sex,
    s.status;