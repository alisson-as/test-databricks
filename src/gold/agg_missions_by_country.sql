-- Gold Table: Missions Aggregated by Country
-- This query provides a summary of space program efforts and achievements per country.
-- Granularity: One row per country.

SELECT
    nationality_cleaned AS country,
    
    -- Astronaut and Mission Counts
    COUNT(DISTINCT astronaut_id) AS total_unique_astronauts,
    COUNT(mission_id) AS total_missions,
    
    -- Aggregated Flight and EVA Statistics
    ROUND(SUM(mission_duration_hours)) AS total_flight_hours,
    ROUND(SUM(mission_eva_hours), 2) AS total_eva_hours,
    
    -- Average Metrics
    ROUND(AVG(age_at_mission), 1) AS avg_astronaut_age_at_mission,
    
    -- ETL Metadata
    current_timestamp() AS gold_load_timestamp

FROM silver.astronauts.missions

GROUP BY
    nationality_cleaned

ORDER BY
    total_unique_astronauts DESC;