-- Gold Table: Astronaut Demographics Summary
-- This query aggregates key metrics by astronaut status (military/civilian) and sex.
-- It's useful for understanding the composition and contribution of different demographic groups.
-- Granularity: One row per combination of status and sex.

SELECT
    status,
    sex,
    
    -- Counts
    COUNT(DISTINCT astronaut_id) AS unique_astronauts,
    COUNT(mission_id) AS total_missions,
    
    -- Aggregated Metrics
    ROUND(SUM(mission_duration_hours)) AS total_flight_hours,
    ROUND(SUM(mission_eva_hours), 2) AS total_eva_hours,
    
    -- Averages
    ROUND(AVG(age_at_mission), 1) AS avg_age_at_first_mission,
    ROUND(AVG(total_career_missions), 1) AS avg_missions_per_astronaut,
    
    -- ETL Metadata
    current_timestamp() AS gold_load_timestamp

FROM silver.astronauts.missions

WHERE 1=1
  AND status IS NOT NULL AND sex IS NOT NULL
  AND sex IN ('female', 'male')

GROUP BY
    status,
    sex

ORDER BY
    status,
    sex;