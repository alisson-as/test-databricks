-- Gold Table: Missions Aggregated by Year
-- This query summarizes space activity on a yearly basis, ideal for time-series analysis.
-- Granularity: One row per year.

SELECT
    mission_year,
    
    -- Yearly Counts
    COUNT(mission_id) AS number_of_missions,
    COUNT(DISTINCT astronaut_id) AS number_of_active_astronauts,
    
    -- Yearly Aggregates
    ROUND(SUM(mission_duration_hours)) AS total_flight_hours_in_year,
    ROUND(SUM(mission_eva_hours), 2) AS total_eva_hours_in_year,
    
    -- ETL Metadata
    current_timestamp() AS gold_load_timestamp

FROM silver.astronauts.missions

WHERE 1=1
AND mission_year > 0

GROUP BY
    mission_year

ORDER BY
    mission_year ASC;