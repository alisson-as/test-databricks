-- Gold Table: EVA Specialists Dimension
-- This table creates a profile for every astronaut who has performed at least one EVA.
-- It's a specialized dimension table for analyzing spacewalking careers.
-- Granularity: One row per astronaut with EVA experience.

WITH base_missions
AS (
    SELECT
        astronaut_id,
        astronaut_name,
        MAX(nationality_cleaned) AS current_nationality,
        COUNT(mission_id) AS total_eva_missions,
        SUM(mission_eva_hours) AS total_eva_hours,
        MIN(mission_year) AS first_eva_year,
        MAX(mission_year) AS last_eva_year
        
    FROM silver.astronauts.missions
    WHERE
        had_eva = true -- Filter for missions with EVA only
    GROUP BY
        astronaut_id, astronaut_name
)
SELECT
    astronaut_id,
    astronaut_name,
    current_nationality,
    
    -- EVA Career Metrics
    total_eva_missions,
    total_eva_hours,
    ROUND(total_eva_hours / total_eva_missions, 2) AS avg_hours_per_eva_mission,
    
    -- Career Timeline
    first_eva_year,
    last_eva_year,
    (last_eva_year - first_eva_year) AS eva_career_span_years,
    
    -- ETL Metadata
    current_timestamp() AS gold_load_timestamp

FROM base_missions

ORDER BY
    total_eva_hours DESC;