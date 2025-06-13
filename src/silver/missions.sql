-- ===================================================================================
-- ETL Job: Bronze to Silver Transformation for Astronaut Missions
-- Description: This script cleans, standardizes, and enriches the raw astronaut 
--              data from the Bronze layer to create a well-structured Silver table.
-- Source Table: bronze.astronauts.list_astronauts
-- Target Table: silver.astronauts.missions
-- Granularity: One record per astronaut per mission
-- Goals: Cleaned and enriched astronaut mission data. Contains one record per astronaut per mission.
-- ===================================================================================

SELECT
    -- === Key Identifiers ===
    -- Renaming for clarity and casting to appropriate types.
    CAST(id AS INT) AS astronaut_id,
    CAST(mission_number AS INT) AS mission_sequence,
    -- Generating a durable, unique key for each mission record.
    sha2(concat_ws('||', id, mission_title, year_of_mission), 256) AS mission_id,

    -- === Astronaut Demographics ===
    -- Cleaning and standardizing astronaut-specific information.
    name AS astronaut_name,
    original_name,
    sex,
    CAST(year_of_birth AS INT) AS birth_year,
    -- Standardizing country names for consistency.
    CASE
        WHEN nationality = 'U.S.S.R/Russia' THEN 'Russia'
        WHEN nationality = 'U.S.' THEN 'USA'
        ELSE nationality
    END AS nationality_cleaned,
    military_civilian AS status,
    CAST(year_of_selection AS INT) AS selection_year,
    selection AS selection_program,

    -- === Mission Details ===
    -- Cleaning and standardizing mission-specific attributes.
    mission_title,
    CAST(year_of_mission AS INT) AS mission_year,
    occupation,
    ascend_shuttle,
    in_orbit AS in_orbit_vehicle,
    descend_shuttle,

    -- === Mission Metrics ===
    -- Casting all metrics to numeric types for analytical use.
    CAST(total_number_of_missions AS INT) AS total_career_missions,
    CAST(hours_mission AS DOUBLE) AS mission_duration_hours,
    CAST(total_hrs_sum AS DOUBLE) AS total_career_hours,
    
    -- === Extravehicular Activity (EVA) Metrics ===
    -- Creating a boolean flag for easier filtering and casting hours to double.
    CAST(eva_hrs_mission AS DOUBLE) > 0 AS had_eva,
    CAST(eva_hrs_mission AS DOUBLE) AS mission_eva_hours,
    CAST(total_eva_hrs AS DOUBLE) AS total_career_eva_hours,

    -- === Enriched/Derived Columns ===
    -- Adding business value by calculating new attributes.
    (CAST(year_of_mission AS INT) - CAST(year_of_birth AS INT)) AS age_at_mission,

    -- === ETL Metadata ===
    -- Tracking when the record was processed and loaded into the Silver layer.
    current_timestamp() AS silver_load_timestamp,
    input_file_name() AS source_bronze_file

FROM bronze.astronauts.list_astronauts

-- Data Quality Filter: Exclude records with critical missing information
-- to ensure the Silver layer contains only valid, usable data.
WHERE 1=1
AND id IS NOT NULL
AND name IS NOT NULL
AND year_of_mission IS NOT NULL