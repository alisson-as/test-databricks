ETL Documentation: Bronze to Silver for Astronaut Missions
1. Overview

This document outlines the SQL transformation logic for processing raw astronaut data from the Bronze layer into a cleaned, validated, and enriched table in the Silver layer. The process enhances data quality, standardizes schemas, and adds analytical value, preparing the data for downstream consumption and business intelligence.

    Source Table: bronze.astronauts.list_astronauts

    Target Table: silver.astronauts.missions

    Granularity: One record per astronaut, per mission.

2. Transformation Logic Details

This section breaks down the key transformations applied to create the Silver table.
2.1. Schema and Naming Conventions

    Type Casting: All columns representing numeric values (e.g., year_of_birth, hours_mission, total_number_of_missions) are explicitly cast to their proper data types (INT or DOUBLE). This is critical for accurate calculations and to prevent string-related errors in downstream analytics.

    Column Renaming: Columns are renamed to follow a consistent snake_case convention, which improves readability and clarity. For example, total_number_of_missions is renamed to total_career_missions to be more descriptive of its content.

2.2. Data Cleaning and Standardization

    Nationality Standardization: The nationality field, which contains inconsistent values like 'U.S.S.R/Russia' and 'U.S.', is cleaned using a CASE statement. This normalizes the values into standard forms ('Russia', 'USA'), ensuring that grouping and filtering by country are reliable and accurate.

    Null Value Handling: The query implicitly handles nulls by filtering out records where key identifiers are missing (see Data Quality section), ensuring a baseline of completeness.

2.3. Key Generation and Enrichment

    mission_id (Unique Key): A new, unique identifier is generated for each mission record using a sha2 hash function on a combination of key business columns. This provides a durable key for downstream joins and guarantees that each row can be uniquely identified.

    age_at_mission (Derived Metric): A new column is created to calculate the astronaut's age at the time of each mission. This adds valuable analytical context that was not present in the source data.

    had_eva (Boolean Flag): A boolean flag (true/false) is derived from the mission_eva_hours column. This simplifies queries that need to filter for missions with or without Extravehicular Activity (EVA), as it is more efficient to filter on a boolean than to check if a numeric value is greater than zero.

2.4. Data Quality and Lineage

    Filtering: A WHERE clause is applied to filter out low-quality records where essential identifiers like id, name, or year_of_mission are missing. This serves as a basic quality gate to improve the reliability of the Silver table.

    Column Removal: The field21 column from the Bronze layer is intentionally excluded from the final table, as it was identified as containing no useful data.

    ETL Metadata:

        silver_load_timestamp: Records the exact timestamp of when the data was processed using current_timestamp(). This is essential for auditing, tracking data freshness, and debugging incremental processing jobs.

        source_bronze_file: Captures the path of the source data file using input_file_name(). This enables full data lineage, making it possible to trace any record in the Silver table back to its specific raw source file.