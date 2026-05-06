-- ============================================================
-- STEP 5 : Automatisation avec Streams & Tasks
-- ============================================================

USE ROLE ELT_ENGINEER;
USE WAREHOUSE ELT_WH;
USE DATABASE NYC_TAXI_DB;

-- ============================================================
-- STREAM : détecte les nouveaux enregistrements dans Bronze
-- ============================================================
CREATE OR REPLACE STREAM NYC_TAXI_DB.SILVER.STREAM_BRONZE_TAXI
    ON TABLE NYC_TAXI_DB.BRONZE.RAW_TAXI_RIDES
    APPEND_ONLY = TRUE;

-- ============================================================
-- TASK 1 : Bronze → Silver (toutes les heures)
-- ============================================================
CREATE OR REPLACE TASK NYC_TAXI_DB.SILVER.TASK_BRONZE_TO_SILVER
    WAREHOUSE = ELT_WH
    SCHEDULE  = '60 MINUTE'
AS
INSERT INTO NYC_TAXI_DB.SILVER.CLEAN_TAXI_RIDES
SELECT
    ID                                              AS trip_id,
    DATE                                            AS trip_date,
    YEAR(DATE)                                      AS trip_year,
    MONTH(DATE)                                     AS trip_month,
    DAY                                             AS trip_day,
    UPPER(TRIM(DAY_OF_WEEK))                        AS day_of_week,
    UPPER(TRIM(PART_OF_DAY))                        AS part_of_day,
    CASE 
        WHEN UPPER(TRIM(DAY_OF_WEEK)) IN ('SATURDAY','SUNDAY') 
        THEN TRUE ELSE FALSE 
    END                                             AS is_weekend,
    CASE UPPER(TRIM(PART_OF_DAY))
        WHEN 'MORNING'   THEN 1
        WHEN 'AFTERNOON' THEN 2
        WHEN 'EVENING'   THEN 3
        WHEN 'NIGHT'     THEN 4
        ELSE 0
    END                                             AS part_of_day_order,
    PICKUP_GEOM                                     AS pickup_geom,
    DROPOFF_GEOM                                    AS dropoff_geom,
    _INGESTED_AT                                    AS bronze_ingested_at,
    CURRENT_TIMESTAMP()                             AS silver_transformed_at
FROM NYC_TAXI_DB.SILVER.STREAM_BRONZE_TAXI
WHERE METADATA$ACTION = 'INSERT';

-- ============================================================
-- TASK 2 : Silver → Gold (même schéma SILVER, dépend de Task 1)
-- ============================================================
CREATE OR REPLACE TASK NYC_TAXI_DB.SILVER.TASK_SILVER_TO_GOLD
    WAREHOUSE = ELT_WH
    AFTER     NYC_TAXI_DB.SILVER.TASK_BRONZE_TO_SILVER
AS
CREATE OR REPLACE TABLE NYC_TAXI_DB.GOLD.DAILY_TRIP_SUMMARY AS
SELECT
    trip_date, trip_year, trip_month, trip_day,
    day_of_week, is_weekend,
    COUNT(trip_id)                                        AS total_trips,
    COUNT(CASE WHEN part_of_day = 'MORNING'   THEN 1 END) AS trips_morning,
    COUNT(CASE WHEN part_of_day = 'AFTERNOON' THEN 1 END) AS trips_afternoon,
    COUNT(CASE WHEN part_of_day = 'EVENING'   THEN 1 END) AS trips_evening,
    COUNT(CASE WHEN part_of_day = 'NIGHT'     THEN 1 END) AS trips_night,
    CURRENT_TIMESTAMP()                                   AS gold_refreshed_at
FROM NYC_TAXI_DB.SILVER.CLEAN_TAXI_RIDES
GROUP BY 1,2,3,4,5,6;

-- ============================================================
-- Activer les tasks (toujours enfant en premier !)
-- ============================================================
ALTER TASK NYC_TAXI_DB.SILVER.TASK_SILVER_TO_GOLD    RESUME;
ALTER TASK NYC_TAXI_DB.SILVER.TASK_BRONZE_TO_SILVER  RESUME;

-- Vérification
SHOW TASKS IN DATABASE NYC_TAXI_DB;
