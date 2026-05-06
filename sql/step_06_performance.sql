-- ============================================================
-- STEP 6 : Performance & Optimisation
-- ============================================================

USE ROLE ELT_ENGINEER;
USE WAREHOUSE ELT_WH;
USE DATABASE NYC_TAXI_DB;

-- ============================================================
-- 1. CLUSTERING KEY sur Silver
-- ============================================================
ALTER TABLE NYC_TAXI_DB.SILVER.CLEAN_TAXI_RIDES
    CLUSTER BY (trip_date, day_of_week);

-- ============================================================
-- 2. Query filtrée par date
-- ============================================================
SELECT 
    day_of_week,
    COUNT(*) AS total_trips
FROM NYC_TAXI_DB.SILVER.CLEAN_TAXI_RIDES
WHERE trip_date BETWEEN '2015-12-01' AND '2015-12-07'
GROUP BY 1
ORDER BY 2 DESC;

-- ============================================================
-- 3. Clustering info
-- ============================================================
SELECT SYSTEM$CLUSTERING_INFORMATION(
    'NYC_TAXI_DB.SILVER.CLEAN_TAXI_RIDES',
    '(trip_date, day_of_week)'
);

-- ============================================================
-- 4. Résumé final de l'architecture
-- ============================================================
WITH summary AS (
    SELECT 'BRONZE' AS layer, 'RAW_TAXI_RIDES'    AS table_name, COUNT(*) AS nb_rows FROM NYC_TAXI_DB.BRONZE.RAW_TAXI_RIDES
    UNION ALL
    SELECT 'SILVER', 'CLEAN_TAXI_RIDES',  COUNT(*) FROM NYC_TAXI_DB.SILVER.CLEAN_TAXI_RIDES
    UNION ALL
    SELECT 'GOLD',   'DAILY_TRIP_SUMMARY', COUNT(*) FROM NYC_TAXI_DB.GOLD.DAILY_TRIP_SUMMARY
    UNION ALL
    SELECT 'GOLD',   'HOURLY_PATTERN',     COUNT(*) FROM NYC_TAXI_DB.GOLD.HOURLY_PATTERN
    UNION ALL
    SELECT 'GOLD',   'WEEKEND_VS_WEEKDAY', COUNT(*) FROM NYC_TAXI_DB.GOLD.WEEKEND_VS_WEEKDAY
)
SELECT * FROM summary
ORDER BY layer, table_name;
