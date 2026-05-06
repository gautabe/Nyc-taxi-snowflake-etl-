-- ============================================================
-- STEP 4 : Couche Gold — Métriques Business
-- ============================================================

USE ROLE ELT_ENGINEER;
USE WAREHOUSE ELT_WH;
USE DATABASE NYC_TAXI_DB;
USE SCHEMA GOLD;

-- ============================================================
-- GOLD TABLE 1 : Volume de trajets par jour
-- ============================================================
CREATE OR REPLACE TABLE NYC_TAXI_DB.GOLD.DAILY_TRIP_SUMMARY AS
SELECT
    trip_date,
    trip_year,
    trip_month,
    trip_day,
    day_of_week,
    is_weekend,
    COUNT(trip_id)                                        AS total_trips,
    COUNT(CASE WHEN part_of_day = 'MORNING'   THEN 1 END) AS trips_morning,
    COUNT(CASE WHEN part_of_day = 'AFTERNOON' THEN 1 END) AS trips_afternoon,
    COUNT(CASE WHEN part_of_day = 'EVENING'   THEN 1 END) AS trips_evening,
    COUNT(CASE WHEN part_of_day = 'NIGHT'     THEN 1 END) AS trips_night,
    CURRENT_TIMESTAMP()                                   AS gold_refreshed_at
FROM NYC_TAXI_DB.SILVER.CLEAN_TAXI_RIDES
GROUP BY 1,2,3,4,5,6;

-- ============================================================
-- GOLD TABLE 2 : Pattern par partie de journée
-- ============================================================
CREATE OR REPLACE TABLE NYC_TAXI_DB.GOLD.HOURLY_PATTERN AS
WITH base AS (
    SELECT
        day_of_week,
        part_of_day,
        part_of_day_order,
        is_weekend,
        COUNT(trip_id) AS total_trips
    FROM NYC_TAXI_DB.SILVER.CLEAN_TAXI_RIDES
    GROUP BY 1,2,3,4
),
day_totals AS (
    SELECT day_of_week, SUM(total_trips) AS day_total
    FROM base
    GROUP BY 1
)
SELECT
    b.day_of_week,
    b.part_of_day,
    b.part_of_day_order,
    b.is_weekend,
    b.total_trips,
    ROUND(b.total_trips * 100.0 / d.day_total, 2) AS pct_of_day,
    CURRENT_TIMESTAMP() AS gold_refreshed_at
FROM base b
JOIN day_totals d ON b.day_of_week = d.day_of_week
ORDER BY b.day_of_week, b.part_of_day_order;

-- ============================================================
-- GOLD TABLE 3 : Weekend vs Weekday
-- ============================================================
CREATE OR REPLACE TABLE NYC_TAXI_DB.GOLD.WEEKEND_VS_WEEKDAY AS
WITH base AS (
    SELECT
        is_weekend,
        CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
        trip_date,
        COUNT(trip_id) AS daily_trips
    FROM NYC_TAXI_DB.SILVER.CLEAN_TAXI_RIDES
    GROUP BY 1,2,3
)
SELECT
    is_weekend,
    day_type,
    COUNT(trip_date)            AS nb_days,
    SUM(daily_trips)            AS total_trips,
    ROUND(AVG(daily_trips), 0)  AS avg_trips_per_day,
    CURRENT_TIMESTAMP()         AS gold_refreshed_at
FROM base
GROUP BY 1,2;

-- ============================================================
-- Vérification finale
-- ============================================================
SELECT 'DAILY_TRIP_SUMMARY' AS table_name, COUNT(*) AS nb_rows FROM NYC_TAXI_DB.GOLD.DAILY_TRIP_SUMMARY
UNION ALL
SELECT 'HOURLY_PATTERN',                   COUNT(*) FROM NYC_TAXI_DB.GOLD.HOURLY_PATTERN
UNION ALL
SELECT 'WEEKEND_VS_WEEKDAY',               COUNT(*) FROM NYC_TAXI_DB.GOLD.WEEKEND_VS_WEEKDAY;





-- Quel jour de la semaine a le plus de trajets ?
SELECT 
    day_of_week,
    is_weekend,
    SUM(total_trips)    AS total_trips,
    AVG(total_trips)    AS avg_trips_per_day
FROM NYC_TAXI_DB.GOLD.DAILY_TRIP_SUMMARY
GROUP BY 1,2
ORDER BY total_trips DESC;
