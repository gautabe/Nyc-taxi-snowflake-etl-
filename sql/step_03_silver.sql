-- ============================================================
-- STEP 3 : Couche Silver — Nettoyage & Standardisation
-- ============================================================

USE ROLE ELT_ENGINEER;
USE WAREHOUSE ELT_WH;
USE DATABASE NYC_TAXI_DB;
USE SCHEMA SILVER;

-- D'abord, on explore ce qu'on a dans Bronze
DESCRIBE TABLE NYC_TAXI_DB.BRONZE.RAW_TAXI_RIDES;



-- ============================================================
-- STEP 3 : Couche Silver — Nettoyage & Standardisation
-- ============================================================

USE ROLE ELT_ENGINEER;
USE WAREHOUSE ELT_WH;
USE DATABASE NYC_TAXI_DB;
USE SCHEMA SILVER;

-- Aperçu des données brutes avant nettoyage
SELECT *
FROM NYC_TAXI_DB.BRONZE.RAW_TAXI_RIDES
LIMIT 5;

-- Audit qualité : compter les NULLs par colonne clé
SELECT
    COUNT(*)                                    AS total_rows,
    COUNT(CASE WHEN ID IS NULL THEN 1 END)      AS null_id,
    COUNT(CASE WHEN DATE IS NULL THEN 1 END)    AS null_date,
    COUNT(CASE WHEN DAY_OF_WEEK IS NULL THEN 1 END) AS null_day_of_week,
    COUNT(CASE WHEN PART_OF_DAY IS NULL THEN 1 END) AS null_part_of_day,
    COUNT(CASE WHEN PICKUP_GEOM IS NULL THEN 1 END) AS null_pickup_geom,
    COUNT(CASE WHEN DROPOFF_GEOM IS NULL THEN 1 END) AS null_dropoff_geom
FROM NYC_TAXI_DB.BRONZE.RAW_TAXI_RIDES;



-- ============================================================
-- Création de la table Silver
-- ============================================================

CREATE OR REPLACE TABLE NYC_TAXI_DB.SILVER.CLEAN_TAXI_RIDES AS
SELECT
    -- Identifiant
    ID                                              AS trip_id,

    -- Dimensions temporelles standardisées
    DATE                                            AS trip_date,
    YEAR(DATE)                                      AS trip_year,
    MONTH(DATE)                                     AS trip_month,
    DAY                                             AS trip_day,
    UPPER(TRIM(DAY_OF_WEEK))                        AS day_of_week,
    UPPER(TRIM(PART_OF_DAY))                        AS part_of_day,

    -- Indicateur weekend (utile pour les agrégats Gold)
    CASE 
        WHEN UPPER(TRIM(DAY_OF_WEEK)) IN ('SATURDAY','SUNDAY') 
        THEN TRUE ELSE FALSE 
    END                                             AS is_weekend,

    -- Catégorie horaire normalisée
    CASE UPPER(TRIM(PART_OF_DAY))
        WHEN 'MORNING'   THEN 1
        WHEN 'AFTERNOON' THEN 2
        WHEN 'EVENING'   THEN 3
        WHEN 'NIGHT'     THEN 4
        ELSE 0
    END                                             AS part_of_day_order,

    -- Géographie
    PICKUP_GEOM                                     AS pickup_geom,
    DROPOFF_GEOM                                    AS dropoff_geom,

    -- Metadata
    _INGESTED_AT                                    AS bronze_ingested_at,
    CURRENT_TIMESTAMP()                             AS silver_transformed_at

FROM NYC_TAXI_DB.BRONZE.RAW_TAXI_RIDES

-- Filtre qualité : on exclut les lignes sans géographie
WHERE PICKUP_GEOM  IS NOT NULL
  AND DROPOFF_GEOM IS NOT NULL
  AND DATE         IS NOT NULL;

-- ============================================================
-- Vérification Silver
-- ============================================================

SELECT
    COUNT(*)                            AS total_rows,
    COUNT(DISTINCT trip_year)           AS nb_years,
    MIN(trip_date)                      AS earliest_date,
    MAX(trip_date)                      AS latest_date,
    COUNT(DISTINCT day_of_week)         AS nb_days_of_week,
    COUNT(DISTINCT part_of_day)         AS nb_parts_of_day
FROM NYC_TAXI_DB.SILVER.CLEAN_TAXI_RIDES;
