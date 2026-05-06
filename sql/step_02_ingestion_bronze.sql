-- ============================================================
-- STEP 2 : Exploration des données source CARTO
-- ============================================================

USE ROLE ELT_ENGINEER;
USE WAREHOUSE ELT_WH;

-- Trouver le nom exact de la database CARTO installée
SHOW DATABASES LIKE '%CARTO%';



-- Explorer la structure de la table source
SHOW TABLES IN DATABASE CARTO_ACADEMY__DATA_FOR_TUTORIALS;

-- Voir les colonnes disponibles
DESCRIBE TABLE CARTO_ACADEMY__DATA_FOR_TUTORIALS.CARTO.NYC_TAXI_RIDES;

-- Aperçu des données brutes
SELECT * 
FROM CARTO_ACADEMY__DATA_FOR_TUTORIALS.CARTO.NYC_TAXI_RIDES 
LIMIT 10;

-- Compter les lignes totales
SELECT COUNT(*) AS total_rows
FROM CARTO_ACADEMY__DATA_FOR_TUTORIALS.CARTO.NYC_TAXI_RIDES;



-- ============================================================
-- STEP 2 : Création de la table Bronze + Ingestion
-- ============================================================

USE ROLE ELT_ENGINEER;
USE WAREHOUSE ELT_WH;
USE DATABASE NYC_TAXI_DB;
USE SCHEMA BRONZE;

-- Création de la table Bronze (copie fidèle de la source, sans transformation)
CREATE OR REPLACE TABLE NYC_TAXI_DB.BRONZE.RAW_TAXI_RIDES AS
SELECT
    *,
    CURRENT_TIMESTAMP()  AS _ingested_at,   -- metadata : quand on a ingéré
    'CARTO_MARKETPLACE'  AS _source          -- metadata : d'où viennent les données
FROM CARTO_ACADEMY__DATA_FOR_TUTORIALS.CARTO.NYC_TAXI_RIDES;

-- Vérification
SELECT COUNT(*)                      AS total_rows,
       MIN(_ingested_at)             AS first_ingested,
       MAX(_ingested_at)             AS last_ingested
FROM NYC_TAXI_DB.BRONZE.RAW_TAXI_RIDES;
