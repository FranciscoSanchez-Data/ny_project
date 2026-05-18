-- models/staging/stg_yellow_taxi_trips.sql
WITH source AS (
    -- Al estar en la carpeta dbt/, subimos un nivel (..) para llegar a data/
    SELECT * FROM delta_scan('../data/silver/yellow_taxi_trips')
)

SELECT * FROM source