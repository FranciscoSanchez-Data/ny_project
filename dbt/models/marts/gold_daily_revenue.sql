-- models/marts/gold_daily_revenue.sql

{{ config(materialized='table') }}

WITH valid_trips AS (
    SELECT *
    FROM {{ ref('stg_yellow_taxi_trips') }}
    WHERE is_valid_trip = TRUE
      -- 🎯 Filtramos por el año real de la fecha del viaje, no por la partición
      AND EXTRACT(YEAR FROM pickup_date) = 2023 
)

SELECT
    pickup_date,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(AVG(tip_amount), 2) AS avg_tip,
    ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
    ROUND(AVG(trip_duration_minutes), 2) AS avg_trip_duration_minutes,
    
    -- Cálculos de cuota de pago (1 = Tarjeta, 2 = Efectivo)
    ROUND((SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) / CAST(COUNT(*) AS FLOAT)) * 100, 2) AS card_payment_share_pct,
    ROUND((SUM(CASE WHEN payment_type = 2 THEN 1 ELSE 0 END) / CAST(COUNT(*) AS FLOAT)) * 100, 2) AS cash_payment_share_pct

FROM valid_trips
GROUP BY pickup_date
ORDER BY pickup_date