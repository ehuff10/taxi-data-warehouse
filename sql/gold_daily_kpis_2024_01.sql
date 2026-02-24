DROP TABLE IF EXISTS gold.daily_kpis_2024_01;

CREATE TABLE gold.daily_kpis_2024_01 AS
SELECT
  pickup_dt::date AS trip_date,
  COUNT(*)        AS trips,
  SUM(total_amount)                         AS total_revenue,
  AVG(total_amount)                         AS avg_total_amount,
  AVG(fare_amount)                          AS avg_fare_amount,
  AVG(trip_distance)                        AS avg_trip_distance,
  AVG(trip_duration_minutes)                AS avg_trip_duration_minutes,
  AVG(CASE WHEN payment_type = 1 THEN 1.0 ELSE 0.0 END) AS pct_credit,
  AVG(CASE WHEN payment_type = 2 THEN 1.0 ELSE 0.0 END) AS pct_cash
FROM silver.cleaned_trips_2024_01
WHERE pickup_dt >= '2024-01-01'::timestamp
  AND pickup_dt <  '2024-02-01'::timestamp
GROUP BY 1
ORDER BY 1;
