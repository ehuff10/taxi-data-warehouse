DROP TABLE IF EXISTS gold.revenue_by_pickup_zone_2024_01;

CREATE TABLE gold.revenue_by_pickup_zone_2024_01 AS
SELECT
  pu_location_id,
  COUNT(*)              AS trips,
  SUM(total_amount)     AS total_revenue,
  AVG(total_amount)     AS avg_total_amount,
  AVG(trip_distance)    AS avg_trip_distance,
  AVG(trip_duration_minutes) AS avg_trip_duration_minutes
FROM silver.cleaned_trips_2024_01
WHERE pickup_dt >= '2024-01-01'::timestamp
  AND pickup_dt <  '2024-02-01'::timestamp
GROUP BY pu_location_id
ORDER BY total_revenue DESC;
