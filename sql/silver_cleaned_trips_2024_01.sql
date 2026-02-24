DROP TABLE IF EXISTS silver.cleaned_trips_2024_01;

CREATE TABLE silver.cleaned_trips_2024_01 AS
SELECT
  CASE
    WHEN vendor_id IS NULL THEN NULL
    WHEN vendor_id::text = 'NaN' THEN NULL
    WHEN vendor_id::text ~ '^[0-9]+(\.0+)?$' THEN vendor_id::numeric::int
    ELSE NULL
  END AS vendor_id,

  tpep_pickup_datetime  AS pickup_dt,
  tpep_dropoff_datetime AS dropoff_dt,

  CASE
    WHEN passenger_count IS NULL THEN NULL
    WHEN passenger_count::text = 'NaN' THEN NULL
    WHEN passenger_count::text ~ '^[0-9]+(\.0+)?$' THEN passenger_count::numeric::int
    ELSE NULL
  END AS passenger_count,

  CASE WHEN trip_distance::text = 'NaN' THEN NULL ELSE trip_distance::numeric END AS trip_distance,

  CASE
    WHEN ratecode_id IS NULL THEN NULL
    WHEN ratecode_id::text = 'NaN' THEN NULL
    WHEN ratecode_id::text ~ '^[0-9]+(\.0+)?$' THEN ratecode_id::numeric::int
    ELSE NULL
  END AS ratecode_id,

  store_and_fwd_flag AS store_and_fwd_flag,

  CASE
    WHEN pu_location_id IS NULL THEN NULL
    WHEN pu_location_id::text = 'NaN' THEN NULL
    WHEN pu_location_id::text ~ '^[0-9]+(\.0+)?$' THEN pu_location_id::numeric::int
    ELSE NULL
  END AS pu_location_id,

  CASE
    WHEN do_location_id IS NULL THEN NULL
    WHEN do_location_id::text = 'NaN' THEN NULL
    WHEN do_location_id::text ~ '^[0-9]+(\.0+)?$' THEN do_location_id::numeric::int
    ELSE NULL
  END AS do_location_id,

  CASE
    WHEN payment_type IS NULL THEN NULL
    WHEN payment_type::text = 'NaN' THEN NULL
    WHEN payment_type::text ~ '^[0-9]+(\.0+)?$' THEN payment_type::numeric::int
    ELSE NULL
  END AS payment_type,

  CASE WHEN fare_amount::text = 'NaN' THEN NULL ELSE fare_amount::numeric END AS fare_amount,
  CASE WHEN extra::text = 'NaN' THEN NULL ELSE extra::numeric END AS extra,
  CASE WHEN mta_tax::text = 'NaN' THEN NULL ELSE mta_tax::numeric END AS mta_tax,
  CASE WHEN tip_amount::text = 'NaN' THEN NULL ELSE tip_amount::numeric END AS tip_amount,
  CASE WHEN tolls_amount::text = 'NaN' THEN NULL ELSE tolls_amount::numeric END AS tolls_amount,
  CASE WHEN improvement_surcharge::text = 'NaN' THEN NULL ELSE improvement_surcharge::numeric END AS improvement_surcharge,
  CASE WHEN total_amount::text = 'NaN' THEN NULL ELSE total_amount::numeric END AS total_amount,
  CASE WHEN congestion_surcharge::text = 'NaN' THEN NULL ELSE congestion_surcharge::numeric END AS congestion_surcharge,
  CASE WHEN airport_fee::text = 'NaN' THEN NULL ELSE airport_fee::numeric END AS airport_fee,

  (tpep_dropoff_datetime - tpep_pickup_datetime) AS trip_duration_interval,
  EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/60.0 AS trip_duration_minutes

FROM bronze.yellow_tripdata_2024_01
WHERE tpep_pickup_datetime IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL
  AND tpep_dropoff_datetime >= tpep_pickup_datetime;

ALTER TABLE silver.cleaned_trips_2024_01
ADD COLUMN trip_id bigserial PRIMARY KEY;

CREATE INDEX IF NOT EXISTS idx_cleaned_2024_01_pickup_dt
  ON silver.cleaned_trips_2024_01 (pickup_dt);

CREATE INDEX IF NOT EXISTS idx_cleaned_2024_01_pu_location
  ON silver.cleaned_trips_2024_01 (pu_location_id);

CREATE INDEX IF NOT EXISTS idx_cleaned_2024_01_do_location
  ON silver.cleaned_trips_2024_01 (do_location_id);
