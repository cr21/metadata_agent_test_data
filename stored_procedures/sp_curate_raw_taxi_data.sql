CREATE OR REPLACE PROCEDURE `project-5c016d48-80d5-4534-b69.orc_dataset.sp_curate_raw_taxi_data`(p_target_date DATE)
BEGIN
  -- Logic: Filter public data, handle nulls, and flag long distances
  INSERT INTO `project-5c016d48-80d5-4534-b69.orc_dataset.stg_curated_taxi_trips`
  SELECT 
    GENERATE_UUID() as trip_id,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    trip_distance,
    fare_amount as base_fare_amount,
    tip_amount,
    (fare_amount + extra + mta_tax + tip_amount) as total_amount_clean,
    IF(trip_distance > 10, TRUE, FALSE) as is_long_distance
  FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022`
  WHERE DATE(pickup_datetime) = p_target_date
    AND fare_amount > 0 
    AND trip_distance > 0;
END;
