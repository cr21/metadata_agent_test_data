CREATE OR REPLACE PROCEDURE `project-5c016d48-80d5-4534-b69.orc_dataset.sp_generate_taxi_kpis`(p_target_date DATE)
BEGIN
  -- Logic: Aggregating curated data into executive KPIs
  INSERT INTO `project-5c016d48-80d5-4534-b69.orc_dataset.fct_taxi_performance_kpi`
  SELECT 
    p_target_date as report_date,
    vendor_id,
    SAFE_DIVIDE(SUM(total_amount_clean), SUM(trip_distance)) as avg_revenue_per_mile,
    SUM(tip_amount) as total_tips_collected,
    -- Efficiency Score: (Total Tips / Total Fare) * 100
    SAFE_DIVIDE(SUM(tip_amount), SUM(base_fare_amount)) * 100 as efficiency_score,
    CURRENT_TIMESTAMP() as last_updated_at
  FROM `project-5c016d48-80d5-4534-b69.orc_dataset.stg_curated_taxi_trips`
  WHERE DATE(pickup_datetime) = p_target_date
  GROUP BY 1, 2;
END;
