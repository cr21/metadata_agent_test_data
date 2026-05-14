-- etl_kpi_customer_orders bigquery stored procedure
CREATE OR REPLACE PROCEDURE `project-5c016d48-80d5-4534-b69.orc_dataset.etl_kpi_customer_orders`(IN p_batch_id INT64, IN p_load_date DATE, INOUT p_status STRING)
BEGIN
  DECLARE v_total_records INT64 DEFAULT 0;
  DECLARE v_kpi_records INT64 DEFAULT 0;
  DECLARE v_error_msg STRING;

  SET p_status = 'STARTED';

  BEGIN
    INSERT INTO `project-5c016d48-80d5-4534-b69.orc_dataset.etl_process_log` (log_time, batch_id, message)
    VALUES (CURRENT_TIMESTAMP(), p_batch_id, 'KPI ETL Started');
  EXCEPTION WHEN ERROR THEN
    -- NOTE: Ignore logging errors
  END;

  -- STEP 1: LOAD STAGING
  -- NOTE: Replaced ON CONFLICT with MERGE
  MERGE `project-5c016d48-80d5-4534-b69.orc_dataset.stg_customer_orders` AS tgt
  USING (
    SELECT
      order_id,
      customer_id,
      order_amount,
      order_date,
      p_load_date AS load_date
    FROM `project-5c016d48-80d5-4534-b69.orc_dataset.external_orders`
    WHERE batch_id = p_batch_id
  ) AS src
  ON tgt.order_id = src.order_id
  WHEN MATCHED THEN
    UPDATE SET
      customer_id = src.customer_id,
      order_amount = src.order_amount,
      order_date = src.order_date,
      load_date = src.load_date
  WHEN NOT MATCHED THEN
    INSERT (order_id, customer_id, order_amount, order_date, load_date)
    VALUES (src.order_id, src.customer_id, src.order_amount, src.order_date, src.load_date);

  SET v_total_records = @@row_count;

  BEGIN
    INSERT INTO `project-5c016d48-80d5-4534-b69.orc_dataset.etl_process_log` (log_time, batch_id, message)
    VALUES (CURRENT_TIMESTAMP(), p_batch_id, CONCAT('Loaded records: ', CAST(v_total_records AS STRING)));
  EXCEPTION WHEN ERROR THEN
    -- NOTE: Ignore logging errors
  END;

  -- STEP 2: TRANSFORM + KPI CALCULATION
  -- NOTE: BigQuery UPDATE ... FROM used (CTE before UPDATE not allowed)
  -- NOTE: SAFE_DIVIDE used to avoid divide-by-zero producing errors
  UPDATE `project-5c016d48-80d5-4534-b69.orc_dataset.stg_customer_orders` AS tgt
  SET
    prev_order_amount = src.prev_order_amount,
    growth_pct        = src.growth_pct,
    rolling_avg_3     = src.rolling_avg_3,
    cumulative_spend  = src.cumulative_spend,
    order_rank        = src.order_rank,
    process_flag      = 'KPI_DONE'
  FROM (
    SELECT
      s.order_id,
      LAG(s.order_amount) OVER (
        PARTITION BY s.customer_id
        ORDER BY s.order_date
      ) AS prev_order_amount,
      SAFE_DIVIDE(
        s.order_amount - LAG(s.order_amount) OVER (PARTITION BY s.customer_id ORDER BY s.order_date),
        LAG(s.order_amount) OVER (PARTITION BY s.customer_id ORDER BY s.order_date)
      ) AS growth_pct,
      AVG(s.order_amount) OVER (
        PARTITION BY s.customer_id
        ORDER BY s.order_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
      ) AS rolling_avg_3,
      SUM(s.order_amount) OVER (
        PARTITION BY s.customer_id
        ORDER BY s.order_date
      ) AS cumulative_spend,
      DENSE_RANK() OVER (
        PARTITION BY s.customer_id
        ORDER BY s.order_amount DESC
      ) AS order_rank
    FROM `project-5c016d48-80d5-4534-b69.orc_dataset.stg_customer_orders` AS s
    WHERE s.load_date = p_load_date
  ) AS src
  WHERE tgt.order_id = src.order_id;

  SET v_kpi_records = @@row_count;

  BEGIN
    INSERT INTO `project-5c016d48-80d5-4534-b69.orc_dataset.etl_process_log` (log_time, batch_id, message)
    VALUES (CURRENT_TIMESTAMP(), p_batch_id, CONCAT('KPI computed for records: ', CAST(v_kpi_records AS STRING)));
  EXCEPTION WHEN ERROR THEN
    -- NOTE: Ignore logging errors
  END;

  -- STEP 3: FINAL MERGE INTO TARGET TABLE
  -- NOTE: Replaced INSERT ... ON CONFLICT with MERGE
  MERGE `project-5c016d48-80d5-4534-b69.orc_dataset.customer_orders` AS tgt
  USING (
    SELECT
      order_id,
      customer_id,
      order_amount,
      prev_order_amount,
      growth_pct,
      rolling_avg_3,
      cumulative_spend,
      order_rank
    FROM `project-5c016d48-80d5-4534-b69.orc_dataset.stg_customer_orders`
    WHERE process_flag = 'KPI_DONE'
  ) AS src
  ON tgt.order_id = src.order_id
  WHEN MATCHED THEN
    UPDATE SET
      order_amount     = src.order_amount,
      prev_amount      = src.prev_order_amount,
      growth_pct       = src.growth_pct,
      rolling_avg      = src.rolling_avg_3,
      total_spend      = src.cumulative_spend,
      rank_in_customer = src.order_rank,
      last_updated     = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN
    INSERT (
      order_id,
      customer_id,
      order_amount,
      prev_amount,
      growth_pct,
      rolling_avg,
      total_spend,
      rank_in_customer,
      created_date,
      last_updated
    )
    VALUES (
      src.order_id,
      src.customer_id,
      src.order_amount,
      src.prev_order_amount,
      src.growth_pct,
      src.rolling_avg_3,
      src.cumulative_spend,
      src.order_rank,
      CURRENT_TIMESTAMP(),
      CURRENT_TIMESTAMP()
    );

  -- STEP 4: ERROR CAPTURE
  INSERT INTO `project-5c016d48-80d5-4534-b69.orc_dataset.etl_error_table` (order_id, error_message, error_date)
  SELECT
    order_id,
    'Negative or invalid amount',
    CURRENT_TIMESTAMP()
  FROM `project-5c016d48-80d5-4534-b69.orc_dataset.stg_customer_orders`
  WHERE order_amount < 0;

  SET p_status = 'COMPLETED';

  BEGIN
    INSERT INTO `project-5c016d48-80d5-4534-b69.orc_dataset.etl_process_log` (log_time, batch_id, message)
    VALUES (CURRENT_TIMESTAMP(), p_batch_id, 'KPI ETL Completed');
  EXCEPTION WHEN ERROR THEN
    -- NOTE: Ignore logging errors
  END;

EXCEPTION WHEN ERROR THEN
  SET v_error_msg = ERROR_MESSAGE();
  BEGIN
    INSERT INTO `project-5c016d48-80d5-4534-b69.orc_dataset.etl_process_log` (log_time, batch_id, message)
    VALUES (CURRENT_TIMESTAMP(), p_batch_id, CONCAT('FATAL ERROR: ', v_error_msg));
  EXCEPTION WHEN ERROR THEN
    -- NOTE: Ignore logging errors
  END;
  SET p_status = 'FAILED';
END;