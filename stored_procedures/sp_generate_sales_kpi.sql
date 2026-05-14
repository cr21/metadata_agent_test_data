--sp_generate_sales_kpi bigquery stored procedures
CREATE OR REPLACE PROCEDURE `project-5c016d48-80d5-4534-b69.etl_demo.sp_generate_sales_kpi`()
BEGIN

  -------------------------------------------------------------------
  -- STEP 1: TEMP TABLE FOR ORDER DETAILS
  -------------------------------------------------------------------

  CREATE TEMP TABLE tmp_order_details AS
  SELECT
      o.order_id,
      o.user_id,
      DATE(o.created_at) AS order_date,
      oi.product_id,
      p.category,
      oi.sale_price,
      u.country,
      u.gender
  FROM `bigquery-public-data.thelook_ecommerce.orders` o
  JOIN `bigquery-public-data.thelook_ecommerce.order_items` oi
      ON o.order_id = oi.order_id
  JOIN `bigquery-public-data.thelook_ecommerce.products` p
      ON oi.product_id = p.id
  JOIN `bigquery-public-data.thelook_ecommerce.users` u
      ON o.user_id = u.id;

  -------------------------------------------------------------------
  -- STEP 2: DAILY SALES KPI
  -------------------------------------------------------------------

  CREATE TEMP TABLE tmp_daily_sales AS
  SELECT
      order_date,
      country,
      category,
      COUNT(DISTINCT order_id) AS total_orders,
      COUNT(DISTINCT user_id) AS total_customers,
      ROUND(SUM(sale_price), 2) AS total_revenue,
      ROUND(AVG(sale_price), 2) AS avg_sale_price
  FROM tmp_order_details
  GROUP BY
      order_date,
      country,
      category;

  -------------------------------------------------------------------
  -- STEP 3: CUSTOMER SEGMENT KPI
  -------------------------------------------------------------------

  CREATE TEMP TABLE tmp_customer_segment AS
  SELECT
      country,
      gender,
      COUNT(DISTINCT user_id) AS customer_count,
      ROUND(SUM(sale_price), 2) AS customer_revenue
  FROM tmp_order_details
  GROUP BY
      country,
      gender;

  -------------------------------------------------------------------
  -- STEP 4: FINAL TARGET TABLE
  -------------------------------------------------------------------

  CREATE OR REPLACE TABLE `project-5c016d48-80d5-4534-b69.orc_dataset.sales_kpi_dashboard`
  AS
  SELECT
      ds.order_date,
      ds.country,
      ds.category,
      ds.total_orders,
      ds.total_customers,
      ds.total_revenue,
      ds.avg_sale_price,
      cs.gender,
      cs.customer_count,
      cs.customer_revenue,
      CURRENT_TIMESTAMP() AS load_timestamp
  FROM tmp_daily_sales ds
  LEFT JOIN tmp_customer_segment cs
      ON ds.country = cs.country;

END;
