---- postgresql stored procedures
CREATE OR REPLACE PROCEDURE etl_kpi_customer_orders(
    IN p_batch_id    INTEGER,
    IN p_load_date   DATE,
    INOUT p_status   TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_total_records INTEGER := 0;
    v_kpi_records   INTEGER := 0;
    v_error_msg     TEXT;
BEGIN
    -- Set initial status
    p_status := 'STARTED';

    -- Log ETL start
    BEGIN
        INSERT INTO etl_process_log(log_time, batch_id, message)
        VALUES (NOW(), p_batch_id, 'KPI ETL Started');
    EXCEPTION WHEN OTHERS THEN
        -- Ignore logging errors
        NULL;
    END;

    ------------------------------------------------------------------
    -- STEP 1: LOAD STAGING
    ------------------------------------------------------------------
    INSERT INTO stg_customer_orders(
        order_id,
        customer_id,
        order_amount,
        order_date,
        load_date
    )
    SELECT
        order_id,
        customer_id,
        order_amount,
        order_date,
        p_load_date
    FROM external_orders
    WHERE batch_id = p_batch_id
    ON CONFLICT (order_id) DO UPDATE
        SET order_amount = EXCLUDED.order_amount,
            customer_id  = EXCLUDED.customer_id,
            order_date   = EXCLUDED.order_date,
            load_date    = EXCLUDED.load_date;

    GET DIAGNOSTICS v_total_records = ROW_COUNT;

    -- Log loaded records
    BEGIN
        INSERT INTO etl_process_log(log_time, batch_id, message)
        VALUES (NOW(), p_batch_id, 'Loaded records: ' || v_total_records);
    EXCEPTION WHEN OTHERS THEN NULL;
    END;

    ------------------------------------------------------------------
    -- STEP 2: TRANSFORM + KPI CALCULATION
    ------------------------------------------------------------------
    WITH src AS (
        SELECT
            order_id,
            customer_id,
            order_amount,
            order_date,
            LAG(order_amount) OVER (
                PARTITION BY customer_id
                ORDER BY order_date
            ) AS prev_order_amount,
            CASE
                WHEN LAG(order_amount) OVER (
                    PARTITION BY customer_id
                    ORDER BY order_date
                ) IS NOT NULL
                THEN
                    (order_amount -
                     LAG(order_amount) OVER (
                        PARTITION BY customer_id
                        ORDER BY order_date
                     )
                    ) /
                     LAG(order_amount) OVER (
                        PARTITION BY customer_id
                        ORDER BY order_date
                     )
                ELSE NULL
            END AS growth_pct,
            AVG(order_amount) OVER (
                PARTITION BY customer_id
                ORDER BY order_date
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ) AS rolling_avg_3,
            SUM(order_amount) OVER (
                PARTITION BY customer_id
                ORDER BY order_date
            ) AS cumulative_spend,
            DENSE_RANK() OVER (
                PARTITION BY customer_id
                ORDER BY order_amount DESC
            ) AS order_rank
        FROM stg_customer_orders
        WHERE load_date = p_load_date
    )
    UPDATE stg_customer_orders tgt
    SET
        prev_order_amount = src.prev_order_amount,
        growth_pct        = src.growth_pct,
        rolling_avg_3     = src.rolling_avg_3,
        cumulative_spend  = src.cumulative_spend,
        order_rank        = src.order_rank,
        process_flag      = 'KPI_DONE'
    FROM src
    WHERE tgt.order_id = src.order_id;

    GET DIAGNOSTICS v_kpi_records = ROW_COUNT;

    -- Log KPI records
    BEGIN
        INSERT INTO etl_process_log(log_time, batch_id, message)
        VALUES (NOW(), p_batch_id, 'KPI computed for records: ' || v_kpi_records);
    EXCEPTION WHEN OTHERS THEN NULL;
    END;

    ------------------------------------------------------------------
    -- STEP 3: FINAL MERGE INTO TARGET TABLE
    ------------------------------------------------------------------
    INSERT INTO customer_orders(
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
    SELECT
        order_id,
        customer_id,
        order_amount,
        prev_order_amount,
        growth_pct,
        rolling_avg_3,
        cumulative_spend,
        order_rank,
        NOW(),
        NOW()
    FROM stg_customer_orders
    WHERE process_flag = 'KPI_DONE'
    ON CONFLICT (order_id) DO UPDATE
    SET
        order_amount     = EXCLUDED.order_amount,
        prev_amount      = EXCLUDED.prev_amount,
        growth_pct       = EXCLUDED.growth_pct,
        rolling_avg      = EXCLUDED.rolling_avg,
        total_spend      = EXCLUDED.total_spend,
        rank_in_customer = EXCLUDED.rank_in_customer,
        last_updated     = NOW();

    ------------------------------------------------------------------
    -- STEP 4: ERROR CAPTURE
    ------------------------------------------------------------------
    INSERT INTO etl_error_table(
        order_id,
        error_message,
        error_date
    )
    SELECT
        order_id,
        'Negative or invalid amount',
        NOW()
    FROM stg_customer_orders
    WHERE order_amount < 0;

    ------------------------------------------------------------------
    -- FINAL STATUS
    ------------------------------------------------------------------
    p_status := 'COMPLETED';

    BEGIN
        INSERT INTO etl_process_log(log_time, batch_id, message)
        VALUES (NOW(), p_batch_id, 'KPI ETL Completed');
    EXCEPTION WHEN OTHERS THEN NULL;
    END;

EXCEPTION
    WHEN OTHERS THEN
        v_error_msg := SQLERRM;

        -- Log fatal error
        BEGIN
            INSERT INTO etl_process_log(log_time, batch_id, message)
            VALUES (NOW(), p_batch_id, 'FATAL ERROR: ' || v_error_msg);
        EXCEPTION WHEN OTHERS THEN NULL;
        END;

        p_status := 'FAILED';
END;
$$;