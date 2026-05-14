-- postgresql databases schema
CREATE TABLE IF NOT EXISTS public.stg_customer_orders
(
    order_id bigint NOT NULL,
    customer_id bigint NOT NULL,
    order_amount numeric(12,2) NOT NULL,
    order_date date NOT NULL,
    load_date date,
    prev_order_amount numeric(12,2),
    growth_pct numeric(10,4),
    rolling_avg_3 numeric(12,2),
    cumulative_spend numeric(14,2),
    order_rank integer,
    process_flag text COLLATE pg_catalog."default",
    CONSTRAINT stg_customer_orders_pkey PRIMARY KEY (order_id)
)