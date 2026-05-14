-- postgresql databases schema
CREATE TABLE IF NOT EXISTS public.customer_orders
(
    order_id bigint NOT NULL,
    customer_id bigint NOT NULL,
    order_amount numeric(12,2),
    prev_amount numeric(12,2),
    growth_pct numeric(10,4),
    rolling_avg numeric(12,2),
    total_spend numeric(14,2),
    rank_in_customer integer,
    created_date timestamp without time zone DEFAULT now(),
    last_updated timestamp without time zone,
    CONSTRAINT customer_orders_pkey PRIMARY KEY (order_id)
)