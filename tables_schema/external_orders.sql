-- postgresql databases schema
CREATE TABLE IF NOT EXISTS public.external_orders
(
    order_id bigint NOT NULL,
    customer_id bigint NOT NULL,
    order_amount numeric(12,2) NOT NULL,
    order_date date NOT NULL,
    batch_id integer NOT NULL,
    CONSTRAINT external_orders_pkey PRIMARY KEY (order_id)
)
