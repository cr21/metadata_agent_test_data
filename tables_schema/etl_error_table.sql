-- postgresql databases schema
CREATE TABLE IF NOT EXISTS public.etl_error_table
(
    error_id bigint NOT NULL DEFAULT nextval('etl_error_table_error_id_seq'::regclass),
    order_id bigint,
    error_message text COLLATE pg_catalog."default",
    error_date timestamp without time zone DEFAULT now(),
    CONSTRAINT etl_error_table_pkey PRIMARY KEY (error_id)
)