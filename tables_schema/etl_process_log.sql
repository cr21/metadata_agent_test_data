-- postgresql databases schema
CREATE TABLE IF NOT EXISTS public.etl_process_log
(
    log_id bigint NOT NULL DEFAULT nextval('etl_process_log_log_id_seq'::regclass),
    log_time timestamp without time zone DEFAULT now(),
    batch_id integer,
    message text COLLATE pg_catalog."default",
    CONSTRAINT etl_process_log_pkey PRIMARY KEY (log_id)
)