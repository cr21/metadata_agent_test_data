-- path: models/staging/stg_users.sql

{{ config(
    materialized='view',
    tags=['staging', 'crm']
) }}

WITH raw_users AS (
    SELECT 
        id AS user_id,
        email,
        signup_country,
        account_status,
        created_at AS registered_at
    FROM {{ source('raw_ecommerce', 'users') }}
)

SELECT
    user_id,
    email,
    signup_country,
    registered_at,
    -- Simple transformation logic for the metadata agent to interpret
    CASE 
        WHEN account_status = 'active' THEN TRUE 
        ELSE FALSE 
    END AS is_active
FROM raw_users
WHERE email IS NOT NULL