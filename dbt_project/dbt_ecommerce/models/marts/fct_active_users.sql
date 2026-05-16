-- path: models/marts/fct_active_users.sql

{{ config(
    materialized='table',
    tags=['marts', 'analytics']
) }}

WITH users_stage AS (
    SELECT 
        user_id,
        signup_country,
        registered_at,
        is_active
    FROM {{ ref('stg_users') }}
),

aggregated_users AS (
    SELECT
        signup_country,
        DATE_TRUNC('month', registered_at) AS cohort_month,
        COUNT(DISTINCT user_id) AS total_registered_users,
        SUM(CASE WHEN is_active THEN 1 ELSE 0 END) AS total_active_users
    FROM users_stage
    GROUP BY 1, 2
)

SELECT
    -- Generating a surrogate key for the mart table
    {{ dbt_utils.generate_surrogate_key(['signup_country', 'cohort_month']) }} AS active_users_pk,
    signup_country,
    cohort_month,
    total_registered_users,
    total_active_users,
    -- Testing if the agent can understand derived metrics
    SAFE_DIVIDE(total_active_users, total_registered_users) AS activity_rate,
    CURRENT_TIMESTAMP() AS updated_at
FROM aggregated_users
ORDER BY cohort_month DESC, signup_country ASC