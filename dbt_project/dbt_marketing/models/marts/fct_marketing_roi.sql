{{ config(materialized='table') }}

WITH clicks AS (
    SELECT * FROM {{ ref('stg_ad_clicks') }}
),

daily_summary AS (
    SELECT
        source_name,
        DATE(click_timestamp) AS date_day,
        COUNT(DISTINCT click_id) AS total_clicks,
        SUM(ad_spend) AS total_cost
    FROM clicks
    GROUP BY 1, 2
)

SELECT
    -- Tests if agent understands dbt_utils macros for surrogate keys
    {{ dbt_utils.generate_surrogate_key(['source_name', 'date_day']) }} AS summary_id,
    source_name,
    date_day,
    total_clicks,
    total_cost,
    -- Tests agent's understanding of derived financial metrics
    SAFE_DIVIDE(total_cost, total_clicks) AS cost_per_click,
    CURRENT_TIMESTAMP() AS processed_at
FROM daily_summary