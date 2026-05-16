{{ config(materialized='view') }}

WITH raw_clicks AS (
    SELECT
        click_id,
        user_id,
        UPPER(utm_source) AS source_name,
        utm_medium,
        click_timestamp,
        cost AS ad_spend
    FROM {{ source('ad_platform', 'ad_clicks') }}
)

SELECT
    click_id,
    user_id,
    source_name,
    utm_medium,
    click_timestamp,
    CAST(ad_spend AS FLOAT64) AS ad_spend
FROM raw_clicks
WHERE click_timestamp >= '2023-01-01'