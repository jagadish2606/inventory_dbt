-- Test for fct_daily_inventory_metrics
WITH daily_metrics AS (
    SELECT * FROM {{ ref('fct_daily_inventory_metrics') }}
)

SELECT
    metric_date,
    store_id,
    product_id,
    sales_revenue
FROM daily_metrics
WHERE 1=1
    -- Check for negative sales
    AND sales_revenue < 0
