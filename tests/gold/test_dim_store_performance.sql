-- Test for dim_store_performance
WITH store_performance AS (
    SELECT * FROM {{ ref('dim_store_performance') }}
)

SELECT
    store_id,
    lifetime_revenue,
    lifetime_cost
FROM store_performance
WHERE 1=1
    -- Check for negative profit
    AND lifetime_profit < 0
