-- Test for view_daily_sales_dashboard
WITH dashboard_data AS (
    SELECT * FROM {{ ref('view_daily_sales_dashboard') }}
)

SELECT
    sale_date,
    store_name,
    product_name,
    units_sold
FROM dashboard_data
WHERE 1=1
    -- Check for invalid sell-through rate
    AND (sell_through_rate_pct < 0 OR sell_through_rate_pct > 100)
UNION ALL
SELECT
    sale_date,
    store_name,
    product_name,
    units_sold
FROM dashboard_data
WHERE 1=1
    -- Check for negative days of coverage
    AND days_of_inventory_coverage < 0
UNION ALL
SELECT
    sale_date,
    store_name,
    product_name,
    units_sold
FROM dashboard_data
WHERE 1=1
    -- Check for sales without revenue
    AND units_sold > 0 AND sales_revenue <= 0
