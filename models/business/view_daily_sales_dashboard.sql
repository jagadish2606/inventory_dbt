{{
    config(
        materialized = 'view',
        schema = 'business',
        alias = 'daily_sales_dashboard',
        tags = ['business', 'platinum']
    )
}}

SELECT
    metric_date AS sale_date,
    store_name,
    city,
    product_name,
    category,

    stock_out_qty    AS units_sold,
    sales_revenue,
    closing_stock    AS ending_inventory,
    stock_status,

    -- Sell-through rate
    ROUND(
        stock_out_qty * 100.0
        / NULLIF(stock_out_qty + closing_stock, 0),
        2
    ) AS sell_through_rate_pct,

    -- Inventory coverage (days)
    CASE
        WHEN closing_stock > 0 AND stock_out_qty > 0
        THEN ROUND(closing_stock / stock_out_qty, 1)
        ELSE NULL
    END AS days_of_inventory_coverage

FROM {{ ref('fct_daily_inventory_metrics') }}
WHERE sales_revenue > 0
ORDER BY sale_date DESC, sales_revenue DESC
