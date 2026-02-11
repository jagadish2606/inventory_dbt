{{
    config(
        materialized = 'table',
        tags = ['silver', 'cleaned']
    )
}}

WITH transaction_summary AS (
    SELECT
        store_id,
        product_id,
        SUM(quantity_change) AS net_quantity_change
    FROM {{ ref('fct_inventory_transactions') }}
    GROUP BY store_id, product_id
),

-- 🔹 avg daily sales over available history
avg_daily_sales AS (
    SELECT
        store_id,
        product_id,
        CASE 
            WHEN COUNT(DISTINCT transaction_date_only) > 0
            THEN ABS(SUM(CASE WHEN quantity_change < 0 THEN quantity_change ELSE 0 END)) / COUNT(DISTINCT transaction_date_only)
            ELSE 0
        END AS avg_daily_sales
    FROM {{ ref('fct_inventory_transactions') }}
    WHERE quantity_change < 0  -- Only sales/outflows
    GROUP BY store_id, product_id
),

product_info AS (
    SELECT * FROM {{ ref('stg_products') }}
),

store_info AS (
    SELECT * FROM {{ ref('stg_stores') }}
)

SELECT
    s.store_id,
    s.store_name,
    s.city,
    s.state,

    p.product_id,
    p.product_name,
    p.category,
    p.brand,

    COALESCE(ts.net_quantity_change, 0) AS current_quantity,

    p.min_stock_level,
    p.max_stock_level,
    p.reorder_point,
    p.cost_price,
    p.retail_price,

    -- Inventory status
    CASE
        WHEN COALESCE(ts.net_quantity_change, 0) <= p.min_stock_level THEN 'CRITICALLY_LOW'
        WHEN COALESCE(ts.net_quantity_change, 0) <= p.reorder_point THEN 'LOW_STOCK'
        WHEN COALESCE(ts.net_quantity_change, 0) >= p.max_stock_level THEN 'OVERSTOCKED'
        ELSE 'OPTIMAL'
    END AS stock_status,

    -- Inventory value
    COALESCE(ts.net_quantity_change, 0) * p.cost_price   AS inventory_cost_value,
    COALESCE(ts.net_quantity_change, 0) * p.retail_price AS inventory_retail_value,

    -- Stock percentage
    ROUND(
        COALESCE(ts.net_quantity_change, 0) * 100.0
        / NULLIF(p.max_stock_level, 0),
        2
    ) AS stock_percentage_of_max,

    -- ✅ FIXED days of supply calculation
    CASE
        WHEN COALESCE(ads.avg_daily_sales, 0) > 0
        THEN ROUND(
            COALESCE(ts.net_quantity_change, 0) / ads.avg_daily_sales,
            1
        )
        ELSE 
            CASE 
                WHEN COALESCE(ts.net_quantity_change, 0) > 0 THEN 999  -- Infinite/very high supply
                ELSE 0
            END
    END AS estimated_days_of_supply,

    CURRENT_TIMESTAMP() AS snapshot_timestamp

FROM store_info s
CROSS JOIN product_info p
LEFT JOIN transaction_summary ts
    ON s.store_id = ts.store_id
   AND p.product_id = ts.product_id
LEFT JOIN avg_daily_sales ads
    ON s.store_id = ads.store_id
   AND p.product_id = ads.product_id
WHERE p.is_active = true
  AND s.is_active = true
