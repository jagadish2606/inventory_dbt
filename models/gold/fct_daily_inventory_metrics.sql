{{
    config(
        materialized = 'table',
        schema = 'gold',
        tags = ['gold', 'business']
    )
}}

WITH daily_transactions AS (
    SELECT
        transaction_date_only AS metric_date,
        store_id,
        product_id,

        COUNT(*) AS transaction_count,
        SUM(quantity_change) AS net_quantity_change,

        SUM(CASE WHEN quantity_change > 0 THEN quantity_change ELSE 0 END) AS stock_in_qty,
        SUM(CASE WHEN quantity_change < 0 THEN ABS(quantity_change) ELSE 0 END) AS stock_out_qty,

        SUM(
                CASE
                    WHEN transaction_type = 'SALE'
                    THEN ABS(cost_value_change)
                    ELSE 0
                END
            ) AS total_cost_value_cha   nge,
        SUM(retail_value_change) AS total_retail_value_change,

        SUM(
            CASE
                WHEN transaction_type = 'SALE'
                THEN ABS(retail_value_change)
                ELSE 0
            END
        ) AS sales_revenue
    FROM {{ ref('fct_inventory_transactions') }}
    GROUP BY transaction_date_only, store_id, product_id
),

current_inventory AS (
    SELECT * FROM {{ ref('dim_current_inventory') }}
)

SELECT
    d.metric_date,
    d.store_id,
    d.product_id,

    c.store_name,
    c.city,
    c.product_name,
    c.category,

    -- Transaction metrics
    d.transaction_count,
    d.net_quantity_change,
    d.stock_in_qty,
    d.stock_out_qty,

    -- Value metrics
    d.total_cost_value_change,
    d.total_retail_value_change,
    d.sales_revenue,

    -- Inventory position
    c.current_quantity AS closing_stock,
    c.stock_status,
    c.inventory_cost_value AS closing_inventory_value,

    -- Stockout risk
    CASE
        WHEN c.current_quantity <= c.reorder_point THEN 1
        ELSE 0
    END AS below_reorder_point_flag,

    -- Turnover metrics
    CASE
        WHEN c.current_quantity > 0
        THEN ROUND(d.stock_out_qty / c.current_quantity, 2)
        ELSE 0
    END AS daily_turnover_ratio

FROM daily_transactions d
JOIN current_inventory c
  ON d.store_id = c.store_id
 AND d.product_id = c.product_id
ORDER BY d.metric_date DESC
