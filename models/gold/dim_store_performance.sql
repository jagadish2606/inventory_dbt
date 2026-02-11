{{
    config(
        materialized = 'table',
        schema = 'gold',
        tags = ['gold', 'business']
    )
}}

WITH store_daily AS (
    SELECT
        store_id,
        metric_date,
        SUM(sales_revenue)            AS daily_revenue,
        SUM(total_cost_value_change)  AS daily_cost,
        SUM(transaction_count)        AS daily_transactions,
        COUNT(DISTINCT product_id)    AS unique_products_sold
    FROM {{ ref('fct_daily_inventory_metrics') }}
    GROUP BY store_id, metric_date
),

store_summary AS (
    SELECT
        store_id,
        MIN(metric_date)                    AS first_sale_date,
        MAX(metric_date)                    AS last_sale_date,
        COUNT(DISTINCT metric_date)         AS active_days,
        AVG(daily_revenue)                  AS avg_daily_revenue,
        SUM(daily_revenue)                  AS total_revenue,
        SUM(daily_cost)                     AS total_cost,
        SUM(daily_transactions)             AS total_transactions,
        AVG(daily_transactions)             AS avg_daily_transactions
    FROM store_daily
    GROUP BY store_id
),

store_info AS (
    SELECT * FROM {{ ref('stg_stores') }}
),

inventory_alerts AS (
    SELECT
        store_id,
        COUNT(CASE WHEN stock_status = 'CRITICALLY_LOW' THEN 1 END) AS critical_low_count,
        COUNT(CASE WHEN stock_status = 'LOW_STOCK' THEN 1 END)      AS low_stock_count,
        COUNT(CASE WHEN stock_status = 'OVERSTOCKED' THEN 1 END)    AS overstocked_count
    FROM {{ ref('dim_current_inventory') }}
    GROUP BY store_id
)

SELECT
    s.store_id,
    s.store_name,
    s.city,
    s.state,
    s.store_size_sqft,
    s.opening_date,

    -- Performance metrics
    COALESCE(ss.total_revenue, 0)                       AS lifetime_revenue,
    COALESCE(ss.total_cost, 0)                          AS lifetime_cost,
    COALESCE(ss.total_revenue - ss.total_cost, 0)       AS lifetime_profit,
    COALESCE(ss.avg_daily_revenue, 0)                   AS avg_daily_revenue,
    COALESCE(ss.avg_daily_transactions, 0)              AS avg_daily_transactions,

    -- Inventory health
    COALESCE(ia.critical_low_count, 0)                  AS critical_stock_items,
    COALESCE(ia.low_stock_count, 0)                     AS low_stock_items,
    COALESCE(ia.overstocked_count, 0)                   AS overstocked_items,

    -- Store age
    DATEDIFF(day, s.opening_date, CURRENT_DATE())       AS store_age_days,

    -- Revenue per square foot
    CASE
        WHEN s.store_size_sqft > 0
        THEN ROUND(ss.total_revenue / s.store_size_sqft, 2)
        ELSE 0
    END AS revenue_per_sqft

FROM store_info s
LEFT JOIN store_summary ss
  ON s.store_id = ss.store_id
LEFT JOIN inventory_alerts ia
  ON s.store_id = ia.store_id
WHERE s.is_active = true
ORDER BY lifetime_revenue DESC
