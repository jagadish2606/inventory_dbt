{{
    config(
        materialized = 'view',
        schema = 'business',
        alias = 'low_stock_alerts',
        tags = ['business', 'platinum']
    )
}}

SELECT
    store_name,
    city,
    state,
    product_name,
    category,
    brand,

    current_quantity     AS available_units,
    min_stock_level      AS minimum_required,
    reorder_point        AS reorder_level,
    stock_status,
    estimated_days_of_supply,

    CASE
        WHEN stock_status = 'CRITICALLY_LOW' THEN 'URGENT - Order immediately'
        WHEN stock_status = 'LOW_STOCK'      THEN 'Warning - Reorder soon'
        ELSE 'Monitor'
    END AS alert_severity,

    CASE
        WHEN estimated_days_of_supply < 3  THEN 'Less than 3 days supply'
        WHEN estimated_days_of_supply < 7  THEN 'Less than 1 week supply'
        WHEN estimated_days_of_supply < 14 THEN 'Less than 2 weeks supply'
        ELSE 'Adequate supply'
    END AS supply_risk

FROM {{ ref('dim_current_inventory') }}
WHERE stock_status IN ('CRITICALLY_LOW', 'LOW_STOCK')
ORDER BY
    CASE
        WHEN stock_status = 'CRITICALLY_LOW' THEN 1
        WHEN stock_status = 'LOW_STOCK'      THEN 2
        ELSE 3
    END,
    estimated_days_of_supply ASC
